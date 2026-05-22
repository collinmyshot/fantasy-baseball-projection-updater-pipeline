# ── position_eligibility.R ────────────────────────────────────────────────────
# Build MLB position player eligibility from Fangraphs fielding + batting data.
#
# Eligibility rules (defaults; all thresholds configurable):
#   >= prev_threshold (20) GP at a position in the PREVIOUS season  → eligible
#   >= curr_threshold (10) GP at a position in the CURRENT season   → eligible
#
# Notes:
#   - "G" (games played) is used, NOT "GS" (games started).
#   - pos=of in the FG API returns combined outfield (LF+CF+RF aggregate).
#   - DH appearances are positionless — do not contribute to any position.
#     Players who exclusively DH will have 0 GP at all positions → UT-only.
#   - Pitchers are not explicitly filtered here; they are naturally excluded
#     downstream by the fbb-tools hitter data pipeline. Ohtani appears as a
#     batter (DH) and will be correctly tagged UT-only.
#   - Players with at least one eligible position are shown as "C/1B/OF" etc.
#     (no "UT" appended). Players below all thresholds are shown as "UT-only".
#
# Public API:
#   build_position_eligibility(prev_season, curr_season, prev_threshold,
#                               curr_threshold, output_path, verbose)
#     → invisible data.frame (also writes CSV if output_path is given)
#
# Dependencies: jsonlite
# ─────────────────────────────────────────────────────────────────────────────

# ── Constants ─────────────────────────────────────────────────────────────────

# Maps FG API pos query parameter → canonical display label.
# pos=of returns combined LF+CF+RF games — no need to query each separately.
POS_ELIG_QUERY_MAP <- c(
  "c"  = "C",
  "1b" = "1B",
  "2b" = "2B",
  "ss" = "SS",
  "3b" = "3B",
  "of" = "OF"
)

# Canonical display order for eligible_positions string (e.g. "C/1B/OF")
POS_ELIG_DISPLAY_ORDER <- c("C", "1B", "2B", "SS", "3B", "OF")

# ── Internal helpers ──────────────────────────────────────────────────────────

# Normalize a name to a plain join key (lowercase, ASCII, non-alphanumeric stripped).
# Mirrors the player_nk() logic in fbb-tools/R/utils_names.R so keys match.
.pos_elig_name_key <- function(x) {
  x <- iconv(as.character(x), from = "UTF-8", to = "ASCII//TRANSLIT")
  tolower(gsub("[^a-z0-9]", "", x))
}

# Light normalization of team abbreviations to project-standard three-letter codes.
.pos_elig_norm_team <- function(x) {
  out <- toupper(trimws(as.character(x)))
  map <- c(
    KC  = "KCR", KCR = "KCR",
    OAK = "ATH", ATH = "ATH",
    SD  = "SDP", SDP = "SDP",
    SF  = "SFG", SFG = "SFG",
    TB  = "TBR", TBR = "TBR",
    WAS = "WSH", WSN = "WSH", WSH = "WSH",
    CWS = "CHW", CHW = "CHW",
    ANA = "LAA", LAA = "LAA",
    FLA = "MIA", MIA = "MIA"
  )
  hit <- out %in% names(map)
  out[hit] <- map[out[hit]]
  out
}

# Parse an NFBC-style position string ("2B/SS", "CI", "MI", etc.) into a
# character vector of canonical positions from POS_ELIG_DISPLAY_ORDER.
# Handles / and , delimiters; expands CI → 1B/3B and MI → 2B/SS.
.expand_nfbc_pos <- function(pos_str) {
  if (is.na(pos_str) || !nzchar(trimws(pos_str))) return(character(0))
  raw <- trimws(strsplit(pos_str, "[/,]")[[1]])
  out <- character(0)
  for (p in raw) {
    if      (p == "CI") out <- c(out, "1B", "3B")
    else if (p == "MI") out <- c(out, "2B", "SS")
    else                out <- c(out, p)
  }
  intersect(POS_ELIG_DISPLAY_ORDER, unique(out))
}

# Fetch with basic retry on transient network errors.
.fg_get_json <- function(url, retries = 3L, pause_sec = 2.0) {
  for (attempt in seq_len(retries)) {
    result <- tryCatch(
      jsonlite::fromJSON(url),
      error = function(e) e
    )
    if (!inherits(result, "error")) return(result)
    if (attempt < retries) Sys.sleep(pause_sec)
  }
  stop(conditionMessage(result))
}

# ── Fetch functions ───────────────────────────────────────────────────────────

# Fetch the full MLB player universe from FG batting leaders.
# Returns data.frame: playerid (chr), mlbam_id (int), name (chr), team (chr).
# For traded players who appear on multiple teams, the first row is kept (the
# row closest to the top of FG's sort, typically the current/most recent team).
fetch_fg_batting_universe <- function(season, verbose = TRUE) {
  if (verbose) message(sprintf("  [pos_elig] Fetching batting universe (%d)...", season))

  url <- sprintf(paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&stats=bat&lg=all&qual=0&type=8",
    "&season=%d&season1=%d&ind=0",    # ind=0 = cumulative (one row per player)
    "&team=0&rost=0&age=0&players=0",
    "&pageitems=5000&pagenum=1"
  ), season, season)

  raw <- tryCatch(
    .fg_get_json(url),
    error = function(e) stop(sprintf(
      "[pos_elig] FG batting universe fetch failed (season=%d): %s", season, conditionMessage(e)
    ))
  )

  d <- raw[["data"]]
  if (!is.data.frame(d) || nrow(d) == 0) {
    stop(sprintf("[pos_elig] No batting data returned for season %d", season))
  }

  out <- data.frame(
    playerid = as.character(d[["playerid"]]),
    mlbam_id = suppressWarnings(as.integer(d[["xMLBAMID"]])),
    name     = trimws(as.character(d[["PlayerName"]])),
    team     = .pos_elig_norm_team(d[["TeamNameAbb"]]),
    stringsAsFactors = FALSE
  )

  # Deduplicate by playerid — keep first occurrence (= current/most-recent team
  # row when FG sorts by current team first).
  out <- out[!duplicated(out$playerid), , drop = FALSE]
  rownames(out) <- NULL

  if (verbose) message(sprintf("  [pos_elig]   %d unique players", nrow(out)))
  out
}

# Fetch FG fielding leaders for a single position and season.
# Returns data.frame: playerid (chr), gp (int).
# GP values are aggregated across teams (handles mid-season trades).
# Returns NULL with a warning on fetch failure.
fetch_fg_fielding_pos <- function(season, pos_query, verbose = TRUE) {
  if (verbose) message(sprintf(
    "  [pos_elig] Fetching fielding GP (%s, %d)...",
    toupper(pos_query), season
  ))

  url <- sprintf(paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=%s&stats=fld&lg=all&qual=0&type=1",
    "&season=%d&season1=%d&ind=1",    # ind=1 keeps per-team rows so we can sum
    "&team=0&rost=0&age=0&players=0",
    "&pageitems=5000&pagenum=1"
  ), pos_query, season, season)

  raw <- tryCatch(
    .fg_get_json(url),
    error = function(e) {
      warning(sprintf(
        "[pos_elig] FG fielding fetch failed (pos=%s, season=%d): %s",
        toupper(pos_query), season, conditionMessage(e)
      ))
      return(NULL)
    }
  )
  if (is.null(raw)) return(NULL)

  d <- raw[["data"]]
  if (!is.data.frame(d) || nrow(d) == 0) {
    if (verbose) message(sprintf(
      "  [pos_elig]   No data (pos=%s, season=%d)", toupper(pos_query), season
    ))
    return(NULL)
  }

  tmp <- data.frame(
    playerid = as.character(d[["playerid"]]),
    gp       = suppressWarnings(as.integer(d[["G"]])),
    stringsAsFactors = FALSE
  )
  tmp$gp[is.na(tmp$gp)] <- 0L

  # Aggregate GP across teams — players traded mid-season get one combined row.
  ids   <- unique(tmp$playerid)
  total <- vapply(ids, function(pid) sum(tmp$gp[tmp$playerid == pid], na.rm = TRUE), integer(1))
  out   <- data.frame(playerid = ids, gp = total, stringsAsFactors = FALSE)
  rownames(out) <- NULL
  out
}

# ── Main builder ──────────────────────────────────────────────────────────────

#' Build position eligibility for the full MLB position player pool.
#'
#' @param prev_season    Integer. The "previous" season used for the higher GP
#'                       threshold (default: current year - 1).
#' @param curr_season    Integer. The current in-progress season (default:
#'                       current calendar year).
#' @param prev_threshold Integer. Min GP at a position in prev_season to be
#'                       eligible. Default 20.
#' @param curr_threshold Integer. Min GP at a position in curr_season to be
#'                       eligible. Default 10.
#' @param adp_fallback   Data.frame, file path (character), or NULL. When
#'                       provided, players with ZERO GP at ALL positions in
#'                       the previous season (true rookies, injury-year players,
#'                       pure DH players who earned no prev GP) get their
#'                       position eligibility supplemented from this source.
#'                       Expected columns: a name column (player_name/name) and
#'                       a positions column (positions/Pos) using / or , as
#'                       separator. CI expands to 1B/3B; MI expands to 2B/SS.
#'                       Curr-season computed eligibility is always unioned in.
#'                       NFBC ADP data (data/processed/{year}_nfbc_adp_clean.csv)
#'                       is the recommended source for fantasy use cases.
#' @param output_path    Character or NULL. If non-NULL, results are written as
#'                       CSV to this path (directory is created if needed).
#' @param verbose        Logical. Print progress messages. Default TRUE.
#'
#' @return Invisible data.frame with columns:
#'   playerid, mlbam_id, name, name_key, team,
#'   eligible_positions, is_ut_only, pos_source,
#'   prev_season, curr_season, prev_threshold, curr_threshold, built_at,
#'   gp_c_prev, gp_c_curr, gp_1b_prev, gp_1b_curr, gp_2b_prev, gp_2b_curr,
#'   gp_ss_prev, gp_ss_curr, gp_3b_prev, gp_3b_curr, gp_of_prev, gp_of_curr
build_position_eligibility <- function(
  prev_season     = as.integer(format(Sys.Date(), "%Y")) - 1L,
  curr_season     = as.integer(format(Sys.Date(), "%Y")),
  prev_threshold  = 20L,
  curr_threshold  = 10L,
  adp_fallback    = NULL,
  output_path     = NULL,
  verbose         = TRUE
) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("[pos_elig] Package 'jsonlite' is required.")
  }

  prev_season    <- as.integer(prev_season)
  curr_season    <- as.integer(curr_season)
  prev_threshold <- as.integer(prev_threshold)
  curr_threshold <- as.integer(curr_threshold)

  if (verbose) message(sprintf(
    "[pos_elig] Building position eligibility | prev=%d (≥%d GP) | curr=%d (≥%d GP)",
    prev_season, prev_threshold, curr_season, curr_threshold
  ))

  # ── 1. Player universe ────────────────────────────────────────────────────
  if (verbose) message("[pos_elig] Step 1: player universe")

  bat_prev <- fetch_fg_batting_universe(prev_season, verbose)

  bat_curr <- tryCatch(
    fetch_fg_batting_universe(curr_season, verbose),
    error = function(e) {
      warning(sprintf(
        "[pos_elig] Could not fetch curr-season batting universe (%d): %s — using prev only.",
        curr_season, conditionMessage(e)
      ))
      NULL
    }
  )

  # Union prev + curr universes; curr-season row takes priority for team column.
  if (!is.null(bat_curr) && nrow(bat_curr) > 0) {
    combined  <- rbind(bat_curr, bat_prev)
    universe  <- combined[!duplicated(combined$playerid), , drop = FALSE]
  } else {
    universe  <- bat_prev
  }
  rownames(universe) <- NULL

  if (verbose) message(sprintf("[pos_elig]   Universe: %d players", nrow(universe)))

  # ── 2. Fielding GP per position ───────────────────────────────────────────
  if (verbose) message("[pos_elig] Step 2: fielding data by position")

  pos_queries <- names(POS_ELIG_QUERY_MAP)   # c("c","1b","2b","ss","3b","of")
  pos_labels  <- unname(POS_ELIG_QUERY_MAP)  # c("C","1B","2B","SS","3B","OF")

  fld_prev <- setNames(vector("list", length(pos_queries)), pos_queries)
  fld_curr <- setNames(vector("list", length(pos_queries)), pos_queries)

  for (pq in pos_queries) {
    fld_prev[[pq]] <- tryCatch(
      fetch_fg_fielding_pos(prev_season, pq, verbose),
      error = function(e) { warning(conditionMessage(e)); NULL }
    )
    fld_curr[[pq]] <- tryCatch(
      fetch_fg_fielding_pos(curr_season, pq, verbose),
      error = function(e) { warning(conditionMessage(e)); NULL }
    )
  }

  # ── 3. Pivot GP onto player universe ─────────────────────────────────────
  if (verbose) message("[pos_elig] Step 3: pivoting GP onto player universe")

  result <- universe

  # Column name suffixes use lowercase position labels (e.g. gp_c_prev, gp_1b_prev)
  # tolower() must come FIRST so uppercase letters are preserved before stripping.
  col_labels <- gsub("[^a-z0-9]", "", tolower(pos_labels))

  for (i in seq_along(pos_queries)) {
    pq       <- pos_queries[i]
    col_p    <- paste0("gp_", col_labels[i], "_prev")
    col_c    <- paste0("gp_", col_labels[i], "_curr")

    if (!is.null(fld_prev[[pq]]) && nrow(fld_prev[[pq]]) > 0) {
      idx <- match(result$playerid, fld_prev[[pq]]$playerid)
      result[[col_p]] <- ifelse(is.na(idx), 0L, fld_prev[[pq]]$gp[idx])
    } else {
      result[[col_p]] <- 0L
    }

    if (!is.null(fld_curr[[pq]]) && nrow(fld_curr[[pq]]) > 0) {
      idx <- match(result$playerid, fld_curr[[pq]]$playerid)
      result[[col_c]] <- ifelse(is.na(idx), 0L, fld_curr[[pq]]$gp[idx])
    } else {
      result[[col_c]] <- 0L
    }
  }

  # ── 4. Apply thresholds → eligible_positions string ──────────────────────
  if (verbose) message("[pos_elig] Step 4: applying thresholds")

  elig_mat <- matrix(FALSE, nrow = nrow(result), ncol = length(pos_labels),
                     dimnames = list(NULL, pos_labels))

  for (i in seq_along(pos_queries)) {
    col_p <- paste0("gp_", col_labels[i], "_prev")
    col_c <- paste0("gp_", col_labels[i], "_curr")
    gp_p  <- result[[col_p]]
    gp_c  <- result[[col_c]]
    elig_mat[, pos_labels[i]] <- (
      (!is.na(gp_p) & gp_p >= prev_threshold) |
      (!is.na(gp_c) & gp_c >= curr_threshold)
    )
  }

  result$eligible_positions <- apply(elig_mat, 1, function(row) {
    elig <- intersect(POS_ELIG_DISPLAY_ORDER, pos_labels[row])
    if (length(elig) == 0) "UT-only" else paste(elig, collapse = ", ")
  })
  result$is_ut_only <- result$eligible_positions == "UT-only"

  # ── 5. Join metadata ──────────────────────────────────────────────────────
  result$name_key       <- .pos_elig_name_key(result$name)
  result$pos_source     <- "computed"
  result$prev_season    <- prev_season
  result$curr_season    <- curr_season
  result$prev_threshold <- prev_threshold
  result$curr_threshold <- curr_threshold
  result$built_at       <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  # ── 5b. ADP fallback for players with no previous-season MLB GP ──────────
  # Applies when adp_fallback is supplied and a player has 0 GP at every
  # position in the PREVIOUS season. This covers:
  #   - True rookies (never played MLB before curr_season)
  #   - Players who spent all of prev_season injured / on IL
  #   - Players who exclusively DHed in prev_season (no fielding GP)
  # Their position eligibility is supplemented by the ADP source, unioned
  # with any curr-season computed eligibility already earned.
  n_adp_fallback <- 0L
  if (!is.null(adp_fallback)) {
    # Accept either a file path (character) or a data.frame directly.
    if (is.character(adp_fallback) && length(adp_fallback) == 1) {
      if (!file.exists(adp_fallback)) {
        warning(sprintf("[pos_elig] adp_fallback path not found: %s — skipping fallback.", adp_fallback))
        adp_fallback <- NULL
      } else {
        adp_fallback <- utils::read.csv(adp_fallback, stringsAsFactors = FALSE)
      }
    }
    if (is.data.frame(adp_fallback) && nrow(adp_fallback) > 0) {
      nm_col  <- intersect(c("player_name", "name", "PlayerName"), names(adp_fallback))[1]
      pos_col <- intersect(c("positions", "Pos", "pos"),           names(adp_fallback))[1]
      if (!is.na(nm_col) && !is.na(pos_col)) {
        adp_nk <- .pos_elig_name_key(adp_fallback[[nm_col]])
        # Players with zero prev-season GP at ALL positions
        prev_gp_cols <- intersect(paste0("gp_", col_labels, "_prev"), names(result))
        prev_total   <- rowSums(as.matrix(result[, prev_gp_cols, drop = FALSE]), na.rm = TRUE)
        rookie_mask  <- prev_total == 0L
        for (i in which(rookie_mask)) {
          ai <- match(result$name_key[i], adp_nk)
          if (is.na(ai)) next
          adp_pos <- .expand_nfbc_pos(as.character(adp_fallback[[pos_col]][ai]))
          if (length(adp_pos) == 0) next
          # Union with any curr-season computed eligibility already earned
          curr_pos <- if (result$is_ut_only[i]) character(0)
                      else strsplit(result$eligible_positions[i], ",\\s*")[[1]]
          merged <- intersect(POS_ELIG_DISPLAY_ORDER, union(curr_pos, adp_pos))
          if (length(merged) == 0) next
          result$eligible_positions[i] <- paste(merged, collapse = ", ")
          result$is_ut_only[i]         <- FALSE
          result$pos_source[i]         <- if (length(curr_pos) > 0) "computed+adp_fallback"
                                          else "adp_fallback"
          n_adp_fallback <- n_adp_fallback + 1L
        }
        if (verbose && n_adp_fallback > 0) {
          message(sprintf(
            "[pos_elig]   ADP fallback applied to %d player(s) with no prev-season GP",
            n_adp_fallback
          ))
        }
      } else {
        warning("[pos_elig] adp_fallback missing required columns (player_name/name, positions/Pos) — skipping.")
      }
    }
  }

  # ── 6. Column ordering ────────────────────────────────────────────────────
  gp_cols <- character(0)
  for (cl in col_labels) {
    gp_cols <- c(gp_cols, paste0("gp_", cl, "_prev"), paste0("gp_", cl, "_curr"))
  }
  id_cols <- c(
    "playerid", "mlbam_id", "name", "name_key", "team",
    "eligible_positions", "is_ut_only", "pos_source",
    "prev_season", "curr_season", "prev_threshold", "curr_threshold", "built_at"
  )
  result <- result[, c(id_cols, intersect(gp_cols, names(result))), drop = FALSE]
  rownames(result) <- NULL

  # ── 7. Summary ────────────────────────────────────────────────────────────
  if (verbose) {
    n_ut    <- sum(result$is_ut_only, na.rm = TRUE)
    n_pos   <- nrow(result) - n_ut
    top_pos <- sort(table(result$eligible_positions), decreasing = TRUE)
    fallback_note <- if (n_adp_fallback > 0) sprintf(" | %d via ADP fallback", n_adp_fallback) else ""
    message(sprintf(
      "[pos_elig] Done: %d players | %d with position(s) | %d UT-only%s",
      nrow(result), n_pos, n_ut, fallback_note
    ))
    if (length(top_pos) > 0) {
      top5 <- head(top_pos, 5)
      message(sprintf(
        "[pos_elig] Top 5 eligibility combos: %s",
        paste(sprintf("%s (%d)", names(top5), as.integer(top5)), collapse = ", ")
      ))
    }
  }

  # ── 8. Write output ───────────────────────────────────────────────────────
  if (!is.null(output_path) && nzchar(output_path)) {
    out_dir <- dirname(output_path)
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    utils::write.csv(result, output_path, row.names = FALSE)
    if (verbose) message(sprintf("[pos_elig] Wrote: %s", output_path))
  }

  invisible(result)
}
