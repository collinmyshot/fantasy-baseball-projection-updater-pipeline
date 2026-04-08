# ── streamonator.R ─────────────────────────────────────────────────────────────
# SP Streamonator: weekly pitcher streaming tool
# Fetches FanGraphs Probables Grid, scores each SP start by week, and joins
# SP Skillz + Park Factor to produce a ranked Streamer Score table.

if (!exists("%||%")) source(file.path("R", "utils.R"))

PROBABLES_GRID_API_URL <- "https://www.fangraphs.com/api/roster-resource/probables-grid/data"

# ── Team abbreviation normalization ───────────────────────────────────────────
# Maps FanGraphs probables grid team abbreviations to park factor team_norm values.
# PF uses: AZ (not ARI), KCR (not KC), SDP (not SD), SFG (not SF), TBR (not TB).

PG_TO_PF_TEAM <- c(
  ARI = "AZ", ARZ = "AZ", AZ  = "AZ",
  ATH = "ATH", OAK = "ATH",
  BAL = "BAL",
  BOS = "BOS",
  CHC = "CHC", CHN = "CHC",
  CHW = "CHW", CHA = "CHW", CWS = "CHW",
  CIN = "CIN",
  CLE = "CLE", CLV = "CLE",
  COL = "COL",
  DET = "DET",
  HOU = "HOU",
  KC  = "KCR", KCR = "KCR",
  LAA = "LAA",
  LAD = "LAD", LA  = "LAD",
  MIA = "MIA", FLA = "MIA",
  MIL = "MIL",
  MIN = "MIN",
  NYM = "NYM",
  NYY = "NYY",
  PHI = "PHI",
  PIT = "PIT",
  SD  = "SDP", SDP = "SDP",
  SEA = "SEA",
  SF  = "SFG", SFG = "SFG",
  STL = "STL",
  TB  = "TBR", TBR = "TBR", TAM = "TBR",
  TEX = "TEX",
  TOR = "TOR",
  WSH = "WSH", WSN = "WSH", WAS = "WSH"
)

normalize_pg_team <- function(x) {
  x <- toupper(trimws(as.character(x)))
  out <- PG_TO_PF_TEAM[x]
  # Fall back to input value when no mapping found
  no_map <- is.na(out)
  out[no_map] <- x[no_map]
  unname(out)
}

# ── Week date range helpers ────────────────────────────────────────────────────

# Returns the Monday of the ISO week containing `date`, offset by N weeks.
week_monday <- function(date, offset_weeks = 0L) {
  date <- as.Date(date)
  dow  <- as.integer(format(date, "%u"))  # 1 = Mon … 7 = Sun
  date - (dow - 1L) + (as.integer(offset_weeks) * 7L)
}

# Returns list(start, end, label) for "current" or "next" week.
# For "current": start = max(today, Monday of this week); end = Sunday.
# For "next":    start = Monday of next week;             end = Sunday.
streamonator_week_range <- function(today = Sys.Date(), which_week = c("current", "next")) {
  which_week <- match.arg(which_week)
  offset <- if (which_week == "next") 1L else 0L
  mon    <- week_monday(today, offset)
  sun    <- mon + 6L
  start  <- if (which_week == "current") max(as.Date(today), as.Date(mon)) else mon
  list(
    start = as.Date(start),
    end   = as.Date(sun),
    label = sprintf("Week of %s – %s", format(mon, "%b %d"), format(sun, "%b %d, %Y"))
  )
}

# ── Fetch probables grid ───────────────────────────────────────────────────────

fetch_probables_raw <- function(url = PROBABLES_GRID_API_URL) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required to fetch the probables grid.")
  }
  fetch_fg_json_with_fallback(url, simplifyVector = TRUE)
}

# ── Parse probables grid ───────────────────────────────────────────────────────
# Produces a tidy data frame with one row per scheduled start:
#   date           <Date>
#   pitcher_name   <chr>   — as returned by FanGraphs
#   pitcher_id     <int>   — FanGraphs playerid
#   pitcher_team   <chr>   — pitcher's team abbrev (normalized)
#   opponent_team  <chr>   — opponent team abbrev (normalized)
#   home_away      <chr>   — "H" or "A"
#
# The function handles two response shapes:
#   (A) Flat data frame — each row is one start
#   (B) List of team objects, each with a nested games array

parse_probables_grid <- function(raw_result) {
  if (!isTRUE(raw_result$ok) || is.null(raw_result$payload)) {
    stop("Probables grid fetch failed: ",
         if (!is.null(raw_result$error)) raw_result$error else "unknown error")
  }

  payload <- raw_result$payload

  # ── Shape (A): flat data frame ─────────────────────────────────────────────
  if (is.data.frame(payload)) {
    return(parse_probables_flat(payload))
  }

  # ── Shape (B): list of team objects ────────────────────────────────────────
  if (is.list(payload)) {
    # Each element may itself be a list (team object) or a data frame row
    rows <- lapply(seq_along(payload), function(i) {
      obj <- payload[[i]]
      if (is.data.frame(obj)) return(parse_probables_flat(obj))
      parse_probables_team_obj(obj)
    })
    rows <- Filter(function(x) !is.null(x) && nrow(x) > 0, rows)
    if (length(rows) == 0) {
      stop("Probables grid returned no parseable start entries.")
    }
    return(do.call(rbind, rows))
  }

  stop(
    "Unexpected probables grid payload structure. Class: ",
    paste(class(payload), collapse = ", "),
    ". Please inspect raw_result$payload and update parse_probables_grid()."
  )
}

# Helper: parse a flat data frame where each row = one start
parse_probables_flat <- function(df) {
  cn <- tolower(names(df))

  col1 <- function(...) {
    opts <- c(...)
    hit  <- match(opts, cn)
    hit  <- hit[!is.na(hit)]
    if (length(hit) == 0) return(rep(NA_character_, nrow(df)))
    as.character(df[[hit[1]]])
  }

  col1_num <- function(...) {
    suppressWarnings(as.integer(as.numeric(col1(...))))
  }

  date_raw      <- col1("date", "gamedate", "game_date")
  pitcher_name  <- col1("startername", "starter_name", "sp", "pitcher", "name", "playername")
  pitcher_id    <- col1_num("playerid", "player_id", "starterid", "starter_id", "mlbamid")
  pitcher_team  <- col1("teamabbrev", "team_abbrev", "team", "teamname", "tm")
  opponent_raw  <- col1("opp", "opponent", "oppteam", "opp_team", "opposingteam")
  home_away_raw <- col1("homeaway", "home_away", "location", "ha")

  # Infer home/away from "@" prefix on opponent when no explicit field
  has_ha_field <- !all(is.na(home_away_raw))
  if (!has_ha_field) {
    home_away <- ifelse(grepl("^@", opponent_raw), "A", "H")
  } else {
    home_away <- toupper(trimws(home_away_raw))
    home_away <- ifelse(home_away %in% c("H", "HOME", "1"), "H",
                 ifelse(home_away %in% c("A", "AWAY", "0"), "A", home_away))
  }

  # Strip leading "@" from opponent
  opponent_clean <- sub("^@", "", opponent_raw)

  out <- data.frame(
    date          = suppressWarnings(as.Date(date_raw)),
    pitcher_name  = trimws(pitcher_name),
    pitcher_id    = pitcher_id,
    pitcher_team  = normalize_pg_team(pitcher_team),
    opponent_team = normalize_pg_team(opponent_clean),
    home_away     = home_away,
    stringsAsFactors = FALSE
  )

  # Drop rows where we couldn't get a date or pitcher name
  out <- out[!is.na(out$date) & nzchar(out$pitcher_name %||% ""), , drop = FALSE]
  rownames(out) <- NULL
  out
}

# Helper: parse a single team-object from a nested list payload
parse_probables_team_obj <- function(obj) {
  # Try to find the team abbreviation
  team_fields <- c("teamabbrev", "TeamAbbrev", "team", "Team", "abbrev", "Abbrev",
                   "shortname", "ShortName")
  pitcher_team <- NA_character_
  for (f in team_fields) {
    if (!is.null(obj[[f]]) && nzchar(obj[[f]])) {
      pitcher_team <- as.character(obj[[f]])
      break
    }
  }

  # Find the games / dates sub-array
  games_fields <- c("games", "dates", "schedule", "data", "Dates", "Games")
  games <- NULL
  for (f in games_fields) {
    if (!is.null(obj[[f]])) {
      games <- obj[[f]]
      break
    }
  }

  if (is.null(games)) return(NULL)

  if (is.data.frame(games)) {
    sub_df <- games
  } else if (is.list(games)) {
    sub_df <- tryCatch(
      as.data.frame(do.call(rbind, lapply(games, as.data.frame)),
                    stringsAsFactors = FALSE),
      error = function(e) NULL
    )
  } else {
    return(NULL)
  }

  if (is.null(sub_df) || nrow(sub_df) == 0) return(NULL)

  # Inject team column if missing
  if (!any(tolower(names(sub_df)) %in% c("teamabbrev", "team_abbrev", "team", "teamname", "tm"))) {
    sub_df$team <- pitcher_team
  }

  parse_probables_flat(sub_df)
}

# ── SP Skillz indexing ─────────────────────────────────────────────────────────
# Converts sp_skillz_score_stabilized to an index centered on 100 (average = 100).
# Scale: ±1 SD ≈ ±10 index points, so most starters fall in the 80–120 range.
# Returns the input data frame with an `sp_skillz_index` column appended.

index_sp_skillz <- function(sp_skillz_df, col = "sp_skillz_score_stabilized") {
  if (is.null(sp_skillz_df) || !is.data.frame(sp_skillz_df)) return(sp_skillz_df)
  if (!col %in% names(sp_skillz_df)) {
    sp_skillz_df$sp_skillz_index <- NA_real_
    return(sp_skillz_df)
  }

  scores <- suppressWarnings(as.numeric(sp_skillz_df[[col]]))
  mu     <- mean(scores, na.rm = TRUE)
  sigma  <- sd(scores, na.rm = TRUE)

  if (is.na(sigma) || sigma == 0) {
    sp_skillz_df$sp_skillz_index <- ifelse(is.na(scores), NA_real_, 100)
    return(sp_skillz_df)
  }

  sp_skillz_df$sp_skillz_index <- round(100 + (scores - mu) / sigma * 10, 1)
  sp_skillz_df
}

# ── Park factor loader ─────────────────────────────────────────────────────────

load_park_factors <- function(
  path = file.path("data", "processed", "park_factors",
                   "park_factors_savant_style_clean_2026_with_id.csv")
) {
  if (!file.exists(path)) {
    warning("Park factors file not found: ", path)
    return(NULL)
  }
  pf <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  # Keep only the columns we need
  keep <- intersect(c("team_norm", "park", "overall_pf_idx_100"), names(pf))
  if (!"team_norm" %in% keep || !"overall_pf_idx_100" %in% keep) {
    warning("Park factors file missing expected columns (team_norm, overall_pf_idx_100).")
    return(NULL)
  }
  pf <- pf[, keep, drop = FALSE]
  pf$overall_pf_idx_100 <- suppressWarnings(as.numeric(pf$overall_pf_idx_100))
  pf
}

# ── Pitcher name matching ──────────────────────────────────────────────────────
# Match probables grid pitcher names to SP Skillz player_name.
# Strategy: (1) exact, (2) case-insensitive, (3) strip punctuation + fuzzy.
# Returns an integer index into sp_skillz_df (or NA if no match).

normalize_pitcher_name <- function(x) {
  x <- tolower(trimws(as.character(x)))
  gsub("[^a-z ]", "", x)
}

match_pitcher_names <- function(pg_names, skillz_names) {
  pg_norm     <- normalize_pitcher_name(pg_names)
  skillz_norm <- normalize_pitcher_name(skillz_names)

  # Exact normalized match
  idx <- match(pg_norm, skillz_norm)

  # For still-unmatched: try last-name-only match as a fallback
  unmatched <- which(is.na(idx))
  if (length(unmatched) > 0) {
    pg_last     <- vapply(strsplit(pg_norm, " "), function(p) {
      if (length(p) == 0) "" else p[length(p)]
    }, character(1))
    skillz_last <- vapply(strsplit(skillz_norm, " "), function(p) {
      if (length(p) == 0) "" else p[length(p)]
    }, character(1))

    for (i in unmatched) {
      if (!nzchar(pg_last[i])) next
      hits <- which(skillz_last == pg_last[i])
      if (length(hits) == 1) idx[i] <- hits[1]
      # If multiple last-name hits, leave as NA (ambiguous)
    }
  }

  idx
}

# ── Streamer Score calculation ─────────────────────────────────────────────────
# All components are indexed around 100 (100 = neutral/average).
# Team Rater is passed as a named numeric vector (team_norm -> index) or NULL.
# When a component is unavailable (NA), it is excluded and weights are
# renormalized over available components only.

compute_streamer_score <- function(sp_idx, tr_idx, pf_idx, w_sp, w_tr, w_pf) {
  n       <- length(sp_idx)
  scores  <- rep(NA_real_, n)

  for (i in seq_len(n)) {
    vals    <- c(sp_idx[i], tr_idx[i], pf_idx[i])
    weights <- c(w_sp,      w_tr,      w_pf)
    avail   <- !is.na(vals) & weights > 0
    if (!any(avail)) next
    scores[i] <- sum(vals[avail] * weights[avail]) / sum(weights[avail])
  }

  round(scores, 1)
}

# ── Main build function ────────────────────────────────────────────────────────
# probables_df  : output of parse_probables_grid()
# sp_skillz_df  : output of read_cached_pitcher_sp_skillz() + add_pitcher_role_sp_skillz()
#                 already indexed via index_sp_skillz()
# park_factors  : output of load_park_factors()
# team_rater    : named numeric vector (team_norm -> index) or NULL
# week_start/end: Date objects defining the window to include
# w_sp / w_tr / w_pf: Streamer Score weights (positive numerics)

build_streamonator_table <- function(
  probables_df,
  sp_skillz_df,
  park_factors,
  team_rater  = NULL,
  week_start  = NULL,
  week_end    = NULL,
  w_sp = 1,
  w_tr = 1,
  w_pf = 1
) {
  if (is.null(probables_df) || nrow(probables_df) == 0) {
    return(empty_streamonator_table())
  }

  df <- probables_df

  # Filter to requested week window
  if (!is.null(week_start)) df <- df[!is.na(df$date) & df$date >= as.Date(week_start), ]
  if (!is.null(week_end))   df <- df[!is.na(df$date) & df$date <= as.Date(week_end),   ]

  if (nrow(df) == 0) return(empty_streamonator_table())

  # ── Join SP Skillz ──────────────────────────────────────────────────────────
  if (!is.null(sp_skillz_df) && nrow(sp_skillz_df) > 0) {
    sp_skillz_df <- index_sp_skillz(sp_skillz_df)

    match_idx <- match_pitcher_names(df$pitcher_name, sp_skillz_df$player_name)

    df$sp_skillz_index      <- sp_skillz_df$sp_skillz_index[match_idx]
    df$sp_skillz_score_stab <- suppressWarnings(
      as.numeric(sp_skillz_df$sp_skillz_score_stabilized[match_idx])
    )
    df$sp_skillz_rank_stab  <- suppressWarnings(
      as.numeric(sp_skillz_df$sp_skillz_rank_stabilized[match_idx])
    )
    df$skillz_match_name    <- sp_skillz_df$player_name[match_idx]
  } else {
    df$sp_skillz_index      <- NA_real_
    df$sp_skillz_score_stab <- NA_real_
    df$sp_skillz_rank_stab  <- NA_real_
    df$skillz_match_name    <- NA_character_
  }

  # ── Join Park Factor ────────────────────────────────────────────────────────
  # Road start → opponent's park; Home start → pitcher's team park
  if (!is.null(park_factors) && nrow(park_factors) > 0) {
    park_lookup_team <- ifelse(df$home_away == "A", df$opponent_team, df$pitcher_team)
    pf_idx           <- match(park_lookup_team, park_factors$team_norm)
    df$park_factor   <- round(park_factors$overall_pf_idx_100[pf_idx], 1)
    df$park_name     <- park_factors$park[pf_idx]
  } else {
    df$park_factor   <- NA_real_
    df$park_name     <- NA_character_
  }

  # ── Join Team Rater ─────────────────────────────────────────────────────────
  if (!is.null(team_rater) && length(team_rater) > 0) {
    df$team_rater <- as.numeric(team_rater[df$opponent_team])
  } else {
    df$team_rater <- NA_real_
  }

  # ── Streamer Score ──────────────────────────────────────────────────────────
  df$streamer_score <- compute_streamer_score(
    sp_idx = df$sp_skillz_index,
    tr_idx = df$team_rater,
    pf_idx = df$park_factor,
    w_sp   = w_sp,
    w_tr   = w_tr,
    w_pf   = w_pf
  )

  # ── Two-start flag ──────────────────────────────────────────────────────────
  starts_per_pitcher <- table(df$pitcher_name)
  df$two_start_flag  <- df$pitcher_name %in% names(starts_per_pitcher[starts_per_pitcher >= 2])

  # ── Sort by Streamer Score descending ───────────────────────────────────────
  df <- df[order(-ifelse(is.na(df$streamer_score), -Inf, df$streamer_score)), , drop = FALSE]
  rownames(df) <- NULL

  df
}

empty_streamonator_table <- function() {
  data.frame(
    date          = as.Date(character(0)),
    pitcher_name  = character(0),
    pitcher_id    = integer(0),
    pitcher_team  = character(0),
    opponent_team = character(0),
    home_away     = character(0),
    sp_skillz_index = numeric(0),
    sp_skillz_score_stab = numeric(0),
    sp_skillz_rank_stab  = numeric(0),
    park_factor   = numeric(0),
    park_name     = character(0),
    team_rater    = numeric(0),
    streamer_score = numeric(0),
    two_start_flag = logical(0),
    stringsAsFactors = FALSE
  )
}

# ── Display table formatter ────────────────────────────────────────────────────
# Prepares the build_streamonator_table() output for DT rendering.

format_streamonator_for_display <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)

  out <- data.frame(
    Date          = format(df$date, "%a %b %d"),
    Pitcher       = df$pitcher_name,
    Team          = df$pitcher_team,
    `H/A`         = df$home_away,
    Opponent      = df$opponent_team,
    `Park Factor` = df$park_factor,
    `SP Skillz`   = df$sp_skillz_index,
    `Team Rater`  = df$team_rater,
    `Streamer Score` = df$streamer_score,
    `2-Start`     = ifelse(df$two_start_flag, "\u2713", ""),
    check.names   = FALSE,
    stringsAsFactors = FALSE
  )

  out
}

# ── My Pitchers SP Skillz lookup ───────────────────────────────────────────────
# Given a vector of player names from the user's roster, returns the SP Skillz
# rows plus their upcoming starts (if probables_df is provided).

build_my_pitchers_table <- function(
  my_pitcher_names,
  sp_skillz_df,
  probables_df = NULL,
  week_start   = NULL,
  week_end     = NULL
) {
  if (is.null(sp_skillz_df) || length(my_pitcher_names) == 0) {
    return(NULL)
  }

  sp_skillz_df <- index_sp_skillz(sp_skillz_df)

  # Match selected names to SP Skillz
  match_idx <- match_pitcher_names(my_pitcher_names, sp_skillz_df$player_name)
  valid     <- !is.na(match_idx)

  if (!any(valid)) return(NULL)

  out <- sp_skillz_df[match_idx[valid], , drop = FALSE]

  # Attach upcoming starts count from probables
  if (!is.null(probables_df) && nrow(probables_df) > 0) {
    prob <- probables_df
    if (!is.null(week_start)) prob <- prob[!is.na(prob$date) & prob$date >= as.Date(week_start), ]
    if (!is.null(week_end))   prob <- prob[!is.na(prob$date) & prob$date <= as.Date(week_end),   ]

    starts_tbl <- table(prob$pitcher_name)

    pg_idx <- match_pitcher_names(out$player_name, names(starts_tbl))
    out$starts_this_week <- ifelse(is.na(pg_idx), 0L, as.integer(starts_tbl[pg_idx]))
  } else {
    out$starts_this_week <- NA_integer_
  }

  # Sort by SP Skillz rank
  if ("sp_skillz_rank_stabilized" %in% names(out)) {
    out <- out[order(suppressWarnings(as.numeric(out$sp_skillz_rank_stabilized)), na.last = TRUE), ]
  }

  rownames(out) <- NULL
  out
}

# ── Diagnostic helper ──────────────────────────────────────────────────────────
# Call this from the R console to inspect what the probables grid API returns
# before the parser has been validated in production.
#
# Usage: diagnose_probables_grid()

diagnose_probables_grid <- function(url = PROBABLES_GRID_API_URL) {
  res <- fetch_probables_raw(url)
  if (!isTRUE(res$ok)) {
    message("Fetch failed: ", res$error)
    return(invisible(res))
  }
  p <- res$payload
  message("Top-level class: ", paste(class(p), collapse = ", "))
  if (is.data.frame(p)) {
    message("Flat data frame: ", nrow(p), " rows, ", ncol(p), " cols")
    message("Columns: ", paste(names(p), collapse = ", "))
    message("\nFirst row:")
    print(utils::head(p, 1))
  } else if (is.list(p)) {
    message("List with ", length(p), " elements")
    message("Element 1 names: ", paste(names(p[[1]]), collapse = ", "))
    sub <- p[[1]]
    if (is.list(sub)) {
      message("Element 1 sub-fields: ")
      for (nm in names(sub)) {
        val <- sub[[nm]]
        message("  $", nm, " [", class(val), "] length=", length(val))
      }
    }
  }
  invisible(res)
}
