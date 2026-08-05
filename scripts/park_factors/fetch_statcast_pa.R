#!/usr/bin/env Rscript
# Fetch plate-appearance-ending pitches from Baseball Savant statcast_search.
#
# Sibling of scripts/fetch_statcast_bbe.R, built for the park K% lens: the BBE
# store keeps only tracked batted balls, so strikeouts and walks never reach it.
# This fetch reuses the exact same raw query (all pitches for a date window;
# game_date_gt/game_date_lt are both INCLUSIVE, verified 2026-07-01) and keeps
# the rows where `events` is non-empty — exactly one such row per plate
# appearance, carrying the PA outcome. Baserunning-truncated pseudo-PAs
# (caught stealing ending an inning mid-PA, pickoffs, etc.) are kept in the
# store and excluded downstream at model time, where the event taxonomy is
# reported with counts.
#
# Shares the chunk framework: per-season 5-day windows (just under Savant's
# 25,000-row response cap; capped windows auto-repair via daily refetch),
# freshness = chunk file mtime >= window end + 1.5 days, manifest keyed by date
# range, stale cross-run duplicate windows dropped at combine time.
#
# Store columns (lean; no launch metrics): game_pk, game_date, game_type,
# home_team, away_team, inning, inning_topbot, at_bat_number, batter, pitcher,
# stand, p_throws, events, season. p_throws is included (the BBE store lacks
# it) because the K% model uses a platoon matchup fixed effect.
#
# at_bat_number is REQUIRED for identity, not analysis: game_pk +
# at_bat_number is what makes a plate appearance unique. Without it a batter
# who faces the same pitcher twice in a game with the same outcome (routine
# against a starter) produces two byte-identical rows, and the combine-time
# unique() collapses them into one. Measured on a 2015-2016 sample, that
# silently destroyed 14% of plate appearances.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_csv      = list(flag = "--output",          default = file.path("data", "raw", "statcast_pa_store.csv")),
  chunks_dir      = list(flag = "--chunks-dir",      default = file.path("data", "raw", "statcast_pa_store_chunks")),
  start_season    = list(flag = "--start-season",    default = 2015, type = "numeric"),
  end_season      = list(flag = "--end-season",      default = as.integer(format(Sys.Date(), "%Y")), type = "numeric"),
  exclude_seasons = list(flag = "--exclude-seasons", default = "2020"),
  step_days       = list(flag = "--step-days",       default = 5, type = "numeric"),
  max_time        = list(flag = "--max-time",        default = 90, type = "numeric"),
  retries         = list(flag = "--retries",         default = 3, type = "numeric"),
  sleep_sec       = list(flag = "--sleep-sec",       default = 0.2, type = "numeric"),
  max_chunks      = list(flag = "--max-chunks",      default = 0, type = "numeric"),
  force           = list(flag = "--force",           default = FALSE, type = "boolean")
))

output_csv      <- parsed$output_csv
chunks_dir      <- parsed$chunks_dir
start_season    <- as.integer(parsed$start_season)
end_season      <- as.integer(parsed$end_season)
exclude_seasons <- parse_int_vec(as.character(parsed$exclude_seasons))
step_days       <- as.integer(parsed$step_days)
max_time        <- as.integer(parsed$max_time)
retries         <- as.integer(parsed$retries)
sleep_sec       <- parsed$sleep_sec
max_chunks      <- as.integer(parsed$max_chunks)
force           <- parsed$force

if (is.na(start_season) || is.na(end_season) || start_season > end_season) {
  stop("Invalid --start-season/--end-season")
}
if (step_days < 1) {
  stop("--step-days must be >= 1")
}
if (retries < 1) {
  stop("--retries must be >= 1")
}

if (!dir.exists(chunks_dir)) {
  dir.create(chunks_dir, recursive = TRUE, showWarnings = FALSE)
}
output_dir <- dirname(output_csv)
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}

build_query_url <- function(game_date_gt, game_date_lt, player_type = "pitcher") {
  base <- "https://baseballsavant.mlb.com/statcast_search/csv"
  params <- c(
    "all=true",
    "hfPT=",
    "hfAB=",
    "hfBBT=",
    "hfPR=",
    "hfZ=",
    "stadium=",
    "hfBBL=",
    "hfNewZones=",
    "hfGT=R%7C",
    "hfSea=",
    "hfSit=",
    paste0("player_type=", player_type),
    "hfOuts=",
    "opponent=",
    "pitcher_throws=",
    "batter_stands=",
    "hfSA=",
    paste0("game_date_gt=", game_date_gt),
    paste0("game_date_lt=", game_date_lt),
    "team=",
    "position=",
    "hfRO=",
    "home_road=",
    "hfFlag=",
    "metric_1=",
    "hfInn=",
    "min_pitches=0",
    "min_results=0",
    "group_by=name",
    "sort_col=pitches",
    "player_event_sort=h_launch_speed",
    "sort_order=desc",
    "min_abs=0",
    "type=details"
  )
  paste0(base, "?", paste(params, collapse = "&"), "&")
}

fetch_csv <- function(url, out_file, max_time, retries) {
  ua <- "Mozilla/5.0"
  last_err <- ""

  for (attempt in seq_len(retries)) {
    old_timeout <- getOption("timeout")
    options(timeout = max_time)
    status <- tryCatch(
      {
        utils::download.file(
          url = url,
          destfile = out_file,
          method = "libcurl",
          quiet = TRUE,
          headers = c("User-Agent" = ua)
        )
      },
      error = function(e) {
        last_err <<- conditionMessage(e)
        1L
      },
      finally = {
        options(timeout = old_timeout)
      }
    )

    if (identical(status, 0L) && file.exists(out_file)) {
      sz <- file.info(out_file)$size
      if (!is.na(sz) && sz > 0) {
        return(list(ok = TRUE, empty = FALSE, error = ""))
      }
      if (!is.na(sz) && sz == 0) {
        return(list(ok = TRUE, empty = TRUE, error = "zero-byte response"))
      }
    }

    if (!identical(status, 0L) && nzchar(last_err) && grepl("resolve host name", last_err, fixed = TRUE)) {
      # Keep the root cause text in manifest instead of a generic fetch failure.
      last_err <- "DNS resolution failed for Baseball Savant host"
    }

    if (attempt < retries) {
      Sys.sleep(1.5 * attempt)
    }
  }

  list(ok = FALSE, empty = NA, error = ifelse(nzchar(last_err), last_err, "curl failed or empty payload"))
}

fetch_window_raw <- function(start_date, end_date, max_time, retries) {
  url <- build_query_url(as.character(start_date), as.character(end_date), player_type = "pitcher")
  tmp_raw <- tempfile(fileext = ".csv")
  fetch_res <- fetch_csv(url, tmp_raw, max_time = max_time, retries = retries)
  if (!isTRUE(fetch_res$ok)) {
    unlink(tmp_raw)
    return(list(ok = FALSE, status = "fetch_failed", n_raw = 0L, df_raw = NULL, error = fetch_res$error))
  }

  if (isTRUE(fetch_res$empty)) {
    unlink(tmp_raw)
    return(list(ok = TRUE, status = "ok", n_raw = 0L, df_raw = data.frame(), error = ""))
  }

  df_raw <- tryCatch(
    utils::read.csv(tmp_raw, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) e
  )
  unlink(tmp_raw)

  if (inherits(df_raw, "error")) {
    return(list(ok = FALSE, status = "parse_failed", n_raw = 0L, df_raw = NULL, error = conditionMessage(df_raw)))
  }
  names(df_raw) <- gsub("^\\ufeff", "", names(df_raw))
  list(ok = TRUE, status = "ok", n_raw = nrow(df_raw), df_raw = df_raw, error = "")
}

fallback_daily_pa <- function(start_date, end_date, max_time, retries) {
  days <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")
  if (length(days) == 0) {
    return(list(ok = TRUE, n_raw = 0L, n_pa = 0L, df_pa = extract_pa(data.frame()), error = ""))
  }

  raw_sum <- 0L
  pa_list <- vector("list", length(days))

  for (i in seq_along(days)) {
    day <- days[[i]]
    res <- fetch_window_raw(day, day, max_time = max_time, retries = retries)
    if (!isTRUE(res$ok)) {
      return(list(ok = FALSE, n_raw = raw_sum, n_pa = 0L, df_pa = NULL, error = paste("daily fallback failed:", res$error)))
    }
    if (res$n_raw >= 25000) {
      return(list(ok = FALSE, n_raw = raw_sum + res$n_raw, n_pa = 0L, df_pa = NULL, error = "daily fallback still hit 25,000-row cap"))
    }
    raw_sum <- raw_sum + as.integer(res$n_raw)
    pa_list[[i]] <- extract_pa(res$df_raw)
  }

  non_empty <- pa_list[vapply(pa_list, nrow, integer(1)) > 0]
  if (length(non_empty) == 0) {
    df_pa <- extract_pa(data.frame())
  } else {
    df_pa <- do.call(rbind, non_empty)
  }

  list(
    ok = TRUE,
    n_raw = raw_sum,
    n_pa = nrow(df_pa),
    df_pa = df_pa,
    error = ""
  )
}

first_present <- function(df, candidates) {
  nms <- names(df)
  idx <- match(tolower(candidates), tolower(nms), nomatch = 0)
  idx <- idx[idx > 0]
  if (length(idx) == 0) {
    return(rep(NA, nrow(df)))
  }
  df[[nms[idx[1]]]]
}

to_numeric <- function(x) suppressWarnings(as.numeric(x))

has_column_ci <- function(df, col_name) {
  any(tolower(names(df)) == tolower(col_name))
}

validate_raw_pa_columns <- function(df_raw) {
  if (nrow(df_raw) == 0) {
    return(list(ok = TRUE, error = ""))
  }
  required <- c(
    "game_date", "events", "batter", "pitcher", "stand", "p_throws", "home_team",
    "at_bat_number"
  )
  missing <- required[!vapply(required, function(x) has_column_ci(df_raw, x), logical(1))]
  if (length(missing) > 0) {
    return(list(
      ok = FALSE,
      error = paste0("missing required columns in Savant payload: ", paste(missing, collapse = ", "))
    ))
  }
  list(ok = TRUE, error = "")
}

extract_pa <- function(df) {
  template <- data.frame(
    game_pk = numeric(0),
    game_date = character(0),
    game_type = character(0),
    home_team = character(0),
    away_team = character(0),
    inning = numeric(0),
    inning_topbot = character(0),
    at_bat_number = numeric(0),
    batter = numeric(0),
    pitcher = numeric(0),
    stand = character(0),
    p_throws = character(0),
    events = character(0),
    season = integer(0),
    stringsAsFactors = FALSE
  )

  if (nrow(df) == 0) {
    return(template)
  }

  out <- data.frame(
    game_pk = to_numeric(first_present(df, c("game_pk"))),
    game_date = as.character(first_present(df, c("game_date"))),
    game_type = as.character(first_present(df, c("game_type"))),
    home_team = as.character(first_present(df, c("home_team"))),
    away_team = as.character(first_present(df, c("away_team"))),
    inning = to_numeric(first_present(df, c("inning"))),
    inning_topbot = as.character(first_present(df, c("inning_topbot"))),
    at_bat_number = to_numeric(first_present(df, c("at_bat_number"))),
    batter = to_numeric(first_present(df, c("batter"))),
    pitcher = to_numeric(first_present(df, c("pitcher"))),
    stand = as.character(first_present(df, c("stand"))),
    p_throws = as.character(first_present(df, c("p_throws"))),
    events = as.character(first_present(df, c("events"))),
    stringsAsFactors = FALSE
  )

  # One row per plate appearance: only the PA-ending pitch carries `events`.
  out$events <- ifelse(is.na(out$events), "", trimws(out$events))
  out <- out[nzchar(out$events) & tolower(out$events) != "null", ]
  out <- out[is.na(out$game_type) | out$game_type == "R", ]

  out$game_date <- as.character(as.Date(out$game_date))
  out$season <- suppressWarnings(as.integer(substr(out$game_date, 1, 4)))
  out <- out[!is.na(out$season), ]
  if (nrow(out) == 0) {
    return(template)
  }
  out
}

build_chunk_table <- function(start_season, end_season, exclude_seasons, step_days) {
  seasons <- seq.int(start_season, end_season)
  seasons <- seasons[!seasons %in% exclude_seasons]

  rows <- vector("list", 0)
  idx <- 0L

  for (season in seasons) {
    season_start <- as.Date(sprintf("%d-03-01", season))
    season_end <- as.Date(sprintf("%d-11-30", season))

    chunk_start <- season_start
    while (chunk_start <= season_end) {
      chunk_end <- min(chunk_start + (step_days - 1L), season_end)
      query_gt <- chunk_start
      query_lt <- chunk_end

      idx <- idx + 1L
      rows[[idx]] <- data.frame(
        chunk_id = sprintf("%03d", idx),
        season = season,
        chunk_start = as.character(chunk_start),
        chunk_end = as.character(chunk_end),
        game_date_gt = as.character(query_gt),
        game_date_lt = as.character(query_lt),
        stringsAsFactors = FALSE
      )

      chunk_start <- chunk_end + 1L
    }
  }

  do.call(rbind, rows)
}

# Chunk ids are per-run sequence numbers, so the same date window can exist
# under different filenames across runs. Keep only the most recently fetched
# file per window.
dedupe_chunk_files_by_window <- function(chunk_files) {
  if (length(chunk_files) == 0) {
    return(chunk_files)
  }
  window_key <- sub("^.*?_(\\d{4}-\\d{2}-\\d{2}_\\d{4}-\\d{2}-\\d{2})_pa\\.csv$", "\\1", basename(chunk_files))
  mtimes <- file.info(chunk_files)$mtime
  ord <- order(window_key, mtimes)
  files_ord <- chunk_files[ord]
  keys_ord <- window_key[ord]
  keep <- !duplicated(keys_ord, fromLast = TRUE)
  n_dropped <- sum(!keep)
  if (n_dropped > 0) {
    message(sprintf(
      "Ignoring %s stale duplicate chunk file(s) whose window is covered by a newer fetch.",
      n_dropped
    ))
  }
  sort(files_ord[keep])
}

combine_chunk_files <- function(chunk_files, output_csv) {
  if (file.exists(output_csv)) {
    file.remove(output_csv)
  }

  wrote <- FALSE
  ref_header <- NULL
  for (f in chunk_files) {
    if (!file.exists(f) || file.info(f)$size == 0) {
      next
    }

    lines <- readLines(f, warn = FALSE)
    if (length(lines) == 0) {
      next
    }

    # Appending rows under a mismatched header silently misaligns every column;
    # refuse instead.
    if (is.null(ref_header)) {
      ref_header <- lines[1]
    } else if (!identical(lines[1], ref_header)) {
      warning(sprintf("Skipping chunk with mismatched header (stale schema?): %s", f))
      next
    }

    if (!wrote) {
      writeLines(lines, con = output_csv)
      wrote <- TRUE
    } else if (length(lines) > 1) {
      write(lines[-1], file = output_csv, append = TRUE)
    }
  }

  wrote
}

required_chunk_columns <- function() {
  c(
    "game_pk",
    "game_date",
    "game_type",
    "home_team",
    "away_team",
    "inning",
    "inning_topbot",
    "at_bat_number",
    "batter",
    "pitcher",
    "stand",
    "p_throws",
    "events",
    "season"
  )
}

chunk_file_has_required_columns <- function(path, req_cols) {
  if (!file.exists(path) || file.info(path)$size <= 0) {
    return(FALSE)
  }
  hdr <- tryCatch(
    names(utils::read.csv(path, nrows = 0, stringsAsFactors = FALSE, check.names = FALSE)),
    error = function(e) character(0)
  )
  if (length(hdr) == 0) {
    return(FALSE)
  }
  all(req_cols %in% hdr)
}

chunks <- build_chunk_table(start_season, end_season, exclude_seasons, step_days)
if (nrow(chunks) == 0) {
  stop("No chunks to process.")
}
if (max_chunks > 0) {
  chunks <- head(chunks, max_chunks)
}

manifest_path <- file.path(chunks_dir, "chunk_manifest.csv")
if (file.exists(manifest_path)) {
  manifest <- utils::read.csv(manifest_path, stringsAsFactors = FALSE)
} else {
  manifest <- data.frame()
}

# Keyed by date range, not chunk_id: chunk ids are per-run sequence numbers and
# collide across runs with different season ranges.
get_prior_status <- function(chunk_start, chunk_end) {
  if (!is.data.frame(manifest) || nrow(manifest) == 0 ||
      !all(c("chunk_start", "chunk_end", "status") %in% names(manifest))) {
    return(NA_character_)
  }
  hit <- manifest[manifest$chunk_start == chunk_start & manifest$chunk_end == chunk_end, , drop = FALSE]
  if (nrow(hit) == 0) {
    return(NA_character_)
  }
  as.character(hit$status[[nrow(hit)]])
}

run_rows <- vector("list", 0)
run_i <- 0L
req_chunk_cols <- required_chunk_columns()

for (i in seq_len(nrow(chunks))) {
  ch <- chunks[i, ]
  chunk_id <- ch$chunk_id
  season <- ch$season
  chunk_file <- file.path(
    chunks_dir,
    sprintf("%s_%s_%s_pa.csv", chunk_id, ch$chunk_start, ch$chunk_end)
  )

  prior_status <- get_prior_status(ch$chunk_start, ch$chunk_end)
  chunk_exists <- file.exists(chunk_file) && file.info(chunk_file)$size > 0
  chunk_has_cols <- chunk_file_has_required_columns(chunk_file, req_chunk_cols)
  chunk_usable <- chunk_exists && chunk_has_cols

  # Skip chunks that start in the future — no data exists yet.
  chunk_start_date <- as.Date(ch$chunk_start)
  chunk_end_date   <- as.Date(ch$chunk_end)
  if (!is.na(chunk_start_date) && chunk_start_date > Sys.Date()) {
    message(sprintf("[%s/%s] chunk %s starts in the future (%s); skipping.", i, nrow(chunks), chunk_id, ch$chunk_start))
    next
  }

  # A cached chunk is only trustworthy if it was fetched after its window could
  # be complete: file mtime >= chunk_end + 1.5 days (late games finish after
  # midnight and Savant needs processing time).
  chunk_mtime <- if (chunk_exists) file.info(chunk_file)$mtime else as.POSIXct(NA)
  window_complete_at <- as.POSIXct(chunk_end_date + 1L, tz = "UTC") + 12 * 3600
  chunk_fetched_complete <- !is.na(chunk_mtime) && chunk_mtime >= window_complete_at

  if (!force && chunk_usable && chunk_fetched_complete && (is.na(prior_status) || identical(prior_status, "ok"))) {
    message(sprintf("[%s/%s] chunk %s already complete; skipping.", i, nrow(chunks), chunk_id))
    next
  }

  if (!force && chunk_usable && !chunk_fetched_complete) {
    message(sprintf(
      "[%s/%s] chunk %s was fetched before its window completed (end=%s); re-fetching.",
      i, nrow(chunks), chunk_id, ch$chunk_end
    ))
  }

  if (!force && chunk_exists && !chunk_has_cols) {
    message(sprintf(
      "[%s/%s] chunk %s exists but is missing required columns (including p_throws); refreshing.",
      i, nrow(chunks), chunk_id
    ))
  }

  message(sprintf(
    "[%s/%s] chunk %s season %s %s..%s",
    i, nrow(chunks), chunk_id, season, ch$chunk_start, ch$chunk_end
  ))

  status <- "ok"
  n_raw <- 0L
  n_pa <- 0L
  err <- ""
  res <- fetch_window_raw(
    start_date = as.Date(ch$chunk_start),
    end_date = as.Date(ch$chunk_end),
    max_time = max_time,
    retries = retries
  )
  if (!isTRUE(res$ok)) {
    status <- res$status
    err <- res$error
  } else {
    n_raw <- as.integer(res$n_raw)
    raw_valid <- validate_raw_pa_columns(res$df_raw)
    if (!isTRUE(raw_valid$ok)) {
      status <- "missing_required_columns"
      err <- raw_valid$error
    } else if (n_raw >= 25000) {
      # Auto-repair capped chunks by refetching each day in the window.
      daily <- fallback_daily_pa(
        start_date = as.Date(ch$chunk_start),
        end_date = as.Date(ch$chunk_end),
        max_time = max_time,
        retries = retries
      )
      if (!isTRUE(daily$ok)) {
        status <- "capped_25000"
        err <- paste("daily fallback failed:", daily$error)
      } else {
        n_raw <- as.integer(daily$n_raw)
        n_pa <- as.integer(daily$n_pa)
        utils::write.csv(daily$df_pa, chunk_file, row.names = FALSE, na = "")
      }
    } else {
      df_pa <- extract_pa(res$df_raw)
      n_pa <- nrow(df_pa)
      utils::write.csv(df_pa, chunk_file, row.names = FALSE, na = "")
    }
  }

  run_i <- run_i + 1L
  run_rows[[run_i]] <- data.frame(
    timestamp_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    chunk_id = chunk_id,
    season = season,
    chunk_start = ch$chunk_start,
    chunk_end = ch$chunk_end,
    status = status,
    n_rows_raw = n_raw,
    n_rows_pa = n_pa,
    chunk_file = chunk_file,
    error = err,
    stringsAsFactors = FALSE
  )

  manifest <- if (nrow(manifest) > 0) rbind(manifest, run_rows[[run_i]]) else run_rows[[run_i]]
  utils::write.csv(manifest, manifest_path, row.names = FALSE, na = "")

  if (sleep_sec > 0) {
    Sys.sleep(sleep_sec)
  }
}
run_manifest <- if (length(run_rows) > 0) do.call(rbind, run_rows) else data.frame()

all_chunk_files <- list.files(chunks_dir, pattern = "_pa\\.csv$", full.names = TRUE)
all_chunk_files <- dedupe_chunk_files_by_window(all_chunk_files)

if (length(all_chunk_files) == 0) {
  stop("No chunk files were produced.")
}

if (nrow(run_manifest) > 0 && any(run_manifest$status != "ok")) {
  bad <- unique(run_manifest$status[run_manifest$status != "ok"])
  stop(sprintf(
    "One or more chunks failed (%s). Fix and rerun; completed chunks are cached in %s.",
    paste(bad, collapse = ", "),
    chunks_dir
  ))
}

ok_combined <- combine_chunk_files(all_chunk_files, output_csv)
if (!ok_combined) {
  stop("Failed to combine chunk files into final output.")
}

final_rows <- tryCatch(nrow(utils::read.csv(output_csv, stringsAsFactors = FALSE)), error = function(e) NA_integer_)
message("Wrote PA output: ", output_csv)
message("Rows in output: ", final_rows)
message("Chunk manifest: ", manifest_path)
