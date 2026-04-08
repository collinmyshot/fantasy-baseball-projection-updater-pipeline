#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_csv      = list(flag = "--output",          default = file.path("data", "raw", "statcast_bbe_store.csv")),
  chunks_dir      = list(flag = "--chunks-dir",      default = file.path("data", "raw", "statcast_bbe_store_chunks")),
  start_season    = list(flag = "--start-season",    default = 2015, type = "numeric"),
  end_season      = list(flag = "--end-season",      default = 2025, type = "numeric"),
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
  # Savant semantics: game_date_gt is exclusive and game_date_lt is inclusive.
  url <- build_query_url(as.character(start_date - 1L), as.character(end_date), player_type = "pitcher")
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

fallback_daily_bbe <- function(start_date, end_date, max_time, retries) {
  days <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")
  if (length(days) == 0) {
    return(list(ok = TRUE, n_raw = 0L, n_bbe = 0L, df_bbe = extract_bbe(data.frame()), error = ""))
  }

  raw_sum <- 0L
  bbe_list <- vector("list", length(days))

  for (i in seq_along(days)) {
    day <- days[[i]]
    res <- fetch_window_raw(day, day, max_time = max_time, retries = retries)
    if (!isTRUE(res$ok)) {
      return(list(ok = FALSE, n_raw = raw_sum, n_bbe = 0L, df_bbe = NULL, error = paste("daily fallback failed:", res$error)))
    }
    if (res$n_raw >= 25000) {
      return(list(ok = FALSE, n_raw = raw_sum + res$n_raw, n_bbe = 0L, df_bbe = NULL, error = "daily fallback still hit 25,000-row cap"))
    }
    raw_sum <- raw_sum + as.integer(res$n_raw)
    bbe_list[[i]] <- extract_bbe(res$df_raw)
  }

  non_empty <- bbe_list[vapply(bbe_list, nrow, integer(1)) > 0]
  if (length(non_empty) == 0) {
    df_bbe <- extract_bbe(data.frame())
  } else {
    df_bbe <- do.call(rbind, non_empty)
  }

  list(
    ok = TRUE,
    n_raw = raw_sum,
    n_bbe = nrow(df_bbe),
    df_bbe = df_bbe,
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

validate_raw_bbe_columns <- function(df_raw) {
  if (nrow(df_raw) == 0) {
    return(list(ok = TRUE, error = ""))
  }
  required <- c("launch_speed", "launch_angle", "game_date", "estimated_ba_using_speedangle")
  missing <- required[!vapply(required, function(x) has_column_ci(df_raw, x), logical(1))]
  if (length(missing) > 0) {
    return(list(
      ok = FALSE,
      error = paste0("missing required columns in Savant payload: ", paste(missing, collapse = ", "))
    ))
  }
  list(ok = TRUE, error = "")
}

extract_bbe <- function(df) {
  template <- data.frame(
    game_pk = numeric(0),
    game_date = character(0),
    game_type = character(0),
    home_team = character(0),
    away_team = character(0),
    inning_topbot = character(0),
    batter = numeric(0),
    pitcher = numeric(0),
    stand = character(0),
    woba_value = numeric(0),
    estimated_woba_using_speedangle = numeric(0),
    estimated_ba_using_speedangle = numeric(0),
    launch_speed = numeric(0),
    launch_angle = numeric(0),
    hc_x = numeric(0),
    hc_y = numeric(0),
    bb_type = character(0),
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
    inning_topbot = as.character(first_present(df, c("inning_topbot"))),
    batter = to_numeric(first_present(df, c("batter"))),
    pitcher = to_numeric(first_present(df, c("pitcher"))),
    stand = as.character(first_present(df, c("stand"))),
    woba_value = to_numeric(first_present(df, c("woba_value"))),
    estimated_woba_using_speedangle = to_numeric(first_present(df, c("estimated_woba_using_speedangle"))),
    estimated_ba_using_speedangle = to_numeric(first_present(df, c("estimated_ba_using_speedangle"))),
    launch_speed = to_numeric(first_present(df, c("launch_speed"))),
    launch_angle = to_numeric(first_present(df, c("launch_angle"))),
    hc_x = to_numeric(first_present(df, c("hc_x"))),
    hc_y = to_numeric(first_present(df, c("hc_y"))),
    bb_type = as.character(first_present(df, c("bb_type"))),
    events = as.character(first_present(df, c("events"))),
    stringsAsFactors = FALSE
  )

  out <- out[!is.na(out$launch_speed) & !is.na(out$launch_angle), ]
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
      query_gt <- chunk_start - 1L
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

combine_chunk_files <- function(chunk_files, output_csv) {
  if (file.exists(output_csv)) {
    file.remove(output_csv)
  }

  wrote <- FALSE
  for (f in chunk_files) {
    if (!file.exists(f) || file.info(f)$size == 0) {
      next
    }

    lines <- readLines(f, warn = FALSE)
    if (length(lines) == 0) {
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
    "inning_topbot",
    "batter",
    "pitcher",
    "stand",
    "woba_value",
    "estimated_woba_using_speedangle",
    "estimated_ba_using_speedangle",
    "launch_speed",
    "launch_angle",
    "hc_x",
    "hc_y",
    "bb_type",
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

get_prior_status <- function(chunk_id) {
  if (!is.data.frame(manifest) || nrow(manifest) == 0 || !"chunk_id" %in% names(manifest)) {
    return(NA_character_)
  }
  hit <- manifest[manifest$chunk_id == chunk_id, , drop = FALSE]
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
    sprintf("%s_%s_%s_bbe.csv", chunk_id, ch$chunk_start, ch$chunk_end)
  )

  prior_status <- get_prior_status(chunk_id)
  chunk_exists <- file.exists(chunk_file) && file.info(chunk_file)$size > 0
  chunk_has_cols <- chunk_file_has_required_columns(chunk_file, req_chunk_cols)
  chunk_usable <- chunk_exists && chunk_has_cols

  if (!force && chunk_usable && (is.na(prior_status) || identical(prior_status, "ok"))) {
    message(sprintf("[%s/%s] chunk %s already complete; skipping.", i, nrow(chunks), chunk_id))
    next
  }

  if (!force && chunk_exists && !chunk_has_cols) {
    message(sprintf(
      "[%s/%s] chunk %s exists but is missing required columns (including estimated_ba_using_speedangle); refreshing.",
      i, nrow(chunks), chunk_id
    ))
  }

  message(sprintf(
    "[%s/%s] chunk %s season %s %s..%s",
    i, nrow(chunks), chunk_id, season, ch$chunk_start, ch$chunk_end
  ))

  status <- "ok"
  n_raw <- 0L
  n_bbe <- 0L
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
    raw_valid <- validate_raw_bbe_columns(res$df_raw)
    if (!isTRUE(raw_valid$ok)) {
      status <- "missing_required_columns"
      err <- raw_valid$error
    } else if (n_raw >= 25000) {
      # Auto-repair capped chunks by refetching each day in the window.
      daily <- fallback_daily_bbe(
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
        n_bbe <- as.integer(daily$n_bbe)
        utils::write.csv(daily$df_bbe, chunk_file, row.names = FALSE, na = "")
      }
    } else {
      df_bbe <- extract_bbe(res$df_raw)
      n_bbe <- nrow(df_bbe)
      utils::write.csv(df_bbe, chunk_file, row.names = FALSE, na = "")
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
    n_rows_bbe = n_bbe,
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

all_chunk_files <- list.files(chunks_dir, pattern = "_bbe\\.csv$", full.names = TRUE)
all_chunk_files <- sort(all_chunk_files)

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
message("Wrote BBE output: ", output_csv)
message("Rows in output: ", final_rows)
message("Chunk manifest: ", manifest_path)
