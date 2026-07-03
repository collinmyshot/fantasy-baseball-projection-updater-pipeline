#!/usr/bin/env Rscript
# Fetch daily league-wide ball drag coefficients from Baseball Savant's drag
# dashboard (https://baseballsavant.mlb.com/drag-dashboard).
#
# There is no CSV/API endpoint: the data is embedded in the page HTML as a
# `serverVals = {...}` JSON object. Two payloads inside:
#   - scatterData: daily league mean drag coefficient (four-seam fastballs,
#     Alan Nathan/Kagan method, adjusted for environmental conditions), one row
#     per game date from 2016-04-30 onward. This is the series we model with.
#   - binnedData: per-season Cd histograms (archived, not processed here).
#
# Savant's CDN serves stale copies of this page; a cache-busting query param
# plus no-cache headers is required to get current data (verified 2026-07-01,
# when the plain URL returned a copy five weeks old).
#
# Outputs:
#   data/raw/savant_drag/drag_servervals_<UTC timestamp>.json  (raw archive)
#   data/processed/drag_daily.csv           game-date level series
#   data/processed/drag_season_summary.csv  pitch-weighted season rollup
#
# The daily CSV is consumed by the park factor model (drag fixed effect) and
# is the planned drag-environment input for the Streamonator.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  url           = list(flag = "--url",           default = "https://baseballsavant.mlb.com/drag-dashboard"),
  raw_dir       = list(flag = "--raw-dir",       default = file.path("data", "raw", "savant_drag")),
  processed_dir = list(flag = "--processed-dir", default = file.path("data", "processed")),
  max_time      = list(flag = "--max-time",      default = 60, type = "numeric"),
  retries       = list(flag = "--retries",       default = 3, type = "numeric")
))

url           <- parsed$url
raw_dir       <- parsed$raw_dir
processed_dir <- parsed$processed_dir
max_time      <- as.integer(parsed$max_time)
retries       <- as.integer(parsed$retries)

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}

for (d in c(raw_dir, processed_dir)) {
  if (!dir.exists(d)) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }
}

fetch_page <- function(base_url, max_time, retries) {
  last_err <- ""
  for (attempt in seq_len(retries)) {
    # Fresh cache-buster per attempt so a stale CDN copy can't satisfy us twice.
    busted <- sprintf("%s?cb=%s", base_url, as.integer(Sys.time()))
    tmp <- tempfile(fileext = ".html")
    old_timeout <- getOption("timeout")
    options(timeout = max_time)
    status <- tryCatch(
      utils::download.file(
        url = busted,
        destfile = tmp,
        method = "libcurl",
        quiet = TRUE,
        headers = c(
          "User-Agent" = "Mozilla/5.0",
          "Cache-Control" = "no-cache",
          "Pragma" = "no-cache"
        )
      ),
      error = function(e) {
        last_err <<- conditionMessage(e)
        1L
      },
      finally = options(timeout = old_timeout)
    )
    if (identical(status, 0L) && file.exists(tmp) && file.info(tmp)$size > 0) {
      html <- paste(readLines(tmp, warn = FALSE), collapse = "\n")
      unlink(tmp)
      return(html)
    }
    unlink(tmp)
    if (attempt < retries) {
      Sys.sleep(1.5 * attempt)
    }
  }
  stop(sprintf("Failed to fetch drag dashboard: %s", ifelse(nzchar(last_err), last_err, "empty response")))
}

# Extract the serverVals JSON object by walking brace depth. The payload
# contains no braces inside string values (keys, ISO dates, numbers only), so
# depth counting is safe here.
extract_server_vals <- function(html) {
  marker_at <- regexpr("serverVals = {", html, fixed = TRUE)
  if (marker_at == -1) {
    stop("Could not find 'serverVals = {' in drag dashboard HTML — page layout may have changed.")
  }
  brace_start <- as.integer(marker_at) + nchar("serverVals = ")
  rest <- substr(html, brace_start, nchar(html))

  opens <- gregexpr("{", rest, fixed = TRUE)[[1]]
  closes <- gregexpr("}", rest, fixed = TRUE)[[1]]
  if (opens[1] == -1 || closes[1] == -1) {
    stop("No braces found after serverVals marker.")
  }

  events <- rbind(
    data.frame(pos = as.integer(opens), delta = 1L),
    data.frame(pos = as.integer(closes), delta = -1L)
  )
  events <- events[order(events$pos), ]
  depth <- cumsum(events$delta)
  end_idx <- which(depth == 0)[1]
  if (is.na(end_idx)) {
    stop("Unbalanced braces while extracting serverVals JSON.")
  }
  substr(rest, 1, events$pos[end_idx])
}

message("Fetching drag dashboard: ", url)
html <- fetch_page(url, max_time = max_time, retries = retries)
blob <- extract_server_vals(html)
vals <- jsonlite::fromJSON(blob)

if (is.null(vals$scatterData) || !is.data.frame(vals$scatterData) || nrow(vals$scatterData) == 0) {
  stop("serverVals parsed but scatterData is missing/empty.")
}

fetched_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

archive_path <- file.path(raw_dir, sprintf("drag_servervals_%s.json", gsub("[:]", "", fetched_at)))
writeLines(blob, archive_path)
message("Archived raw serverVals: ", archive_path)

daily <- vals$scatterData
required_cols <- c("game_date", "year", "num_pitches", "num_games", "mean_cd")
missing_cols <- setdiff(required_cols, names(daily))
if (length(missing_cols) > 0) {
  stop(sprintf("scatterData missing expected columns: %s", paste(missing_cols, collapse = ", ")))
}

daily_out <- data.frame(
  game_date = as.Date(substr(as.character(daily$game_date), 1, 10)),
  season = as.integer(daily$year),
  mean_cd = as.numeric(daily$mean_cd),
  num_pitches = as.integer(daily$num_pitches),
  num_games = as.integer(daily$num_games),
  stringsAsFactors = FALSE
)
daily_out <- daily_out[!is.na(daily_out$game_date) & is.finite(daily_out$mean_cd), ]
daily_out <- daily_out[order(daily_out$game_date), ]
daily_out$fetched_at_utc <- fetched_at

# Sanity gates: Cd for MLB baseballs lives in a narrow band; anything outside
# means the page schema or units changed and we should fail loudly.
if (any(daily_out$mean_cd < 0.25 | daily_out$mean_cd > 0.45)) {
  bad <- daily_out[daily_out$mean_cd < 0.25 | daily_out$mean_cd > 0.45, ]
  stop(sprintf(
    "Implausible mean_cd values (outside 0.25-0.45) on %s date(s), e.g. %s = %.4f",
    nrow(bad), bad$game_date[1], bad$mean_cd[1]
  ))
}

daily_path <- file.path(processed_dir, "drag_daily.csv")
utils::write.csv(daily_out, daily_path, row.names = FALSE, na = "")

season_summary <- do.call(rbind, lapply(split(daily_out, daily_out$season), function(df) {
  data.frame(
    season = df$season[1],
    n_days = nrow(df),
    first_date = min(df$game_date),
    last_date = max(df$game_date),
    mean_cd_pitch_weighted = sum(df$mean_cd * df$num_pitches) / sum(df$num_pitches),
    total_pitches_sampled = sum(df$num_pitches),
    stringsAsFactors = FALSE
  )
}))
rownames(season_summary) <- NULL
season_summary$fetched_at_utc <- fetched_at

summary_path <- file.path(processed_dir, "drag_season_summary.csv")
utils::write.csv(season_summary, summary_path, row.names = FALSE, na = "")

message(sprintf("Wrote %s daily rows (%s to %s): %s",
                nrow(daily_out), min(daily_out$game_date), max(daily_out$game_date), daily_path))
message("Wrote season summary: ", summary_path)

current_season <- as.integer(format(Sys.Date(), "%Y"))
cur <- season_summary[season_summary$season == current_season, ]
if (nrow(cur) == 1 && as.Date(cur$last_date) < (Sys.Date() - 3)) {
  warning(sprintf(
    "Latest %s drag date is %s (more than 3 days old) — possible stale CDN copy despite cache-busting.",
    current_season, cur$last_date
  ))
}
