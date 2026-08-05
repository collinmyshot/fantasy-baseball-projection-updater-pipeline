#!/usr/bin/env Rscript
# Fetch Baseball Savant's own park factor leaderboard as an external check on
# the iPF family of models. Savant publishes strikeout and walk park indexes
# (index_so, index_bb) that no other public source carries, which is what makes
# them the validation target for the park K% lens.
#
# There is no CSV/API endpoint: the table is embedded in the page HTML as
# `var data = [{...}]`. Savant's CDN serves stale copies, so a cache-busting
# query param plus no-cache headers is used (same gotcha as the drag dashboard).
#
# Savant's indexes are a 3-year rolling window by default (the payload carries
# its own `year_range`, which is recorded in the output so the comparison
# window is never guessed). Keyed by venue_id, which joins directly to the iPF
# base_park_id convention (`venue_<id>`).
#
# Output: data/raw/savant_park_factors_<year>.csv

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  year      = list(flag = "--year",      default = as.integer(format(Sys.Date(), "%Y")) - 1, type = "numeric"),
  bat_side  = list(flag = "--bat-side",  default = ""),
  out_dir   = list(flag = "--out-dir",   default = file.path("data", "raw")),
  max_time  = list(flag = "--max-time",  default = 60, type = "numeric"),
  retries   = list(flag = "--retries",   default = 3, type = "numeric")
))

year     <- as.integer(parsed$year)
bat_side <- parsed$bat_side
out_dir  <- parsed$out_dir
max_time <- as.integer(parsed$max_time)
retries  <- as.integer(parsed$retries)

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
}

build_url <- function(year, bat_side) {
  sprintf(
    paste0(
      "https://baseballsavant.mlb.com/leaderboard/statcast-park-factors",
      "?type=year&year=%d&batSide=%s&stat=index_wOBA&condition=All&rolling=&parks=mlb&cb=%s"
    ),
    year, utils::URLencode(bat_side, reserved = TRUE), as.integer(Sys.time())
  )
}

fetch_page <- function(url, max_time, retries) {
  last_err <- ""
  for (attempt in seq_len(retries)) {
    res <- tryCatch(
      {
        con <- url(url, headers = c(
          "User-Agent" = "Mozilla/5.0",
          "Cache-Control" = "no-cache",
          "Pragma" = "no-cache"
        ))
        on.exit(try(close(con), silent = TRUE), add = TRUE)
        old <- getOption("timeout"); options(timeout = max_time)
        on.exit(options(timeout = old), add = TRUE)
        paste(readLines(con, warn = FALSE), collapse = "\n")
      },
      error = function(e) {
        last_err <<- conditionMessage(e)
        NULL
      }
    )
    if (!is.null(res) && nzchar(res)) {
      return(res)
    }
    if (attempt < retries) Sys.sleep(1.5 * attempt)
  }
  stop(sprintf("Failed to fetch Savant park factors page: %s", last_err))
}

# The payload is `var data = [ ... ];` — bracket-match from the opening [ so a
# nested ] inside a string cannot terminate the extraction early.
extract_data_array <- function(html) {
  marker <- regexpr("var\\s+data\\s*=\\s*\\[", html)
  if (marker < 0) {
    stop("Could not find `var data = [` in the Savant page. The page layout may have changed.")
  }
  start <- marker + attr(marker, "match.length") - 1L
  chars <- strsplit(substring(html, start), "")[[1]]
  depth <- 0L
  in_str <- FALSE
  esc <- FALSE
  end_i <- NA_integer_
  for (i in seq_along(chars)) {
    ch <- chars[i]
    if (in_str) {
      if (esc) {
        esc <- FALSE
      } else if (ch == "\\") {
        esc <- TRUE
      } else if (ch == "\"") {
        in_str <- FALSE
      }
      next
    }
    if (ch == "\"") {
      in_str <- TRUE
    } else if (ch == "[") {
      depth <- depth + 1L
    } else if (ch == "]") {
      depth <- depth - 1L
      if (depth == 0L) {
        end_i <- i
        break
      }
    }
  }
  if (is.na(end_i)) {
    stop("Unterminated data array in the Savant page.")
  }
  paste(chars[seq_len(end_i)], collapse = "")
}

url_full <- build_url(year, bat_side)
message("Fetching Savant park factors: ", url_full)
html <- fetch_page(url_full, max_time = max_time, retries = retries)
json_txt <- extract_data_array(html)
dat <- jsonlite::fromJSON(json_txt, simplifyDataFrame = TRUE)

if (!is.data.frame(dat) || nrow(dat) == 0) {
  stop("Savant park factor payload parsed to zero rows.")
}

num_cols <- grep("^(index_|n_pa$|venue_id$)", names(dat), value = TRUE)
for (nm in num_cols) {
  dat[[nm]] <- suppressWarnings(as.numeric(dat[[nm]]))
}

if (!"index_so" %in% names(dat)) {
  stop("Savant payload has no index_so column; the leaderboard schema may have changed.")
}

# Join key matching the iPF base_park_id convention.
dat$base_park_id <- paste0("venue_", dat$venue_id)
dat$fetched_at_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

out_path <- file.path(out_dir, sprintf("savant_park_factors_%d.csv", year))
utils::write.csv(dat, out_path, row.names = FALSE, na = "")

yr <- unique(as.character(dat$year_range))
message(sprintf("Wrote %d rows: %s", nrow(dat), out_path))
message(sprintf("Savant year_range in payload: %s", paste(yr, collapse = ", ")))
message(sprintf(
  "index_so range: %.0f to %.0f | index_bb range: %.0f to %.0f",
  min(dat$index_so, na.rm = TRUE), max(dat$index_so, na.rm = TRUE),
  min(dat$index_bb, na.rm = TRUE), max(dat$index_bb, na.rm = TRUE)
))
