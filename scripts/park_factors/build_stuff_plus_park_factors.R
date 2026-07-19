#!/usr/bin/env Rscript
# build_stuff_plus_park_factors.R
#
# Fetches per-start Stuff+/Location+/Pitching+ from FanGraphs (type=36,
# single-date queries) for every regular-season start in 2022-2025.
# Joins each start to MLB venue and roof status via the MLB Stats API,
# then computes each start's delta relative to the pitcher's season average
# (option B: controls for which pitchers happen to visit which parks).
#
# Output: data/processed/park_factors/stuff_plus_by_park.csv
#
# Caching: per-date FG data and venue maps are cached separately under
#   data/processed/park_factors/stuff_plus_cache/<season>/
#   <date>_fg.rds    — FanGraphs start-level Stuff+ for that date
#   <date>_venue.rds — MLB venue/roof map for that date
# Re-running skips anything already cached. To force a re-fetch, delete
# the relevant .rds files.
#
# Runtime: ~15-20 min first run (~750 FG calls at 1.2s each).
# Subsequent runs finish in seconds.

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
})

# =============================================================================
# Inlined helpers (from R/sp_skillz.R and R/fangraphs_projections.R)
# =============================================================================

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

normalize_col_key <- function(x) {
  x <- tolower(as.character(x))
  x <- gsub("%", "pct", x, fixed = TRUE)
  gsub("[^a-z0-9]", "", x)
}

strip_html_tags <- function(x) {
  x <- as.character(x)
  x <- gsub("<[^>]+>", "", x)
  x <- gsub("&amp;", "&", x, fixed = TRUE)
  x <- gsub("&nbsp;", " ", x, fixed = TRUE)
  trimws(x)
}

select_first_match <- function(data, candidates) {
  original_names        <- names(data)
  normalized_names      <- normalize_col_key(original_names)
  normalized_candidates <- normalize_col_key(candidates)
  idx <- match(normalized_candidates, normalized_names, nomatch = 0)
  idx <- idx[idx > 0]
  if (length(idx) == 0) return(rep(NA, nrow(data)))
  data[[original_names[idx[1]]]]
}

as_numeric_clean <- function(x) {
  if (is.numeric(x)) return(as.numeric(x))
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x <- gsub(",", "", x, fixed = TRUE)
  x <- gsub("%", "", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

baseball_ip_to_decimal <- function(x) {
  x_num <- as_numeric_clean(x)
  out   <- rep(NA_real_, length(x_num))
  keep  <- !is.na(x_num)
  if (!any(keep)) return(out)
  whole      <- floor(x_num[keep])
  frac_digit <- round((x_num[keep] - whole) * 10)
  frac_part  <- ifelse(frac_digit == 1, 1/3, ifelse(frac_digit == 2, 2/3, x_num[keep] - whole))
  out[keep]  <- whole + frac_part
  out
}

FG_BROWSER_USER_AGENT <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

fetch_fg_json_with_fallback <- function(url, retries = 3L, retry_sleep_sec = 1.5) {
  retries    <- max(1L, as.integer(retries))
  last_error <- NULL

  for (attempt in seq_len(retries)) {
    payload <- tryCatch(
      jsonlite::fromJSON(url, simplifyVector = TRUE),
      error = function(e) { last_error <<- conditionMessage(e); NULL }
    )
    if (!is.null(payload)) return(list(ok = TRUE, payload = payload, error = NULL))

    curl_bin <- Sys.which("curl")
    if (nzchar(curl_bin)) {
      tmp_json  <- tempfile(fileext = ".json")
      curl_args <- c(
        "-sS", "-L", "--fail", "--compressed",
        "--retry", "2", "--retry-delay", "1",
        "--max-time", "30", "--connect-timeout", "10",
        "-A", FG_BROWSER_USER_AGENT,
        "-H", "Accept: application/json, text/plain, */*",
        "-H", "Referer: https://www.fangraphs.com/leaders/major-league",
        "-H", "Origin: https://www.fangraphs.com",
        url, "-o", tmp_json
      )
      curl_result <- tryCatch(
        system2(curl_bin, args = curl_args, stdout = TRUE, stderr = TRUE),
        error = function(e) e
      )
      if (!inherits(curl_result, "error")) {
        curl_status <- attr(curl_result, "status")
        if (is.null(curl_status) || as.integer(curl_status) == 0L) {
          payload <- tryCatch(
            jsonlite::fromJSON(tmp_json, simplifyVector = TRUE),
            error = function(e) { last_error <<- conditionMessage(e); NULL }
          )
          unlink(tmp_json)
          if (!is.null(payload)) return(list(ok = TRUE, payload = payload, error = NULL))
        } else {
          last_error <- sprintf("curl exit %s", as.integer(curl_status))
          unlink(tmp_json)
        }
      } else {
        last_error <- conditionMessage(curl_result)
      }
    }

    if (attempt < retries) Sys.sleep(retry_sleep_sec * attempt)
  }

  list(ok = FALSE, payload = NULL, error = last_error %||% "No payload returned")
}

# =============================================================================
# Project paths
# =============================================================================

proj_root <- tryCatch({
  script_path <- normalizePath(
    sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1]),
    mustWork = FALSE
  )
  dirname(dirname(dirname(script_path)))
}, error = function(e) normalizePath(".."))

OUTPUT_DIR  <- file.path(proj_root, "data", "processed", "park_factors")
CACHE_DIR   <- file.path(OUTPUT_DIR, "stuff_plus_cache")
OUTPUT_FILE <- file.path(OUTPUT_DIR, "stuff_plus_by_park.csv")

# =============================================================================
# Constants
# =============================================================================

SEASONS       <- 2021:2026   # excludes 2020; 2026 uses partial season to date
MIN_SEASON_GS <- 3L    # pitchers with fewer GS excluded from delta calc
SLEEP_SEC     <- 1.2   # seconds between FG API calls

# MLB Stats API team ID -> FanGraphs team abbreviation.
# IDs are stable across seasons; only the Athletics abbreviation changes.
# Note: build_mlb_id_to_fg(season) handles the OAK -> ATH transition.
build_mlb_id_to_fg <- function(season) {
  ath <- if (season >= 2025) "ATH" else "OAK"
  c(
    "108" = "LAA",  "109" = "ARI",  "110" = "BAL",  "111" = "BOS",
    "112" = "CHC",  "113" = "CIN",  "114" = "CLE",  "115" = "COL",
    "116" = "DET",  "117" = "HOU",  "118" = "KCR",  "119" = "LAD",
    "120" = "WSN",  "121" = "NYM",  "133" = ath,    "134" = "PIT",
    "135" = "SDP",  "136" = "SEA",  "137" = "SFG",  "138" = "STL",
    "139" = "TBR",  "140" = "TEX",  "141" = "TOR",  "142" = "MIN",
    "143" = "PHI",  "144" = "ATL",  "145" = "CHW",  "146" = "MIA",
    "147" = "NYY",  "158" = "MIL"
  )
}

# Venue name -> roof category. Covers renames across 2022-2025.
VENUE_ROOF_TYPE <- c(
  "Chase Field"           = "retractable",   # ARI
  "Minute Maid Park"      = "retractable",   # HOU (through 2023)
  "Daikin Park"           = "retractable",   # HOU (renamed 2024)
  "loanDepot park"        = "retractable",   # MIA
  "Marlins Park"          = "retractable",   # MIA (pre-rename)
  "American Family Field" = "retractable",   # MIL
  "Globe Life Field"      = "retractable",   # TEX
  "Rogers Centre"         = "retractable",   # TOR
  "T-Mobile Park"         = "retractable",   # SEA
  "Tropicana Field"       = "dome"           # TBR
)

# =============================================================================
# URL builders
# =============================================================================

fg_stuff_url_date <- function(date, season) {
  paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&lg=all&type=36&stats=sta",
    "&season=", season, "&season1=", season,
    "&month=1000&ind=0&qual=0&team=0",
    "&startdate=", format(as.Date(date), "%Y-%m-%d"),
    "&enddate=",   format(as.Date(date), "%Y-%m-%d"),
    "&pageitems=60&pagenum=1"
  )
}

fg_stuff_url_season <- function(season) {
  paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&lg=all&type=36&stats=sta",
    "&season=", season, "&season1=", season,
    "&month=0&ind=0&qual=0&team=0",
    "&pageitems=2000&pagenum=1"
  )
}

# gameType=R ensures only regular season games on per-date schedule queries
mlb_schedule_url <- function(date) {
  paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?date=", format(as.Date(date), "%Y-%m-%d"),
    "&sportId=1&gameType=R&hydrate=venue,weather"
  )
}

mlb_season_dates_url <- function(season) {
  paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?sportId=1&season=", season,
    "&gameType=R&fields=dates,date"
  )
}

# =============================================================================
# Fetch + parse helpers
# =============================================================================

parse_fg_stuff <- function(payload, date = NA, season = NA) {
  if (is.null(payload) || !("data" %in% names(payload))) return(NULL)
  df <- payload$data
  # Guard: FG sometimes returns [] (empty list) rather than a data frame
  if (is.null(df) || !is.data.frame(df) || nrow(df) == 0) return(NULL)

  out <- tibble(
    season      = as.integer(season),
    date        = as.Date(date),
    player_name = strip_html_tags(select_first_match(df, c("PlayerName", "Name", "name"))),
    fg_team     = strip_html_tags(select_first_match(df, c("TeamNameAbb", "Team", "team"))),
    gs          = as_numeric_clean(select_first_match(df, c("GS", "gs"))),
    ip          = baseball_ip_to_decimal(select_first_match(df, c("IP", "ip"))),
    sp_stuff    = as_numeric_clean(select_first_match(df, c("sp_stuff"))),
    sp_location = as_numeric_clean(select_first_match(df, c("sp_location"))),
    sp_pitching = as_numeric_clean(select_first_match(df, c("sp_pitching")))
  )

  out[!is.na(out$player_name) & !is.na(out$sp_stuff), ]
}

fetch_fg_date <- function(date, season) {
  url    <- fg_stuff_url_date(date, season)
  result <- fetch_fg_json_with_fallback(url, retries = 2L, retry_sleep_sec = 2.0)
  if (!result$ok) {
    message("  [WARN] FG fetch failed ", date, ": ", result$error)
    return(NULL)
  }
  parse_fg_stuff(result$payload, date = date, season = season)
}

fetch_fg_season_avg <- function(season) {
  url    <- fg_stuff_url_season(season)
  result <- fetch_fg_json_with_fallback(url, retries = 3L, retry_sleep_sec = 2.0)
  if (!result$ok) stop("Season avg fetch failed (", season, "): ", result$error)
  df <- parse_fg_stuff(result$payload, season = season)
  if (is.null(df) || nrow(df) == 0) stop("Empty season averages for ", season)
  df
}

get_season_game_dates <- function(season) {
  url    <- mlb_season_dates_url(season)
  result <- tryCatch(jsonlite::fromJSON(url, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(result) || length(result$dates) == 0) return(character(0))
  dates  <- vapply(result$dates, function(d) d$date %||% NA_character_, character(1))
  sort(unique(dates[!is.na(dates)]))
}

# Returns tibble: fg_team | venue_name | roof_status
# One row per team (home + away) so join by fg_team works for either side.
# Uses team IDs (always present in schedule response) mapped via MLB_ID_TO_FG.
fetch_mlb_venue_map <- function(date, season) {
  mlb_id_to_fg <- build_mlb_id_to_fg(season)

  url    <- mlb_schedule_url(date)
  result <- tryCatch(jsonlite::fromJSON(url, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(result) || length(result$dates) == 0) return(NULL)

  games <- result$dates[[1]]$games
  if (is.null(games) || length(games) == 0) return(NULL)

  rows <- lapply(games, function(g) {
    home_id  <- as.character(g$teams$home$team$id %||% "")
    away_id  <- as.character(g$teams$away$team$id %||% "")
    home_fg  <- mlb_id_to_fg[home_id]
    away_fg  <- mlb_id_to_fg[away_id]
    if (is.na(home_fg)) home_fg <- ""
    if (is.na(away_fg)) away_fg <- ""

    venue_name  <- as.character(g$venue$name %||% "")
    weather_raw <- tolower(as.character(g$weather$condition %||% ""))

    roof_type <- VENUE_ROOF_TYPE[venue_name]
    roof_type <- if (is.na(roof_type)) "outdoor" else roof_type

    roof_status <- switch(roof_type,
      dome        = "closed",
      outdoor     = "outdoor",
      retractable = {
        if (grepl("closed", weather_raw))    "closed"
        else if (grepl("open", weather_raw)) "open"
        else                                 "retractable_unknown"
      }
    )

    list(home_fg = home_fg, away_fg = away_fg,
         venue_name = venue_name, roof_status = roof_status)
  })

  bind_rows(lapply(rows, function(r) {
    tibble(
      fg_team     = c(r$home_fg, r$away_fg),
      venue_name  = r$venue_name,
      roof_status = r$roof_status
    )
  })) |>
    filter(nzchar(fg_team))
}

# Join FG per-start data to venue info directly on fg_team
join_venue_info <- function(fg_starts, venue_map) {
  if (is.null(fg_starts) || nrow(fg_starts) == 0) return(NULL)
  if (is.null(venue_map)  || nrow(venue_map)  == 0) return(NULL)
  fg_starts |>
    left_join(distinct(venue_map, fg_team, .keep_all = TRUE), by = "fg_team")
}

# =============================================================================
# Main pipeline
# =============================================================================

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

all_starts <- vector("list", length(SEASONS))
names(all_starts) <- as.character(SEASONS)

for (season in SEASONS) {
  message("\n=== Season ", season, " ===")
  season_cache_dir <- file.path(CACHE_DIR, as.character(season))
  dir.create(season_cache_dir, recursive = TRUE, showWarnings = FALSE)

  # --- Season averages (normalization baseline) ---
  avg_cache <- file.path(season_cache_dir, "season_avgs.rds")
  if (file.exists(avg_cache)) {
    season_avgs <- readRDS(avg_cache)
    message("  Season avgs: loaded from cache (", nrow(season_avgs), " pitchers)")
  } else {
    message("  Fetching season averages from FanGraphs...")
    season_avgs <- fetch_fg_season_avg(season) |>
      filter(gs >= MIN_SEASON_GS) |>
      select(player_name,
             season_sp_stuff    = sp_stuff,
             season_sp_location = sp_location,
             season_sp_pitching = sp_pitching)
    saveRDS(season_avgs, avg_cache)
    Sys.sleep(SLEEP_SEC)
    message("  Season avgs: ", nrow(season_avgs), " pitchers with >= ", MIN_SEASON_GS, " GS")
  }

  # --- Regular season game dates ---
  dates <- get_season_game_dates(season)
  message("  Game dates: ", length(dates))

  # --- Per-date fetch loop ---
  # FG data and venue maps are cached separately (_fg.rds / _venue.rds).
  # This means fixing the join logic doesn't require re-fetching from FG.
  season_rows <- vector("list", length(dates))
  names(season_rows) <- dates
  n_cached  <- 0L
  n_fetched <- 0L
  n_empty   <- 0L

  for (date in dates) {
    fg_cache    <- file.path(season_cache_dir, paste0(date, "_fg.rds"))
    venue_cache <- file.path(season_cache_dir, paste0(date, "_venue.rds"))

    # Load or fetch FG starts
    if (file.exists(fg_cache)) {
      fg_data <- readRDS(fg_cache)
    } else {
      fg_data <- fetch_fg_date(date, season)
      saveRDS(fg_data, fg_cache)
      Sys.sleep(SLEEP_SEC)
      n_fetched <- n_fetched + 1L
    }

    # Load or fetch venue map
    if (file.exists(venue_cache)) {
      venue_map <- readRDS(venue_cache)
    } else {
      venue_map <- fetch_mlb_venue_map(date, season)
      saveRDS(venue_map, venue_cache)
    }

    combined <- join_venue_info(fg_data, venue_map)
    season_rows[[date]] <- combined

    if (!file.exists(fg_cache) || !file.exists(venue_cache)) {
      # already counted above
    } else {
      n_cached <- n_cached + 1L
    }

    if (is.null(combined) || nrow(combined) == 0) n_empty <- n_empty + 1L

    if ((n_fetched + n_cached) %% 20 == 0 && (n_fetched + n_cached) > 0) {
      message("    ... ", n_fetched, " fetched | ", n_cached, " cached | at ", date)
    }
  }

  message("  Done: ", n_fetched, " fetched, ", n_cached, " cached, ", n_empty, " empty/unmatched")

  # --- Combine and compute deltas ---
  season_df <- bind_rows(Filter(Negate(is.null), season_rows))
  if (nrow(season_df) == 0) {
    message("  No starts for ", season, " — skipping")
    next
  }

  season_df <- season_df |>
    left_join(season_avgs, by = "player_name") |>
    filter(!is.na(season_sp_stuff), !is.na(sp_stuff), !is.na(venue_name)) |>
    mutate(
      delta_stuff    = sp_stuff    - season_sp_stuff,
      delta_location = sp_location - season_sp_location,
      delta_pitching = sp_pitching - season_sp_pitching
    )

  all_starts[[as.character(season)]] <- season_df
  message("  ", nrow(season_df), " starts with complete data")
}

# =============================================================================
# Aggregate by park (raw)
# =============================================================================

message("\n=== Aggregating by park ===")

all_data <- bind_rows(Filter(Negate(is.null), all_starts))
if (nrow(all_data) == 0) stop("No data to aggregate — check fetch failures above.")

park_raw <- all_data |>
  filter(!is.na(venue_name), !is.na(delta_stuff)) |>
  group_by(venue_name, roof_status) |>
  summarise(
    n_starts            = n(),
    mean_stuff_delta    = round(mean(delta_stuff,    na.rm = TRUE), 2),
    mean_location_delta = round(mean(delta_location, na.rm = TRUE), 2),
    mean_pitching_delta = round(mean(delta_pitching, na.rm = TRUE), 2),
    sd_stuff_delta      = round(sd(delta_stuff,      na.rm = TRUE), 2),
    seasons_covered     = paste(sort(unique(season)), collapse = ", "),
    .groups = "drop"
  ) |>
  as.data.frame()

message("Raw park rows: ", nrow(park_raw))

# =============================================================================
# Post-processing
# =============================================================================

# Helper: pool two rows (by venue_name) into one using weighted means + pooled SD
pool_venue_rows <- function(df, venues, canonical_name, canonical_roof = NULL) {
  rows <- df[df$venue_name %in% venues, ]
  n    <- rows$n_starts
  N    <- sum(n)
  wt   <- n / N
  pm   <- function(col) round(sum(wt * rows[[col]]), 2)
  ps   <- function(m_col, s_col) {
    mu <- sum(wt * rows[[m_col]])
    round(sqrt(sum(n * (rows[[s_col]]^2 + (rows[[m_col]] - mu)^2)) / N), 2)
  }
  seasons <- paste(sort(unique(unlist(strsplit(
    paste(rows$seasons_covered, collapse = ", "), ",\\s*")))), collapse = ", ")
  data.frame(
    venue_name          = canonical_name,
    roof_status         = if (!is.null(canonical_roof)) canonical_roof else rows$roof_status[1],
    n_starts            = N,
    mean_stuff_delta    = pm("mean_stuff_delta"),
    mean_location_delta = pm("mean_location_delta"),
    mean_pitching_delta = pm("mean_pitching_delta"),
    sd_stuff_delta      = ps("mean_stuff_delta", "sd_stuff_delta"),
    seasons_covered     = seasons,
    stringsAsFactors    = FALSE
  )
}

df <- park_raw

# 1. Filter small-sample special-event venues (< 100 starts)
df <- df[df$n_starts >= 100, ]

# 2. Chase Field: retractable_unknown → open
#    (Phoenix: roof almost always closed in summer; unknowns are spring/fall open games)
df$roof_status[df$venue_name == "Chase Field" &
               df$roof_status == "retractable_unknown"] <- "open"

# 3. loanDepot park: retractable_unknown → closed, then pool with existing closed row
#    (Miami: roof open only ~5 games/season; unknowns are effectively closed)
df$roof_status[df$venue_name == "loanDepot park" &
               df$roof_status == "retractable_unknown"] <- "closed"
if (sum(df$venue_name == "loanDepot park") > 1) {
  merged <- pool_venue_rows(df[df$venue_name == "loanDepot park", ],
                            rep("loanDepot park", sum(df$venue_name == "loanDepot park")),
                            "loanDepot park", canonical_roof = "closed")
  df <- df[df$venue_name != "loanDepot park", ]
  df <- rbind(df, merged)
}

# 4. Remaining retractable_unknown → open
#    (T-Mobile, AmFam, Rogers Centre: open-by-default parks; API only flags "Roof Closed")
df$roof_status[df$roof_status == "retractable_unknown"] <- "open"

# 5. Merge same-building parks (name changes only, same climate)
#    Minute Maid Park + Daikin Park → Daikin Park  (venue_2392, HOU)
if (all(c("Minute Maid Park", "Daikin Park") %in% df$venue_name)) {
  merged <- pool_venue_rows(df[df$venue_name %in% c("Minute Maid Park", "Daikin Park"), ],
                            c("Minute Maid Park", "Daikin Park"),
                            "Daikin Park", canonical_roof = "closed")
  df <- df[!df$venue_name %in% c("Minute Maid Park", "Daikin Park"), ]
  df <- rbind(df, merged)
}

#    Guaranteed Rate Field + Rate Field → Guaranteed Rate Field  (venue_4, CHW)
if (all(c("Guaranteed Rate Field", "Rate Field") %in% df$venue_name)) {
  merged <- pool_venue_rows(df[df$venue_name %in% c("Guaranteed Rate Field", "Rate Field"), ],
                            c("Guaranteed Rate Field", "Rate Field"),
                            "Guaranteed Rate Field", canonical_roof = "outdoor")
  df <- df[!df$venue_name %in% c("Guaranteed Rate Field", "Rate Field"), ]
  df <- rbind(df, merged)
}

# 6. Team abbreviation lookup (FG-style)
team_map <- c(
  "Coors Field"                  = "COL",
  "Comerica Park"                = "DET",
  "George M. Steinbrenner Field" = "TBR",
  "Kauffman Stadium"             = "KCR",
  "PNC Park"                     = "PIT",
  "Sutter Health Park"           = "ATH",
  "Progressive Field"            = "CLE",
  "Chase Field"                  = "ARI",
  "Guaranteed Rate Field"        = "CHW",
  "Truist Park"                  = "ATL",
  "Wrigley Field"                = "CHC",
  "Dodger Stadium"               = "LAD",
  "Target Field"                 = "MIN",
  "Oracle Park"                  = "SFG",
  "Nationals Park"               = "WSN",
  "Busch Stadium"                = "STL",
  "Globe Life Field"             = "TEX",
  "Great American Ball Park"     = "CIN",
  "T-Mobile Park"                = "SEA",
  "Fenway Park"                  = "BOS",
  "American Family Field"        = "MIL",
  "Citi Field"                   = "NYM",
  "Oriole Park at Camden Yards"  = "BAL",
  "Oakland Coliseum"             = "OAK",
  "Rogers Centre"                = "TOR",
  "Angel Stadium"                = "LAA",
  "Yankee Stadium"               = "NYY",
  "Citizens Bank Park"           = "PHI",
  "Petco Park"                   = "SDP",
  "loanDepot park"               = "MIA",
  "Daikin Park"                  = "HOU",
  "Tropicana Field"              = "TBR"
)
df$Team <- team_map[df$venue_name]

# 7. Rename, reorder, sort
df <- df[, c("Team", "venue_name", "roof_status",
             "mean_stuff_delta", "mean_location_delta", "mean_pitching_delta",
             "seasons_covered", "n_starts")]
names(df) <- c("Team", "Park", "Roof Status",
               "Stuff+ Delta", "Loc+ Delta", "Pitching+ Delta",
               "Seasons Incl.", "n_starts")
df <- df[order(df$"Stuff+ Delta"), ]

message("Final rows: ", nrow(df))
message("\n--- Preview ---")
print(df[, c("Team", "Park", "Roof Status", "Stuff+ Delta", "n_starts")],
      row.names = FALSE)

write.csv(df, OUTPUT_FILE, row.names = FALSE)
message("\nWrote: ", OUTPUT_FILE)
