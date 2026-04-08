#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  season    = list(flag = "--season",    default = 2026, type = "numeric"),
  out_csv   = list(flag = "--out-csv",   default = file.path("data", "manual", "mlb_home_parks_2026_verified.csv")),
  game_type = list(flag = "--game-type", default = "R")
))

season    <- as.integer(parsed$season)
out_csv   <- parsed$out_csv
game_type <- parsed$game_type
sport_id  <- 1L

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}
if (!requireNamespace("rvest", quietly = TRUE)) {
  stop("Package 'rvest' is required.")
}

cleanup_text <- function(x) {
  x <- as.character(x)
  x <- gsub("\\[.*$", "", x)
  x <- gsub("[†‡*]+", "", x)
  trimws(x)
}

build_from_wikipedia <- function(season) {
  wiki_url <- "https://en.wikipedia.org/wiki/List_of_current_Major_League_Baseball_stadiums"
  html <- tryCatch(
    {
      rvest::read_html(wiki_url)
    },
    error = function(e) {
      # fallback to local cache if present
      cache <- "/tmp/mlb_stadiums.html"
      if (!file.exists(cache)) {
        stop(sprintf("Wikipedia fetch failed and cache missing: %s", conditionMessage(e)))
      }
      rvest::read_html(cache)
    }
  )

  tabs <- rvest::html_table(html, fill = TRUE)
  idx <- which(vapply(tabs, function(tb) {
    nms <- names(tb)
    any(grepl("^Name$", nms, ignore.case = TRUE)) &&
      any(grepl("^Team$", nms, ignore.case = TRUE))
  }, logical(1)))
  if (length(idx) == 0) {
    stop("Could not find current stadium table on Wikipedia page.")
  }

  tb <- tabs[[idx[1]]]
  names(tb) <- cleanup_text(names(tb))
  name_col <- names(tb)[grepl("^Name$", names(tb), ignore.case = TRUE)][1]
  team_col <- names(tb)[grepl("^Team$", names(tb), ignore.case = TRUE)][1]
  cap_col <- names(tb)[grepl("^Capacity$", names(tb), ignore.case = TRUE)][1]

  out <- tb[, c(name_col, team_col, cap_col)]
  names(out) <- c("venue_name", "team_name", "capacity")
  out$venue_name <- cleanup_text(out$venue_name)
  out$team_name <- cleanup_text(out$team_name)

  abbr_map <- c(
    "Arizona Diamondbacks" = "AZ",
    "Athletics" = "ATH",
    "Atlanta Braves" = "ATL",
    "Baltimore Orioles" = "BAL",
    "Boston Red Sox" = "BOS",
    "Chicago Cubs" = "CHC",
    "Chicago White Sox" = "CHW",
    "Cincinnati Reds" = "CIN",
    "Cleveland Guardians" = "CLE",
    "Colorado Rockies" = "COL",
    "Detroit Tigers" = "DET",
    "Houston Astros" = "HOU",
    "Kansas City Royals" = "KC",
    "Los Angeles Angels" = "LAA",
    "Los Angeles Dodgers" = "LAD",
    "Miami Marlins" = "MIA",
    "Milwaukee Brewers" = "MIL",
    "Minnesota Twins" = "MIN",
    "New York Mets" = "NYM",
    "New York Yankees" = "NYY",
    "Philadelphia Phillies" = "PHI",
    "Pittsburgh Pirates" = "PIT",
    "San Diego Padres" = "SDP",
    "San Francisco Giants" = "SFG",
    "Seattle Mariners" = "SEA",
    "St. Louis Cardinals" = "STL",
    "Tampa Bay Rays" = "TBR",
    "Texas Rangers" = "TEX",
    "Toronto Blue Jays" = "TOR",
    "Washington Nationals" = "WSH"
  )

  out$team_abbr <- abbr_map[out$team_name]
  out <- out[!is.na(out$team_abbr), ]
  out <- out[!duplicated(out$team_abbr), ]
  out <- out[order(out$team_abbr), ]

  out$team_id <- NA_integer_
  out$venue_id <- NA_integer_
  out$n_home_games <- NA_integer_
  out$n_home_games_total <- NA_integer_
  out$n_unique_venues <- 1L
  out$season <- season

  out <- out[, c(
    "team_id", "team_abbr", "team_name",
    "venue_id", "venue_name",
    "n_home_games", "n_home_games_total", "n_unique_venues", "season"
  )]
  rownames(out) <- NULL
  out
}

teams_url <- sprintf("https://statsapi.mlb.com/api/v1/teams?sportId=%s", sport_id)
schedule_url <- sprintf(
  "https://statsapi.mlb.com/api/v1/schedule?sportId=%s&season=%s&gameType=%s",
  sport_id,
  season,
  utils::URLencode(game_type, reserved = TRUE)
)

out <- tryCatch({
  teams_payload <- jsonlite::fromJSON(teams_url, simplifyVector = FALSE)
  sched_payload <- jsonlite::fromJSON(schedule_url, simplifyVector = FALSE)

  teams <- teams_payload$teams
  team_rows <- do.call(rbind, lapply(teams, function(t) {
    data.frame(
      team_id = as.integer(t$id),
      team_name = as.character(t$name),
      team_abbr = as.character(t$abbreviation),
      stringsAsFactors = FALSE
    )
  }))

  game_rows <- list()
  dates <- sched_payload$dates
  if (length(dates) == 0) {
    stop(sprintf("No schedule dates returned for season %s.", season))
  }

  for (d in dates) {
    games <- d$games
    if (length(games) == 0) next
    for (g in games) {
      home_team_id <- as.integer(g$teams$home$team$id)
      venue_id <- as.integer(g$venue$id)
      venue_name <- as.character(g$venue$name)
      game_pk <- as.integer(g$gamePk)
      game_date <- substr(as.character(g$gameDate), 1, 10)

      game_rows[[length(game_rows) + 1L]] <- data.frame(
        game_pk = game_pk,
        game_date = game_date,
        home_team_id = home_team_id,
        venue_id = venue_id,
        venue_name = venue_name,
        stringsAsFactors = FALSE
      )
    }
  }

  games <- do.call(rbind, game_rows)

  agg <- stats::aggregate(
    rep(1L, nrow(games)),
    by = list(home_team_id = games$home_team_id, venue_id = games$venue_id, venue_name = games$venue_name),
    FUN = sum
  )
  names(agg)[4] <- "n_home_games"

  team_total <- stats::aggregate(
    agg$n_home_games,
    by = list(home_team_id = agg$home_team_id),
    FUN = sum
  )
  names(team_total)[2] <- "n_home_games_total"

  team_venues <- stats::aggregate(
    rep(1L, nrow(agg)),
    by = list(home_team_id = agg$home_team_id),
    FUN = sum
  )
  names(team_venues)[2] <- "n_unique_venues"

  primary <- do.call(rbind, lapply(split(agg, agg$home_team_id), function(df) {
    df <- df[order(df$n_home_games, decreasing = TRUE), ]
    df[1, , drop = FALSE]
  }))
  rownames(primary) <- NULL

  out <- merge(primary, team_total, by = "home_team_id", all.x = TRUE)
  out <- merge(out, team_venues, by = "home_team_id", all.x = TRUE)
  out <- merge(out, team_rows, by.x = "home_team_id", by.y = "team_id", all.x = TRUE)

  out <- out[, c(
    "home_team_id", "team_abbr", "team_name",
    "venue_id", "venue_name",
    "n_home_games", "n_home_games_total", "n_unique_venues"
  )]
  names(out)[1] <- "team_id"
  out$season <- season

  out <- out[order(out$team_abbr), ]
  rownames(out) <- NULL
  out
}, error = function(e) {
  warning(sprintf("StatsAPI fetch failed (%s). Falling back to Wikipedia stadium table.", conditionMessage(e)))
  build_from_wikipedia(season)
})

out_dir <- dirname(out_csv)
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
}
utils::write.csv(out, out_csv, row.names = FALSE, na = "")

message("Wrote verified home park mapping: ", out_csv)
message("Teams: ", nrow(out))
message("Teams with multiple venues: ", sum(out$n_unique_venues > 1, na.rm = TRUE))
