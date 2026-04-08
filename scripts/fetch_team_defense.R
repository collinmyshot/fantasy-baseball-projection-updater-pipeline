#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_csv      = list(flag = "--output-csv",      default = file.path("data", "manual", "team_defense_2015_2025.csv")),
  source_csv      = list(flag = "--source-csv",      default = file.path("data", "manual", "team_defense_2015_2025_sources.csv")),
  start_season    = list(flag = "--start-season",    default = 2015, type = "numeric"),
  end_season      = list(flag = "--end-season",      default = 2025, type = "numeric"),
  exclude_seasons = list(flag = "--exclude-seasons", default = "2020")
))

output_csv      <- parsed$output_csv
source_csv      <- parsed$source_csv
start_season    <- as.integer(parsed$start_season)
end_season      <- as.integer(parsed$end_season)
exclude_seasons <- parse_int_vec(as.character(parsed$exclude_seasons))

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}

if (!is.finite(start_season) || !is.finite(end_season) || start_season > end_season) {
  stop("Invalid season range.")
}

seasons <- seq.int(start_season, end_season, by = 1L)
seasons <- seasons[!seasons %in% exclude_seasons]
if (length(seasons) == 0) {
  stop("No seasons to fetch after exclusions.")
}

sum_or_na <- function(x) {
  x <- as.numeric(x)
  if (length(x) == 0 || all(is.na(x))) {
    return(NA_real_)
  }
  sum(x, na.rm = TRUE)
}

norm_team <- function(x) {
  out <- toupper(trimws(as.character(x)))
  out[is.na(out)] <- ""
  out <- gsub("[^A-Z0-9]", "", out)
  mapped <- c(
    KC = "KCR", KCR = "KCR",
    OAK = "ATH", ATH = "ATH",
    ARI = "ARI", ARZ = "ARI",
    SD = "SDP", SDP = "SDP",
    SF = "SFG", SFG = "SFG",
    TB = "TBR", TBR = "TBR",
    WAS = "WSH", WSN = "WSH", WSH = "WSH",
    CWS = "CHW", CHW = "CHW",
    ANA = "LAA", LAA = "LAA",
    FLA = "MIA", MIA = "MIA"
  )
  hit <- out %in% names(mapped)
  out[hit] <- mapped[out[hit]]
  out
}

savant_team_map <- c(
  "Angels" = "LAA",
  "Astros" = "HOU",
  "Athletics" = "ATH",
  "Blue Jays" = "TOR",
  "Braves" = "ATL",
  "Brewers" = "MIL",
  "Cardinals" = "STL",
  "Cubs" = "CHC",
  "D-backs" = "ARI",
  "Diamondbacks" = "ARI",
  "Dodgers" = "LAD",
  "Giants" = "SFG",
  "Guardians" = "CLE",
  "Indians" = "CLE",
  "Mariners" = "SEA",
  "Marlins" = "MIA",
  "Mets" = "NYM",
  "Nationals" = "WSH",
  "Orioles" = "BAL",
  "Padres" = "SDP",
  "Phillies" = "PHI",
  "Pirates" = "PIT",
  "Rangers" = "TEX",
  "Rays" = "TBR",
  "Red Sox" = "BOS",
  "Reds" = "CIN",
  "Rockies" = "COL",
  "Royals" = "KCR",
  "Tigers" = "DET",
  "Twins" = "MIN",
  "White Sox" = "CHW",
  "Yankees" = "NYY"
)

fetch_fg_team_fielding <- function(season) {
  url <- sprintf(
    "https://www.fangraphs.com/api/leaders/major-league/data?pos=all&stats=fld&lg=all&qual=0&type=1&season=%d&season1=%d&ind=1&team=0&rost=0&age=0&players=0&pageitems=5000&pagenum=1",
    season, season
  )
  x <- jsonlite::fromJSON(url)
  d <- x[["data"]]
  if (!is.data.frame(d) || nrow(d) == 0) {
    return(data.frame())
  }

  team <- norm_team(d[["TeamNameAbb"]])
  drs <- suppressWarnings(as.numeric(d[["DRS"]]))
  uzr <- suppressWarnings(as.numeric(d[["UZR"]]))
  keep <- grepl("^[A-Z]{2,3}$", team) & !team %in% c("TMS")

  df <- data.frame(
    season = as.integer(season),
    team = team[keep],
    drs = drs[keep],
    uzr = uzr[keep],
    stringsAsFactors = FALSE
  )
  if (nrow(df) == 0) {
    return(data.frame())
  }

  agg_drs <- stats::aggregate(df$drs, by = list(season = df$season, team = df$team), FUN = sum_or_na)
  agg_uzr <- stats::aggregate(df$uzr, by = list(season = df$season, team = df$team), FUN = sum_or_na)
  names(agg_drs)[3] <- "drs"
  names(agg_uzr)[3] <- "uzr"

  out <- merge(agg_drs, agg_uzr, by = c("season", "team"), all = TRUE)
  out[order(out$team), , drop = FALSE]
}

fetch_savant_team_oaa <- function(season) {
  url <- sprintf(
    "https://baseballsavant.mlb.com/leaderboard/outs_above_average?year=%d&csv=true",
    season
  )
  raw <- tryCatch(
    utils::read.csv(url, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) data.frame()
  )
  if (!is.data.frame(raw) || nrow(raw) == 0) {
    return(data.frame())
  }

  team_name <- trimws(as.character(raw[["display_team_name"]]))
  oaa <- suppressWarnings(as.numeric(raw[["outs_above_average"]]))

  team <- unname(savant_team_map[team_name])
  team <- norm_team(team)

  keep <- !is.na(team) & nzchar(team) & !team %in% c("TMS")
  df <- data.frame(
    season = as.integer(season),
    team = team[keep],
    oaa = oaa[keep],
    stringsAsFactors = FALSE
  )
  if (nrow(df) == 0) {
    return(data.frame())
  }

  agg <- stats::aggregate(df$oaa, by = list(season = df$season, team = df$team), FUN = sum_or_na)
  names(agg)[3] <- "oaa"
  agg[order(agg$team), , drop = FALSE]
}

fg_list <- vector("list", length(seasons))
oaa_list <- vector("list", length(seasons))
for (idx in seq_along(seasons)) {
  s <- seasons[[idx]]
  message(sprintf("Fetching defense inputs for season %d ...", s))
  fg_list[[idx]] <- fetch_fg_team_fielding(s)
  oaa_list[[idx]] <- fetch_savant_team_oaa(s)
}

fg_all <- do.call(rbind, fg_list)
oaa_all <- do.call(rbind, oaa_list)

if (!is.data.frame(fg_all) || nrow(fg_all) == 0) {
  stop("No FanGraphs defense rows were fetched.")
}

keys <- unique(fg_all[, c("season", "team")])
keys <- keys[order(keys$season, keys$team), , drop = FALSE]
defense <- merge(keys, fg_all, by = c("season", "team"), all.x = TRUE)
defense <- merge(defense, oaa_all, by = c("season", "team"), all.x = TRUE)
defense <- defense[, c("season", "team", "oaa", "drs", "uzr")]
defense <- defense[order(defense$season, defense$team), , drop = FALSE]
rownames(defense) <- NULL

out_dir <- dirname(output_csv)
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
}
utils::write.csv(defense, output_csv, row.names = FALSE, na = "")

source_rows <- data.frame(
  metric = c("oaa", "drs", "uzr"),
  source = c(
    "https://baseballsavant.mlb.com/leaderboard/outs_above_average",
    "https://www.fangraphs.com/leaders/major-league?stats=fld",
    "https://www.fangraphs.com/leaders/major-league?stats=fld"
  ),
  method = c(
    "Summed player-level Statcast OAA to team-season.",
    "Summed player-level FanGraphs DRS to team-season.",
    "Summed player-level FanGraphs UZR to team-season."
  ),
  seasons = sprintf("%d-%d (excluding %s)", start_season, end_season, paste(sort(exclude_seasons), collapse = ",")),
  created_utc = format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ"),
  stringsAsFactors = FALSE
)
utils::write.csv(source_rows, source_csv, row.names = FALSE, na = "")

message(sprintf("Wrote defense table: %s (%d rows)", output_csv, nrow(defense)))
message(sprintf("Wrote defense source metadata: %s", source_csv))
