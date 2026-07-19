#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

# iPF formulation (July 2026): the Overall factor is the main model's wOBAcon
# park effect taken directly (no component blend). BACON, HR, XBH, and Carry
# are component lenses estimated by their own models.
parsed <- parse_cli_args(list(
  output_dir = list(flag = "--output-dir", default = file.path("data", "processed", "park_factors"))
))

output_dir <- parsed$output_dir

required_files <- c(
  file.path(output_dir, "park_factors_overall.csv"),
  file.path(output_dir, "park_factors_bacon_overall.csv"),
  file.path(output_dir, "park_factors_hr_overall.csv"),
  file.path(output_dir, "park_factors_xbh_overall.csv"),
  file.path(output_dir, "team_park_era_audit.csv")
)

missing <- required_files[!file.exists(required_files)]
if (length(missing) > 0) {
  stop(sprintf("Missing required input files:\n- %s", paste(missing, collapse = "\n- ")))
}

read_pf_component <- function(path, comp_label) {
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  keep <- c("park_era_id", "home_team", "delta_overall", "park_se", "n_bbe")
  keep <- keep[keep %in% names(d)]
  d <- d[, keep, drop = FALSE]
  names(d)[names(d) == "delta_overall"] <- sprintf("%s_delta", comp_label)
  names(d)[names(d) == "park_se"] <- sprintf("%s_se", comp_label)
  names(d)[names(d) == "n_bbe"] <- sprintf("%s_n_bbe", comp_label)
  names(d)[names(d) == "home_team"] <- sprintf("%s_team", comp_label)
  d
}

format_years <- function(min_season, max_season) {
  ifelse(
    is.na(min_season) | is.na(max_season),
    NA_character_,
    ifelse(min_season == max_season, as.character(min_season), paste0(min_season, "-", max_season))
  )
}

pretty_team <- function(x) {
  x <- toupper(as.character(x))
  mapped <- c(KCR = "KC", WSH = "WSH", CHW = "CHW", AZ = "AZ", ATH = "ATH", TBR = "TBR", SDP = "SDP", SFG = "SFG")
  hit <- x %in% names(mapped)
  x[hit] <- mapped[x[hit]]
  x
}

team_full_name <- function(team_id) {
  x <- toupper(as.character(team_id))
  mapped <- c(
    AZ = "Diamondbacks",
    ATL = "Braves",
    BAL = "Orioles",
    BOS = "Red Sox",
    CHC = "Cubs",
    CHW = "White Sox",
    CIN = "Reds",
    CLE = "Guardians",
    COL = "Rockies",
    DET = "Tigers",
    HOU = "Astros",
    KCR = "Royals",
    KC = "Royals",
    LAA = "Angels",
    LAD = "Dodgers",
    MIA = "Marlins",
    MIL = "Brewers",
    MIN = "Twins",
    NYM = "Mets",
    NYY = "Yankees",
    ATH = "Athletics",
    PHI = "Phillies",
    PIT = "Pirates",
    SDP = "Padres",
    SEA = "Mariners",
    SFG = "Giants",
    STL = "Cardinals",
    TBR = "Rays",
    TEX = "Rangers",
    TOR = "Blue Jays",
    WSH = "Nationals"
  )
  out <- mapped[x]
  out[is.na(out)] <- x[is.na(out)]
  as.character(out)
}

park_suffix <- function(park_era_id) {
  sub("^.*__", "", as.character(park_era_id))
}

venue_from_team_suffix <- function(team, suffix) {
  team <- toupper(as.character(team))
  suffix <- tolower(as.character(suffix))

  default_venue <- c(
    AZ = "Chase Field",
    ATL = "Turner Field",
    BAL = "Oriole Park at Camden Yards",
    BOS = "Fenway Park",
    CHC = "Wrigley Field",
    CHW = "Rate Field",
    CIN = "Great American Ball Park",
    CLE = "Progressive Field",
    COL = "Coors Field",
    DET = "Comerica Park",
    HOU = "Daikin Park",
    KCR = "Kauffman Stadium",
    LAA = "Angel Stadium",
    LAD = "Dodger Stadium",
    MIA = "loanDepot park",
    MIL = "American Family Field",
    MIN = "Target Field",
    NYM = "Citi Field",
    NYY = "Yankee Stadium",
    ATH = "Oakland Coliseum",
    PHI = "Citizens Bank Park",
    PIT = "PNC Park",
    SDP = "Petco Park",
    SEA = "T-Mobile Park",
    SFG = "Oracle Park",
    STL = "Busch Stadium",
    TBR = "Tropicana Field",
    TEX = "Globe Life Park",
    TOR = "Rogers Centre",
    WSH = "Nationals Park"
  )

  if (suffix == "base") {
    if (team == "ATL") {
      return("Turner Field")
    }
    if (team == "TEX") {
      return("Globe Life Park")
    }
    return(default_venue[[team]] %||% "Unknown Venue")
  }

  key <- paste0(team, "__", suffix)
  override <- c(
    "ATL__suntrust" = "Truist Park",
    "ATH__sutter_health" = "Sutter Health Park",
    "AZ__humidor" = "Chase Field",
    "BAL__wall_deep" = "Oriole Park at Camden Yards",
    "BAL__wall_medium" = "Oriole Park at Camden Yards",
    "CLE__wind_tunnel" = "Progressive Field",
    "DET__cf_in_412" = "Comerica Park",
    "KCR__walls_in" = "Kauffman Stadium",
    "MIA__cf_in_407" = "loanDepot park",
    "MIA__fence_reduced" = "loanDepot park",
    "NYM__rf_in_362" = "Citi Field",
    "SFG__bullpen_center" = "Oracle Park",
    "TBR__steinbrenner" = "George M. Steinbrenner Field",
    "TBR__trop_return" = "Tropicana Field",
    "TEX__globe_life" = "Globe Life Field",
    "TOR__dunedin" = "TD Ballpark",
    "TOR__buffalo" = "Sahlen Field"
  )
  if (key %in% names(override)) {
    return(override[[key]])
  }

  default_venue[[team]] %||% "Unknown Venue"
}

bacon <- read_pf_component(file.path(output_dir, "park_factors_bacon_overall.csv"), "bacon")
hr <- read_pf_component(file.path(output_dir, "park_factors_hr_overall.csv"), "hr")
xbh <- read_pf_component(file.path(output_dir, "park_factors_xbh_overall.csv"), "xbh")

tbl <- merge(bacon, hr, by = "park_era_id", all = TRUE)
tbl <- merge(tbl, xbh, by = "park_era_id", all = TRUE)

# Carry (fly ball and line drive distance, feet) is a newer output; tolerate
# its absence so the script still runs on old build dirs.
optional_component <- function(name, comp_label) {
  path <- file.path(output_dir, sprintf("park_factors_%s_overall.csv", name))
  if (!file.exists(path)) {
    warning(sprintf("Component file missing, columns will be NA: %s", path))
    return(NULL)
  }
  read_pf_component(path, comp_label)
}
carry <- optional_component("distance", "carry")
if (!is.null(carry)) {
  tbl <- merge(tbl, carry, by = "park_era_id", all.x = TRUE)
} else {
  tbl$carry_delta <- NA_real_
  tbl$carry_se <- NA_real_
}

# The Overall factor: the main model's wOBAcon-residual park effect, direct.
overall_main <- utils::read.csv(file.path(output_dir, "park_factors_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)
overall_main <- overall_main[, c("park_era_id", "delta_woba_over_xwoba_overall", "overall_se")]
names(overall_main) <- c("park_era_id", "overall_delta", "overall_se")
tbl <- merge(tbl, overall_main, by = "park_era_id", all.x = TRUE)

team_cols <- c("bacon_team", "hr_team", "xbh_team")
tbl$team_id <- NA_character_
for (nm in team_cols) {
  if (nm %in% names(tbl)) {
    need <- is.na(tbl$team_id) | !nzchar(tbl$team_id)
    tbl$team_id[need] <- tbl[[nm]][need]
  }
}
tbl$team_id <- toupper(trimws(tbl$team_id))

audit <- utils::read.csv(file.path(output_dir, "team_park_era_audit.csv"), stringsAsFactors = FALSE, check.names = FALSE)
audit$season <- suppressWarnings(as.integer(audit$season))
audit$n_bbe <- suppressWarnings(as.numeric(audit$n_bbe))

years_min <- stats::aggregate(audit$season, by = list(park_era_id = audit$park_era_id), FUN = min, na.rm = TRUE)
years_max <- stats::aggregate(audit$season, by = list(park_era_id = audit$park_era_id), FUN = max, na.rm = TRUE)
years_n <- stats::aggregate(audit$n_bbe, by = list(park_era_id = audit$park_era_id), FUN = sum, na.rm = TRUE)
names(years_min)[2] <- "year_start"
names(years_max)[2] <- "year_end"
names(years_n)[2] <- "total_bbe"

years <- merge(years_min, years_max, by = "park_era_id", all = TRUE)
years <- merge(years, years_n, by = "park_era_id", all = TRUE)

tbl <- merge(tbl, years, by = "park_era_id", all.x = TRUE)
tbl$years_used <- format_years(tbl$year_start, tbl$year_end)

tbl$suffix <- park_suffix(tbl$park_era_id)
tbl$team_name <- team_full_name(tbl$team_id)
tbl$park_name <- mapply(venue_from_team_suffix, tbl$team_id, tbl$suffix, USE.NAMES = FALSE)

tbl$bacon_idx_100 <- std_index(tbl$bacon_delta)
tbl$hr_idx_100 <- std_index(tbl$hr_delta)
tbl$xbh_idx_100 <- std_index(tbl$xbh_delta)
tbl$carry_idx_100 <- std_index(tbl$carry_delta)
tbl$overall_pf_100 <- std_index(tbl$overall_delta)

tbl$rank <- rank(-tbl$overall_pf_100, ties.method = "min", na.last = "keep")
tbl$hr_rank <- rank(-tbl$hr_delta, ties.method = "min", na.last = "keep")
tbl$bacon_rank <- rank(-tbl$bacon_delta, ties.method = "min", na.last = "keep")
tbl$xbh_rank <- rank(-tbl$xbh_delta, ties.method = "min", na.last = "keep")
tbl$carry_rank <- rank(-tbl$carry_delta, ties.method = "min", na.last = "keep")

out <- tbl[, c(
  "park_era_id",
  "rank",
  "team_id",
  "team_name",
  "park_name",
  "years_used",
  "overall_delta",
  "bacon_delta",
  "hr_delta",
  "xbh_delta",
  "carry_delta",
  "bacon_idx_100",
  "hr_idx_100",
  "xbh_idx_100",
  "carry_idx_100",
  "overall_pf_100",
  "total_bbe",
  "overall_se",
  "bacon_se",
  "hr_se",
  "xbh_se",
  "carry_se",
  "bacon_rank",
  "hr_rank",
  "xbh_rank",
  "carry_rank"
)]

names(out) <- c(
  "park_era_id",
  "rank",
  "team_id",
  "team",
  "park",
  "years_used",
  "overall_resid",
  "bacon_resid",
  "hr_resid",
  "xbh_resid",
  "carry_ft",
  "bacon_idx_100",
  "hr_idx_100",
  "xbh_idx_100",
  "carry_idx_100",
  "overall_pf_idx_100",
  "total_bbe",
  "overall_se",
  "bacon_se",
  "hr_se",
  "xbh_se",
  "carry_se",
  "bacon_rank",
  "hr_rank",
  "xbh_rank",
  "carry_rank"
)

out$team_id <- pretty_team(out$team_id)
out <- out[order(out$rank, out$team, out$park), ]
rownames(out) <- NULL

out_with_id <- out
out <- out[, c(
  "rank",
  "team_id",
  "team",
  "park",
  "years_used",
  "overall_resid",
  "bacon_resid",
  "hr_resid",
  "xbh_resid",
  "carry_ft",
  "bacon_idx_100",
  "hr_idx_100",
  "xbh_idx_100",
  "carry_idx_100",
  "overall_pf_idx_100",
  "total_bbe",
  "overall_se",
  "bacon_se",
  "hr_se",
  "xbh_se",
  "carry_se",
  "bacon_rank",
  "hr_rank",
  "xbh_rank",
  "carry_rank"
)]

utils::write.csv(
  out,
  file.path(output_dir, "park_factors_savant_style.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  out_with_id,
  file.path(output_dir, "park_factors_savant_style_with_id.csv"),
  row.names = FALSE,
  na = ""
)

message("Built Savant-style park factor table: ", file.path(output_dir, "park_factors_savant_style.csv"))
