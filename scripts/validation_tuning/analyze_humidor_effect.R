#!/usr/bin/env Rscript
# Humidor era test: did the 2022 universal humidor mandate shift park behavior
# enough to justify park-era splits at 2022?
#
# Design (difference-in-differences): compare each park's season-centered
# residual behavior in 2021 (pre) vs 2022-2023 (post), contrasting the ~20
# parks that first received humidors in 2022 ("treated") against the clubs
# that already used them in 2021 ("control"). Humidor adoption list is from
# Baseball Savant's drag dashboard page (fetched 2026-07-01): in 2021 ten
# clubs used humidors — Astros, Blue Jays (Rogers Centre only), Cardinals,
# Diamondbacks, Mariners, Marlins, Mets, Rangers, Red Sox, Rockies; in 2022
# all 30 clubs did.
#
# TOR is excluded from both groups: their 2021 home games were played in
# Dunedin and Buffalo, so Rogers Centre has no 2021 baseline.
#
# Outcomes (all season-centered so league-wide ball changes cancel):
#   resid_c    wOBAcon - xwOBAcon
#   hr_rate_c  HR per batted-ball event
#
# Output: data/processed/park_factors/humidor_did_analysis.csv + console
# summary. Research artifact — no model changes are made here.

source(file.path("R", "utils.R"))
source(file.path("R", "park_factors.R"))

parsed <- parse_cli_args(list(
  bbe_input  = list(flag = "--bbe-input",  default = file.path("data", "raw", "statcast_bbe_store.csv")),
  output_dir = list(flag = "--output-dir", default = file.path("data", "processed", "park_factors")),
  pre_season  = list(flag = "--pre-season",  default = 2021, type = "numeric"),
  post_first  = list(flag = "--post-first",  default = 2022, type = "numeric"),
  post_last   = list(flag = "--post-last",   default = 2023, type = "numeric"),
  exclude_teams = list(flag = "--exclude-teams", default = "")
))

bbe_input  <- parsed$bbe_input
output_dir <- parsed$output_dir
pre_season <- as.integer(parsed$pre_season)
post_first <- as.integer(parsed$post_first)
post_last  <- as.integer(parsed$post_last)

# 2021 humidor clubs per Savant drag dashboard; TOR handled separately (no
# Rogers Centre games in 2021).
humidor_2021_clubs <- c("HOU", "STL", "AZ", "SEA", "MIA", "NYM", "TEX", "BOS", "COL")

message("Reading BBE store: ", bbe_input)
raw <- utils::read.csv(bbe_input, stringsAsFactors = FALSE, check.names = FALSE)
bbe <- standardize_bbe_columns(raw)
bbe <- bbe[!is.na(bbe$game_date), ]
bbe$season <- as.integer(format(bbe$game_date, "%Y"))
bbe <- bbe[bbe$season %in% c(pre_season, post_first:post_last), ]
bbe <- bbe[is.na(bbe$game_type) | bbe$game_type == "" | toupper(bbe$game_type) == "R", ]
bbe <- bbe[!is.na(bbe$launch_speed) & !is.na(bbe$launch_angle), ]
bbe <- bbe[is.finite(bbe$woba_con) & is.finite(bbe$xwoba_con), ]
bbe$resid <- bbe$woba_con - bbe$xwoba_con
bbe$is_hr <- as.integer(tolower(trimws(bbe$events)) == "home_run")

bbe <- bbe[bbe$home_team != "TOR", ]

# Parks with documented era events (dimension changes) between the pre and
# post windows confound the humidor contrast — exclude on request.
exclude_teams <- toupper(trimws(strsplit(as.character(parsed$exclude_teams), ",")[[1]]))
exclude_teams <- exclude_teams[nzchar(exclude_teams)]
if (length(exclude_teams) > 0) {
  message("Excluding teams with confounding era events: ", paste(exclude_teams, collapse = ", "))
  bbe <- bbe[!bbe$home_team %in% exclude_teams, ]
}

# Season-center outcomes so league-wide ball/environment shifts cancel and only
# park-relative movement remains.
bbe$resid_c <- bbe$resid - ave(bbe$resid, bbe$season, FUN = mean)
bbe$hr_c <- bbe$is_hr - ave(bbe$is_hr, bbe$season, FUN = mean)

bbe$window <- ifelse(bbe$season == pre_season, "pre", "post")

agg <- stats::aggregate(
  cbind(resid_c, hr_c) ~ home_team + window,
  data = bbe,
  FUN = mean
)
n_agg <- stats::aggregate(
  rep(1, nrow(bbe)),
  by = list(home_team = bbe$home_team, window = bbe$window),
  FUN = sum
)
names(n_agg)[3] <- "n_bbe"
agg <- merge(agg, n_agg, by = c("home_team", "window"))

wide <- merge(
  agg[agg$window == "pre", c("home_team", "resid_c", "hr_c", "n_bbe")],
  agg[agg$window == "post", c("home_team", "resid_c", "hr_c", "n_bbe")],
  by = "home_team",
  suffixes = c("_pre", "_post")
)

wide$group <- ifelse(wide$home_team %in% humidor_2021_clubs, "control_had_humidor_2021", "treated_new_humidor_2022")
wide$delta_resid <- wide$resid_c_post - wide$resid_c_pre
wide$delta_hr <- wide$hr_c_post - wide$hr_c_pre

# Per-park sampling noise on the delta: sd of centered residual is ~homogeneous
# across parks, so use pooled sd / sqrt(n) per window, combined in quadrature.
sd_resid <- stats::sd(bbe$resid_c)
sd_hr <- stats::sd(bbe$hr_c)
wide$se_delta_resid <- sd_resid * sqrt(1 / wide$n_bbe_pre + 1 / wide$n_bbe_post)
wide$se_delta_hr <- sd_hr * sqrt(1 / wide$n_bbe_pre + 1 / wide$n_bbe_post)
wide$z_delta_resid <- wide$delta_resid / wide$se_delta_resid
wide$z_delta_hr <- wide$delta_hr / wide$se_delta_hr

wide <- wide[order(wide$group, -abs(wide$z_delta_hr)), ]

grp <- function(g, col) wide[[col]][wide$group == g]
did_resid <- mean(grp("treated_new_humidor_2022", "delta_resid")) - mean(grp("control_had_humidor_2021", "delta_resid"))
did_hr <- mean(grp("treated_new_humidor_2022", "delta_hr")) - mean(grp("control_had_humidor_2021", "delta_hr"))

se_group <- function(g, col) {
  v <- grp(g, col)
  stats::sd(v) / sqrt(length(v))
}
did_resid_se <- sqrt(se_group("treated_new_humidor_2022", "delta_resid")^2 + se_group("control_had_humidor_2021", "delta_resid")^2)
did_hr_se <- sqrt(se_group("treated_new_humidor_2022", "delta_hr")^2 + se_group("control_had_humidor_2021", "delta_hr")^2)

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}
utils::write.csv(wide, file.path(output_dir, "humidor_did_analysis.csv"), row.names = FALSE, na = "")

message(sprintf("Pre window: %s | Post window: %s-%s", pre_season, post_first, post_last))
message(sprintf("Treated (new humidor 2022): %s parks | Control (had humidor 2021): %s parks",
                sum(wide$group == "treated_new_humidor_2022"), sum(wide$group == "control_had_humidor_2021")))
message(sprintf("DiD wOBAcon resid (treated - control): %+.5f (SE %.5f, z = %.2f)", did_resid, did_resid_se, did_resid / did_resid_se))
message(sprintf("DiD HR per BBE     (treated - control): %+.5f (SE %.5f, z = %.2f)", did_hr, did_hr_se, did_hr / did_hr_se))
message("Per-park deltas written to: ", file.path(output_dir, "humidor_did_analysis.csv"))
