#!/usr/bin/env Rscript
# Figure: roster quality leaks into public single-year park factors.
#
# Panel A: FanGraphs single-year park factor (pf_1yr) for each team-season vs
#          that team's ROAD runs per game (a park-free roster-quality proxy).
#          Positive slope = offense quality inflating the home park estimate.
# Panel B: this model's park effect (wOBA-scale era delta mapped to the same
#          team-seasons) vs the same road runs per game. Flat = the isolation
#          machinery is doing its job.
#
# Inputs:
#   data/raw/fg_park_factors_by_year.csv        (season, team nickname, pf_1yr)
#   data/raw/team_road_runs_2015_2025.csv        (MLB Stats API, road R/G)
#   data/processed/park_factors/team_park_transitions.csv (primary era per team-season)
#   data/processed/park_factors/park_factors_overall.csv  (era-level wOBA deltas)
# Output:
#   data/processed/park_factors/pf_roster_quality.png (+ optional copy path arg)

suppressPackageStartupMessages(library(ggplot2))
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  pf_dir   = list(flag = "--pf-dir",   default = file.path("data", "processed", "park_factors")),
  out_png  = list(flag = "--out",      default = file.path("data", "processed", "park_factors", "pf_roster_quality.png")),
  copy_to  = list(flag = "--copy-to",  default = "")
))

fg <- utils::read.csv(file.path("data", "raw", "fg_park_factors_by_year.csv"), stringsAsFactors = FALSE)
road <- utils::read.csv(file.path("data", "raw", "team_road_runs_2015_2025.csv"), stringsAsFactors = FALSE)
transitions <- utils::read.csv(file.path(parsed$pf_dir, "team_park_transitions.csv"), stringsAsFactors = FALSE)
overall <- utils::read.csv(file.path(parsed$pf_dir, "park_factors_overall.csv"), stringsAsFactors = FALSE)

# MLB Stats API full names -> FG nicknames by suffix match (handles two-word
# nicknames like Red Sox / Blue Jays / White Sox).
nicknames <- sort(unique(fg$team))
match_nickname <- function(full_name) {
  hits <- nicknames[vapply(nicknames, function(nk) endsWith(full_name, nk), logical(1))]
  if (length(hits) == 1) return(hits)
  if (length(hits) > 1) return(hits[which.max(nchar(hits))])
  NA_character_
}
road$team <- vapply(road$team_name, match_nickname, character(1))
if (any(is.na(road$team))) {
  stop(sprintf("Unmapped team names: %s", paste(unique(road$team_name[is.na(road$team)]), collapse = ", ")))
}

# Abbreviation -> nickname for this model's park effects.
abbr_to_nick <- c(
  AZ = "Diamondbacks", ATL = "Braves", BAL = "Orioles", BOS = "Red Sox",
  CHC = "Cubs", CHW = "White Sox", CIN = "Reds", CLE = "Guardians",
  COL = "Rockies", DET = "Tigers", HOU = "Astros", KCR = "Royals",
  LAA = "Angels", LAD = "Dodgers", MIA = "Marlins", MIL = "Brewers",
  MIN = "Twins", NYM = "Mets", NYY = "Yankees", ATH = "Athletics",
  PHI = "Phillies", PIT = "Pirates", SDP = "Padres", SEA = "Mariners",
  SFG = "Giants", STL = "Cardinals", TBR = "Rays", TEX = "Rangers",
  TOR = "Blue Jays", WSH = "Nationals"
)
# FG historic nickname quirks (Indians pre-2022 in older FG files).
fg$team[fg$team == "Indians"] <- "Guardians"

panel_a <- merge(
  fg[, c("season", "team", "pf_1yr")],
  road[, c("season", "team", "road_rpg")],
  by = c("season", "team")
)
panel_a <- panel_a[is.finite(panel_a$pf_1yr) & is.finite(panel_a$road_rpg), ]

transitions$team <- abbr_to_nick[transitions$home_team]
mine <- merge(
  transitions[, c("season", "team", "park_era_id")],
  overall[, c("park_era_id", "delta_woba_over_xwoba_overall")],
  by = "park_era_id"
)
panel_b <- merge(mine, road[, c("season", "team", "road_rpg")], by = c("season", "team"))
panel_b <- panel_b[is.finite(panel_b$delta_woba_over_xwoba_overall) & is.finite(panel_b$road_rpg), ]

r2 <- function(x, y) round(stats::cor(x, y)^2, 3)
r2_a <- r2(panel_a$road_rpg, panel_a$pf_1yr)
r2_b <- r2(panel_b$road_rpg, panel_b$delta_woba_over_xwoba_overall)

navy <- "#1f3556"
orange <- "#b77343"

theme_pf <- theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 12),
    plot.subtitle = element_text(color = "#4a5a4f", size = 10),
    panel.grid.minor = element_blank()
  )

p_a <- ggplot(panel_a, aes(x = road_rpg, y = pf_1yr)) +
  geom_point(color = navy, alpha = 0.55, size = 1.8) +
  geom_smooth(method = "lm", se = FALSE, color = orange, linewidth = 1) +
  labs(
    title = "FanGraphs single-year park factor",
    subtitle = sprintf("Each point is a team-season, 2015-2025. R^2 = %.3f", r2_a),
    x = "Road runs per game (roster quality, park-free)",
    y = "FG single-year park factor"
  ) +
  theme_pf

p_b <- ggplot(panel_b, aes(x = road_rpg, y = delta_woba_over_xwoba_overall)) +
  geom_point(color = navy, alpha = 0.55, size = 1.8) +
  geom_smooth(method = "lm", se = FALSE, color = orange, linewidth = 1) +
  labs(
    title = "iPF park effect",
    subtitle = sprintf("Same team-seasons. R^2 = %.3f", r2_b),
    x = "Road runs per game (roster quality, park-free)",
    y = "Park effect (wOBA on contact, delta)"
  ) +
  theme_pf

if (!requireNamespace("patchwork", quietly = TRUE)) {
  # Fall back to a simple side-by-side grid without extra dependencies.
  png(parsed$out_png, width = 1400, height = 620, res = 130)
  gridExtra_ok <- requireNamespace("gridExtra", quietly = TRUE)
  if (gridExtra_ok) {
    gridExtra::grid.arrange(p_a, p_b, ncol = 2)
  } else {
    graphics::par(mfrow = c(1, 2))
    print(p_a)
    print(p_b)
  }
  dev.off()
} else {
  combined <- patchwork::wrap_plots(p_a, p_b, ncol = 2)
  ggsave(parsed$out_png, combined, width = 11, height = 4.8, dpi = 130)
}

message(sprintf("Panel A (FG 1yr vs road R/G): n=%s, R^2=%.3f", nrow(panel_a), r2_a))
message(sprintf("Panel B (this model vs road R/G): n=%s, R^2=%.3f", nrow(panel_b), r2_b))
message("Wrote figure: ", parsed$out_png)

if (nzchar(parsed$copy_to)) {
  file.copy(parsed$out_png, parsed$copy_to, overwrite = TRUE)
  message("Copied to: ", parsed$copy_to)
}
