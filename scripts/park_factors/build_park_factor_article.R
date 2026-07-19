#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(ggplot2)
})

out_dir <- file.path("data", "processed", "park_factors", "article")
fig_dir <- file.path(out_dir, "figs")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

root <- file.path("data", "processed", "park_factors")
overall <- read_csv(file.path(root, "park_factors_savant_style_clean_2026.csv"), show_col_types = FALSE)
h1 <- read_csv(file.path(root, "park_factors_savant_style_clean_2026_1H.csv"), show_col_types = FALSE)
h2 <- read_csv(file.path(root, "park_factors_savant_style_clean_2026_2H.csv"), show_col_types = FALSE)
known <- read_csv(file.path(root, "park_factor_known_effects_2026.csv"), show_col_types = FALSE)
val_sum <- read_csv(file.path(root, "validation_summary.csv"), show_col_types = FALSE)
val_det <- read_csv(file.path(root, "validation_detail.csv"), show_col_types = FALSE)
weights <- read_csv(file.path(root, "park_factors_savant_style_weights.csv"), show_col_types = FALSE)
invariance <- read_csv(file.path(root, "invariance_checks.csv"), show_col_types = FALSE)
meta <- read_csv(file.path(root, "run_metadata.csv"), show_col_types = FALSE)

weighted_mean <- function(x, w) {
  ok <- is.finite(x) & is.finite(w)
  if (!any(ok)) return(NA_real_)
  sum(x[ok] * w[ok]) / sum(w[ok])
}

rows_modeled <- as.integer(as.numeric(meta$value[meta$key == "rows_modeled"]))
seasons_modeled <- meta$value[meta$key == "seasons_modeled"]
exclude_2020 <- meta$value[meta$key == "exclude_seasons"]

mean_rmse_model <- weighted_mean(val_sum$rmse_model, val_sum$n_park_half)
mean_rmse_zero <- weighted_mean(val_sum$rmse_zero, val_sum$n_park_half)
mean_rmse_prev <- weighted_mean(val_sum$rmse_prev, val_sum$n_park_half)
mean_corr <- weighted_mean(val_sum$corr_model_vs_realized, val_sum$n_park_half)
mean_corr_prev <- weighted_mean(val_sum$corr_prev_vs_realized, val_sum$n_park_half)
mean_slope <- weighted_mean(val_sum$calibration_slope, val_sum$n_park_half)

def_corr <- invariance$value[invariance$metric == "corr_park_effect_vs_home_team_defense"]
off_corr <- invariance$value[invariance$metric == "corr_park_effect_vs_home_team_xwoba_con"]

# Figure 1: overall PF bar
plot_overall <- overall %>%
  mutate(ParkLabel = paste0(Team, " - ", Park)) %>%
  arrange(`Overall Park Factor`) %>%
  mutate(ParkLabel = factor(ParkLabel, levels = ParkLabel))

p1 <- ggplot(plot_overall, aes(x = ParkLabel, y = `Overall Park Factor`, fill = `Overall Park Factor` >= 100)) +
  geom_col() +
  coord_flip() +
  scale_fill_manual(values = c("TRUE" = "#d7301f", "FALSE" = "#2b8cbe"), guide = "none") +
  geom_hline(yintercept = 100, linetype = 2, color = "#444444") +
  labs(
    title = "Overall Park Factor Index (100 = average)",
    x = "",
    y = "Overall PF"
  ) +
  theme_minimal(base_size = 11)

ggsave(file.path(fig_dir, "overall_pf_bar.png"), p1, width = 10, height = 8, dpi = 150)

# Figure 2: validation RMSE
val_long <- bind_rows(
  val_sum %>% transmute(season, model = "Current model", rmse = rmse_model),
  val_sum %>% transmute(season, model = "Zero baseline", rmse = rmse_zero),
  val_sum %>% transmute(season, model = "Previous park mean", rmse = rmse_prev)
)

p2 <- ggplot(val_long, aes(x = season, y = rmse, color = model)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_color_manual(values = c("Current model" = "#1b9e77", "Zero baseline" = "#d95f02", "Previous park mean" = "#7570b3")) +
  labs(title = "Rolling Holdout RMSE by Season", x = "Holdout season", y = "RMSE", color = "") +
  theme_minimal(base_size = 11)

ggsave(file.path(fig_dir, "validation_rmse.png"), p2, width = 10, height = 6, dpi = 150)

# Figure 3: predicted vs realized
set.seed(42)
plot_dat <- val_det
if (nrow(plot_dat) > 3000) {
  plot_dat <- dplyr::sample_n(plot_dat, 3000)
}

p3 <- ggplot(plot_dat, aes(x = pred_effect, y = realized)) +
  geom_point(alpha = 0.35, size = 1.2, color = "#2c7fb8") +
  geom_smooth(method = "lm", se = FALSE, color = "#d95f02", linewidth = 1) +
  geom_abline(slope = 1, intercept = 0, linetype = 2, color = "#666666") +
  labs(title = "Holdout Fit: Predicted vs Realized Park-Half Residual", x = "Predicted", y = "Realized") +
  theme_minimal(base_size = 11)

ggsave(file.path(fig_dir, "pred_vs_realized.png"), p3, width = 9, height = 6, dpi = 150)

# Figure 4: HR-BACON divergence
hb <- known %>% filter(Analysis == "HR vs BACON Gap")
if (nrow(hb) > 0) {
  p4 <- ggplot(hb, aes(x = reorder(paste(Team, Park, sep = " - "), Difference), y = Difference, fill = Difference > 0)) +
    geom_col() +
    coord_flip() +
    scale_fill_manual(values = c("TRUE" = "#d7301f", "FALSE" = "#2b8cbe"), guide = "none") +
    geom_hline(yintercept = 0, linetype = 2, color = "#444444") +
    labs(title = "HR PF minus BACON PF", x = "", y = "Difference") +
    theme_minimal(base_size = 11)
  ggsave(file.path(fig_dir, "hr_minus_bacon.png"), p4, width = 10, height = 6.5, dpi = 150)
}

# Figure 5: 1H vs 2H movers
h_split <- h1 %>%
  select(Team, Park, `Overall Park Factor`) %>%
  rename(pf_1h = `Overall Park Factor`) %>%
  inner_join(
    h2 %>% select(Team, Park, `Overall Park Factor`) %>% rename(pf_2h = `Overall Park Factor`),
    by = c("Team", "Park")
  ) %>%
  mutate(diff = pf_2h - pf_1h) %>%
  arrange(desc(abs(diff)))

p5 <- ggplot(h_split %>% head(15), aes(x = reorder(paste(Team, Park, sep = " - "), diff), y = diff, fill = diff > 0)) +
  geom_col() +
  coord_flip() +
  scale_fill_manual(values = c("TRUE" = "#d7301f", "FALSE" = "#2b8cbe"), guide = "none") +
  geom_hline(yintercept = 0, linetype = 2, color = "#444444") +
  labs(title = "Largest 1H vs 2H PF Movers", subtitle = "Positive means more hitter-friendly in 2H", x = "", y = "2H - 1H") +
  theme_minimal(base_size = 11)

ggsave(file.path(fig_dir, "half_split_movers.png"), p5, width = 10, height = 6.5, dpi = 150)

# Tables
fmt_num <- function(x, d = 2) format(round(x, d), nsmall = d, trim = TRUE)

weights_tbl <- weights %>% mutate(weight = fmt_num(weight, 2))

overall_top <- overall %>%
  arrange(desc(`Overall Park Factor`)) %>%
  slice_head(n = 10) %>%
  mutate(`Total BBE` = as.integer(round(`Total BBE`, 0)))

overall_bottom <- overall %>%
  arrange(`Overall Park Factor`) %>%
  slice_head(n = 10) %>%
  mutate(`Total BBE` = as.integer(round(`Total BBE`, 0)))

render_table_md <- function(df) {
  # simple markdown table builder (no extra dependencies)
  cols <- names(df)
  hdr <- paste0("| ", paste(cols, collapse = " | "), " |")
  sep <- paste0("|", paste(rep("---", length(cols)), collapse = "|"), "|")
  rows <- apply(df, 1, function(r) paste0("| ", paste(r, collapse = " | "), " |"))
  paste(c(hdr, sep, rows), collapse = "\n")
}

md_lines <- c(
  "# Building Fantasy-Forward Park Factors from Statcast Batted-Ball Residuals",
  "",
  paste0("_Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M %Z"), "_"),
  "",
  "## Executive Summary",
  "",
  "This version is built for fantasy pitcher streaming decisions. It explicitly separates hit environment (BACON) and home-run environment, while controlling for player/team quality and team defense.",
  "",
  "- Data scope: seasons ", seasons_modeled,
  "- Excluded season: ", exclude_2020,
  paste0("- BBE rows modeled: ", format(rows_modeled, big.mark = ",")),
  paste0("- Weighted holdout RMSE: model=", sprintf("%.4f", mean_rmse_model), ", zero=", sprintf("%.4f", mean_rmse_zero), ", prior-park=", sprintf("%.4f", mean_rmse_prev)),
  paste0("- Weighted holdout correlation: model=", sprintf("%.3f", mean_corr), ", prior-park=", sprintf("%.3f", mean_corr_prev)),
  paste0("- Calibration slope (weighted mean): ", sprintf("%.3f", mean_slope)),
  paste0("- Invariance corr with home-team offense: ", sprintf("%.3f", off_corr)),
  paste0("- Invariance corr with home-team defense: ", sprintf("%.3f", def_corr)),
  "",
  "## Data and Construction",
  "",
  "We use Statcast BBE-level records and model event residuals as actual minus expected outcomes.",
  "",
  "Primary residual targets:",
  "",
  "- `resid = wOBAcon - xwOBAcon`",
  "- `bacon_resid = hit_on_contact - xBA_on_contact`",
  "- `hr_resid = HR_on_contact - xHR_on_contact`",
  "- `xbh_resid = XBH_on_contact - xXBH_on_contact`",
  "",
  "Hierarchical random effects absorb batter/pitcher/team talent and leave park-era and park-era-half effects as the signal of interest.",
  "",
  "Defense adjustment uses a season-level composite of OAA, DRS, and UZR (z-scored within season, then averaged; when OAA is unavailable, DRS/UZR dominate the composite for that row).",
  "",
  "## Chosen Fantasy Weights",
  "",
  render_table_md(weights_tbl),
  "",
  "Overall index currently uses your selected blend: BACON 0.45, HR 0.35, XBH 0.20.",
  "",
  "## Validation",
  "",
  "![Rolling Holdout RMSE](figs/validation_rmse.png)",
  "",
  "![Predicted vs Realized](figs/pred_vs_realized.png)",
  "",
  "## 2026 Park Landscape",
  "",
  "![Overall PF Index](figs/overall_pf_bar.png)",
  "",
  "### Top 10 Hitter-Friendly Parks",
  "",
  render_table_md(overall_top),
  "",
  "### Top 10 Pitcher-Friendly Parks",
  "",
  render_table_md(overall_bottom),
  "",
  "## Known Park Effects",
  "",
  "![HR minus BACON divergence](figs/hr_minus_bacon.png)",
  "",
  "![Largest 1H vs 2H movers](figs/half_split_movers.png)",
  "",
  "## Comparison vs Public Park Factor Frameworks",
  "",
  "- Statcast park factors are highly useful and event-informed, but this build explicitly applies mixed-effects controls for roster quality and a defense composite in the estimation stage.",
  "- FanGraphs park factors are robust at aggregate run/stat scales; this build is optimized for fantasy streaming use-cases with BBE-level residual decomposition.",
  "- Direct apples-to-apples external RMSE race is limited by differences in published targets/units (public park factors are not exposed as park-half residual forecasts with the same holdout framing).",
  "",
  "## Assumptions and Limits",
  "",
  "- 2020 excluded entirely.",
  "- Park-era segmentation is only as good as the park-event map.",
  "- Weather/drag terms are noisy; 1H/2H split is intentionally coarse.",
  "- New park eras have wider uncertainty even with shrinkage.",
  "",
  "## Source Links",
  "",
  "- Statcast search CSV: https://baseballsavant.mlb.com/statcast_search/csv",
  "- Savant OAA leaderboard: https://baseballsavant.mlb.com/leaderboard/outs_above_average",
  "- Savant park factors page: https://baseballsavant.mlb.com/leaderboard/statcast-park-factors",
  "- FanGraphs fielding leaders (DRS/UZR): https://www.fangraphs.com/leaders/major-league?stats=fld",
  "- MLB team pages (venue verification): https://www.mlb.com/team",
  "- Local source logs: data/manual/team_defense_2015_2025_sources.csv and data/manual/mlb_home_parks_2026_verified_sources.csv",
  ""
)

md_path <- file.path(out_dir, "park_factor_article_2026.md")
writeLines(md_lines, md_path, useBytes = TRUE)

# HTML (no pandoc required via markdown package)
html_path <- file.path(out_dir, "park_factor_article_2026.html")
if (requireNamespace("markdown", quietly = TRUE)) {
  markdown::markdownToHTML(file = md_path, output = html_path, stylesheet = NULL, fragment.only = FALSE)
}

message("Article markdown: ", md_path)
message("Article html: ", html_path)
message("Figures dir: ", fig_dir)

