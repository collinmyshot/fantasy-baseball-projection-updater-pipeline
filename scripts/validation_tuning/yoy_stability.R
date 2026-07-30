#!/usr/bin/env Rscript
# yoy_stability.R  — Year-over-year repeatability of every metric (2024->25->26)
# "Which of these are skills (repeat) vs noise (wash out)?"
#
# Method: same-player self-correlation across consecutive seasons, pooled over the
# two transitions, PA-weighted, requiring both seasons to clear a competitive-swing
# floor (reduces measurement-noise attenuation on the per-swing geometry metrics).
# NOTE: YoY r conflates sampling noise + real talent change -> LOWER BOUND on
# reliability. 2024 (tracking started ~mid-May) and 2026 (partial) are light.
#
# SCOPE: this is the BAT-SPEED / SWING-MAP article line of work, not the
# streamonators. Reads data/processed/swing_map/hitter_swing_map.csv. Sibling
# scripts on the same dataset: wide_corr_stability.R (feature x outcome matrix
# plus the stable-AND-predictive synthesis) and plot_miss_scatters.R (the
# article's scatter figures).
#
# ⚠ NO VERDICT RECORDED. I have no stored result for this run, so nothing is
#   asserted here about which metrics proved to be skills. Re-run it for the
#   answer rather than assuming one.
#
# Usage: Rscript scripts/validation_tuning/yoy_stability.R

suppressPackageStartupMessages({library(readr); library(dplyr); library(ggplot2)})

OUT          <- "/Users/ckaufman/Documents/New project/data/processed/swing_map"
FLOOR_SWINGS <- 300   # require >= this many competitive swings in BOTH seasons
tbl <- read_csv(file.path(OUT, "hitter_swing_map.csv"), show_col_types = FALSE)

# metric, family, label  (family = grouping; "Outcome" = result stat, NOT a defensive out)
M <- tribble(
  ~metric,                      ~family,             ~label,
  "avg_bat_speed_all",          "Mechanics",         "Bat speed",
  "swing_length_all",           "Mechanics",         "Swing length",
  "hard_swing_rate_all",        "Mechanics",         "Hard-swing rate",
  "squared_up_per_swing_all",   "Mechanics",         "Squared-up/swing",
  "blast_per_swing_all",        "Mechanics",         "Blast/swing",
  "miss_distance_all",          "Miss-geometry",     "Miss distance",
  "perfect_percent_all",        "Miss-geometry",     "Perfect%",
  "flawed_percent_all",         "Miss-geometry",     "Flawed%",
  "early_percent_all",          "Miss-geometry",     "Early%",
  "on_time_percent_all",        "Miss-geometry",     "On-time%",
  "late_percent_all",           "Miss-geometry",     "Late%",
  "tied_up_percent_all",        "Miss-geometry",     "Tied-up%",
  "centered_percent_all",       "Miss-geometry",     "Centered%",
  "flailed_percent_all",        "Miss-geometry",     "Flailed%",
  "over_percent_all",           "Miss-geometry",     "Over%",
  "lined_up_percent_all",       "Miss-geometry",     "Lined-up%",
  "under_percent_all",          "Miss-geometry",     "Under%",
  "ev90",                       "Power/quality",     "EV90",
  "max_ev",                     "Power/quality",     "Max EV",
  "hardhit_pct",                "Power/quality",     "HardHit%",
  "barrel_pct",                 "Power/quality",     "Barrel%",
  "la",                         "Power/quality",     "Launch angle",
  "iso",                        "Power/quality",     "ISO",
  "xwoba",                      "Power/quality",     "xwOBA",
  "k_pct",                      "Plate discipline",  "K%",
  "bb_pct",                     "Plate discipline",  "BB%",
  "whiff_rate",                 "Plate discipline",  "Whiff%",
  "pull_pct",                   "Batted-ball",       "Pull%",
  "oppo_pct",                   "Batted-ball",       "Oppo%",
  "fb_pct",                     "Batted-ball",       "FB%",
  "gb_pct",                     "Batted-ball",       "GB%",
  "pull_air_rate",              "Batted-ball",       "Pull-air%",
  "babip",                      "Batted-ball",       "BABIP",
  "avg",                        "Batted-ball",       "AVG"
)

pairs <- function(y1, y2) {
  cols <- c("pa", "n_swings_all", M$metric)
  a <- tbl |> filter(year == y1) |> select(id, all_of(cols))
  b <- tbl |> filter(year == y2) |> select(id, all_of(cols))
  inner_join(a, b, by = "id", suffix = c("_y1", "_y2")) |>
    filter(n_swings_all_y1 >= FLOOR_SWINGS, n_swings_all_y2 >= FLOOR_SWINGS)
}
p24 <- pairs(2024, 2025); p25 <- pairs(2025, 2026)

wcor <- function(x, y, w) {
  k <- is.finite(x) & is.finite(y) & is.finite(w); x<-x[k]; y<-y[k]; w<-w[k]/sum(w[k])
  mx<-sum(w*x); my<-sum(w*y); sum(w*(x-mx)*(y-my)) / sqrt(sum(w*(x-mx)^2) * sum(w*(y-my)^2))
}
hm <- function(a, b) 2*a*b/(a+b)

res <- M |> rowwise() |> mutate(
  r_24_25 = cor(p24[[paste0(metric,"_y1")]], p24[[paste0(metric,"_y2")]], use="complete.obs"),
  r_25_26 = cor(p25[[paste0(metric,"_y1")]], p25[[paste0(metric,"_y2")]], use="complete.obs"),
  r_pooled = {
    x <- c(p24[[paste0(metric,"_y1")]], p25[[paste0(metric,"_y1")]])
    y <- c(p24[[paste0(metric,"_y2")]], p25[[paste0(metric,"_y2")]])
    w <- c(hm(p24$pa_y1, p24$pa_y2), hm(p25$pa_y1, p25$pa_y2)); wcor(x, y, w)
  }
) |> ungroup() |> arrange(desc(r_pooled))

cat(sprintf("Swing floor: >=%d both seasons | pairs: 24->25 n=%d, 25->26 n=%d\n\n",
            FLOOR_SWINGS, nrow(p24), nrow(p25)))
cat(sprintf("%-18s %-17s %7s %7s %8s\n","metric","family","24-25","25-26","POOLED"))
cat(strrep("-", 61), "\n")
for (i in seq_len(nrow(res))) with(res[i,],
  cat(sprintf("%-18s %-17s %7.2f %7.2f %8.2f\n", label, family, r_24_25, r_25_26, r_pooled)))
write_csv(res, file.path(OUT, "yoy_stability.csv"))

fam_col <- c("Mechanics"="#2f7d3a","Miss-geometry"="#b77343","Batted-ball"="#9a8f3a",
             "Plate discipline"="#7a5fa0","Power/quality"="#3a6ea5")
res2 <- res |> mutate(label = factor(label, levels = rev(label)),
                      family = factor(family, levels = names(fam_col)))
p <- ggplot(res2, aes(r_pooled, label, fill = family)) +
  geom_col(width = 0.72) +
  geom_text(aes(label = sprintf("%.2f", r_pooled)), hjust = -0.15, size = 3) +
  scale_fill_manual(values = fam_col) +
  scale_x_continuous(limits = c(0, 1), expand = expansion(c(0, 0.08))) +
  labs(title = "Year-over-year repeatability (2024-26)",
       subtitle = sprintf("Same-player self-correlation, PA-weighted, both seasons >=%d swings · higher = more skill",
                          FLOOR_SWINGS),
       x = "Pooled YoY correlation", y = NULL, fill = NULL) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face="bold"), panel.grid.major.y = element_blank(),
        plot.subtitle = element_text(colour="#4a5a4f", size=9.5), legend.position = "top")
ggsave(file.path(OUT, "plots/yoy_stability.png"), p, width = 9, height = 10, dpi = 150, bg = "white")
cat("\nWrote yoy_stability.csv + plots/yoy_stability.png\n")
