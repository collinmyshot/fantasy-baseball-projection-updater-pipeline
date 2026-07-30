#!/usr/bin/env Rscript
# plot_miss_scatters.R
# Scatter+trend plots for the swing-timing MISS metrics, pooled 2024-26
# (one point per player-season, PA >= 100). fbb-tools research-article style.
#
# SCOPE: BAT-SPEED / SWING-MAP article figures, not the streamonators. This is
# a FIGURE-PRODUCING script (the only one in this folder) — it emits plots for
# the article, it does not test a hypothesis or return a verdict. Siblings:
# yoy_stability.R and wide_corr_stability.R do the analysis behind it.
#   1. Early%  vs Pull%
#   2. Under%  vs Fly Ball%
#   3. Miss Distance vs K%
#   4. Miss Distance vs Whiff%
#   5. Miss Distance (NON-FASTBALL whiffs) vs Whiff%   [breaking + offspeed combined]
#
# NOTE on plot 5: the whiff-cut pitch-type files carry whiff_rate == 1 by
# construction, so a *non-fastball* whiff RATE is not available from them. The
# y-axis here is the player's OVERALL whiff% (same as plot 4); x is non-fastball
# miss distance. A true non-FB whiff% would require the matching `_all` pitch-type
# files (total swings by pitch type).

suppressPackageStartupMessages({library(readr); library(dplyr); library(ggplot2); library(scales)})

DL       <- "/Users/ckaufman/Downloads"
MAP      <- "/Users/ckaufman/Documents/New project/data/processed/swing_map/hitter_swing_map.csv"
PLOT_DIR <- "/Users/ckaufman/Documents/New project/data/processed/swing_map/plots"
dir.create(PLOT_DIR, recursive = TRUE, showWarnings = FALSE)
MIN_NONFB_WHIFFS <- 20   # sample floor for non-FB miss distance (offspeed counts run small)

tbl <- read_csv(MAP, show_col_types = FALSE)   # already PA >= 100

# ── Build non-fastball miss distance (x) and true non-FB whiff% (y) ──────────
# x: miss distance on non-FB WHIFFS (whiff-cut files), weighted by whiff counts
# y: whiff% on non-FB swings (all-cut files): whiffs = whiff_rate * n_swings,
#    so non-FB whiff% = (brk_whiffs + off_whiffs) / (brk_swings + off_swings)
rd_whiff <- function(pt) {
  d <- suppressWarnings(read_csv(file.path(DL, sprintf("bat-tracking-swing-timing_2024-26_whiff_%s.csv", pt)),
                                 show_col_types = FALSE, name_repair = "minimal"))
  names(d)[1] <- "id"; d |> select(id, year, md = miss_distance, nw = n_swings)
}
rd_all <- function(pt) {
  d <- suppressWarnings(read_csv(file.path(DL, sprintf("bat-tracking-swing-timing_2024-26_all_%s.csv", pt)),
                                 show_col_types = FALSE, name_repair = "minimal"))
  names(d)[1] <- "id"; d |> select(id, year, wr = whiff_rate, ns = n_swings)
}
mdw <- full_join(rd_whiff("breaking"), rd_whiff("offspeed"), by = c("id","year"), suffix = c("_brk","_off"))
alc <- full_join(rd_all("breaking"),   rd_all("offspeed"),   by = c("id","year"), suffix = c("_brk","_off"))
nonfb <- full_join(mdw, alc, by = c("id","year")) |>
  mutate(across(c(md_brk, nw_brk, md_off, nw_off, wr_brk, ns_brk, wr_off, ns_off),
                \(x) ifelse(is.na(x), 0, x)),
         nonfb_whiffs        = nw_brk + nw_off,
         miss_distance_nonfb = ifelse(nonfb_whiffs > 0,
                                      (md_brk * nw_brk + md_off * nw_off) / nonfb_whiffs, NA_real_),
         nonfb_swings        = ns_brk + ns_off,
         nonfb_whiff_pct     = ifelse(nonfb_swings > 0,
                                      (wr_brk * ns_brk + wr_off * ns_off) / nonfb_swings, NA_real_)) |>
  select(id, year, miss_distance_nonfb, nonfb_whiffs, nonfb_whiff_pct, nonfb_swings)
tbl <- tbl |> left_join(nonfb, by = c("id", "year"))

# ── fbb-tools research chart theme ────────────────────────────────────────────
fbb_chart_theme <- function() {
  theme_minimal(base_size = 15) +
    theme(
      plot.background    = element_rect(fill = "white", colour = NA),
      panel.background   = element_rect(fill = "white", colour = NA),
      panel.grid.major.x = element_line(colour = "#dddddd", linewidth = 0.3),
      panel.grid.minor.x = element_line(colour = "#eeeeee", linewidth = 0.2),
      panel.grid.major.y = element_blank(),
      panel.grid.minor.y = element_blank(),
      axis.title    = element_text(size = 13, face = "bold"),
      axis.text.x   = element_text(size = 11),
      axis.text.y   = element_text(size = 12),
      plot.title    = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 10.5, colour = "#4a5a4f"),
      plot.margin   = margin(12, 18, 12, 12)
    )
}
PT_COL   <- "#2f7d3a"
LINE_COL <- "#b77343"
SUB_DEF  <- "MLB hitters, 2024-2026 pooled  |  one point per player-season  |  PA >= 100"

make_scatter <- function(d, x_col, y_col, x_lab, y_lab, title,
                         x_pct = TRUE, y_pct = TRUE, subtitle = SUB_DEF) {
  d <- d |> filter(!is.na(.data[[x_col]]), !is.na(.data[[y_col]]))
  r  <- cor(d[[x_col]], d[[y_col]])
  xr <- range(d[[x_col]]); yr <- range(d[[y_col]])
  p <- ggplot(d, aes(x = .data[[x_col]], y = .data[[y_col]])) +
    geom_point(alpha = 0.4, size = 2, colour = PT_COL) +
    geom_smooth(method = "lm", se = FALSE, colour = LINE_COL, linewidth = 1) +
    annotate("text", x = xr[1] + 0.03 * diff(xr), y = yr[2] - 0.02 * diff(yr),
             label = sprintf("atop(bold(R^2 == %.3f), bold(n == %d))", r^2, nrow(d)),
             parse = TRUE, hjust = 0, vjust = 1, size = 5, colour = "#333333") +
    labs(x = x_lab, y = y_lab, title = title, subtitle = subtitle) +
    fbb_chart_theme()
  if (x_pct) p <- p + scale_x_continuous(labels = label_percent(accuracy = 1))
  if (y_pct) p <- p + scale_y_continuous(labels = label_percent(accuracy = 1))
  p
}

specs <- list(
  list(f="scatter_early_vs_pull.png",    x="early_percent_all", y="pull_pct",
       xl="Early% (all competitive swings)", yl="Pull%", t="Early% vs Pull%", xp=TRUE,  yp=TRUE, d=tbl),
  list(f="scatter_under_vs_fb.png",       x="under_percent_all", y="fb_pct",
       xl="Under% (all competitive swings)", yl="Fly Ball%", t="Under% vs Fly Ball%", xp=TRUE, yp=TRUE, d=tbl),
  list(f="scatter_missdist_vs_k.png",     x="miss_distance_all", y="k_pct",
       xl="Avg Miss Distance (inches)", yl="K%", t="Miss Distance vs K%", xp=FALSE, yp=TRUE, d=tbl),
  list(f="scatter_missdist_vs_whiff.png", x="miss_distance_all", y="whiff_rate",
       xl="Avg Miss Distance (inches)", yl="Whiff%", t="Miss Distance vs Whiff%", xp=FALSE, yp=TRUE, d=tbl),
  list(f="scatter_missdist_nonfb_vs_whiff.png", x="miss_distance_nonfb", y="nonfb_whiff_pct",
       xl="Avg Miss Distance, non-FB whiffs (inches)", yl="Whiff% (non-fastball)",
       t="Miss Distance vs Whiff% - Non-Fastballs", xp=FALSE, yp=TRUE,
       d=filter(tbl, nonfb_whiffs >= MIN_NONFB_WHIFFS),
       sub="Non-fastball = breaking + offspeed  |  PA >= 100  |  >= 20 non-FB whiffs")
)

plots <- list()
for (s in specs) {
  sub <- if (!is.null(s$sub)) s$sub else SUB_DEF
  p <- make_scatter(s$d, s$x, s$y, s$xl, s$yl, s$t, s$xp, s$yp, subtitle = sub)
  plots[[s$t]] <- p
  ggsave(file.path(PLOT_DIR, s$f), p, width = 6.5, height = 5, dpi = 150, bg = "white")
  dd <- s$d |> filter(!is.na(.data[[s$x]]), !is.na(.data[[s$y]]))
  cat(sprintf("  wrote %-40s R² = %.3f  (n = %d)\n", s$f,
              cor(dd[[s$x]], dd[[s$y]])^2, nrow(dd)))
}

if (requireNamespace("patchwork", quietly = TRUE)) {
  pw <- patchwork::wrap_plots(plots, ncol = 2)
  ggsave(file.path(PLOT_DIR, "scatter_miss_panel.png"), pw, width = 13, height = 15, dpi = 150, bg = "white")
  cat("  wrote scatter_miss_panel.png (5-panel)\n")
}
cat(sprintf("\nAll plots in: %s\n", PLOT_DIR))
