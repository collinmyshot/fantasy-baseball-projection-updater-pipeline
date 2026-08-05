#!/usr/bin/env Rscript
# Figure for the article's strikeout section: park K% against park Overall.
#
# The point of the picture is that the two axes carry independent information.
# If K were just another way of saying "hitter-friendly", the cloud would lean.
# It does not (R^2 = 0.058), and the two labelled corners are the illustration:
# Coors Field is the most hitter-friendly park on contact and the most
# pitcher-friendly on strikeouts, and T-Mobile Park is the reverse.
#
# Output: data/processed/park_factors/pf_k_vs_overall.png (+ optional copy).

source(file.path("R", "utils.R"))

suppressPackageStartupMessages({
  library(ggplot2)
})

parsed <- parse_cli_args(list(
  pf_dir   = list(flag = "--pf-dir", default = file.path("data", "processed", "park_factors")),
  out_png  = list(flag = "--out",    default = file.path("data", "processed", "park_factors", "pf_k_vs_overall.png")),
  copy_to  = list(flag = "--copy-to", default = "")
))

d <- utils::read.csv(
  file.path(parsed$pf_dir, "park_factors_savant_style_clean_2026_with_id.csv"),
  stringsAsFactors = FALSE, check.names = FALSE
)

baseline_k <- 0.2215  # league K per PA in the modelled window; see k_model_run_metadata.csv
meta_path <- file.path(parsed$pf_dir, "k_model_run_metadata.csv")
if (file.exists(meta_path)) {
  m <- utils::read.csv(meta_path, stringsAsFactors = FALSE)
  v <- suppressWarnings(as.numeric(m$value[m$key == "baseline_k_rate"]))
  if (length(v) == 1 && is.finite(v)) baseline_k <- v
}

d$k_pct <- 100 * (baseline_k + d$k_resid)
r2 <- stats::cor(d$k_idx_100, d$overall_pf_idx_100)^2

# Label only the parks a reader needs to find: the two corners of the story
# plus the extremes of each axis.
label_teams <- unique(c(
  d$team_id[which.max(d$k_idx_100)], d$team_id[which.min(d$k_idx_100)],
  d$team_id[which.max(d$overall_pf_idx_100)], d$team_id[which.min(d$overall_pf_idx_100)],
  "HOU", "BOS"
))
d$lab <- ifelse(d$team_id %in% label_teams, d$park, NA_character_)

p <- ggplot(d, aes(x = overall_pf_idx_100, y = k_pct)) +
  geom_hline(yintercept = 100 * baseline_k, linetype = "dashed", color = "#b9c2bb", linewidth = 0.4) +
  geom_vline(xintercept = 100, linetype = "dashed", color = "#b9c2bb", linewidth = 0.4) +
  geom_point(size = 2.6, color = "#2c4a6e", alpha = 0.85) +
  ggrepel::geom_text_repel(
    aes(label = lab), na.rm = TRUE, size = 3.1, color = "#172733",
    seed = 42, min.segment.length = 0, segment.color = "#9aa8a0", box.padding = 0.5
  ) +
  scale_y_continuous(labels = function(x) sprintf("%.0f%%", x)) +
  labs(
    title = "A park's effect on strikeouts is not its effect on contact",
    # Plain "R-squared" rather than the superscript glyph: the default PNG
    # device font renders U+00B2 as a placeholder box.
    subtitle = sprintf(
      "Each point is one of the 30 current parks. R-squared between the two axes is %.3f. Dashed lines mark league average.",
      r2
    ),
    x = "iPF Overall (contact, 100 = average park)",
    y = "Strikeout rate for a league-average matchup"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 12),
    plot.subtitle = element_text(color = "#4a5a4f", size = 9.5),
    panel.grid.minor = element_blank(),
    axis.title = element_text(size = 10, color = "#3a4a40")
  )

if (!requireNamespace("ggrepel", quietly = TRUE)) {
  stop("Package 'ggrepel' is required for the label placement.")
}

ggsave(parsed$out_png, p, width = 8.2, height = 5.0, dpi = 150)
message("Wrote figure: ", parsed$out_png)
message(sprintf("  R^2 between K index and Overall index: %.4f", r2))
message(sprintf("  K range: %.1f%% (%s) to %.1f%% (%s)",
                min(d$k_pct), d$team_id[which.min(d$k_pct)],
                max(d$k_pct), d$team_id[which.max(d$k_pct)]))

if (nzchar(parsed$copy_to)) {
  file.copy(parsed$out_png, parsed$copy_to, overwrite = TRUE)
  message("Copied to: ", parsed$copy_to)
}
