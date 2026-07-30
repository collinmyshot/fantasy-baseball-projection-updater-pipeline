#!/usr/bin/env Rscript
# wide_corr_stability.R
# (1) Wide correlation matrix: every geometry/mechanics feature (all/bip/whiff +
#     pitch-type splits) x every outcome.
# (2) Synthesis: for each feature, YoY stability vs. peak correlation with outcomes
#     -> find the metrics that are BOTH stable and predictive.
#
# SCOPE: BAT-SPEED / SWING-MAP article work, not the streamonators. Siblings on
# the same dataset: yoy_stability.R (the stability axis this script consumes)
# and plot_miss_scatters.R (article figures).
#
# ⚠ NO VERDICT RECORDED — nothing asserted here about which features won the
#   stable-and-predictive quadrant. Re-run for the answer.
#
# DEPENDENCY: reads the FLOORED hitter_swing_map.csv from data/processed/swing_map,
# and writes back there. Note DL points at ~/Downloads, so any manual CSV this
# needs must be sitting there. See the README in this folder for the list of
# scripts with ~/Downloads dependencies.
#
# Usage: Rscript scripts/validation_tuning/wide_corr_stability.R

suppressPackageStartupMessages({library(readr); library(dplyr); library(tidyr); library(ggplot2)})

DL  <- "/Users/ckaufman/Downloads"
OUT <- "/Users/ckaufman/Documents/New project/data/processed/swing_map"
base <- read_csv(file.path(OUT, "hitter_swing_map.csv"), show_col_types = FALSE)  # floored

geom <- c("miss_distance","perfect_percent","flawed_percent","early_percent","on_time_percent",
          "late_percent","tied_up_percent","centered_percent","flailed_percent",
          "over_percent","lined_up_percent","under_percent")
mech <- c("avg_bat_speed","swing_length","hard_swing_rate","squared_up_per_swing","blast_per_swing")
outcomes <- c("xwoba","woba","barrel_pct","hardhit_pct","ev90","max_ev","la","iso","slg",
              "k_pct","bb_pct","pull_pct","oppo_pct","fb_pct","gb_pct","pull_air_rate","babip","avg")
# outcomes that are "value/batted-ball" (exclude K/BB, ~tautological with whiff geometry)
val_out <- setdiff(outcomes, c("k_pct","bb_pct"))

# ── attach pitch-type splits (all_fb/breaking/offspeed, whiff_fb/breaking/offspeed) ──
cuts_pt <- c("all_fb","all_breaking","all_offspeed","whiff_fb","whiff_breaking","whiff_offspeed")
for (cut in cuts_pt) {
  d <- suppressWarnings(read_csv(file.path(DL, sprintf("bat-tracking-swing-timing_2024-26_%s.csv", cut)),
                                 show_col_types = FALSE, name_repair = "minimal"))
  names(d)[1] <- "id"
  d <- d |> select(id, year, any_of(geom)) |> rename_with(~paste0(.x, "_", cut), any_of(geom))
  base <- base |> left_join(d, by = c("id", "year"))
}

# candidate input features
inputs <- c(as.vector(outer(geom, c("all","bip","whiff"), paste, sep="_")),
            as.vector(outer(mech, c("all","bip","whiff"), paste, sep="_")),
            as.vector(outer(geom, cuts_pt, paste, sep="_")))
inputs <- intersect(inputs, names(base))
# drop degenerate (mostly-NA or no variance)
ok <- sapply(inputs, function(c) {
  x <- base[[c]]; (mean(!is.na(x)) > 0.5) && (sd(x, na.rm=TRUE) > 1e-9) && !is.na(sd(x, na.rm=TRUE))
})
inputs <- inputs[ok]
cat(sprintf("Wide feature set: %d inputs x %d outcomes (n=%d player-seasons)\n", length(inputs), length(outcomes), nrow(base)))

# ── (1) wide correlation matrix ──
M <- outer(inputs, outcomes, Vectorize(function(i,o) suppressWarnings(cor(base[[i]], base[[o]], use="pairwise.complete.obs"))))
dimnames(M) <- list(inputs, outcomes)
write.csv(round(M,3), file.path(OUT, "wide_correlation_matrix.csv"))
cat("Wrote wide_correlation_matrix.csv\n")

# ── (2) YoY stability for every input ──
pairs <- function(y1,y2){
  a<-base|>filter(year==y1)|>select(id,pa,all_of(inputs)); b<-base|>filter(year==y2)|>select(id,pa,all_of(inputs))
  inner_join(a,b,by="id",suffix=c("_y1","_y2"))
}
p24<-pairs(2024,2025); p25<-pairs(2025,2026)
wcor<-function(x,y,w){k<-is.finite(x)&is.finite(y)&is.finite(w);x<-x[k];y<-y[k];w<-w[k]/sum(w[k]);mx<-sum(w*x);my<-sum(w*y);sum(w*(x-mx)*(y-my))/sqrt(sum(w*(x-mx)^2)*sum(w*(y-my)^2))}
hm<-function(a,b) 2*a*b/(a+b)
stab <- sapply(inputs, function(m){
  x<-c(p24[[paste0(m,"_y1")]],p25[[paste0(m,"_y1")]]); y<-c(p24[[paste0(m,"_y2")]],p25[[paste0(m,"_y2")]])
  w<-c(hm(p24$pa_y1,p24$pa_y2),hm(p25$pa_y1,p25$pa_y2)); tryCatch(wcor(x,y,w), error=function(e) NA)
})

# ── synthesis table ──
absr <- abs(M)
syn <- tibble(
  feature   = inputs,
  group     = ifelse(grepl("^avg_bat_speed|^swing_length|^hard_swing|^squared_up|^blast", inputs), "Mechanics",
               ifelse(grepl("_fb$|_breaking$|_offspeed$", inputs), "Geometry: pitch-type", "Geometry: base cut")),
  stability = round(stab, 3),
  peak_r    = round(apply(absr, 1, max, na.rm=TRUE), 3),
  best_out  = outcomes[apply(absr, 1, which.max)],
  peak_val_r= round(apply(absr[, val_out], 1, max, na.rm=TRUE), 3),
  best_val  = val_out[apply(absr[, val_out], 1, which.max)],
  r_xwoba   = round(M[, "xwoba"], 3)
)
write_csv(syn, file.path(OUT, "stability_vs_predictiveness.csv"))

cat("\n===== BOTH STABLE (>=0.75) AND PREDICTIVE OF VALUE (|r|>=0.30) =====\n")
win <- syn |> filter(stability >= 0.75, peak_val_r >= 0.30) |> arrange(desc(peak_val_r))
print(as.data.frame(win |> select(feature, group, stability, peak_val_r, best_val, r_xwoba)), row.names=FALSE)

cat("\n--- top 12 by overall peak_r (incl. K/whiff) for context ---\n")
print(as.data.frame(syn |> arrange(desc(peak_r)) |> select(feature, stability, peak_r, best_out) |> head(12)), row.names=FALSE)

# ── (3) scatter: stability vs value-predictiveness ──
lab <- syn |> filter(stability >= 0.7 & peak_val_r >= 0.28 | peak_val_r >= 0.40)
p <- ggplot(syn, aes(stability, peak_val_r, colour = group)) +
  geom_vline(xintercept = 0.75, linetype = "dashed", colour = "#999999") +
  geom_hline(yintercept = 0.30, linetype = "dashed", colour = "#999999") +
  geom_point(alpha = 0.8, size = 2.4) +
  ggrepel::geom_text_repel(data = lab, aes(label = feature), size = 2.6, max.overlaps = 20, seg.color="#ccc") +
  scale_colour_manual(values = c("Mechanics"="#2f7d3a","Geometry: base cut"="#b77343","Geometry: pitch-type"="#3a6ea5")) +
  labs(title = "Stable AND predictive? (top-right = keepers)",
       subtitle = "x = YoY repeatability · y = peak |r| with a value/batted-ball outcome · dashed = 0.75 / 0.30",
       x = "YoY stability", y = "Peak correlation with a value outcome", colour = NULL) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face="bold"), legend.position = "top",
        plot.subtitle = element_text(colour="#4a5a4f", size=9))
ggsave(file.path(OUT, "plots/stability_vs_predictiveness.png"), p, width = 10, height = 8, dpi = 150, bg = "white")
cat("\nWrote stability_vs_predictiveness.csv + plots/stability_vs_predictiveness.png\n")
