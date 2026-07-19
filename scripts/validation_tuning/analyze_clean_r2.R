#!/usr/bin/env Rscript
# analyze_clean_r2.R
# ---------------------------------------------------------------------------
# Step 3: fix the projection-scaling leak in the blended R^2 (the article's
# "projections explain ~X% of weekly variance" number). The composite
# value_5x5_proj z-scored the projection against the week's REALIZED pool
# moments (not knowable in advance). Recompute against PROJECTED (pre-knowable)
# moments and report the honest number. Per-category R^2 was already clean
# (raw projected count vs raw realized count, no week-moment scaling).
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"
f <- fread(file.path(ROOT,"data/processed/hitter_week_features.csv"))[has_steamer==1 & is.finite(value_5x5_proj)]

zc <- function(x){ s <- sd(x); if (is.na(s) || s==0) rep(0, length(x)) else (x - mean(x))/s }
f[, `:=`(zr=zc(r_proj), zhr=zc(hr_proj), zrbi=zc(rbi_proj), zsb=zc(sb_proj), zav=zc(avgval_proj)), by=week_start]
f[, v_clean := zr + zhr + zrbi + zsb + zav]

leaky <- cor(f$value_5x5_proj, f$value_5x5)^2
clean <- cor(f$v_clean,       f$value_5x5)^2
reg   <- summary(lm(value_5x5 ~ r_proj + hr_proj + rbi_proj + sb_proj + avgval_proj, f))$r.squared

cat("Blended weekly 5x5 value -- variance explained by 'just Steamer':\n")
cat(sprintf("  leaky (realized-week moments) : R^2 = %.4f  (%.1f%%)\n", leaky, 100*leaky))
cat(sprintf("  CLEAN (projected-week moments): R^2 = %.4f  (%.1f%%)   <- honest article number\n", clean, 100*clean))
cat(sprintf("  regression (best weights, UB) : R^2 = %.4f  (%.1f%%)\n", reg, 100*reg))
cat(sprintf("\n  Leak inflation: %+.2f pts of R^2. %s\n", 100*(leaky-clean),
            if (abs(leaky-clean) < 0.005) "Immaterial -- the ~6% claim survives." else "Material -- use the clean number."))
