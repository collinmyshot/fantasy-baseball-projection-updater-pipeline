#!/usr/bin/env Rscript
# Composite-weight derivation v2 — fantasy-points target.
#
# The v1 derivation (derive_park_factor_weights.R) found the blend of
# BACON/HR/XBH park deltas that best reconstructs the wOBAcon-residual park
# effect. That target guarantees BACON dominance (it is nearly the same
# variable) and says nothing about fantasy value.
#
# v2 regresses the Ottoneu-points park effect (pts_resid component model,
# park delta in FG points per BBE) on the three component deltas with FREE
# coefficients — each beta is the unit conversion "points per unit of
# component delta". Reported weights are contribution shares
#   share_c = beta_c * sd(delta_c) / sum_j |beta_j| * sd(delta_j)
# bootstrapped over park eras for confidence intervals.
#
# Also reported: how well the existing 45/35/20 blend and the direct points
# component line up with the points target, so the "just use the points PF
# directly" option can be judged on numbers.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_dir = list(flag = "--output-dir", default = file.path("data", "processed", "park_factors")),
  seed       = list(flag = "--seed",       default = 42, type = "numeric"),
  n_boot     = list(flag = "--n-boot",     default = 400, type = "numeric"),
  min_bbe    = list(flag = "--min-bbe",    default = 1000, type = "numeric"),
  weight_bacon_old = list(flag = "--weight-bacon-old", default = 0.45, type = "numeric"),
  weight_hr_old    = list(flag = "--weight-hr-old",    default = 0.35, type = "numeric"),
  weight_xbh_old   = list(flag = "--weight-xbh-old",   default = 0.20, type = "numeric")
))

output_dir <- parsed$output_dir
seed       <- as.integer(parsed$seed)
n_boot     <- as.integer(parsed$n_boot)
min_bbe    <- as.integer(parsed$min_bbe)
w_old <- c(bacon = parsed$weight_bacon_old, hr = parsed$weight_hr_old, xbh = parsed$weight_xbh_old)
w_old <- w_old / sum(w_old)

read_component <- function(name, col_out) {
  path <- file.path(output_dir, sprintf("park_factors_%s_overall.csv", name))
  if (!file.exists(path)) {
    stop(sprintf("Missing component file: %s", path))
  }
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  out <- d[, c("park_era_id", "delta_overall", "n_bbe")]
  names(out) <- c("park_era_id", col_out, sprintf("n_bbe_%s", col_out))
  out
}

dat <- read_component("points", "pts")
dat <- merge(dat, read_component("bacon", "bacon")[, c("park_era_id", "bacon")], by = "park_era_id")
dat <- merge(dat, read_component("hr", "hr")[, c("park_era_id", "hr")], by = "park_era_id")
dat <- merge(dat, read_component("xbh", "xbh")[, c("park_era_id", "xbh")], by = "park_era_id")

dat <- dat[is.finite(dat$pts) & is.finite(dat$bacon) & is.finite(dat$hr) & is.finite(dat$xbh) &
             is.finite(dat$n_bbe_pts) & dat$n_bbe_pts >= min_bbe, ]

if (nrow(dat) < 10) {
  stop(sprintf("Not enough park eras after filtering (n=%s).", nrow(dat)))
}

fit_and_shares <- function(d) {
  fit <- stats::lm(pts ~ bacon + hr + xbh, data = d, weights = d$n_bbe_pts)
  beta <- stats::coef(fit)[c("bacon", "hr", "xbh")]
  contrib <- abs(beta) * c(stats::sd(d$bacon), stats::sd(d$hr), stats::sd(d$xbh))
  shares <- contrib / sum(contrib)
  list(beta = beta, shares = shares, fit = fit)
}

main <- fit_and_shares(dat)
pred_pts <- as.numeric(stats::predict(main$fit))

r2 <- summary(main$fit)$r.squared

# Leave-one-out generalization check for the regression blend.
loo_pred <- vapply(seq_len(nrow(dat)), function(i) {
  f <- stats::lm(pts ~ bacon + hr + xbh, data = dat[-i, ], weights = dat$n_bbe_pts[-i])
  as.numeric(stats::predict(f, newdata = dat[i, , drop = FALSE]))
}, numeric(1))
loo_cor <- stats::cor(loo_pred, dat$pts)

# Benchmarks against the points target.
old_blend <- w_old[["bacon"]] * dat$bacon + w_old[["hr"]] * dat$hr + w_old[["xbh"]] * dat$xbh
cor_old_blend <- stats::cor(old_blend, dat$pts)
cor_rank_old <- stats::cor(old_blend, dat$pts, method = "spearman")
cor_bacon_only <- stats::cor(dat$bacon, dat$pts)

set.seed(seed)
boot_shares <- matrix(NA_real_, nrow = n_boot, ncol = 3, dimnames = list(NULL, c("bacon", "hr", "xbh")))
boot_beta <- matrix(NA_real_, nrow = n_boot, ncol = 3, dimnames = list(NULL, c("bacon", "hr", "xbh")))
for (b in seq_len(n_boot)) {
  idx <- sample.int(nrow(dat), replace = TRUE)
  res <- tryCatch(fit_and_shares(dat[idx, ]), error = function(e) NULL)
  if (!is.null(res)) {
    boot_shares[b, ] <- res$shares
    boot_beta[b, ] <- res$beta
  }
}

q <- function(x, p) stats::quantile(x, probs = p, na.rm = TRUE, names = FALSE)

summary_tbl <- data.frame(
  component = c("bacon_resid", "hr_resid", "xbh_resid"),
  beta_points_per_unit = as.numeric(main$beta),
  share = as.numeric(main$shares),
  share_boot_median = c(q(boot_shares[, "bacon"], .5), q(boot_shares[, "hr"], .5), q(boot_shares[, "xbh"], .5)),
  share_boot_p05 = c(q(boot_shares[, "bacon"], .05), q(boot_shares[, "hr"], .05), q(boot_shares[, "xbh"], .05)),
  share_boot_p95 = c(q(boot_shares[, "bacon"], .95), q(boot_shares[, "hr"], .95), q(boot_shares[, "xbh"], .95)),
  stringsAsFactors = FALSE
)

diag_tbl <- data.frame(
  n_park_eras = nrow(dat),
  n_boot = n_boot,
  min_bbe_filter = min_bbe,
  r2_regression = r2,
  loo_cor = loo_cor,
  cor_old_45_35_20_blend_vs_points = cor_old_blend,
  spearman_old_blend_vs_points = cor_rank_old,
  cor_bacon_only_vs_points = cor_bacon_only,
  stringsAsFactors = FALSE
)

pred_tbl <- data.frame(
  park_era_id = dat$park_era_id,
  points_delta = dat$pts,
  pred_from_components = pred_pts,
  old_blend_45_35_20 = old_blend,
  n_bbe = dat$n_bbe_pts,
  stringsAsFactors = FALSE
)

utils::write.csv(summary_tbl, file.path(output_dir, "park_factor_weight_recommendation_v2.csv"), row.names = FALSE, na = "")
utils::write.csv(diag_tbl, file.path(output_dir, "park_factor_weight_v2_diagnostics.csv"), row.names = FALSE, na = "")
utils::write.csv(pred_tbl, file.path(output_dir, "park_factor_weight_v2_fit_table.csv"), row.names = FALSE, na = "")
utils::write.csv(as.data.frame(boot_shares), file.path(output_dir, "park_factor_weight_v2_bootstrap_draws.csv"), row.names = FALSE, na = "")

message("--- Weight derivation v2 (points target) ---")
for (k in seq_len(nrow(summary_tbl))) {
  message(sprintf(
    "  %s: beta=%.3f pts/unit | share=%.3f (boot median %.3f, 90%% CI %.3f-%.3f)",
    summary_tbl$component[k], summary_tbl$beta_points_per_unit[k], summary_tbl$share[k],
    summary_tbl$share_boot_median[k], summary_tbl$share_boot_p05[k], summary_tbl$share_boot_p95[k]
  ))
}
message(sprintf("  Regression R^2 = %.3f | leave-one-out cor = %.3f", r2, loo_cor))
message(sprintf("  Old 45/35/20 blend vs points target: pearson %.3f, spearman %.3f", cor_old_blend, cor_rank_old))
message(sprintf("  BACON alone vs points target: pearson %.3f", cor_bacon_only))
