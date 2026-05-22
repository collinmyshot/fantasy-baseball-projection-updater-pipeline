# ── Step 6: Standings Overlay ─────────────────────────────────────────────────
# PURPOSE: Category-by-category breakdown of current standings.
#   Shows where YOLO ranks in each cat, roto pts, gaps to adjacent teams,
#   and a summary of strengths/weaknesses.
#
# INPUTS (update each week from Fantrax Standings > Stat Totals screenshot):
#   STANDINGS block below

MY_TEAM <- "Young Outliers, Legendary Outputs"

# ── STANDINGS (update each week) ──────────────────────────────────────────────
standings <- data.frame(
  team = c(
    "Maxsulli912",
    "Woo!",
    "mlshaon",
    "New Team 12",
    "Disposable Heroes",
    "Sproat GOAT",
    "Harry D's Wild Things",
    "Vantasner Danger Meridian",
    "Norbrook",
    "Young Outliers, Legendary Outputs",
    "Yuma Scorpions"
  ),
  pts  = c(80.0, 68.5, 62.5, 61.0, 60.0, 59.0, 58.0, 54.0, 53.0, 53.0, 51.0),
  R    = c(212, 183, 193, 186, 160, 208, 210, 227, 171, 220, 156),
  HR   = c(52,  51,  54,  45,  44,  51,  47,  49,  46,  71,  35),
  RBI  = c(189, 162, 200, 184, 163, 182, 173, 162, 176, 196, 151),
  SB   = c(27,  38,  30,  32,  40,  19,  38,  43,  31,  27,  26),
  OBP  = c(.337,.338,.343,.318,.308,.332,.348,.324,.333,.342,.336),
  ERA  = c(3.31,3.61,3.96,3.88,3.80,4.13,4.29,4.58,4.13,4.85,3.37),
  WHIP = c(1.078,1.150,1.301,1.194,1.270,1.238,1.308,1.369,1.390,1.405,1.036),
  K    = c(242, 237, 177, 245, 238, 254, 216, 224, 259, 236, 232),
  W    = c(19,  11,  12,  20,  22,  11,  13,  12,  10,  10,  15),
  SV   = c(10,  16,   8,   6,  13,  11,  10,  11,  16,   2,  11),
  stringsAsFactors = FALSE
)

n <- nrow(standings)

# ── RANK CATEGORIES ───────────────────────────────────────────────────────────
# Higher = better for all except ERA and WHIP
rank_cat <- function(x, higher_better = TRUE) {
  if (higher_better) rank(x, ties.method = "average")
  else               rank(-x, ties.method = "average")
}

standings$rk_R    <- rank_cat(standings$R)
standings$rk_HR   <- rank_cat(standings$HR)
standings$rk_RBI  <- rank_cat(standings$RBI)
standings$rk_SB   <- rank_cat(standings$SB)
standings$rk_OBP  <- rank_cat(standings$OBP)
standings$rk_ERA  <- rank_cat(standings$ERA, higher_better = FALSE)
standings$rk_WHIP <- rank_cat(standings$WHIP, higher_better = FALSE)
standings$rk_K    <- rank_cat(standings$K)
standings$rk_W    <- rank_cat(standings$W)
standings$rk_SV   <- rank_cat(standings$SV)

rk_cols <- c("rk_R","rk_HR","rk_RBI","rk_SB","rk_OBP",
              "rk_ERA","rk_WHIP","rk_K","rk_W","rk_SV")
cat_names <- c("R","HR","RBI","SB","OBP","ERA","WHIP","K","W","SV")

standings$roto_pts_check <- rowSums(standings[, rk_cols])

# ── OUTPUT: FULL STANDINGS WITH CATEGORY RANKS ───────────────────────────────
cat("=== STEP 6: STANDINGS OVERLAY — CATEGORY RANKS ===\n")
cat(sprintf("Source: Fantrax Stat Totals — %s\n\n", Sys.Date()))

cat(sprintf("  %-35s %5s | %4s %4s %4s %4s %4s | %4s %4s %4s %4s %4s\n",
    "Team", "Pts", "R", "HR", "RBI", "SB", "OBP", "ERA", "WHIP", "K", "W", "SV"))
cat("  ", strrep("-", 105), "\n", sep="")

# Sort by pts descending
standings <- standings[order(-standings$pts), ]

for (i in seq_len(nrow(standings))) {
  s   <- standings[i, ]
  me  <- s$team == MY_TEAM
  pfx <- if (me) "►" else " "

  cat(sprintf("%s %-35s %5.1f | %4.1f %4.1f %4.1f %4.1f %4.1f | %4.1f %4.1f %4.1f %4.1f %4.1f\n",
    pfx, substr(s$team, 1, 35), s$pts,
    s$rk_R, s$rk_HR, s$rk_RBI, s$rk_SB, s$rk_OBP,
    s$rk_ERA, s$rk_WHIP, s$rk_K, s$rk_W, s$rk_SV))
}
cat("  (category values = roto rank, 11=1st place, 1=last)\n\n")

# ── YOLO PROFILE ──────────────────────────────────────────────────────────────
me <- standings[standings$team == MY_TEAM, ]

cat("── YOLO CATEGORY PROFILE ─────────────────────────────────────────────────\n")
cat(sprintf("  Overall: %.1f pts (rank %d of %d)\n\n",
    me$pts, which(standings$team == MY_TEAM), n))

# For each category: my value, my rank, value at rank above and below
cat(sprintf("  %-6s %8s %5s | %8s %5s | %8s %5s\n",
    "Cat", "My Val", "Rank", "1-Up Val", "Gap↑", "1-Dn Val", "Gap↓"))
cat("  ", strrep("-", 65), "\n", sep="")

raw_cols  <- c("R","HR","RBI","SB","OBP","ERA","WHIP","K","W","SV")
higher_ok <- c(TRUE,TRUE,TRUE,TRUE,TRUE,FALSE,FALSE,TRUE,TRUE,TRUE)

for (j in seq_along(raw_cols)) {
  col  <- raw_cols[j]
  rk_c <- paste0("rk_", col)
  hb   <- higher_ok[j]

  my_val  <- me[[col]]
  my_rk   <- me[[rk_c]]

  # sort all values worst-to-best (ascending rank)
  ord         <- order(standings[[rk_c]], decreasing = FALSE)
  vals_sorted <- standings[[col]][ord]
  # find YOLO's position in sorted order by team name
  my_pos <- which(standings$team[ord] == MY_TEAM)[1]

  # team 1 rank above (better rank = my_pos + 1 in ascending list)
  if (!is.na(my_pos) && my_pos < n) {
    val_up  <- vals_sorted[my_pos + 1]
    gap_up  <- if (hb) val_up - my_val else my_val - val_up
  } else {
    val_up <- NA; gap_up <- NA
  }

  # team 1 rank below
  if (!is.na(my_pos) && my_pos > 1) {
    val_dn  <- vals_sorted[my_pos - 1]
    gap_dn  <- if (hb) my_val - val_dn else val_dn - my_val
  } else {
    val_dn <- NA; gap_dn <- NA
  }

  fmt_val <- function(v, col) {
    if (is.na(v)) return("—")
    if (col == "OBP") sprintf("%.3f", v)
    else if (col %in% c("ERA","WHIP")) sprintf("%.3f", v)
    else sprintf("%.0f", v)
  }
  fmt_gap <- function(g, col) {
    if (is.na(g)) return("—")
    if (col == "OBP") sprintf("+%.3f", g)
    else if (col %in% c("ERA","WHIP")) sprintf("-%.3f", g)
    else sprintf("+%.0f", g)
  }

  star <- if (my_rk >= 9) "★" else if (my_rk <= 2) "✗" else " "

  cat(sprintf("  %-6s %8s %4.1f%s | %8s %5s | %8s %5s\n",
    col,
    fmt_val(my_val, col), my_rk, star,
    fmt_val(val_up, col), fmt_gap(gap_up, col),
    fmt_val(val_dn, col), fmt_gap(gap_dn, col)))
}

cat("\n  ★ = top-3 category (ranks 9-11)  |  ✗ = bottom-2 (ranks 1-2)\n\n")

# ── POINTS MOVEMENT SUMMARY ───────────────────────────────────────────────────
cat("── POINTS AT STAKE (gain from moving up one rank) ────────────────────────\n")
cat("  Moving up/down one roto rank = +/-1 pt in that category.\n")
cat("  Categories below show gap to the team one rank above YOLO.\n\n")

cat(sprintf("  %-6s %5s  %s\n", "Cat", "Rank", "Gap to move up"))
cat("  ", strrep("-", 40), "\n", sep="")

for (j in seq_along(raw_cols)) {
  col  <- raw_cols[j]
  rk_c <- paste0("rk_", col)
  hb   <- higher_ok[j]

  my_rk  <- me[[rk_c]]
  my_val <- me[[col]]

  ord        <- order(standings[[rk_c]])
  vals_sorted <- standings[[col]][ord]
  my_pos     <- which(abs(standings[[rk_c]][ord] - my_rk) < 0.01)[1]

  if (!is.na(my_pos) && my_pos < n) {
    val_up <- vals_sorted[my_pos + 1]
    gap    <- if (hb) val_up - my_val else my_val - val_up
    direction <- if (hb) "need more" else "need lower"

    if (col == "OBP")        gap_str <- sprintf("%.3f %s", gap, direction)
    else if (col %in% c("ERA","WHIP")) gap_str <- sprintf("%.3f %s", gap, direction)
    else                     gap_str <- sprintf("%.0f %s", gap, direction)

    cat(sprintf("  %-6s %5.1f  %s\n", col, my_rk, gap_str))
  } else {
    cat(sprintf("  %-6s %5.1f  (already 1st)\n", col, my_rk))
  }
}

cat("\n── SUMMARY ───────────────────────────────────────────────────────────────\n")

strengths <- cat_names[sapply(paste0("rk_", cat_names), function(rc) me[[rc]] >= 9)]
weaknesses <- cat_names[sapply(paste0("rk_", cat_names), function(rc) me[[rc]] <= 3)]

cat(sprintf("  Dominant (rank 9-11): %s\n",
    if (length(strengths)) paste(strengths, collapse=", ") else "none"))
cat(sprintf("  Weak     (rank 1-3):  %s\n",
    if (length(weaknesses)) paste(weaknesses, collapse=", ") else "none"))
