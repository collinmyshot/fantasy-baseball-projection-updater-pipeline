#!/usr/bin/env Rscript
# build_platoon_grids.R
# ---------------------------------------------------------------------------
# League-average platoon grids for the methodology article: batter side (row)
# x pitcher hand (column) for AVG, HR rate, SB rate. Real data: FanGraphs
# splits leaderboards (month=13 vs LHP, month=14 vs RHP), all batters qual=0,
# aggregated 2015-2025 (ind=0). Batter side via MLBAM hand lookup; SWITCH
# hitters are assigned to the side they actually bat in that matchup
# (S vs RHP -> LHB row; S vs LHP -> RHB row).
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages({library(jsonlite); library(data.table)}))
ROOT <- "/Users/ckaufman/Documents/New project"
options(timeout = 180)

fg_split <- function(month) {
  url <- paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&stats=bat&lg=all&qual=0&type=8",
    "&season=2025&season1=2015&ind=0&team=0&rost=0&age=0&players=0",
    "&pageitems=5000&pagenum=1&month=", month)
  pl <- fromJSON(url, simplifyVector = TRUE)
  d <- as.data.table(pl$data)
  d[, .(mlbam_id = suppressWarnings(as.integer(xMLBAMID)), pa = PA, ab = AB, h = H, hr = HR, sb = SB,
        woba = as.numeric(wOBA))]
}
vsL <- fg_split(13); vsR <- fg_split(14)
cat(sprintf("fetched: vsLHP %d batters (%.0fk PA) | vsRHP %d batters (%.0fk PA)\n",
    nrow(vsL), sum(vsL$pa)/1e3, nrow(vsR), sum(vsR$pa)/1e3))

hand <- fread(file.path(ROOT, "data/raw/hitter_game_logs_cache/hand_lookup.csv"))
vsL <- merge(vsL, hand[, .(mlbam_id = person_id, bat_side)], by = "mlbam_id")
vsR <- merge(vsR, hand[, .(mlbam_id = person_id, bat_side)], by = "mlbam_id")
cat(sprintf("hand match: vsL %.1f%% | vsR %.1f%% of PA\n",
    100*sum(vsL$pa)/sum(fg_split(13)$pa), 100*sum(vsR$pa)/sum(fg_split(14)$pa)))

# effective side in the matchup: S bats opposite the pitcher
vsL[, side := fifelse(bat_side == "S", "R", bat_side)]
vsR[, side := fifelse(bat_side == "S", "L", bat_side)]
cell <- function(d, s) {
  x <- d[side == s]
  data.table(pa = sum(x$pa), avg = sum(x$h)/sum(x$ab),
             woba = weighted.mean(x$woba, x$pa, na.rm = TRUE),
             hr600 = 600*sum(x$hr)/sum(x$pa), sb600 = 600*sum(x$sb)/sum(x$pa))
}
grid <- rbindlist(list(
  cbind(row = "LHB", col = "vsLHP", cell(vsL, "L")),
  cbind(row = "LHB", col = "vsRHP", cell(vsR, "L")),
  cbind(row = "RHB", col = "vsLHP", cell(vsL, "R")),
  cbind(row = "RHB", col = "vsRHP", cell(vsR, "R"))))
grid[, `:=`(avg = round(avg, 3), woba = round(woba, 3), hr600 = round(hr600, 1), sb600 = round(sb600, 1))]
cat("\nLeague platoon grid, 2015-2025 (switch hitters counted on their actual side):\n")
print(grid, row.names = FALSE)
fwrite(grid, file.path(ROOT, "data/processed/platoon_grid_2015_2025.csv"))
cat("\nPlatoon deltas (advantage=opposite hand minus disadvantage=same hand), for colour thresholds:\n")
d1 <- grid[row=="LHB"]; d2 <- grid[row=="RHB"]
cat(sprintf("  LHB advantage vsRHP: AVG %+.3f | wOBA %+.3f | HR/600 %+.1f | SB/600 %+.1f\n",
    d1[col=="vsRHP",avg]-d1[col=="vsLHP",avg], d1[col=="vsRHP",woba]-d1[col=="vsLHP",woba],
    d1[col=="vsRHP",hr600]-d1[col=="vsLHP",hr600], d1[col=="vsRHP",sb600]-d1[col=="vsLHP",sb600]))
cat(sprintf("  RHB advantage vsLHP: AVG %+.3f | wOBA %+.3f | HR/600 %+.1f | SB/600 %+.1f\n",
    d2[col=="vsLHP",avg]-d2[col=="vsRHP",avg], d2[col=="vsLHP",woba]-d2[col=="vsRHP",woba],
    d2[col=="vsLHP",hr600]-d2[col=="vsRHP",hr600], d2[col=="vsLHP",sb600]-d2[col=="vsRHP",sb600]))
