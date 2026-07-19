#!/usr/bin/env Rscript
# build_hitter_ghw_calibration.R
# ---------------------------------------------------------------------------
# GOOD HITTER WEEK (pace standard) -- calibration + reporting lens.
# NOT a training change: engines stay per-game Poisson counts. For a Poisson
# engine P(>=k) is a monotone transform of lambda, so top-5 picks are
# IDENTICAL under probability vs expected-count ranking; this script only
# adds evaluation/communication layers:
#   (1) PACE table: NFBC Main Event 80th-pctile season targets / (measured
#       scoring weeks x 14 starters) -> per-slot-week pace + integer threshold.
#       Jensen caveat: the pace is a TEAM-SUM requirement; per-slot thresholds
#       are a keeping-pace heuristic, not a per-hitter necessity.
#   (2) CALIBRATION (the real test of the Poisson tail assumption): LOSO
#       weekly P(>=k) = 1 - ppois(k-1, sum lambda_g) vs realized frequency,
#       decile bins, per category pool. Same-week correlation between games
#       would show up here as overconfidence (predicted spread > realized).
#   (3) BRIER vs Steamer-only probabilities (proj rate x league PA/G x games).
#   (4) REPORTING LENS: top-5 good-week hit rate, model vs Steamer, paired
#       week-clustered bootstrap -- EXPECTED to be noisier than production
#       lift (binary discards magnitude; picks overlap heavily).
#   (5) PACE SURPLUS: realized units above pace per streamed slot-week,
#       model vs Steamer (the cumulative-roto-honest absolute unit).
# Window 2016-2025 ex-2020; panel = shipped v2 machinery + hand-split park;
# superset panel (adds R/RBI engines) cached to the shared winrate RDS path.
# Outputs -> data/processed/hitter_article/ghw_{thresholds,calibration,eval}.csv
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
OUT  <- file.path(ROOT, "data/processed/hitter_article")
SCRATCH <- "/private/tmp/claude-501/-Users-ckaufman-Documents-New-project/57b453dd-83f5-4aa1-abf9-e40500bf9c39/scratchpad"
dir.create(SCRATCH, showWarnings = FALSE, recursive = TRUE)
PANEL <- file.path(SCRATCH, "winrate_panel_handsplit.rds")   # shared cache (superset)
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))
MB <- c(hr = 100, avg = 500, sb = 150); EMULT <- 0.5

need_build <- TRUE
if (file.exists(PANEL)) { gl <- readRDS(PANEL); if ("p_r" %in% names(gl)) need_build <- FALSE }
if (need_build) {
files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id")); raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])
onf <- quote(singles + bb + hbp)
mk_todate <- function(numexpr, denexpr, idcol, m_pseudo) {
  ag <- raw[!is.na(get(idcol)), .(num=sum(eval(numexpr)), den=sum(eval(denexpr))), by=.(season, ent=get(idcol), game_date)]
  setorder(ag, ent, season, game_date)
  ag[, `:=`(cn=cumsum(num)-num, cd=cumsum(den)-den), by=.(ent, season)]
  seas <- ag[, .(num=sum(num), den=sum(den)), by=.(season, ent)][, rate := num/pmax(den,1)]
  lg   <- ag[, .(lg_rate=sum(num)/sum(den)), by=season]
  ag <- merge(ag, copy(seas)[, season := season+1][, .(season, ent, prior_rate=rate)], by=c("season","ent"), all.x=TRUE)
  ag <- merge(ag, copy(lg)[, season := season+1][, .(season, lg_prior=lg_rate)], by="season", all.x=TRUE)
  ag[is.na(prior_rate), prior_rate := lg_prior]
  ag[, blend := (cn + m_pseudo*prior_rate)/(cd + m_pseudo)]
  ag <- merge(ag, lg, by="season", all.x=TRUE)
  ag[, .(season, ent, game_date, feat = blend - lg_rate)]
}
team_bab <- mk_todate(quote(h-hr), quote(pmax(ab-so-hr,0L)), "opp_team_id", 2000*EMULT)
team_hr  <- mk_todate(quote(hr),   quote(pa),                "opp_team_id", 3000*EMULT)
team_sb  <- mk_todate(quote(sb),   onf,                      "opp_team_id",  750*EMULT)
pit_h    <- mk_todate(quote(h),    quote(pa),                "opp_starter_id", 400*EMULT)
pit_hr   <- mk_todate(quote(hr),   quote(pa),                "opp_starter_id", 400*EMULT)
pit_sb   <- mk_todate(quote(sb),   onf,                      "opp_starter_id", 150*EMULT)
team_off <- mk_todate(quote(r),    quote(pa),                "team_id",       3000*EMULT)
opp_ra   <- mk_todate(quote(r),    quote(pa),                "opp_team_id",   3000*EMULT)

gl <- raw[started == TRUE & !is.na(lineup_slot)]
gl[, home := as.integer(is_home)]
gl[, lhp := as.integer(opp_starter_throws=="L")]
gl[, week_start := game_date - (as.integer(format(game_date,"%u"))-1L)]
setorder(gl, person_id, season, game_date)
Mode7 <- function(v){ v<-v[!is.na(v)]; if(!length(v)) NA_integer_ else as.integer(names(sort(table(v),decreasing=TRUE))[1]) }
gl[, slot_lag := shift(lineup_slot, 1, type="lag"), by=.(person_id, season)]
gl[, slot_m7 := vapply(seq_len(.N), function(i) Mode7(slot_lag[max(1,i-6):i]), integer(1)), by=.(person_id, season)]
gl[is.na(slot_m7), slot_m7 := lineup_slot]
slot_pa <- gl[, .(pa_g = mean(pa)), by=lineup_slot]
gl <- merge(gl, slot_pa[, .(slot_m7=lineup_slot, exp_pa=pa_g)], by="slot_m7")
gl[, `:=`(s12 = as.integer(slot_m7<=2), s345 = as.integer(slot_m7>=3 & slot_m7<=5),
          s67 = as.integer(slot_m7>=6 & slot_m7<=7))]
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb), by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_obp=OBP, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA,
                     proj_r_pa=R/PA, proj_rbi_pa=RBI/PA, proj_ab_pa=AB/PA, proj_pa=PA)],
              by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE); gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch  + MB["avg"]*proj_avg)  /(cab + MB["avg"]) - proj_avg]
gl[, bat_td_dhr  := (chr + MB["hr"] *proj_hr_pa)/(cpa + MB["hr"])  - proj_hr_pa]
gl[, bat_td_dsb  := (csb + MB["sb"] *proj_sb_pa)/(cpa + MB["sb"])  - proj_sb_pa]

hand <- fread(file.path(ROOT,"data/processed/park_factors/park_factors_by_hand_era.csv"))
gl[, home_team_id := fifelse(home==1L, team_id, opp_team_id)]
gl[, bat_hand_eff := fifelse(bat_side=="S", fifelse(opp_starter_throws=="L","R","L"), bat_side)]
gl[!bat_hand_eff %in% c("L","R"), bat_hand_eff := "R"]
hj <- hand[, .(home_team_id=team_id, hand, year_start, year_end, bacon_idx_hand, hr_idx_hand)]
er <- hj[gl[, .(rid=.I, home_team_id, hand=bat_hand_eff, season)], on=.(home_team_id, hand), allow.cartesian=TRUE]
er <- er[season >= year_start & season <= year_end][order(rid, -year_start)][!duplicated(rid)]
gl[er$rid, `:=`(park_bacon = (er$bacon_idx_hand-100)/100, park_hr = (er$hr_idx_hand-100)/100)]

pitpy <- fread(file.path(ROOT,"data/raw/pitcher_obp_against_2015_2025.csv"))[bf>=100,
          .(season, opp_starter_id=person_id, pit_k_py=so_pitched/bf)][, season := season+1]
rppy <- rbindlist(lapply(2016:2025, function(y){
  d <- fread(sprintf("%s/data/raw/savant_running_game/pitcher_running_%d.csv", ROOT, y))
  d[, .(season=y+1, opp_starter_id=player_id, rp_py=runs_prevented_on_running_attr)] }))
adp <- rbindlist(lapply(2016:2025, function(y)
  fread(sprintf("%s/data/raw/nfbc_adp_history/nfbc_adp_with_position_%d.csv", ROOT, y))[
    is_pitcher == FALSE & !is.na(mlbam_id) & !is.na(nfbc_adp),
    .(season=y, person_id=as.integer(mlbam_id), nfbc_adp)]))
adp <- unique(adp, by=c("season","person_id"))[, adp_z := -as.numeric(scale(log(nfbc_adp))), by=season]

jn <- function(g, td, nm, keys){ setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys, "game_date"), all.x=TRUE) }
gl <- jn(gl, copy(team_bab), "team_bab_td", "opp_team_id")
gl <- jn(gl, copy(team_hr),  "team_hr_td",  "opp_team_id")
gl <- jn(gl, copy(team_sb),  "team_sb_td",  "opp_team_id")
gl <- jn(gl, copy(opp_ra),   "opp_ra_td",   "opp_team_id")
gl <- jn(gl, copy(team_off), "team_off_td", "team_id")
gl <- jn(gl, copy(pit_h),    "pit_h_td",    "opp_starter_id")
gl <- jn(gl, copy(pit_hr),   "pit_hr_td",   "opp_starter_id")
gl <- jn(gl, copy(pit_sb),   "pit_sb_td",   "opp_starter_id")
gl <- merge(gl, pitpy, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rppy,  by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, adp[, .(season, person_id, adp_z, nfbc_adp)], by=c("season","person_id"), all.x=TRUE)
gl <- gl[season >= 2016 & season != 2020 & !is.na(park_bacon)]
gl[, adp_z := fifelse(is.na(adp_z), min(adp_z, na.rm=TRUE), adp_z), by=season]
for (cn in c("team_bab_td","team_hr_td","team_sb_td","opp_ra_td","team_off_td",
             "pit_h_td","pit_hr_td","pit_sb_td","pit_k_py","rp_py"))
  gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]

gl[, platoon_adv := as.integer(bat_side=="S" | (bat_side=="L" & opp_starter_throws=="R") | (bat_side=="R" & opp_starter_throws=="L"))]
SPECS <- list(
  h   = c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
  hr  = c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
  sb  = c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","pit_sb_td","adp_z"),
  r   = c("proj_obp","proj_hr_pa","s12","s345","s67","team_off_td","opp_ra_td","home"),
  rbi = c("proj_obp","proj_hr_pa","s12","s345","s67","team_off_td","opp_ra_td","home"))
for (y in names(SPECS)) gl[[paste0("p_",y)]] <- NA_real_
for (s in unique(gl$season)) for (y in names(SPECS)) {
  fit <- glm(reformulate(SPECS[[y]], y), poisson, gl[season != s])
  gl[season == s, (paste0("p_",y)) := predict(fit, .SD, type="response")]
}
gl[, blend_sb  := (csb + MB["sb"]*proj_sb_pa)/(cpa + MB["sb"])]
gl[, blend_hr  := (chr + MB["hr"]*proj_hr_pa)/(cpa + MB["hr"])]
gl[, blend_avg := (ch  + MB["avg"]*proj_avg)/(cab + MB["avg"])]
lgavg <- gl[, .(lg = sum(h)/sum(ab)), by=season]; gl <- merge(gl, lgavg, by="season")
gl[, dow := as.integer(game_date - week_start)][, block := fifelse(dow<=3,"MonThu","FriSun")]
saveRDS(gl, PANEL); cat("panel built (superset):", nrow(gl), "rows\n")
}

# ---- (1) PACE TABLE: measured scoring weeks, real 80th-pctile targets ----------
tg <- fread(file.path(ROOT, "data/processed/2025_historical_targets_80th.csv"))
me <- tg[`Draft.Type` == "Main Event"]
Wk <- gl[, .(w = uniqueN(week_start)), by=season]
W  <- round(mean(Wk$w), 1)
cat(sprintf("measured scoring weeks per season (Mon-Sun periods with games): %.1f [%d-%d]\n",
    W, min(Wk$w), max(Wk$w)))
paces <- data.table(
  category  = c("HR","SB","R","RBI","AVG"),
  target80  = c(me$HR, me$SB, me$R, me$RBI, me$AVG),
  pace_slot = c(me$HR, me$SB, me$R, me$RBI, NA) / (W * 14))
paces[category=="AVG", pace_slot := me$AVG]
paces[, threshold := c("HR >= 1","SB >= 1 (>=2 = green light)","R >= 3","RBI >= 3","pace surplus H - .2564*AB (no binary)")]

# dynamic per-week category pools (same as the eval)
memb <- gl[, .(bsb=blend_sb[1], bhr=blend_hr[1], bavg=blend_avg[1]), by=.(season, week_start, person_id)]
memb[, `:=`(rk_sb=frank(-bsb,ties.method="first"), rk_hr=frank(-bhr,ties.method="first"),
            rk_avg=frank(-bavg,ties.method="first")), by=.(season, week_start)]
lgpag <- gl[, .(pag = mean(pa)), by=season]                 # league PA per started game

wk_units <- function(rkcol) {
  pop <- memb[get(rkcol) <= 150, .(season, week_start, person_id)]
  gc <- merge(gl, pop, by=c("season","week_start","person_id"))
  u <- gc[, .(g=.N, hr=sum(hr), sb=sum(sb), h=sum(h), ab=sum(ab), r=sum(r), rbi=sum(rbi),
              lam_hr=sum(p_hr), lam_sb=sum(p_sb), lam_h=sum(p_h), lam_r=sum(p_r), lam_rbi=sum(p_rbi),
              exp_ab=sum(exp_pa*proj_ab_pa), adp=nfbc_adp[1],
              phr_pa=proj_hr_pa[1], psb_pa=proj_sb_pa[1], pavg=proj_avg[1], pab=proj_ab_pa[1]),
          by=.(season, week_start, person_id)]
  merge(u, lgpag, by="season")
}
uhr <- wk_units("rk_hr"); usb <- wk_units("rk_sb"); uav <- wk_units("rk_avg")

# ---- (2)+(3) CALIBRATION + BRIER ----------------------------------------------
calib <- function(u, lamcol, ycol, k, stratecol, label) {
  d <- copy(u)
  d[, pm := 1 - ppois(k-1, get(lamcol))]                                  # model P(>=k)
  d[, ps := 1 - ppois(k-1, get(stratecol) * pag * g)]                     # steamer P(>=k)
  d[, y  := as.integer(get(ycol) >= k)]
  bins <- d[, .(n=.N, p_pred=mean(pm), p_real=mean(y)),
            by=.(bin = cut(pm, quantile(pm, 0:10/10), include.lowest=TRUE, labels=FALSE))][order(bin)]
  bins[, `:=`(category=label, thresh=k)]
  # Brier: model vs steamer, week-clustered bootstrap on the difference
  bw <- d[, .(bm=mean((pm-y)^2), bs=mean((ps-y)^2), n=.N), by=.(season, week_start)]
  db <- replicate(1500, { i<-sample.int(nrow(bw),nrow(bw),TRUE)
        sum(bw$n[i]*(bw$bs[i]-bw$bm[i]))/sum(bw$n[i]) })
  brier <- data.table(category=label, thresh=k,
    brier_model=round(weighted.mean(bw$bm,bw$n),4), brier_steamer=round(weighted.mean(bw$bs,bw$n),4),
    improve=sprintf("%+.4f [%+.4f,%+.4f]%s", mean(db), quantile(db,.025), quantile(db,.975),
                    ifelse(quantile(db,.025)>0," REAL","")))
  # weighted calibration slope/intercept (realized ~ predicted)
  cf <- coef(lm(p_real ~ p_pred, data=bins, weights=n))
  list(bins=bins, brier=brier, slope=sprintf("%s>=%d: intercept %.3f slope %.3f", label, k, cf[1], cf[2]))
}
runs <- list(
  calib(uhr, "lam_hr", "hr", 1L, "phr_pa", "HR"),
  calib(uhr, "lam_hr", "hr", 2L, "phr_pa", "HR"),
  calib(usb, "lam_sb", "sb", 1L, "psb_pa", "SB"),
  calib(usb, "lam_sb", "sb", 2L, "psb_pa", "SB"))
allbins  <- rbindlist(lapply(runs, `[[`, "bins"))
allbrier <- rbindlist(lapply(runs, `[[`, "brier"))

# ---- (4)+(5) top-5 good-week hit rate + pace surplus, model vs Steamer ---------
lens <- function(u, lamcol, ycol, k, stratecol, pace, label) {
  d <- copy(u); d[, tb := runif(.N)]
  d[, st_exp := get(stratecol) * pag * g]
  top5 <- function(rcol) d[, { o<-order(-get(rcol), tb); kk<-min(5,.N)
      .(clear=sum(get(ycol)[o][1:kk] >= k), tot=kk, prod=sum(get(ycol)[o][1:kk])) }, by=.(season, week_start)]
  m <- top5(lamcol); s <- top5("st_exp")
  j <- merge(m, s, by=c("season","week_start"), suffixes=c(".m",".s"))
  hr_m <- 100*sum(j$clear.m)/sum(j$tot.m); hr_s <- 100*sum(j$clear.s)/sum(j$tot.s)
  dh <- replicate(1500, { i<-sample.int(nrow(j),nrow(j),TRUE)
        100*sum(j$clear.m[i])/sum(j$tot.m[i]) - 100*sum(j$clear.s[i])/sum(j$tot.s[i]) })
  sur_m <- sum(j$prod.m)/sum(j$tot.m) - pace; sur_s <- sum(j$prod.s)/sum(j$tot.s) - pace
  ds <- replicate(1500, { i<-sample.int(nrow(j),nrow(j),TRUE)
        sum(j$prod.m[i])/sum(j$tot.m[i]) - sum(j$prod.s[i])/sum(j$tot.s[i]) })
  data.table(category=label, thresh=k,
    good_rate_model=round(hr_m,1), good_rate_steamer=round(hr_s,1),
    hit_rate_diff=sprintf("%+.1f [%+.1f,%+.1f]%s", mean(dh), quantile(dh,.025), quantile(dh,.975),
                          ifelse(quantile(dh,.025)>0," REAL","")),
    surplus_model=round(sur_m,3), surplus_steamer=round(sur_s,3),
    surplus_diff=sprintf("%+.3f [%+.3f,%+.3f]%s", mean(ds), quantile(ds,.025), quantile(ds,.975),
                         ifelse(quantile(ds,.025)>0," REAL","")))
}
pace_hr <- me$HR/(W*14); pace_sb <- me$SB/(W*14)
strm <- function(u) u[is.na(adp) | adp > 150]                # availability filter
ev <- rbind(
  lens(uhr, "lam_hr", "hr", 1L, "phr_pa", pace_hr, "HR")[,        pool := "top150"],
  lens(usb, "lam_sb", "sb", 1L, "psb_pa", pace_sb, "SB")[,        pool := "top150"],
  lens(usb, "lam_sb", "sb", 2L, "psb_pa", pace_sb, "SB")[,        pool := "top150"],
  lens(strm(uhr), "lam_hr", "hr", 1L, "phr_pa", pace_hr, "HR")[,  pool := "streamable"],
  lens(strm(usb), "lam_sb", "sb", 1L, "psb_pa", pace_sb, "SB")[,  pool := "streamable"],
  lens(strm(usb), "lam_sb", "sb", 2L, "psb_pa", pace_sb, "SB")[,  pool := "streamable"])
# AVG: pace-surplus only (binary bar is AB-perverse). Surplus = H - .2564*AB per pick.
avg_lens <- function(dav0, pool_lbl) {
  dav <- copy(dav0); dav[, tb := runif(.N)]
  dav[, `:=`(mval = lam_h - me$AVG*exp_ab, sval = (pavg - me$AVG)*pab*pag*g)]
  t5a <- function(rcol) dav[, { o<-order(-get(rcol), tb); kk<-min(5,.N)
      .(sur=sum(h[o][1:kk] - me$AVG*ab[o][1:kk]), tot=kk) }, by=.(season, week_start)]
  am <- t5a("mval"); as_ <- t5a("sval")
  ja <- merge(am, as_, by=c("season","week_start"), suffixes=c(".m",".s"))
  dsa <- replicate(1500, { i<-sample.int(nrow(ja),nrow(ja),TRUE)
        sum(ja$sur.m[i])/sum(ja$tot.m[i]) - sum(ja$sur.s[i])/sum(ja$tot.s[i]) })
  data.table(category="AVG", thresh=NA_integer_,
    good_rate_model=NA_real_, good_rate_steamer=NA_real_,
    hit_rate_diff="n/a (AB-perverse, surplus only)",
    surplus_model=round(sum(ja$sur.m)/sum(ja$tot.m),3), surplus_steamer=round(sum(ja$sur.s)/sum(ja$tot.s),3),
    surplus_diff=sprintf("%+.3f [%+.3f,%+.3f]%s", mean(dsa), quantile(dsa,.025), quantile(dsa,.975),
                         ifelse(quantile(dsa,.025)>0," REAL","")), pool=pool_lbl)
}
ev <- rbind(ev, avg_lens(uav, "top150"), avg_lens(strm(uav), "streamable"), fill=TRUE)

fwrite(paces,    file.path(OUT, "ghw_thresholds.csv"))
fwrite(allbins,  file.path(OUT, "ghw_calibration.csv"))
fwrite(ev,       file.path(OUT, "ghw_eval.csv"))

cat(sprintf("\n== PACE (Main Event 80th pctile / %.1f weeks / 14 starters; Jensen caveat applies) ==\n", W))
print(paces, row.names=FALSE)
cat("\n== CALIBRATION (LOSO weekly P(>=k) vs realized, decile bins) ==\n")
for (r in runs) cat(" ", r$slope, "\n")
print(dcast(allbins, bin ~ category + thresh, value.var=c("p_pred","p_real"))[order(bin)], digits=2, row.names=FALSE)
cat("\n== BRIER: model vs Steamer-only (positive improve = model better) ==\n")
print(allbrier, row.names=FALSE)
cat("\n== TOP-5 LENS: good-week hit rate + pace surplus per slot-week ==\n")
print(ev, row.names=FALSE)
