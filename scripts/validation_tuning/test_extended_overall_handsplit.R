#!/usr/bin/env Rscript
# test_shrunk_stacking.R -- improvement path #3
# ---------------------------------------------------------------------------
# Reliability-shrunk stacking for the OVERALL blend: per category, decompose
# pred_z = base_z (Steamer) + delta_z (model's deviation), then learn per-
# category coefficients (a_c, b_c) on TRAIN seasons only:
#     realized_z_c ~ 0 + a_c * base_z_c + b_c * delta_z_c
# b_c = how much of the model's deviation to trust (the reliability weight).
# Overall_shrunk = sum_c (a_c*base_z + b_c*delta_z). LOSO by season.
# Compare at week horizon: Steamer vs plain blend vs shrunk blend
# (gap-10 pick-acc + top-5 weekly win-rate, bootstrap by week).
# Panel = v2 machinery (identical to export_hitter_stream_engine.R).
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))
MB <- c(hr = 100, avg = 500, sb = 150); EMULT <- 0.5

files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id"))
raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])

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
onf <- quote(singles + bb + hbp)
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
gl[, platoon_adv := as.integer(bat_side=="S" | (bat_side=="L" & opp_starter_throws=="R") |
                               (bat_side=="R" & opp_starter_throws=="L"))]
gl[, week_start := game_date - (as.integer(format(game_date,"%u"))-1L)]
setorder(gl, person_id, season, game_date)
Mode7 <- function(v) { v <- v[!is.na(v)]; if (!length(v)) NA_integer_ else as.integer(names(sort(table(v), decreasing=TRUE))[1]) }
gl[, slot_lag := shift(lineup_slot, 1, type="lag"), by=.(person_id, season)]
gl[, slot_m7 := vapply(seq_len(.N), function(i) Mode7(slot_lag[max(1,i-6):i]), integer(1)), by=.(person_id, season)]
gl[is.na(slot_m7), slot_m7 := lineup_slot]
slot_pa <- gl[, .(pa_g = mean(pa)), by=lineup_slot]
gl <- merge(gl, slot_pa[, .(slot_m7=lineup_slot, exp_pa=pa_g)], by="slot_m7")
gl[, `:=`(s12 = as.integer(slot_m7<=2), s345 = as.integer(slot_m7>=3 & slot_m7<=5),
          s67 = as.integer(slot_m7>=6 & slot_m7<=7))]
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb),
   by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_obp=OBP, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA,
                     proj_r_pa=R/PA, proj_rbi_pa=RBI/PA, proj_ab_pa=AB/PA, proj_pa=PA)], by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE)
gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch  + MB["avg"]*proj_avg)  /(cab + MB["avg"]) - proj_avg]
gl[, bat_td_dhr  := (chr + MB["hr"] *proj_hr_pa)/(cpa + MB["hr"])  - proj_hr_pa]
gl[, bat_td_dsb  := (csb + MB["sb"] *proj_sb_pa)/(cpa + MB["sb"])  - proj_sb_pa]

# HAND-SPLIT iPF park (effective batting hand vs the starter) -- see build_park_factors_by_hand_era.R
hand <- fread(file.path(ROOT,"data/processed/park_factors/park_factors_by_hand_era.csv"))
gl[, home_team_id := fifelse(home==1L, team_id, opp_team_id)]
gl[, bat_hand_eff := fifelse(bat_side=="S", fifelse(opp_starter_throws=="L","R","L"), bat_side)]
gl[!bat_hand_eff %in% c("L","R"), bat_hand_eff := "R"]
hj <- hand[, .(home_team_id=team_id, hand, year_start, year_end, bacon_idx_hand, hr_idx_hand)]
er <- hj[gl[, .(rid=.I, home_team_id, hand=bat_hand_eff, season)], on=.(home_team_id, hand), allow.cartesian=TRUE]
er <- er[season >= year_start & season <= year_end][order(rid, -year_start)][!duplicated(rid)]
gl[er$rid, `:=`(park_bacon = (er$bacon_idx_hand-100)/100, park_hr = (er$hr_idx_hand-100)/100)]

pitpy <- fread(file.path(ROOT,"data/raw/pitcher_obp_against_2015_2025.csv"))[bf>=100,
          .(season, opp_starter_id=person_id, pit_k_py=so_pitched/bf)]
pitpy[, season := season+1]
rppy <- rbindlist(lapply(2016:2025, function(y){
  d <- fread(sprintf("%s/data/raw/savant_running_game/pitcher_running_%d.csv", ROOT, y))
  d[, .(season=y+1, opp_starter_id=player_id, rp_py=runs_prevented_on_running_attr)] }))
adp <- rbindlist(lapply(2016:2025, function(y)
  fread(sprintf("%s/data/raw/nfbc_adp_history/nfbc_adp_with_position_%d.csv", ROOT, y))[
    is_pitcher == FALSE & !is.na(mlbam_id) & !is.na(nfbc_adp),
    .(season=y, person_id=as.integer(mlbam_id), nfbc_adp)]))
adp <- unique(adp, by=c("season","person_id"))
adp[, adp_z := -as.numeric(scale(log(nfbc_adp))), by=season]

jn <- function(g, td, nm, keys) { setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys, "game_date"), all.x=TRUE) }
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
gl <- merge(gl, adp[, .(season, person_id, adp_z)], by=c("season","person_id"), all.x=TRUE)
gl[, adp_min := min(adp_z, na.rm=TRUE), by=season]
gl[is.na(adp_z), adp_z := adp_min]
gl <- gl[season >= 2016 & season != 2020 & !is.na(park_bacon)]
fillc <- c("team_bab_td","team_hr_td","team_sb_td","opp_ra_td","team_off_td",
           "pit_h_td","pit_hr_td","pit_sb_td","pit_k_py","rp_py")
for (cn in fillc) gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]

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
lgavg <- gl[, .(lg = sum(h)/sum(ab)), by=season]
gl <- merge(gl, lgavg, by="season")

uw <- gl[, .(r=sum(r), hr=sum(hr), rbi=sum(rbi), sb=sum(sb), av=sum(h)-lg[1]*sum(ab),
             pr=sum(p_r), phr=sum(p_hr), prbi=sum(p_rbi), psb=sum(p_sb),
             pav=sum(p_h) - lg[1]*sum(exp_pa*proj_ab_pa),
             br=proj_r_pa[1]*.N, bhr=proj_hr_pa[1]*.N, brbi=proj_rbi_pa[1]*.N, bsb=proj_sb_pa[1]*.N,
             bav=(proj_avg[1]-lg[1])*.N*proj_ab_pa[1]),
         by=.(season, person_id, week_start)]
uw[, pool := as.character(week_start)]
zin <- function(x, pool) { m <- ave(x, pool, FUN=mean); s <- ave(x, pool, FUN=sd); fifelse(is.na(s)|s==0, 0, (x-m)/s) }
CATS <- c("r","hr","rbi","sb","av")
for (v in c(CATS, paste0("p", CATS), paste0("b", CATS)))
  uw[[paste0("z_",v)]] <- zin(uw[[v]], uw$pool)
for (cc in CATS) uw[[paste0("d_",cc)]] <- uw[[paste0("z_p",cc)]] - uw[[paste0("z_b",cc)]]

# ---- per-category stacking coefficients (LOSO) ---------------------------------
uw[, `:=`(pred_shrunk = NA_real_)]
lam <- list()
for (s in unique(uw$season)) {
  tr <- uw[season != s]
  sh <- rep(0, nrow(uw[season == s]))
  for (cc in CATS) {
    f <- lm(reformulate(c(paste0("z_b",cc), paste0("d_",cc)), paste0("z_",cc)), tr)
    a <- coef(f)[paste0("z_b",cc)]; b <- coef(f)[paste0("d_",cc)]
    lam[[paste0(s,"_",cc)]] <- data.table(season_out=s, cat=cc, a=round(a,3), b=round(b,3))
    sh <- sh + a*uw[season == s][[paste0("z_b",cc)]] + b*uw[season == s][[paste0("d_",cc)]]
  }
  uw[season == s, pred_shrunk := sh]
}
lamt <- rbindlist(lam)
cat("Stacking coefficients (a = weight on Steamer z, b = trust in model delta), by held-out season:\n")
print(dcast(lamt, cat ~ season_out, value.var = "b"), row.names = FALSE)
cat("  (a-coefficients all ~", round(mean(lamt$a),2), ")\n\n")

uw[, pred_plain := z_pr + z_phr + z_prbi + z_psb + z_pav]
uw[, base := z_br + z_bhr + z_brbi + z_bsb + z_bav]
uw[, val  := z_r + z_hr + z_rbi + z_sb + z_av]
uw[, tb := runif(.N)]

cmp <- function(rankcol, label) {
  ws <- uw[, { n<-.N; g<-10
    if (n>g) { o1<-order(-base,tb); rv<-val[o1]; o2<-order(-get(rankcol),tb); re<-val[o2]
      .(numb=sum(rv[1:(n-g)]>rv[(g+1):n])+0.5*sum(rv[1:(n-g)]==rv[(g+1):n]),
        nume=sum(re[1:(n-g)]>re[(g+1):n])+0.5*sum(re[1:(n-g)]==re[(g+1):n]), den=n-g) } else .(numb=0,nume=0,den=0) }, by=pool]
  accb <- 100*sum(ws$numb)/sum(ws$den); acce <- 100*sum(ws$nume)/sum(ws$den); W <- nrow(ws)
  lf <- replicate(3000, { s<-sample.int(W,W,TRUE); 100*sum(ws$nume[s])/sum(ws$den[s])-100*sum(ws$numb[s])/sum(ws$den[s]) })
  sel <- uw[, { om<-order(-get(rankcol),tb); ob<-order(-base,tb); k<-min(5,.N)
                .(m=sum(val[om][1:k]), b=sum(val[ob][1:k])) }, by=pool]
  wrt <- 100*mean(fifelse(sel$m>sel$b, 1, fifelse(sel$m==sel$b, 0.5, 0)))
  bs <- replicate(2000, { s<-sample.int(nrow(sel),nrow(sel),TRUE)
        100*mean(fifelse(sel$m[s]>sel$b[s],1,fifelse(sel$m[s]==sel$b[s],0.5,0))) })
  cat(sprintf("  %-14s pick-acc lift %+0.2f [%+0.2f,%+0.2f] %s | top-5 win-rate vs Steamer %.0f%% [%.0f,%.0f] %s\n",
      label, acce-accb, quantile(lf,.025), quantile(lf,.975), if (quantile(lf,.025)>0) "REAL" else "null",
      wrt, quantile(bs,.025), quantile(bs,.975), if (quantile(bs,.025)>50) "REAL" else "null"))
}
cat(sprintf("== OVERALL at week horizon (n=%d hitter-weeks, 2022-2025) ==\n", nrow(uw)))
cmp("pred_plain",  "plain blend")
cmp("pred_shrunk", "SHRUNK blend")
