#!/usr/bin/env Rscript
# build_hitter_article_winrates.R
# ---------------------------------------------------------------------------
# ONE-PROVENANCE validation ladder for the Hitter Streamonator methodology,
# SOBER-FIRST (per user decision):
#   (1) Steamer-alone pairwise accuracy, gap-10  -> the coin-flip floor
#   (2) model pairwise accuracy, gap-10          -> tiny per-decision edge
#   (3) top-5 WIN-RATE vs Steamer (share of weeks) + CI
#   (4) top-5 PRODUCTION LIFT vs Steamer (%, or AVG points) + CI
# For SB / HR / AVG at game / half-week / week. Panel = the shipped v2 machinery
# (dynamic pools by to-date talent, tuned shrinkage EMULT=0.5, modal-7 slot,
# ADP prior in SB, bat_td deltas) with HAND-SPLIT iPF park in the HR + AVG
# engines. Window 2016-2025 ex-2020. SB effects carry an era caveat (2023 rules).
# Output: data/processed/hitter_article/winrate_ladder.csv
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
OUT  <- file.path(ROOT, "data/processed/hitter_article"); dir.create(OUT, showWarnings=FALSE, recursive=TRUE)
SCRATCH <- "/private/tmp/claude-501/-Users-ckaufman-Documents-New-project/57b453dd-83f5-4aa1-abf9-e40500bf9c39/scratchpad"
dir.create(SCRATCH, showWarnings=FALSE, recursive=TRUE)
PANEL <- file.path(SCRATCH, "winrate_panel_handsplit.rds")
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))
MB <- c(hr=100, avg=500, sb=150); EMULT <- 0.5

if (file.exists(PANEL)) { gl <- readRDS(PANEL); cat("panel cache:", nrow(gl), "rows\n") } else {
files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id")); raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])
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
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb), by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA, proj_ab_pa=AB/PA, proj_pa=PA)],
              by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE); gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch  + MB["avg"]*proj_avg)  /(cab + MB["avg"]) - proj_avg]
gl[, bat_td_dhr  := (chr + MB["hr"] *proj_hr_pa)/(cpa + MB["hr"])  - proj_hr_pa]
gl[, bat_td_dsb  := (csb + MB["sb"] *proj_sb_pa)/(cpa + MB["sb"])  - proj_sb_pa]

# ---- HAND-SPLIT iPF park (effective batting hand) -----------------------------
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
gl <- jn(gl, copy(pit_h),    "pit_h_td",    "opp_starter_id")
gl <- jn(gl, copy(pit_hr),   "pit_hr_td",   "opp_starter_id")
gl <- jn(gl, copy(pit_sb),   "pit_sb_td",   "opp_starter_id")
gl <- merge(gl, pitpy, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rppy,  by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, adp[, .(season, person_id, adp_z, nfbc_adp)], by=c("season","person_id"), all.x=TRUE)
gl <- gl[season >= 2016 & season != 2020 & !is.na(park_bacon)]
gl[, adp_z := fifelse(is.na(adp_z), min(adp_z, na.rm=TRUE), adp_z), by=season]
for (cn in c("team_bab_td","team_hr_td","team_sb_td","pit_h_td","pit_hr_td","pit_sb_td","pit_k_py","rp_py"))
  gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]

SPECS <- list(
  h  = c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
  hr = c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
  sb = c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","pit_sb_td","adp_z"))
gl[, platoon_adv := as.integer(bat_side=="S" | (bat_side=="L" & opp_starter_throws=="R") | (bat_side=="R" & opp_starter_throws=="L"))]
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
saveRDS(gl, PANEL); cat("panel built:", nrow(gl), "rows\n")
}

# ---- dynamic pools (top-150 by to-date blended talent as of week_start) --------
memb <- gl[, .(bsb=blend_sb[1], bhr=blend_hr[1], bavg=blend_avg[1]), by=.(season, week_start, person_id)]
memb[, `:=`(rk_sb=frank(-bsb,ties.method="first"), rk_hr=frank(-bhr,ties.method="first"),
            rk_avg=frank(-bavg,ties.method="first")), by=.(season, week_start)]

CATS <- list(
  SB  = list(pred="p_sb", out=quote(sum(sb)),               st=quote(proj_sb_pa[1]*.N), rk="rk_sb",  kind="count"),
  HR  = list(pred="p_hr", out=quote(sum(hr)),               st=quote(proj_hr_pa[1]*.N), rk="rk_hr",  kind="count"),
  AVG = list(pred="p_h",  out=quote(sum(h) - lg[1]*sum(ab)),st=quote((proj_avg[1]-lg[1])*.N*proj_ab_pa[1]), rk="rk_avg", kind="avg"))

pairacc <- function(u, rankcol, gap=10) {          # gap-conditioned within-pool accuracy
  ws <- u[, { n<-.N; if(n>gap){ o<-order(-get(rankcol)); rv<-out[o]
      .(num=sum(rv[1:(n-gap)]>rv[(gap+1):n])+0.5*sum(rv[1:(n-gap)]==rv[(gap+1):n]), den=n-gap) } else .(num=0,den=0) }, by=pool]
  100*sum(ws$num)/sum(ws$den) }
wr_ci <- function(jw, B=1500) {
  pt <- 100*mean(fifelse(jw$m>jw$s,1,fifelse(jw$m==jw$s,0.5,0)))
  bs <- replicate(B,{ i<-sample.int(nrow(jw),nrow(jw),TRUE); 100*mean(fifelse(jw$m[i]>jw$s[i],1,fifelse(jw$m[i]==jw$s[i],0.5,0))) })
  sprintf("%.0f [%.0f,%.0f]", pt, quantile(bs,.025), quantile(bs,.975)) }

res <- list()
for (cat_ in names(CATS)) {
  cc <- CATS[[cat_]]
  pop <- memb[get(cc$rk) <= 150, .(season, week_start, person_id)]
  gc <- merge(gl, pop, by=c("season","week_start","person_id"))
  for (hz in c("game","half","week")) {
    byc <- switch(hz, game=c("season","person_id","game_date"),
                      half=c("season","person_id","week_start","block"),
                      week=c("season","person_id","week_start"))
    u <- gc[, .(out=eval(cc$out), pred=sum(get(cc$pred)), steamer=eval(cc$st),
                H=sum(h), AB=sum(ab), adp=nfbc_adp[1], week_start=week_start[1], d0=min(game_date)), by=byc]
    u[, pool := switch(hz, game=as.character(d0), half=paste(week_start, d0>week_start+3), week=as.character(week_start))]
    u[, tb := runif(.N)]
    acc_st <- pairacc(u, "steamer"); acc_md <- pairacc(u, "pred")
    topk <- function(dd, rcol, K=5) dd[, { o<-order(-get(rcol), tb); k<-min(K,.N)
        .(v=sum(out[o][1:k]), H=sum(H[o][1:k]), AB=sum(AB[o][1:k]), wk=week_start[1]) }, by=pool]
    sm <- topk(u, "pred"); ss <- topk(u, "steamer")
    jw <- merge(sm[, .(pool, m=v, wk, Hm=H, ABm=AB)], ss[, .(pool, s=v, Hs=H, ABs=AB)], by="pool")
    jwk <- jw[, .(m=sum(m), s=sum(s), Hm=sum(Hm), ABm=sum(ABm), Hs=sum(Hs), ABs=sum(ABs)), by=wk]
    win <- wr_ci(jwk)
    # streamable pool: outside top-150 draft picks (undrafted or ADP>150)
    us <- u[is.na(adp) | adp > 150]
    sms <- topk(us, "pred"); sss <- topk(us, "steamer")
    jws <- merge(sms[, .(pool, m=v, wk)], sss[, .(pool, s=v)], by="pool")[, .(m=sum(m), s=sum(s)), by=wk][is.finite(m)]
    win_stream <- if (nrow(jws) > 5) wr_ci(jws) else "n/a"
    if (cc$kind == "avg") {                                   # AVG: report points, not %
      pt <- 1000*(sum(jwk$Hm)/sum(jwk$ABm) - sum(jwk$Hs)/sum(jwk$ABs))
      bs <- replicate(1500,{ i<-sample.int(nrow(jwk),nrow(jwk),TRUE)
        1000*(sum(jwk$Hm[i])/sum(jwk$ABm[i]) - sum(jwk$Hs[i])/sum(jwk$ABs[i])) })
      lift <- sprintf("%+.0f pts [%+.0f,%+.0f]", pt, quantile(bs,.025), quantile(bs,.975))
    } else {
      pt <- 100*(sum(jwk$m)-sum(jwk$s))/sum(jwk$s)
      bs <- replicate(1500,{ i<-sample.int(nrow(jwk),nrow(jwk),TRUE); 100*(sum(jwk$m[i])-sum(jwk$s[i]))/sum(jwk$s[i]) })
      lift <- sprintf("%+.1f%% [%+.1f,%+.1f]", pt, quantile(bs,.025), quantile(bs,.975))
    }
    res[[paste0(cat_,hz)]] <- data.table(category=cat_, horizon=hz,
        steamer_pairwise=round(acc_st,1), model_pairwise=round(acc_md,1),
        win_rate=win, win_rate_streamable=win_stream, lift=lift)
  }
}
allres <- rbindlist(res)
fwrite(allres, file.path(OUT, "winrate_ladder.csv"))
cat("\n== VALIDATION LADDER (sober-first), 2016-2025 ex-2020, hand-split park ==\n")
print(allres, row.names=FALSE)
cat("\nwrote", nrow(allres), "rows ->", file.path(OUT,"winrate_ladder.csv"), "\n")
