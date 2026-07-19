#!/usr/bin/env Rscript
# compare_streamonator_extended.R
# ---------------------------------------------------------------------------
# The article-grade evaluation pass, all on ONE window (2016-2025, ex-2020):
#  (1) category win-rates vs Steamer at game/half/week (replaces the 2022-25
#      numbers so every article figure shares one provenance)
#  (2) week-horizon win-rates vs random / L14 / L30 (intro baseline table)
#  (3) K-SWEEP: top-1/3/5/10 win-rate vs Steamer (is K=5 cherry-picked?)
#  (4) AVAILABILITY FILTER: same test on the streamable pool only
#      (preseason NFBC ADP > 150 or undrafted -- who you can actually add)
#  (5) TALENT-DECILE comparator: realized per-game production ratio, top vs
#      bottom decile of the Steamer projection alone (context for the 2.6x
#      matchup-decile figure: how much does TALENT separate on the same scale?)
# Panel machinery identical to compare_streamonator_v2 (dynamic pools, tuned
# shrinkage, modal-7 slot, ADP prior in SB). Panel cached to scratch RDS.
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
SCRATCH <- "/private/tmp/claude-501/-Users-ckaufman-Documents-New-project/b1414f12-4c91-4780-a4ed-5fb1e469e838/scratchpad"
dir.create(SCRATCH, showWarnings = FALSE, recursive = TRUE)
PANEL_RDS <- file.path(SCRATCH, "ext_panel.rds")
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))
MB <- c(hr = 100, avg = 500, sb = 150); EMULT <- 0.5

if (file.exists(PANEL_RDS)) {
  gl <- readRDS(PANEL_RDS); cat("panel loaded from cache:", nrow(gl), "rows\n")
} else {
files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id"))
raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])
assign("raw", raw, envir = .GlobalEnv)

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
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb),
   by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA, proj_ab_pa=AB/PA, proj_pa=PA)],
              by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE)
gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch  + MB["avg"]*proj_avg)  /(cab + MB["avg"]) - proj_avg]
gl[, bat_td_dhr  := (chr + MB["hr"] *proj_hr_pa)/(cpa + MB["hr"])  - proj_hr_pa]
gl[, bat_td_dsb  := (csb + MB["sb"] *proj_sb_pa)/(cpa + MB["sb"])  - proj_sb_pa]

ipf <- fread(file.path(ROOT,"data/processed/park_factors/park_factors_savant_style_with_id.csv"))
ipf[, `:=`(year_start = as.integer(sub("-.*","",years_used)), year_end = as.integer(sub(".*-","",years_used)))]
tmap <- fread(file.path(ROOT,"data/raw/mlbam_team_map.csv"))
ipf2 <- merge(ipf[!is.na(bacon_idx_100), .(team, year_start, year_end, bacon=bacon_idx_100, hridx=hr_idx_100)],
              tmap, by.x="team", by.y="club")
ipf2 <- ipf2[, .(home_team_id=team_id, year_start, year_end, bacon, hridx)]
gl[, home_team_id := fifelse(home==1L, team_id, opp_team_id)]
er <- ipf2[gl[, .(rid=.I, home_team_id, season)], on=.(home_team_id), allow.cartesian=TRUE]
er <- er[season >= year_start & season <= year_end][order(rid, -year_start)][!duplicated(rid)]
gl[er$rid, `:=`(park_bacon = (er$bacon-100)/100, park_hr = (er$hridx-100)/100)]

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
gl <- jn(gl, copy(pit_h),    "pit_h_td",    "opp_starter_id")
gl <- jn(gl, copy(pit_hr),   "pit_hr_td",   "opp_starter_id")
gl <- jn(gl, copy(pit_sb),   "pit_sb_td",   "opp_starter_id")
gl <- merge(gl, pitpy, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rppy,  by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, adp[, .(season, person_id, adp_z, nfbc_adp)], by=c("season","person_id"), all.x=TRUE)
gl[, adp_min := min(adp_z, na.rm=TRUE), by=season]
gl[is.na(adp_z), adp_z := adp_min]
gl <- gl[season >= 2016 & season != 2020 & !is.na(park_bacon)]
for (cn in c("team_bab_td","team_hr_td","team_sb_td","pit_h_td","pit_hr_td","pit_sb_td","pit_k_py","rp_py"))
  gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]

SPECS <- list(
  h  = c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
  hr = c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
  sb = c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","pit_sb_td","adp_z"))
for (y in names(SPECS)) gl[[paste0("p_",y)]] <- NA_real_
for (s in unique(gl$season)) for (y in names(SPECS)) {
  fit <- glm(reformulate(SPECS[[y]], y), poisson, gl[season != s])
  gl[season == s, (paste0("p_",y)) := predict(fit, .SD, type="response")]
}
gl[, blend_sb  := (csb + MB["sb"]*proj_sb_pa)/(cpa + MB["sb"])]
gl[, blend_hr  := (chr + MB["hr"]*proj_hr_pa)/(cpa + MB["hr"])]
gl[, blend_avg := (ch  + MB["avg"]*proj_avg)/(cab + MB["avg"])]
lgavg <- gl[, .(lg = sum(h)/sum(ab)), by=season]
gl <- merge(gl, lgavg, by="season")
gl[, dow := as.integer(game_date - week_start)][, block := fifelse(dow<=3,"MonThu","FriSun")]
saveRDS(gl, PANEL_RDS); cat("panel built + cached:", nrow(gl), "rows\n")
}

# trailing L14/L30 (naive recency strategies)
files2 <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
day <- rbindlist(lapply(files2, function(p) fread(p, select=c("game_pk","person_id","game_date","pa","ab","h","hr","sb"))))
day <- unique(day, by=c("game_pk","person_id"))
day[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","sb")) day[[cn]] <- n0(day[[cn]])
day <- day[, .(g=.N, sb=sum(sb), hr=sum(hr), h=sum(h), ab=sum(ab), pa=sum(pa)), by=.(person_id, season, game_date)]
setorder(day, person_id, season, game_date)
day[, `:=`(cg=cumsum(g), csb2=cumsum(sb), chr2=cumsum(hr), chh=cumsum(h), cabb=cumsum(ab), cpaa=cumsum(pa)), by=.(person_id, season)]
ckey <- day[, .(person_id, season, game_date, cg, csb2, chr2, chh, cabb, cpaa)]; setkey(ckey, person_id, season, game_date)
getc <- function(q) {
  r <- ckey[q, on=.(person_id, season, game_date), roll=TRUE]
  for (cn in c("cg","csb2","chr2","chh","cabb","cpaa")) r[is.na(get(cn)), (cn) := 0L]
  r
}

memb <- gl[, .(bsb=blend_sb[1], bhr=blend_hr[1], bavg=blend_avg[1]), by=.(season, week_start, person_id)]
memb[, `:=`(rk_sb=frank(-bsb, ties.method="first"), rk_hr=frank(-bhr, ties.method="first"),
            rk_avg=frank(-bavg, ties.method="first")), by=.(season, week_start)]

CATS <- list(
  sb  = list(pred="p_sb", out=quote(sum(sb)), st=quote(proj_sb_pa[1]*.N), rk="rk_sb",  t="sb",  lab="SB",  proj="proj_sb_pa"),
  hr  = list(pred="p_hr", out=quote(sum(hr)), st=quote(proj_hr_pa[1]*.N), rk="rk_hr",  t="hr",  lab="HR",  proj="proj_hr_pa"),
  avg = list(pred="p_h",  out=quote(sum(h) - lg[1]*sum(ab)), st=quote((proj_avg[1]-lg[1])*.N*proj_ab_pa[1]), rk="rk_avg", t="avg", lab="AVG", proj="proj_avg"))

wr_ci <- function(j, B=1200) {   # j: pool, m, b, wk
  jw <- j[, .(m=sum(m), b=sum(b)), by=wk]; W <- nrow(jw)
  pt <- 100*mean(fifelse(jw$m>jw$b, 1, fifelse(jw$m==jw$b, 0.5, 0)))
  bs <- replicate(B, { s<-sample.int(W,W,TRUE); 100*mean(fifelse(jw$m[s]>jw$b[s],1,fifelse(jw$m[s]==jw$b[s],0.5,0))) })
  sprintf("%.0f [%.0f,%.0f]", pt, quantile(bs,.025), quantile(bs,.975))
}
res <- list(); ksw <- list(); av <- list(); td <- list()
for (cat_ in names(CATS)) {
  cc <- CATS[[cat_]]
  pop <- memb[get(cc$rk) <= 150, .(season, week_start, person_id)]
  gc <- merge(gl, pop, by=c("season","week_start","person_id"))
  # (5) talent-decile ratio, game level: decile by Steamer projection alone
  gc[, tdec := cut(get(cc$proj), quantile(get(cc$proj), 0:10/10), include.lowest=TRUE, labels=1:10)]
  o <- switch(cat_, sb=gc$sb, hr=gc$hr, avg=gc$h/pmax(gc$ab,1))
  tt <- data.table(dec=gc$tdec, o=o)[, .(m=mean(o)), by=dec][order(dec)]
  td[[cat_]] <- sprintf("%s talent decile top/bottom = %.2f", cc$lab, tt[dec==10,m]/max(tt[dec==1,m], 1e-9))
  for (hz in c("game","half","week")) {
    byc <- switch(hz, game=c("season","person_id","game_date"),
                      half=c("season","person_id","week_start","block"),
                      week=c("season","person_id","week_start"))
    u <- gc[, .(out=eval(cc$out), pred=sum(get(cc$pred)), steamer=eval(cc$st),
                week_start=week_start[1], d0=min(game_date), adp=nfbc_adp[1]), by=byc]
    u[, pool := switch(hz, game=as.character(d0), half=paste(week_start, d0>week_start+3), week=as.character(week_start))]
    u[, tb := runif(.N)]
    if (hz == "week") {
      for (k in c(14L, 30L)) {
        a <- getc(u[, .(person_id, season, game_date = d0 - 1L)])
        b <- getc(u[, .(person_id, season, game_date = d0 - 1L - k)])
        u[[paste0("t",k)]] <- switch(cc$t,
          sb  = (a$csb2-b$csb2)/pmax(a$cg-b$cg,1),
          hr  = (a$chr2-b$chr2)/pmax(a$cpaa-b$cpaa,1),
          avg = (a$chh-b$chh)/pmax(a$cabb-b$cabb,1))
      }
    }
    topk <- function(d, rcol, K) d[, { o<-order(-get(rcol), tb); k<-min(K,.N)
                                       .(v=sum(out[o][1:k]), pm=mean(out)*min(K,.N), wk=week_start[1]) }, by=pool]
    sel_m <- topk(u, "pred", 5); sel_s <- topk(u, "steamer", 5)
    j <- merge(sel_m[, .(pool, m=v, wk)], sel_s[, .(pool, b=v)], by="pool")
    row <- data.table(cat=cc$lab, horizon=hz, win_steamer = wr_ci(j))
    if (hz == "week") {
      jr <- merge(sel_m[, .(pool, m=v, wk)], sel_m[, .(pool, b=pm)], by="pool")
      row$win_random <- wr_ci(jr)
      for (k in c(14,30)) {
        sk <- topk(u, paste0("t",k), 5)
        jk <- merge(sel_m[, .(pool, m=v, wk)], sk[, .(pool, b=v)], by="pool")
        row[[paste0("win_L",k)]] <- wr_ci(jk)
      }
      for (K in c(1,3,5,10)) {
        sm <- topk(u, "pred", K); ss <- topk(u, "steamer", K)
        jK <- merge(sm[, .(pool, m=v, wk)], ss[, .(pool, b=v)], by="pool")
        ksw[[paste0(cat_,K)]] <- data.table(cat=cc$lab, K=K, win_steamer=wr_ci(jK))
      }
      ua <- u[is.na(adp) | adp > 150]                       # the streamable pool
      sma <- topk(ua, "pred", 5); ssa <- topk(ua, "steamer", 5)
      ja <- merge(sma[, .(pool, m=v, wk)], ssa[, .(pool, b=v)], by="pool")
      av[[cat_]] <- data.table(cat=cc$lab, pool="ADP>150 or undrafted",
                               n_hw=nrow(ua), win_steamer=wr_ci(ja))
    }
    res[[paste0(cat_,hz)]] <- row
  }
}
cat("\n== (1) win-rate vs Steamer by horizon (top-5, 2016-2025 ex-2020) ==\n")
print(rbindlist(res, fill=TRUE), row.names=FALSE)
cat("\n== (3) K-sweep, week horizon ==\n")
print(dcast(rbindlist(ksw), cat ~ K, value.var="win_steamer"), row.names=FALSE)
cat("\n== (4) availability-filtered (streamable pool), week, top-5 ==\n")
print(rbindlist(av), row.names=FALSE)
cat("\n== (5) talent-decile comparators (game level) ==\n")
for (x in td) cat(" ", x, "\n")
