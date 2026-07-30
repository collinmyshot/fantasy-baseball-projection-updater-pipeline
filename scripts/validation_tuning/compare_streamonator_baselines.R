#!/usr/bin/env Rscript
# compare_streamonator_baselines.R
# ---------------------------------------------------------------------------
# ⚠ THIS IS THE **HITTER** STREAMONATOR, NOT THE SP ONE.
#   Despite sitting next to the SP scripts, all three compare_streamonator_*.R
#   files evaluate the HITTER tool (SB / HR / AVG on hitter_game_logs).
#   The SP-side equivalent is streamonator_v1_v2_comparison.R. Don't mix them:
#   different data spine, different target metric, different baselines.
# ---------------------------------------------------------------------------
# THE HEADLINE COMPARISON: streamonator (live per-game engines) vs
#   (a) RANDOM  -- pick-acc vs 50%; top-5 vs pool-average-five
#   (b) L14/L30 -- rank purely by trailing 14/30-day category rate (naive
#                  recency streamer; ties broken at random, seeded)
#   (c) STEAMER -- preseason projected rate x games
# Categories on their live populations (top-150 by proj rate/season, started
# games), horizons game/half-week/week, LOSO 2022-2025.
# Metrics: gap-10 pick-acc; top-5 selection lift; WEEK WIN-RATE = share of
# pools where model's top-5 out-produced the baseline's top-5 (ties=0.5).
#
# Output: streamonator_baseline_comparison.csv
#
# ── RESULT (run 2026-07-04) — v1 of the four-way. SUPERSEDED by v2. ─────────
#   Weekly top-5 win-rates (model vs strategy, ties = 0.5):
#     vs RANDOM      82-96% everywhere
#     vs L14 / L30   HR 79-89%, AVG 67-82%, SB 54-64%
#                    -> naive recency streaming is the WORST strategy tested
#                       (pick-acc ~50-52%), beaten even by plain Steamer
#     vs STEAMER     HR 63/67/68% and AVG 63/61/66% (game/half/week, all
#                    CIs above 50) = beats projections ~2/3 of weeks
#                    SB does NOT beat Steamer weekly: 53/50/44
#   Read on SB: its edge is real in TOTAL production but CONCENTRATED in
#   green-light weeks and ties elsewhere. SB = spot tool; HR/AVG = every-week
#   tools.
#
#   ⚠ SB LATER FLIPPED. compare_streamonator_v2.R fixes the design issues below
#   and SB goes 44 -> 67% weekly win-rate vs Steamer. Quote v2, not this file.
#
# ── KNOWN LIMITATIONS OF THIS VERSION (the v2 to-do list) ───────────────────
#   * static top-150 populations miss in-season risers  <- biggest one
#   * iPF era factors carry mild backtest look-ahead
#   * shrinkage pseudo-counts untuned
#   * one-game slot memory
#   * ceiling tables are retrodictive upper bounds
#   * tie-breaks deterministic (fixed here with seeded random)
#   * multiple comparisons unadjusted
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))

files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_catcher_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id"))
raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])

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
team_bab <- mk_todate(quote(h-hr), quote(pmax(ab-so-hr,0L)), "opp_team_id", 2000)
team_hr  <- mk_todate(quote(hr),   quote(pa),                "opp_team_id", 3000)
pit_h    <- mk_todate(quote(h),    quote(pa),                "opp_starter_id", 400)
pit_hr   <- mk_todate(quote(hr),   quote(pa),                "opp_starter_id", 400)
onf <- quote(singles + bb + hbp)
team_sb  <- mk_todate(quote(sb), onf, "opp_team_id",    750)
cat_sb   <- mk_todate(quote(sb), onf, "opp_catcher_id", 400)
pit_sb   <- mk_todate(quote(sb), onf, "opp_starter_id", 150)

gl <- raw[started == TRUE & !is.na(lineup_slot)]
gl[, home := as.integer(is_home)]
gl[, lhp := as.integer(opp_starter_throws=="L")]
gl[, platoon_adv := as.integer(bat_side=="S" | (bat_side=="L" & opp_starter_throws=="R") |
                               (bat_side=="R" & opp_starter_throws=="L"))]
gl[, week_start := game_date - (as.integer(format(game_date,"%u"))-1L)]
setorder(gl, person_id, season, game_date)
gl[, slot_pit := shift(lineup_slot, 1, type="lag"), by=.(person_id, season)]
gl[is.na(slot_pit), slot_pit := lineup_slot]
slot_pa <- gl[, .(pa_g = mean(pa)), by=lineup_slot]
gl <- merge(gl, slot_pa[, .(slot_pit=lineup_slot, exp_pa=pa_g)], by="slot_pit")
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa), by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA, proj_ab_pa=AB/PA, proj_pa=PA)],
              by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE)
gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch + 900*proj_avg)/(cab + 900) - proj_avg]
gl[, bat_td_dhr  := (chr + 170*proj_hr_pa)/(cpa + 170) - proj_hr_pa]

ipf <- fread(file.path(ROOT,"data/processed/park_factors/park_factors_savant_style_with_id.csv"))
ipf[, `:=`(year_start = as.integer(sub("-.*","",years_used)), year_end = as.integer(sub(".*-","",years_used)))]
tmap <- fread(file.path(ROOT,"data/raw/mlbam_team_map.csv"))
ipf <- merge(ipf[!is.na(bacon_idx_100), .(team, year_start, year_end, bacon=bacon_idx_100, hridx=hr_idx_100)],
             tmap, by.x="team", by.y="club")
ipf <- ipf[, .(home_team_id=team_id, year_start, year_end, bacon, hridx)]
gl[, home_team_id := fifelse(home==1L, team_id, opp_team_id)]
er <- ipf[gl[, .(rid=.I, home_team_id, season)], on=.(home_team_id), allow.cartesian=TRUE]
er <- er[season >= year_start & season <= year_end][order(rid, -year_start)][!duplicated(rid)]
gl[er$rid, `:=`(park_bacon = (er$bacon-100)/100, park_hr = (er$hridx-100)/100)]

pitpy <- fread(file.path(ROOT,"data/raw/pitcher_obp_against_2021_2025.csv"))[bf>=100,
          .(season, opp_starter_id=person_id, pit_k_py=so_pitched/bf)]
pitpy[, season := season+1]
rppy <- rbindlist(lapply(2021:2025, function(y){
  d <- fread(sprintf("%s/data/raw/savant_running_game/pitcher_running_%d.csv", ROOT, y))
  d[, .(season=y+1, opp_starter_id=player_id, rp_py=runs_prevented_on_running_attr)] }))
jn <- function(g, td, nm, keys) { setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys, "game_date"), all.x=TRUE) }
gl <- jn(gl, copy(team_bab), "team_bab_td", "opp_team_id")
gl <- jn(gl, copy(team_hr),  "team_hr_td",  "opp_team_id")
gl <- jn(gl, copy(team_sb),  "team_sb_td",  "opp_team_id")
gl <- jn(gl, copy(pit_h),    "pit_h_td",    "opp_starter_id")
gl <- jn(gl, copy(pit_hr),   "pit_hr_td",   "opp_starter_id")
gl <- jn(gl, copy(pit_sb),   "pit_sb_td",   "opp_starter_id")
gl <- jn(gl, copy(cat_sb),   "cat_sb_td",   "opp_catcher_id")
gl <- merge(gl, pitpy, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rppy,  by=c("season","opp_starter_id"), all.x=TRUE)
gl <- gl[season >= 2022 & !is.na(park_bacon)]
for (cn in c("team_bab_td","team_hr_td","team_sb_td","pit_h_td","pit_hr_td","pit_sb_td","cat_sb_td","pit_k_py","rp_py"))
  gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]

SPECS <- list(
  h  = c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
  hr = c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
  sb = c("proj_sb_pa","exp_pa","lhp","home","rp_py","team_sb_td","cat_sb_td","pit_sb_td"))
for (y in names(SPECS)) gl[[paste0("p_",y)]] <- NA_real_
for (s in unique(gl$season)) for (y in names(SPECS)) {
  fit <- glm(reformulate(SPECS[[y]], y), poisson, gl[season != s])
  gl[season == s, (paste0("p_",y)) := predict(fit, .SD, type="response")]
}

# trailing L14/L30 category rates (strictly before unit start; naive recency)
day <- raw[, .(g=.N, sb=sum(sb), hr=sum(hr), h=sum(h), ab=sum(ab), pa=sum(pa)), by=.(person_id, season, game_date)]
setorder(day, person_id, season, game_date)
day[, `:=`(cg=cumsum(g), csb=cumsum(sb), chr=cumsum(hr), chh=cumsum(h), cabb=cumsum(ab), cpaa=cumsum(pa)), by=.(person_id, season)]
ckey <- day[, .(person_id, season, game_date, cg, csb, chr, chh, cabb, cpaa)]; setkey(ckey, person_id, season, game_date)
getc <- function(q) {
  r <- ckey[q, on=.(person_id, season, game_date), roll=TRUE]
  r[, .(cg=fifelse(is.na(cg),0L,cg), csb=fifelse(is.na(csb),0L,csb), chr=fifelse(is.na(chr),0L,chr),
        chh=fifelse(is.na(chh),0L,chh), cabb=fifelse(is.na(cabb),0L,cabb), cpaa=fifelse(is.na(cpaa),0L,cpaa))]
}
add_trailing <- function(u, d0col) {
  for (k in c(14L, 30L)) {
    a <- getc(u[, .(person_id, season, game_date = get(d0col) - 1L)])
    b <- getc(u[, .(person_id, season, game_date = get(d0col) - 1L - k)])
    u[[paste0("sb", k)]]  <- (a$csb - b$csb) / pmax(a$cg - b$cg, 1)
    u[[paste0("hr", k)]]  <- (a$chr - b$chr) / pmax(a$cpaa - b$cpaa, 1)
    u[[paste0("avg", k)]] <- (a$chh - b$chh) / pmax(a$cabb - b$cabb, 1)
  }
  u
}

lgavg <- gl[, .(lg = sum(h)/sum(ab)), by=season]
gl <- merge(gl, lgavg, by="season")
gl[, dow := as.integer(game_date - week_start)][, block := fifelse(dow<=3,"MonThu","FriSun")]

CATS <- list(
  sb  = list(proj="proj_sb_pa", pred="p_sb", out=quote(sum(sb)), lab="SB"),
  hr  = list(proj="proj_hr_pa", pred="p_hr", out=quote(sum(hr)), lab="HR"),
  avg = list(proj=NA,           pred="p_h",  out=quote(sum(h) - lg[1]*sum(ab)), lab="AVG"))

res <- list()
for (cat in names(CATS)) {
  cc <- CATS[[cat]]
  pop <- if (cat == "avg") {
    pr <- unique(gl[proj_pa >= 200, .(season, person_id, proj_avg)]); pr[, rk := frank(-proj_avg, ties.method="first"), by=season]
    pr[rk<=150, .(season, person_id)]
  } else {
    pr <- unique(gl[, .(season, person_id, v=get(cc$proj))]); pr[, rk := frank(-v, ties.method="first"), by=season]
    pr[rk<=150, .(season, person_id)]
  }
  gc <- merge(gl, pop, by=c("season","person_id"))
  for (hz in c("game","half","week")) {
    byc <- switch(hz, game=c("season","person_id","game_date"),
                      half=c("season","person_id","week_start","block"),
                      week=c("season","person_id","week_start"))
    u <- gc[, .(out=eval(cc$out), games=.N, pred=sum(get(cc$pred)),
                steamer = if (cat=="avg") (proj_avg[1]-lg[1])*.N*proj_ab_pa[1] else get(cc$proj)[1]*.N,
                week_start=week_start[1], d0=min(game_date)), by=byc]
    u[, pool := switch(hz, game=as.character(d0), half=paste(week_start, d0>week_start+3), week=as.character(week_start))]
    u <- add_trailing(u, "d0")
    u[, `:=`(rk14 = get(paste0(cat,"14")), rk30 = get(paste0(cat,"30")))]
    u[, tb := runif(.N)]                                     # fair random tie-break for ALL rankers
    top5 <- function(rcol) u[, { o <- order(-get(rcol), tb); k <- min(5,.N)
                                 .(v=sum(out[o][1:k]), n=k, pm=mean(out)*min(5,.N), wk=week_start[1]) }, by=pool]
    pacc <- function(rcol) {
      ws <- u[, { n<-.N; g<-10
        if (n>g) { o1<-order(-get(rcol), tb); rv<-out[o1]
          .(num=sum(rv[1:(n-g)]>rv[(g+1):n])+0.5*sum(rv[1:(n-g)]==rv[(g+1):n]), den=n-g) } else .(num=0,den=0) }, by=pool]
      100*sum(ws$num)/sum(ws$den)
    }
    sm <- top5("pred"); s_st <- top5("steamer"); s14 <- top5("rk14"); s30 <- top5("rk30")
    wr <- function(sbase) {
      j <- merge(sm[, .(pool, m=v, wk)], sbase[, .(pool, b=v)], by="pool")
      jw <- j[, .(m=sum(m), b=sum(b)), by=wk]; W <- nrow(jw)
      pt <- 100*mean(fifelse(jw$m>jw$b, 1, fifelse(jw$m==jw$b, 0.5, 0)))
      bs <- replicate(2000, { s<-sample.int(W,W,TRUE); 100*mean(fifelse(jw$m[s]>jw$b[s],1,fifelse(jw$m[s]==jw$b[s],0.5,0))) })
      c(pt, quantile(bs,.025), quantile(bs,.975))
    }
    wrnd <- { j <- sm[, .(m=v, r=pm, wk)]; jw <- j[, .(m=sum(m), r=sum(r)), by=wk]
              100*mean(fifelse(jw$m>jw$r, 1, fifelse(jw$m==jw$r, 0.5, 0))) }
    w_st <- wr(s_st); w14 <- wr(s14); w30 <- wr(s30)
    res[[length(res)+1]] <- data.table(cat=cc$lab, horizon=hz,
      acc_model=round(pacc("pred"),1), acc_steamer=round(pacc("steamer"),1),
      acc_L14=round(pacc("rk14"),1), acc_L30=round(pacc("rk30"),1),
      win_vs_random=round(wrnd), win_vs_steamer=sprintf("%.0f [%.0f,%.0f]", w_st[1], w_st[2], w_st[3]),
      win_vs_L14=sprintf("%.0f [%.0f,%.0f]", w14[1], w14[2], w14[3]),
      win_vs_L30=sprintf("%.0f [%.0f,%.0f]", w30[1], w30[2], w30[3]))
  }
}
out <- rbindlist(res)
cat("pick-acc: gap-10 pairwise accuracy (50 = random). win_vs_X: % of weeks the model's top-5 out-produced X's top-5 (ties=0.5).\n\n")
print(out, row.names=FALSE)
fwrite(out, file.path(ROOT, "data/processed/streamonator_baseline_comparison.csv"))
