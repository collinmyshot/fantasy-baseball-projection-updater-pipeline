#!/usr/bin/env Rscript
# build_hitter_article_effects.R
# ---------------------------------------------------------------------------
# ONE-PROVENANCE effect tables for the Hitter Streamonator methodology.
# For all FIVE categories (HR, SB, AVG=h, R, RBI), emit p10->p90 multipliers
# in TWO columns:
#   LIVE    = shipped engine spec (engine_coefs.csv), features constructed the
#             way the deployed tool sees them: to-date entity rates (strictly
#             before today) + prior-year Savant; hand-split iPF park by batter.
#   CEILING = same features but opponent rates use the FULL current season
#             (hindsight upper bound). Park/slot/batter rows are identical to
#             LIVE by construction, so the table shows exactly which features
#             degrade live (the DIPS story) vs which are April-knowable.
# Window: 2016-2025 ex-2020 (matches the win-rate eval provenance).
# Output: data/processed/hitter_article/effect_tables.csv
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"; set.seed(42)
OUT  <- file.path(ROOT, "data/processed/hitter_article"); dir.create(OUT, showWarnings=FALSE, recursive=TRUE)
n0 <- function(x) fifelse(is.na(x), 0L, as.integer(x))

# ---- spine --------------------------------------------------------------------
files <- list.files(file.path(ROOT,"data/processed/hitter_game_logs"), pattern="hitter_game_logs.*csv$", full.names=TRUE)
raw <- rbindlist(lapply(files, function(p) fread(p, select=c("game_pk","game_date","person_id","position","started",
        "lineup_slot","is_home","team_id","opp_team_id","opp_starter_id","opp_starter_throws","bat_side",
        "pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles"))))
raw <- unique(raw, by=c("game_pk","person_id"))
raw <- raw[is.na(position) | position != "P"]
raw[, game_date := as.IDate(game_date)][, season := year(game_date)]
for (cn in c("pa","ab","h","hr","so","sb","r","rbi","bb","hbp","singles")) raw[[cn]] <- n0(raw[[cn]])
onf <- quote(singles + bb + hbp)

# ---- entity rates: to-date (LIVE) and full-season (CEILING) --------------------
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
mk_fullseason <- function(numexpr, denexpr, idcol) {          # hindsight: same-year rate, centered
  ag <- raw[!is.na(get(idcol)), .(num=sum(eval(numexpr)), den=sum(eval(denexpr))), by=.(season, ent=get(idcol))]
  lg <- ag[, .(lg_rate=sum(num)/sum(den)), by=season]
  ag <- merge(ag, lg, by="season")
  ag[, .(season, ent, feat = num/pmax(den,1) - lg_rate)]
}
TD <- list(
  team_bab = list(quote(h-hr), quote(pmax(ab-so-hr,0L)), "opp_team_id",    2000),
  team_hr  = list(quote(hr),   quote(pa),                "opp_team_id",    3000),
  team_sb  = list(onf,         NULL,                     "opp_team_id",     750),   # den handled below
  pit_h    = list(quote(h),    quote(pa),                "opp_starter_id",  400),
  pit_hr   = list(quote(hr),   quote(pa),                "opp_starter_id",  400),
  pit_sb   = list(onf,         NULL,                     "opp_starter_id",  150),
  team_off = list(quote(r),    quote(pa),                "team_id",        3000),
  opp_ra   = list(quote(r),    quote(pa),                "opp_team_id",    3000))
# sb-denominators are on-first opportunities
sbnum <- quote(sb)
td_live <- list(); td_ceil <- list()
for (nm in names(TD)) {
  a <- TD[[nm]]
  if (nm %in% c("team_sb","pit_sb")) { num <- sbnum; den <- onf } else { num <- a[[1]]; den <- a[[2]] }
  td_live[[nm]] <- mk_todate(num, den, a[[3]], a[[4]])
  td_ceil[[nm]] <- mk_fullseason(num, den, a[[3]])
}

# ---- startable panel ----------------------------------------------------------
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
gl[, `:=`(s12 = as.integer(slot_pit<=2), s345 = as.integer(slot_pit>=3 & slot_pit<=5),
          s67 = as.integer(slot_pit>=6 & slot_pit<=7))]
gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb),
   by=.(person_id, season)]

st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                     proj_avg=AVG, proj_obp=OBP, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA,
                     proj_r_pa=R/PA, proj_rbi_pa=RBI/PA, proj_ab_pa=AB/PA, proj_pa=PA)], by=c("season","person_id"))
gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE)
gl <- gl[!is.na(proj_avg)]
gl[, bat_td_davg := (ch  + 900*proj_avg)  /(cab + 900) - proj_avg]
gl[, bat_td_dhr  := (chr + 170*proj_hr_pa)/(cpa + 170) - proj_hr_pa]
gl[, bat_td_dsb  := (csb + 150*proj_sb_pa)/(cpa + 150) - proj_sb_pa]

# ---- HAND-SPLIT iPF park (effective batting hand vs the starter) ---------------
hand <- fread(file.path(ROOT,"data/processed/park_factors/park_factors_by_hand_era.csv"))
gl[, home_team_id := fifelse(home==1L, team_id, opp_team_id)]
gl[, bat_hand_eff := fifelse(bat_side=="S", fifelse(opp_starter_throws=="L","R","L"), bat_side)]
gl[!bat_hand_eff %in% c("L","R"), bat_hand_eff := "R"]                    # rare missing -> RHB (modal)
hj <- hand[, .(home_team_id=team_id, hand, year_start, year_end,
               bacon_idx_hand, hr_idx_hand)]
er <- hj[gl[, .(rid=.I, home_team_id, hand=bat_hand_eff, season)], on=.(home_team_id, hand), allow.cartesian=TRUE]
er <- er[season >= year_start & season <= year_end][order(rid, -year_start)][!duplicated(rid)]
gl[er$rid, `:=`(park_bacon = (er$bacon_idx_hand-100)/100, park_hr = (er$hr_idx_hand-100)/100)]

# ---- prior-year (LIVE) + same-season (CEILING) Savant/K ------------------------
pit <- fread(file.path(ROOT,"data/raw/pitcher_obp_against_2015_2025.csv"))[bf>=100,
        .(season, opp_starter_id=person_id, k=so_pitched/bf)]
pit_k_live <- copy(pit)[, .(season=season+1, opp_starter_id, pit_k_py=k)]           # prior year
pit_k_ceil <- copy(pit)[, .(season, opp_starter_id, pit_k_fs=k)]                     # same year
rp <- rbindlist(lapply(2016:2025, function(y){
  d <- fread(sprintf("%s/data/raw/savant_running_game/pitcher_running_%d.csv", ROOT, y))
  d[, .(season=y, opp_starter_id=player_id, rp=runs_prevented_on_running_attr)] }))
rp_live <- copy(rp)[, .(season=season+1, opp_starter_id, rp_py=rp)]
rp_ceil <- copy(rp)[, .(season, opp_starter_id, rp_fs=rp)]

adp <- rbindlist(lapply(2016:2025, function(y)
  fread(sprintf("%s/data/raw/nfbc_adp_history/nfbc_adp_with_position_%d.csv", ROOT, y))[
    is_pitcher == FALSE & !is.na(mlbam_id) & !is.na(nfbc_adp),
    .(season=y, person_id=as.integer(mlbam_id), nfbc_adp)]))
adp <- unique(adp, by=c("season","person_id"))
adp[, adp_z := -as.numeric(scale(log(nfbc_adp))), by=season]

jn <- function(g, td, nm, keys) { setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys, "game_date"), all.x=TRUE) }
jf <- function(g, td, nm, keys) { setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys), all.x=TRUE) }
for (nm in names(TD)) gl <- jn(gl, copy(td_live[[nm]]), paste0(nm,"_td"), TD[[nm]][[3]])
for (nm in names(TD)) gl <- jf(gl, copy(td_ceil[[nm]]), paste0(nm,"_fs"), TD[[nm]][[3]])
gl <- merge(gl, pit_k_live, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, pit_k_ceil, by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rp_live,    by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, rp_ceil,    by=c("season","opp_starter_id"), all.x=TRUE)
gl <- merge(gl, adp[, .(season, person_id, adp_z)], by=c("season","person_id"), all.x=TRUE)

gl <- gl[season >= 2016 & season != 2020 & !is.na(park_bacon)]
gl[, adp_z := fifelse(is.na(adp_z), min(adp_z, na.rm=TRUE), adp_z), by=season]  # undrafted -> season min
fillcols <- c(paste0(names(TD),"_td"), paste0(names(TD),"_fs"),
              "pit_k_py","pit_k_fs","rp_py","rp_fs")
for (cn in fillcols) gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]
cat(sprintf("panel: %d startable games, %d players, seasons %s\n",
    nrow(gl), uniqueN(gl$person_id), paste(range(gl$season), collapse="-")))

# ---- spec pairs (LIVE vs CEILING differ only in opponent-rate construction) ----
# each entry: response, category-relevant pop rule, and the ordered feature list
# for LIVE; the CEILING list swaps the opponent features to their _fs analogues.
CATS <- list(
  HR  = list(resp="hr",  pop=quote(rk_hr <=150),
             live=c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
             swap=c(pit_k_py="pit_k_fs", pit_hr_td="pit_hr_fs", team_hr_td="team_hr_fs")),
  SB  = list(resp="sb",  pop=quote(rk_sb <=150),
             live=c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","pit_sb_td","adp_z"),
             swap=c(rp_py="rp_fs", team_sb_td="team_sb_fs", pit_sb_td="pit_sb_fs")),
  AVG = list(resp="h",   pop=quote(rk_avg<=150 & proj_pa>=200),
             live=c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
             swap=c(pit_k_py="pit_k_fs", pit_h_td="pit_h_fs", team_bab_td="team_bab_fs")),
  R   = list(resp="r",   pop=quote(TRUE),
             live=c("proj_obp","proj_hr_pa","s12","s345","s67","team_off_td","opp_ra_td","home"),
             swap=c(team_off_td="team_off_fs", opp_ra_td="opp_ra_fs")),
  RBI = list(resp="rbi", pop=quote(TRUE),
             live=c("proj_obp","proj_hr_pa","s12","s345","s67","team_off_td","opp_ra_td","home"),
             swap=c(team_off_td="team_off_fs", opp_ra_td="opp_ra_fs")))
# rank at the PLAYER level (one row per player-season), not per game
rkp <- unique(gl[, .(season, person_id, proj_hr_pa, proj_sb_pa, proj_avg)])
rkp[, `:=`(rk_hr=frank(-proj_hr_pa,ties.method="first"), rk_sb=frank(-proj_sb_pa,ties.method="first"),
           rk_avg=frank(-proj_avg,ties.method="first")), by=season]
gl <- merge(gl, rkp[, .(season, person_id, rk_hr, rk_sb, rk_avg)], by=c("season","person_id"))

BIN <- c("home","platoon_adv","lhp","s12","s345","s67")                   # 0/1 features
q <- function(v,p) as.numeric(quantile(v, p, na.rm=TRUE))
effect_tab <- function(d, resp, feats) {
  cont <- setdiff(feats, BIN)
  ds <- copy(d); for (cn in cont) ds[[paste0("z_",cn)]] <- as.numeric(scale(ds[[cn]]))
  rhs <- c(paste0("z_",cont), intersect(feats, BIN))
  fit <- glm(reformulate(rhs, resp), poisson, ds)
  co  <- coef(fit)
  mult <- sapply(feats, function(cn) {
    if (cn %in% BIN) exp(co[[cn]])
    else exp(co[[paste0("z_",cn)]] * (q(d[[cn]],.9)-q(d[[cn]],.1)) / sd(d[[cn]], na.rm=TRUE)) })
  round(mult, 3)
}
res <- list()
for (nm in names(CATS)) {
  cc <- CATS[[nm]]; d <- gl[eval(cc$pop)]
  ceil_feats <- cc$live; for (k in names(cc$swap)) ceil_feats[ceil_feats==k] <- cc$swap[[k]]
  ml <- effect_tab(d, cc$resp, cc$live)
  mc <- effect_tab(d, cc$resp, ceil_feats)
  # talent scale check: within-tier separation of REALIZED RATE (num/den) by
  # projection decile -- rate-based to avoid the rate-vs-playing-time confound.
  projcol <- switch(nm, HR="proj_hr_pa", SB="proj_sb_pa", AVG="proj_avg", R="proj_obp", RBI="proj_obp")
  num <- switch(cc$resp, h=d$h, d[[cc$resp]]); den <- switch(cc$resp, h=d$ab, d$pa)
  qq <- unique(q(d[[projcol]], 0:10/10))                       # dedupe breaks (discrete proj over games)
  d[, tdec := cut(get(projcol), qq, include.lowest=TRUE, labels=FALSE)]
  tt <- data.table(dec=d$tdec, num=num, den=den)[!is.na(dec), .(rate=sum(num)/sum(den)), by=dec][order(dec)]
  scale_ratio <- tt[dec==max(dec),rate]/max(tt[dec==min(dec),rate],1e-9)
  res[[nm]] <- data.table(category=nm, n=nrow(d), feature=cc$live, is_prior=startsWith(cc$live,"proj_"),
                          live_mult=ml, ceiling_mult=mc,
                          talent_within_tier=round(scale_ratio,2))
  cat(sprintf("\n== %s (n=%d, within-tier talent top/bottom RATE = %.2fx) ==\n", nm, nrow(d), scale_ratio))
  print(res[[nm]][is_prior==FALSE, .(feature, live_mult, ceiling_mult)], row.names=FALSE)
}
allres <- rbindlist(res)
fwrite(allres, file.path(OUT, "effect_tables.csv"))
cat(sprintf("\nwrote %d rows -> %s/effect_tables.csv\n", nrow(allres), OUT))
