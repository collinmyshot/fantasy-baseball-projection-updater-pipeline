#!/usr/bin/env Rscript
# compare_streamonator_v2.R
# ---------------------------------------------------------------------------
# IMPROVEMENT-QUEUE RERUN of the four-way comparison. Changes vs v1:
#  (1) DYNAMIC populations: weekly top-150 by to-date-BLENDED talent rate
#      (risers qualify mid-season; strictly point-in-time)
#  (2) TUNED shrinkage: batter pseudo-count + entity-pseudo multiplier chosen
#      on a 2022->2023 split ONLY, then frozen (leak-free, coarse grid)
#  (3) SB engine gains preseason ADP prior (adp_z; undrafted = season min)
#  (4) slot = MODAL lineup slot over previous 7 started games
# Also computes, for the interpretation question: top-5 overlap with Steamer
# and gap-10 accuracy on DISAGREEMENT pairs only (week horizon).
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
onf <- quote(singles + bb + hbp)
ENT <- function(mult) list(
  team_bab = mk_todate(quote(h-hr), quote(pmax(ab-so-hr,0L)), "opp_team_id", 2000*mult),
  team_hr  = mk_todate(quote(hr),   quote(pa),                "opp_team_id", 3000*mult),
  pit_h    = mk_todate(quote(h),    quote(pa),                "opp_starter_id", 400*mult),
  pit_hr   = mk_todate(quote(hr),   quote(pa),                "opp_starter_id", 400*mult),
  team_sb  = mk_todate(quote(sb), onf, "opp_team_id",    750*mult),
  cat_sb   = mk_todate(quote(sb), onf, "opp_catcher_id", 400*mult),
  pit_sb   = mk_todate(quote(sb), onf, "opp_starter_id", 150*mult))

base_panel <- function() {
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
  gl[, `:=`(ch=cumsum(h)-h, cab=cumsum(ab)-ab, chr=cumsum(hr)-hr, cpa=cumsum(pa)-pa, csb=cumsum(sb)-sb), by=.(person_id, season)]
  st <- fread(file.path(ROOT,"data/raw/steamer_bat_archive_all.csv"))
  st <- st[!is.na(xMLBAMID) & PA>0][order(archive_season, xMLBAMID, -PA)]
  st2 <- unique(st[, .(season=archive_season, person_id=as.integer(xMLBAMID),
                       proj_avg=AVG, proj_hr_pa=HR/PA, proj_sb_pa=SB/PA, proj_ab_pa=AB/PA, proj_pa=PA)],
                by=c("season","person_id"))
  gl <- merge(gl, st2, by=c("season","person_id"), all.x=TRUE)
  gl <- gl[!is.na(proj_avg)]
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
  gl <- merge(gl, pitpy, by=c("season","opp_starter_id"), all.x=TRUE)
  gl <- merge(gl, rppy,  by=c("season","opp_starter_id"), all.x=TRUE)
  adp <- rbindlist(lapply(2022:2025, function(y)
    fread(sprintf("%s/data/raw/nfbc_adp_history/nfbc_adp_with_position_%d.csv", ROOT, y))[
      is_pitcher == FALSE & !is.na(mlbam_id) & !is.na(nfbc_adp),
      .(season=y, person_id=as.integer(mlbam_id), nfbc_adp)]))
  adp <- unique(adp, by=c("season","person_id"))
  adp[, adp_z := -as.numeric(scale(log(nfbc_adp))), by=season]
  gl <- merge(gl, adp[, .(season, person_id, adp_z)], by=c("season","person_id"), all.x=TRUE)
  gl[, adp_min := min(adp_z, na.rm=TRUE), by=season]
  gl[is.na(adp_z), adp_z := adp_min]
  gl
}
gl0 <- base_panel()

attach_ent <- function(gl, E) {
  jn <- function(g, td, nm, keys) { setnames(td, c("ent","feat"), c(keys, nm)); merge(g, td, by=c("season", keys, "game_date"), all.x=TRUE) }
  gl <- jn(gl, copy(E$team_bab), "team_bab_td", "opp_team_id")
  gl <- jn(gl, copy(E$team_hr),  "team_hr_td",  "opp_team_id")
  gl <- jn(gl, copy(E$team_sb),  "team_sb_td",  "opp_team_id")
  gl <- jn(gl, copy(E$pit_h),    "pit_h_td",    "opp_starter_id")
  gl <- jn(gl, copy(E$pit_hr),   "pit_hr_td",   "opp_starter_id")
  gl <- jn(gl, copy(E$pit_sb),   "pit_sb_td",   "opp_starter_id")
  gl <- jn(gl, copy(E$cat_sb),   "cat_sb_td",   "opp_catcher_id")
  gl <- gl[season >= 2022 & !is.na(park_bacon)]
  for (cn in c("team_bab_td","team_hr_td","team_sb_td","pit_h_td","pit_hr_td","pit_sb_td","cat_sb_td","pit_k_py","rp_py"))
    gl[is.na(get(cn)), (cn) := mean(gl[[cn]], na.rm=TRUE)]
  gl
}
SPEC <- function(mb_hr, mb_avg, mb_sb) list(
  h  = list(f=c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td"),
            bat=function(g) g[, bat_td_davg := (ch + mb_avg*proj_avg)/(cab + mb_avg) - proj_avg]),
  hr = list(f=c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td"),
            bat=function(g) g[, bat_td_dhr := (chr + mb_hr*proj_hr_pa)/(cpa + mb_hr) - proj_hr_pa]),
  sb = list(f=c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","cat_sb_td","pit_sb_td","adp_z"),
            bat=function(g) g[, bat_td_dsb := (csb + mb_sb*proj_sb_pa)/(cpa + mb_sb) - proj_sb_pa]))

# ---- (2) tune on 2022 -> 2023 ONLY, freeze --------------------------------------
cat("== Tuning (fit 2022, validate 2023; frozen thereafter) ==\n")
dev_ <- function(y, mu) 2*sum(ifelse(y>0, y*log(y/mu), 0) - (y-mu))
best <- list()
for (mult in c(0.5, 1, 2)) {
  E <- ENT(mult); gm <- attach_ent(copy(gl0), E)
  for (cat_ in c("hr","h","sb")) {
    grid <- switch(cat_, hr=c(100,170,300), h=c(500,900,1500), sb=c(150,300,600))
    for (mb in grid) {
      sp <- SPEC(if (cat_=="hr") mb else 170, if (cat_=="h") mb else 900, if (cat_=="sb") mb else 300)[[cat_]]
      g2 <- copy(gm); sp$bat(g2)
      fit <- glm(reformulate(sp$f, cat_), poisson, g2[season == 2022])
      mu <- predict(fit, g2[season == 2023], type="response")
      d  <- dev_(g2[season == 2023][[cat_]], mu)
      key <- cat_
      if (is.null(best[[key]]) || d < best[[key]]$d) best[[key]] <- list(d=d, mb=mb, mult=mult)
    }
  }
}
for (k in names(best)) cat(sprintf("  %s: batter pseudo=%d, entity multiplier=%.1f\n", k, best[[k]]$mb, best[[k]]$mult))

# ---- final panel with tuned constants -------------------------------------------
EMULT <- round(mean(sapply(best, `[[`, "mult")), 1)                 # one shared entity multiplier (modal-ish)
cat(sprintf("  shared entity multiplier used: %.1f\n\n", EMULT))
E <- ENT(EMULT); gl <- attach_ent(copy(gl0), E)
SP <- SPEC(best$hr$mb, best$h$mb, best$sb$mb)
for (cat_ in names(SP)) SP[[cat_]]$bat(gl)
for (y in names(SP)) gl[[paste0("p_",y)]] <- NA_real_
for (s in unique(gl$season)) for (y in names(SP)) {
  fit <- glm(reformulate(SP[[y]]$f, y), poisson, gl[season != s])
  gl[season == s, (paste0("p_",y)) := predict(fit, .SD, type="response")]
}

# ---- (1) dynamic weekly membership: top-150 by to-date-blended talent -----------
gl[, blend_sb  := (csb + best$sb$mb*proj_sb_pa)/(cpa + best$sb$mb)]
gl[, blend_hr  := (chr + best$hr$mb*proj_hr_pa)/(cpa + best$hr$mb)]
gl[, blend_avg := (ch  + best$h$mb*proj_avg)/(cab + best$h$mb)]
memb <- gl[, .(bsb=blend_sb[1], bhr=blend_hr[1], bavg=blend_avg[1], ppa=proj_pa[1]),
           by=.(season, week_start, person_id)]
memb[, `:=`(rk_sb=frank(-bsb, ties.method="first"), rk_hr=frank(-bhr, ties.method="first")), by=.(season, week_start)]
memb[, rk_avg := frank(-bavg, ties.method="first"), by=.(season, week_start)]

day <- raw[, .(g=.N, sb=sum(sb), hr=sum(hr), h=sum(h), ab=sum(ab), pa=sum(pa)), by=.(person_id, season, game_date)]
setorder(day, person_id, season, game_date)
day[, `:=`(cg=cumsum(g), csb2=cumsum(sb), chr2=cumsum(hr), chh=cumsum(h), cabb=cumsum(ab), cpaa=cumsum(pa)), by=.(person_id, season)]
ckey <- day[, .(person_id, season, game_date, cg, csb2, chr2, chh, cabb, cpaa)]; setkey(ckey, person_id, season, game_date)
getc <- function(q) {
  r <- ckey[q, on=.(person_id, season, game_date), roll=TRUE]
  for (cn in c("cg","csb2","chr2","chh","cabb","cpaa")) r[is.na(get(cn)), (cn) := 0L]
  r
}
lgavg <- gl[, .(lg = sum(h)/sum(ab)), by=season]
gl <- merge(gl, lgavg, by="season")
gl[, dow := as.integer(game_date - week_start)][, block := fifelse(dow<=3,"MonThu","FriSun")]

CATS <- list(
  sb  = list(pred="p_sb", out=quote(sum(sb)), st=quote(proj_sb_pa[1]*.N), rk="rk_sb",  t14="sb", lab="SB"),
  hr  = list(pred="p_hr", out=quote(sum(hr)), st=quote(proj_hr_pa[1]*.N), rk="rk_hr",  t14="hr", lab="HR"),
  avg = list(pred="p_h",  out=quote(sum(h) - lg[1]*sum(ab)), st=quote((proj_avg[1]-lg[1])*.N*proj_ab_pa[1]), rk="rk_avg", t14="avg", lab="AVG"))
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
                week_start=week_start[1], d0=min(game_date)), by=byc]
    u[, pool := switch(hz, game=as.character(d0), half=paste(week_start, d0>week_start+3), week=as.character(week_start))]
    for (k in c(14L, 30L)) {
      a <- getc(u[, .(person_id, season, game_date = d0 - 1L)])
      b <- getc(u[, .(person_id, season, game_date = d0 - 1L - k)])
      u[[paste0("t",k)]] <- switch(cc$t14,
        sb  = (a$csb2-b$csb2)/pmax(a$cg-b$cg,1),
        hr  = (a$chr2-b$chr2)/pmax(a$cpaa-b$cpaa,1),
        avg = (a$chh-b$chh)/pmax(a$cabb-b$cabb,1))
    }
    u[, tb := runif(.N)]
    top5 <- function(rcol) u[, { o <- order(-get(rcol), tb); k <- min(5,.N)
                                 .(v=sum(out[o][1:k]), pm=mean(out)*min(5,.N), wk=week_start[1]) }, by=pool]
    pacc <- function(rcol) {
      ws <- u[, { n<-.N; g<-10
        if (n>g) { o1<-order(-get(rcol), tb); rv<-out[o1]
          .(num=sum(rv[1:(n-g)]>rv[(g+1):n])+0.5*sum(rv[1:(n-g)]==rv[(g+1):n]), den=n-g) } else .(num=0,den=0) }, by=pool]
      100*sum(ws$num)/sum(ws$den)
    }
    sm <- top5("pred"); s_st <- top5("steamer"); s14 <- top5("t14"); s30 <- top5("t30")
    wr <- function(sbase) {
      j <- merge(sm[, .(pool, m=v, wk)], sbase[, .(pool, b=v)], by="pool")
      jw <- j[, .(m=sum(m), b=sum(b)), by=wk]; W <- nrow(jw)
      pt <- 100*mean(fifelse(jw$m>jw$b, 1, fifelse(jw$m==jw$b, 0.5, 0)))
      bs <- replicate(2000, { s<-sample.int(W,W,TRUE); 100*mean(fifelse(jw$m[s]>jw$b[s],1,fifelse(jw$m[s]==jw$b[s],0.5,0))) })
      sprintf("%.0f [%.0f,%.0f]", pt, quantile(bs,.025), quantile(bs,.975))
    }
    wrnd <- { j <- merge(sm[, .(pool, m=v, wk)], sm[, .(pool, r=pm)], by="pool")
              jw <- j[, .(m=sum(m), r=sum(r)), by=wk]
              round(100*mean(fifelse(jw$m>jw$r, 1, fifelse(jw$m==jw$r, 0.5, 0)))) }
    ov <- dis <- NA
    if (hz == "week") {
      ovl <- u[, { om <- order(-pred, tb)[1:min(5,.N)]; os <- order(-steamer, tb)[1:min(5,.N)]
                   .(ov = length(intersect(om, os))/min(5,.N)) }, by=pool]
      ov <- round(100*mean(ovl$ov))
      dd <- u[, { n<-.N; g<-10
        if (n>g) { o1<-order(-pred, tb); rv<-out[o1]; sv<-steamer[o1]
          hi<-1:(n-g); lo<-(g+1):n; dis_ <- sv[hi] < sv[lo]
          .(num=sum(rv[hi][dis_]>rv[lo][dis_])+0.5*sum(rv[hi][dis_]==rv[lo][dis_]), den=sum(dis_)) }
        else .(num=0,den=0) }, by=pool]
      dis <- round(100*sum(dd$num)/max(sum(dd$den),1), 1)
    }
    res[[length(res)+1]] <- data.table(cat=cc$lab, horizon=hz,
      acc_model=round(pacc("pred"),1), acc_steamer=round(pacc("steamer"),1),
      acc_L14=round(pacc("t14"),1), acc_L30=round(pacc("t30"),1),
      win_rnd=wrnd, win_steamer=wr(s_st), win_L14=wr(s14), win_L30=wr(s30),
      top5_overlap_pct=ov, disagree_acc=dis)
  }
}
out <- rbindlist(res)
print(out, row.names=FALSE)
fwrite(out, file.path(ROOT, "data/processed/streamonator_baseline_comparison_v2.csv"))
