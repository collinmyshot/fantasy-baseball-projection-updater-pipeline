#!/usr/bin/env Rscript
# analyze_article_round3.R -- data for the round-3 article rewrite.
# Uses the cached 2016-2025 panel (ext_panel.rds) so every figure shares one
# window. Produces: (Part 3) proj-only baseline spread + per-feature p10->p90
# multipliers, unified; (Part 6) top-N win-rate sweep 1..50, bottom-N dud test,
# similar-pair pairwise accuracy Streamonator vs projection, streamable pool @200.
suppressWarnings(suppressMessages(library(data.table))); set.seed(42)
ROOT <- "/Users/ckaufman/Documents/New project"
SCRATCH <- "/private/tmp/claude-501/-Users-ckaufman-Documents-New-project/b1414f12-4c91-4780-a4ed-5fb1e469e838/scratchpad"
gl <- readRDS(file.path(SCRATCH, "ext_panel.rds"))
cat("panel:", nrow(gl), "rows, seasons", paste(sort(unique(gl$season)), collapse="/"), "\n")

SPECS <- list(
  HR = list(y="hr", proj="proj_hr_pa", f=c("proj_hr_pa","bat_td_dhr","exp_pa","platoon_adv","park_hr","home","pit_k_py","pit_hr_td","team_hr_td")),
  SB = list(y="sb", proj="proj_sb_pa", f=c("proj_sb_pa","bat_td_dsb","exp_pa","lhp","home","rp_py","team_sb_td","pit_sb_td","adp_z")),
  AVG= list(y="h",  proj="proj_avg",   f=c("proj_avg","bat_td_davg","exp_pa","park_bacon","home","pit_k_py","pit_h_td","team_bab_td")))

cat("\n===== PART 3: effect sizes, 2016-2025 (proj-only baseline, then features on top) =====\n")
mult <- function(co, v, x) exp(co[[v]] * (quantile(x,.9,na.rm=TRUE) - quantile(x,.1,na.rm=TRUE)))
for (cn in names(SPECS)) {
  s <- SPECS[[cn]]
  b0 <- glm(reformulate(s$proj, s$y), poisson, gl)
  base_mult <- mult(coef(b0), s$proj, gl[[s$proj]])
  ff <- glm(reformulate(s$f, s$y), poisson, gl); co <- coef(ff)
  cat(sprintf("\n[%s] projection-ALONE p10->p90 spread: x%.2f\n", cn, base_mult))
  for (v in setdiff(s$f, s$proj))
    cat(sprintf("   + %-14s x%.2f\n", v, mult(co, v, gl[[v]])))
}

# ---- Part 6 machinery ----
gl[, dow := as.integer(game_date - week_start)][, block := fifelse(dow<=3,"MonThu","FriSun")]
memb <- gl[, .(bsb=blend_sb[1], bhr=blend_hr[1], bavg=blend_avg[1]), by=.(season, week_start, person_id)]
memb[, `:=`(rk_sb=frank(-bsb,ties.method="first"), rk_hr=frank(-bhr,ties.method="first"), rk_avg=frank(-bavg,ties.method="first")), by=.(season,week_start)]
CATS <- list(SB=list(pred="p_sb",out=quote(sum(sb)),st=quote(proj_sb_pa[1]*.N),rk="rk_sb",proj="proj_sb_pa"),
             HR=list(pred="p_hr",out=quote(sum(hr)),st=quote(proj_hr_pa[1]*.N),rk="rk_hr",proj="proj_hr_pa"),
             AVG=list(pred="p_h", out=quote(sum(h)-lg[1]*sum(ab)),st=quote((proj_avg[1]-lg[1])*.N*proj_ab_pa[1]),rk="rk_avg",proj="proj_avg"))
wk_pool <- function(cc) {
  gc <- merge(gl, memb[get(cc$rk)<=150, .(season,week_start,person_id)], by=c("season","week_start","person_id"))
  u <- gc[, .(out=eval(cc$out), pred=sum(get(cc$pred)), steamer=eval(cc$st), adp=nfbc_adp[1], proj=get(cc$proj)[1]),
          by=.(season,person_id,week_start)]
  u[, tb := runif(.N)]; u
}
winrate <- function(u, K, side="top", pcol="pred") {
  s <- u[, { o<-order(if(side=="top") -get(pcol) else get(pcol), tb); o2<-order(if(side=="top") -steamer else steamer, tb)
             k<-min(K,.N); .(m=sum(out[o][1:k]), b=sum(out[o2][1:k])) }, by=week_start]
  100*mean(fifelse(s$m>s$b,1,fifelse(s$m==s$b,.5,0)))
}
cat("\n===== PART 6a: TOP-N win-rate vs projection (weekly) =====\n")
Ns <- c(1,3,5,10,20,50); sweep <- list()
for (cn in names(CATS)) { u <- wk_pool(CATS[[cn]])
  sweep[[cn]] <- sapply(Ns, function(K) round(winrate(u,K)))
  cat(sprintf("  %-4s  N=%s\n", cn, paste(sprintf("%d:%d%%",Ns,sweep[[cn]]), collapse="  "))) }

cat("\n===== PART 6b: BOTTOM-5 dud test (does Streamonator's worst produce LESS than projection's worst?) =====\n")
for (cn in names(CATS)) { u <- wk_pool(CATS[[cn]])
  # side=bottom: model picks its lowest-5, steamer its lowest-5; want model's to be LOWER (fewer)
  s <- u[, { o<-order(pred,tb); o2<-order(steamer,tb); k<-min(5,.N); .(m=sum(out[o][1:k]), b=sum(out[o2][1:k])) }, by=week_start]
  cat(sprintf("  %-4s Streamonator bottom-5 < projection bottom-5 in %.0f%% of weeks\n", cn,
      100*mean(fifelse(s$m<s$b,1,fifelse(s$m==s$b,.5,0))))) }

cat("\n===== PART 6c: SIMILAR-PAIR pairwise accuracy (adjacent-in-projection pairs) =====\n")
pair_acc <- function(u, rankcol) {
  ws <- u[, { setorder(.SD, -proj); n<-.N; g<-10
    if (n>g) { r_out <- out; r_rank <- get(rankcol)
      hi<-1:(n-g); lo<-(g+1):n; win <- (r_rank[hi]>r_rank[lo])
      # among pairs the ranker orders one way, how often does realized agree
      concord <- ifelse(r_rank[hi]>r_rank[lo], r_out[hi]>r_out[lo], r_out[lo]>r_out[hi])
      ties <- r_out[hi]==r_out[lo]
      .(num=sum(concord[!ties])+0.5*sum(ties), den=length(hi)) } else .(num=0,den=0) }, by=week_start]
  100*sum(ws$num)/sum(ws$den)
}
for (cn in names(CATS)) { u <- wk_pool(CATS[[cn]])
  cat(sprintf("  %-4s similar-pair pick accuracy: Streamonator %.1f%% vs projection %.1f%%\n",
      cn, pair_acc(u,"pred"), pair_acc(u,"steamer"))) }

cat("\n===== PART 6d: STREAMABLE pool (ADP rank > 200 or undrafted), top-5 weekly =====\n")
for (cn in names(CATS)) { u <- wk_pool(CATS[[cn]]); ua <- u[is.na(adp) | adp>200]
  cat(sprintf("  %-4s full pool %.0f%%  |  streamable(>200) %.0f%% (n=%d hitter-weeks)\n",
      cn, winrate(u,5), winrate(ua,5), nrow(ua))) }
