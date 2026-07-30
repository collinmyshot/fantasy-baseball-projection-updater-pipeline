#!/usr/bin/env Rscript
# analyze_topk_sweep.R -- Q3: extend the top-N win-rate sweep to find the ceiling,
# on the FULL startable pool (not top-150), and compare against the K-free
# pairwise metrics (all-pairs + similar-pair). 2016-2025 shipped/live panel.
#
# HITTER streamonator. Exists to answer "is K=5 cherry-picked?" — i.e. does the
# reported top-5 win-rate survive sweeping K over 1/3/5/10/20/50/100/150/200/300.
#
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║ DOES NOT RUN AS COMMITTED — DEAD SCRATCHPAD DEPENDENCY                   ║
# ╠══════════════════════════════════════════════════════════════════════════╣
# ║ Reads ext_panel.rds out of the SCRATCH path below, a scratchpad from      ║
# ║ session b1414f12 that no longer exists. This script does NOT build the    ║
# ║ panel — compare_streamonator_extended.R does.                            ║
# ║                                                                          ║
# ║ TO REVIVE, in order:                                                     ║
# ║   1. repoint SCRATCH in BOTH files to something durable, e.g.            ║
# ║        file.path("data","processed","hitter_stream_eval")                ║
# ║   2. run compare_streamonator_extended.R to rebuild ext_panel.rds        ║
# ║   3. run this                                                            ║
# ║ Step 2 is the expensive one; this sweep is cheap once the panel exists.  ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# ⚠ NO RESULT IS RECORDED for the sweep itself. The related published figures
#   (win-rates SB 62 / HR 61 / AVG 66, pairwise floor 50-53%) come from
#   compare_streamonator_extended.R, not from this file. Re-run to get the
#   K-ceiling answer; do not assume it was ever concluded.
suppressWarnings(suppressMessages(library(data.table))); set.seed(42)
SCRATCH <- "/private/tmp/claude-501/-Users-ckaufman-Documents-New-project/b1414f12-4c91-4780-a4ed-5fb1e469e838/scratchpad"
gl <- readRDS(file.path(SCRATCH, "ext_panel.rds"))
CATS <- list(SB=list(pred="p_sb",out=quote(sum(sb)),st=quote(proj_sb_pa[1]*.N),proj="proj_sb_pa"),
             HR=list(pred="p_hr",out=quote(sum(hr)),st=quote(proj_hr_pa[1]*.N),proj="proj_hr_pa"),
             AVG=list(pred="p_h", out=quote(sum(h)-lg[1]*sum(ab)),st=quote((proj_avg[1]-lg[1])*.N*proj_ab_pa[1]),proj="proj_avg"))
Ks <- c(1,3,5,10,20,50,100,150,200,300)
cat("Full startable pool per category, weekly. Win-rate vs projection at each K, then K-free pairwise.\n\n")
for (cn in names(CATS)) {
  cc <- CATS[[cn]]
  u <- gl[, .(out=eval(cc$out), pred=sum(get(cc$pred)), steamer=eval(cc$st), proj=get(cc$proj)[1]),
          by=.(season,person_id,week_start)]
  u[, tb := runif(.N)]
  poolsz <- round(u[, .N, by=week_start][, mean(N)])
  wr <- sapply(Ks, function(K) {
    s <- u[, { o<-order(-pred,tb); o2<-order(-steamer,tb); k<-min(K,.N); .(m=sum(out[o][1:k]), b=sum(out[o2][1:k])) }, by=week_start]
    round(100*mean(fifelse(s$m>s$b,1,fifelse(s$m==s$b,.5,0)))) })
  ap <- { ws <- u[, { setorder(.SD,-pred); n<-.N; if(n>1){tot<-0.0;ct<-0.0
             for(i in 1:(n-1)){tot<-tot+sum(out[i]>out[(i+1):n])+.5*sum(out[i]==out[(i+1):n]);ct<-ct+(n-i)}; .(num=as.numeric(tot),den=as.numeric(ct))} else .(num=0.0,den=0.0)}, by=week_start]
           round(100*sum(ws$num)/sum(ws$den),1) }
  sp <- { ws <- u[, { setorder(.SD,-steamer); n<-.N; g<-20
             if(n>g){hi<-1:(n-g);lo<-(g+1):n; ph<-pred[hi]>pred[lo]; cc2<-ifelse(ph,out[hi]>out[lo],out[lo]>out[hi]); ti<-out[hi]==out[lo]
               .(num=as.numeric(sum(cc2[!ti])+.5*sum(ti)),den=as.numeric(length(hi)))} else .(num=0.0,den=0.0)}, by=week_start]
           round(100*sum(ws$num)/sum(ws$den),1) }
  cat(sprintf("%-4s (pool~%d/wk):  %s\n", cn, poolsz, paste(sprintf("K%d:%d",Ks,wr),collapse="  ")))
  cat(sprintf("       K-free:  all-pairs %.1f%%  |  similar-pair (Streamonator, gap20) %.1f%%\n\n", ap, sp))
}
