#!/usr/bin/env Rscript
# streamonator_defense_bucket_compare.R
#
# Q (user): did INCLUDING DEFENSE change the % of starts identified as good/bad
# by the 105+ / 95-105 / <95 buckets?  Two things to separate:
#   (i)  bucket SIZES  — how many starts get flagged Start vs Sit
#   (ii) bucket ACCURACY — %Good in the Start bucket, %Bad in the Sit bucket
#
# Configs (all at fixed 95/105 thresholds, N=22,917 non-placeholder sample):
#   A) 6:3:1     baseline, no defense           (SP:TR:PF)
#   B) 6:2:1:1   defense as its own component    (SP:TR:PF:Def)  [best 4-way]
#   C) 6:3:1     defense folded into PF, weights UNCHANGED
#                (SP:TR:E, where E = 50/50 z-blend of PF_inv & Def)
#
# Cached; no API.  Usage: Rscript scripts/validation_tuning/streamonator_defense_bucket_compare.R

CACHE_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
DEF_FILE  <- file.path("data", "manual", "team_defense_2015_2025.csv")
SEASONS   <- 2021:2025
TEAM_MAP <- c(
  ARI="AZ", ARZ="AZ", AZ="AZ", ATH="ATH", OAK="ATH", BAL="BAL", BOS="BOS",
  CHC="CHC", CHN="CHC", CHW="CHW", CHA="CHW", CWS="CHW", CIN="CIN", CLE="CLE",
  CLV="CLE", COL="COL", DET="DET", HOU="HOU", KC="KCR", KCR="KCR", LAA="LAA",
  LAD="LAD", LA="LAD", MIA="MIA", FLA="MIA", MIL="MIL", MIN="MIN", NYM="NYM",
  NYY="NYY", PHI="PHI", PIT="PIT", SD="SDP", SDP="SDP", SEA="SEA", SF="SFG",
  SFG="SFG", STL="STL", TB="TBR", TBR="TBR", TAM="TBR", TEX="TEX", TOR="TOR",
  WSH="WSH", WSN="WSH", WAS="WSH")
norm_team <- function(x){ o<-TEAM_MAP[toupper(trimws(as.character(x)))]; o[is.na(o)]<-toupper(trimws(as.character(x)))[is.na(o)]; unname(o) }

s <- do.call(rbind, lapply(SEASONS, function(yr){ d<-read.csv(file.path(CACHE_DIR,sprintf("starts_%d.csv",yr)),stringsAsFactors=FALSE); d$season<-yr; d }))
s$pitcher_team <- norm_team(s$pitcher_team)
s$whip <- ifelse(!is.na(s$ip)&s$ip>0,(s$h+s$bb)/s$ip,Inf)
ip_ok <- !is.na(s$ip)&s$ip>=5
k_ok  <- !is.na(s$k)&!is.na(s$ip)&s$k>=(floor(s$ip)-1)
er_ok <- !is.na(s$er)&!is.na(s$ip)&((s$ip>=6&s$er<=2)|(s$ip>=5&s$ip<6&s$er<=3)|(s$ip>=4&s$ip<5&s$er<=2)|(s$ip<4&s$er<=1))
whip_ok <- !is.na(s$whip)&s$whip<=1.18
s$gsm <- as.integer(ip_ok)+as.integer(k_ok)+as.integer(er_ok)+as.integer(whip_ok)

# defense composite -> index (own team, season z, 1:1:1)
def <- read.csv(DEF_FILE,stringsAsFactors=FALSE); def <- def[def$season %in% SEASONS,]
def$team_norm <- norm_team(def$team)
zbs <- function(x,se) ave(x,se,FUN=function(v){mu<-mean(v,na.rm=TRUE);sg<-sd(v,na.rm=TRUE);if(is.na(sg)||sg==0) rep(NA_real_,length(v)) else (v-mu)/sg})
def$dc <- rowMeans(cbind(zbs(def$oaa,def$season),zbs(def$drs,def$season),zbs(def$uzr,def$season)),na.rm=TRUE)
def$dc[!is.finite(def$dc)] <- NA_real_
s$dc <- def$dc[match(paste(s$pitcher_team,s$season),paste(def$team_norm,def$season))]

score_wavg <- function(mat,w){ pres<-!is.na(mat); num<-rowSums(sweep(ifelse(pres,mat,0),2,w,`*`)); den<-as.vector(pres%*%w); ifelse(den>0,num/den,NA_real_) }
SP<-s$sp_skillz_index; TRi<-s$team_rater_inv; PFi<-s$park_factor_inv
s$def_index <- 100 + as.numeric(scale(s$dc))*10
s$score_631 <- score_wavg(cbind(SP,TRi,PFi),c(6,3,1))

base <- s[!s$spz_placeholder & !is.na(s$score_631) & !is.na(s$gsm) & !is.na(s$def_index),]
cat(sprintf("Sample N = %d\n",nrow(base)))

# merged environment E (50/50 z-blend of PF_inv and Def), rescaled to 100+-10
z_pf<-as.numeric(scale(base$park_factor_inv)); z_df<-as.numeric(scale(base$def_index))
e_raw<-0.5*ifelse(is.na(z_pf),0,z_pf)+0.5*z_df; e_raw[is.na(z_pf)]<-z_df[is.na(z_pf)]
base$E <- 100 + as.numeric(scale(e_raw))*10

base$A <- score_wavg(cbind(base$sp_skillz_index,base$team_rater_inv,base$park_factor_inv),c(6,3,1))
base$B <- score_wavg(cbind(base$sp_skillz_index,base$team_rater_inv,base$park_factor_inv,base$def_index),c(6,2,1,1))
base$C <- score_wavg(cbind(base$sp_skillz_index,base$team_rater_inv,base$E),c(6,3,1))

bkt <- function(sc) ifelse(sc>105,"Start (>105)",ifelse(sc<95,"Sit (<95)","Flip (95-105)"))
report <- function(sc,lab){
  b<-bkt(sc); g<-base$gsm
  cat(sprintf("\n--- %s ---\n",lab))
  cat(sprintf("  %-14s %6s %7s | %7s %6s %7s\n","bucket","n","%start","%Good","%OK","%Bad"))
  for(k in c("Start (>105)","Flip (95-105)","Sit (<95)")){
    sel<-b==k
    cat(sprintf("  %-14s %6d %6.1f%% | %6.1f%% %5.1f%% %6.1f%%\n",
      k,sum(sel),100*sum(sel)/length(sc),100*mean(g[sel]>=3),100*mean(g[sel]==2),100*mean(g[sel]<=1)))
  }
  comp <- (100*mean(g[b=="Start (>105)"]>=3) + 100*mean(g[b=="Sit (<95)"]<=1))/2
  cat(sprintf("  Overall %%Good=%.1f%%  | bucket composite (avg %%Good-Start & %%Bad-Sit) = %.1f%%\n",
      100*mean(g>=3), comp))
  invisible(b)
}
bA<-report(base$A,"A) 6:3:1  baseline (no defense)")
bB<-report(base$B,"B) 6:2:1:1  defense as own component (best 4-way)")
bC<-report(base$C,"C) 6:3:1  defense folded into PF, weights unchanged")

# bucket-change accounting vs baseline
cat("\n================ DID STARTS CHANGE BUCKETS? (vs baseline A) ================\n")
movecount <- function(bX,lab){
  chg<-sum(bX!=bA)
  cat(sprintf("\n  %s: %d of %d starts changed bucket (%.1f%%)\n",lab,chg,length(bA),100*chg/length(bA)))
  tt<-table(from=bA,to=bX); off<-tt; diag(off)<-0
  idx<-which(off>0,arr.ind=TRUE)
  for(i in order(-off[idx])){
    r<-idx[i,1];c<-idx[i,2]; if(off[r,c]==0) next
    cat(sprintf("    %-14s -> %-14s : %d\n",rownames(tt)[r],colnames(tt)[c],off[r,c]))
  }
}
movecount(bB,"B) 6:2:1:1")
movecount(bC,"C) folded-PF 6:3:1")
cat("\nDone.\n")
