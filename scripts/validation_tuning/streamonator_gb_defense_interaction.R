#!/usr/bin/env Rscript
# streamonator_gb_defense_interaction.R
#
# Q: does own-team defense matter MORE for groundball pitchers? (more balls in
#    play to fielders).  If yes, the per-start defense signal — inert league-wide
#    (Spearman~0.045) — should sharpen in high-GB% tiers.
#
# GB% source: FanGraphs BBE leaderboards in ~/Downloads (as-sp preferred, all-p
# fallback), joined to the non-placeholder ("SP Skillz = truth") start sample by
# MLBAMID + season.  Defense = team-total OAA+DRS+UZR composite (CAVEAT: team-total
# blends infield+outfield; GB pitchers benefit mostly from INFIELD defense).
#
# ── ANSWER (run 2026-06-22): YES, but too small to act on. SETTLED. ─────────
#   The interaction is real and correctly signed, controlling for SP Skillz:
#     def_z x gb_z coefficient +0.029, z = 2.13, p = 0.033
#   But it is less than half the defense main effect, and it concentrates in the
#   extreme tail rather than spreading across the pool:
#     GB% >= 60% tier (n=245): Spearman 0.14, +16pp tercile gap
#     moderate GB tiers:       ~0.04-0.07  (i.e. league-average inert)
#   Probably UNDERESTIMATED: team_defense_2015_2025.csv is team-total defense,
#   which dilutes the INFIELD signal groundballers actually consume. No
#   infield-only OAA in that file.
#   Operationally: even for groundballers, defense stays minor per start
#   (Spearman ~0.047 vs SP Skillz ~0.27). Not a red herring, but it does not
#   overturn 6:3:1. Nothing shipped.
#
# ── DATA GOTCHA ─────────────────────────────────────────────────────────────
#   Read the FanGraphs BBE CSVs WITHOUT fileEncoding="UTF-8-BOM" — that setting
#   corrupts the parse. Fix the header column after reading instead:
#     names(d)[1] <- "Season"
#
# Usage: Rscript scripts/validation_tuning/streamonator_gb_defense_interaction.R

DL<-"/Users/ckaufman/Downloads"; CACHE_DIR<-"data/processed/streamonator_weight_analysis"
DEF_FILE<-"data/manual/team_defense_2015_2025.csv"; SEASONS<-2021:2025
TEAM_MAP<-c(ARI="AZ",ARZ="AZ",AZ="AZ",ATH="ATH",OAK="ATH",BAL="BAL",BOS="BOS",CHC="CHC",CHN="CHC",CHW="CHW",
  CHA="CHW",CWS="CHW",CIN="CIN",CLE="CLE",CLV="CLE",COL="COL",DET="DET",HOU="HOU",KC="KCR",KCR="KCR",LAA="LAA",
  LAD="LAD",LA="LAD",MIA="MIA",FLA="MIA",MIL="MIL",MIN="MIN",NYM="NYM",NYY="NYY",PHI="PHI",PIT="PIT",SD="SDP",
  SDP="SDP",SEA="SEA",SF="SFG",SFG="SFG",STL="STL",TB="TBR",TBR="TBR",TAM="TBR",TEX="TEX",TOR="TOR",WSH="WSH",WSN="WSH",WAS="WSH")
norm_team<-function(x){o<-TEAM_MAP[toupper(trimws(as.character(x)))];o[is.na(o)]<-toupper(trimws(as.character(x)))[is.na(o)];unname(o)}

# ── GB% lookup (prefer as-sp, fall back all-p; dedupe by max Pitches) ──────────
read_gb<-function(f,src){d<-read.csv(file.path(DL,f),stringsAsFactors=FALSE)
  names(d)[1]<-"Season"   # un-mangle BOM-prefixed first column
  data.frame(mlbam=as.integer(d$MLBAMID),season=as.integer(d$Season),gb=as.numeric(d[["GB."]]),pit=as.numeric(d$Pitches),src=src)}
g<-rbind(read_gb("fangraphs-leaderboards(3)_as-sp.csv",1), read_gb("fangraphs-leaderboards(2)_allp.csv",2))
g<-g[!is.na(g$mlbam)&!is.na(g$gb)&g$season %in% SEASONS,]
g<-g[order(g$mlbam,g$season,g$src,-g$pit),]; g<-g[!duplicated(paste(g$mlbam,g$season)),]

# ── Starts -> GSM -> base sample -> defense ───────────────────────────────────
s<-do.call(rbind,lapply(SEASONS,function(yr){d<-read.csv(file.path(CACHE_DIR,sprintf("starts_%d.csv",yr)),stringsAsFactors=FALSE);d$season<-yr;d}))
s$pitcher_team<-norm_team(s$pitcher_team)
s$whip<-ifelse(!is.na(s$ip)&s$ip>0,(s$h+s$bb)/s$ip,Inf)
ip_ok<-!is.na(s$ip)&s$ip>=5; k_ok<-!is.na(s$k)&!is.na(s$ip)&s$k>=(floor(s$ip)-1)
er_ok<-!is.na(s$er)&!is.na(s$ip)&((s$ip>=6&s$er<=2)|(s$ip>=5&s$ip<6&s$er<=3)|(s$ip>=4&s$ip<5&s$er<=2)|(s$ip<4&s$er<=1))
whip_ok<-!is.na(s$whip)&s$whip<=1.18
s$gsm<-as.integer(ip_ok)+as.integer(k_ok)+as.integer(er_ok)+as.integer(whip_ok)
def<-read.csv(DEF_FILE,stringsAsFactors=FALSE);def<-def[def$season %in% SEASONS,];def$team<-norm_team(def$team)
zbs<-function(x,se) ave(x,se,FUN=function(v){mu<-mean(v,na.rm=TRUE);sg<-sd(v,na.rm=TRUE);if(is.na(sg)||sg==0) rep(NA_real_,length(v)) else (v-mu)/sg})
def$dc<-rowMeans(cbind(zbs(def$oaa,def$season),zbs(def$drs,def$season),zbs(def$uzr,def$season)),na.rm=TRUE)
s$dc<-def$dc[match(paste(s$pitcher_team,s$season),paste(def$team,def$season))]
s$gb<-g$gb[match(paste(s$pitcher_id,s$season),paste(g$mlbam,g$season))]

base<-s[!s$spz_placeholder&!is.na(s$gsm)&!is.na(s$dc),]
cat(sprintf("Non-placeholder starts: %d  | with GB%% matched: %d (%.1f%%)\n",
    nrow(base),sum(!is.na(base$gb)),100*mean(!is.na(base$gb))))
base<-base[!is.na(base$gb),]
base$good<-as.integer(base$gsm>=3)
base$def_z<-as.numeric(scale(base$dc)); base$gb_z<-as.numeric(scale(base$gb))
base$good_adj<-residuals(lm(good~sp_skillz_index,data=base))+mean(base$good)
cat(sprintf("Analysis N=%d | GB%% mean=%.1f%% (range %.0f-%.0f)\n",
    nrow(base),100*mean(base$gb),100*min(base$gb),100*max(base$gb)))
cat(sprintf("Overall Spearman(defense, GSM) = %.4f  (reference: inert league-wide)\n",
    cor(base$def_z,base$gsm,method="spearman")))

# ── 1. Defense signal by GB% tier ─────────────────────────────────────────────
cat("\n=============== 1. DEFENSE SIGNAL BY GROUNDBALL TIER ===============\n")
br<-c(-Inf,.40,.45,.50,.55,.60,Inf); labs<-c("<40","40-45","45-50","50-55","55-60",">=60")
base$tier<-cut(base$gb,br,labels=labs)
cat(sprintf("  %-7s %6s %7s | %16s | %s\n","GBtier","n","meanGB","Spearman(def,GSM)","def top-vs-bot 3rd good_adj gap"))
for(t in labs){ d<-base[base$tier==t,]; if(nrow(d)<50){cat(sprintf("  %-7s %6d  (too few)\n",t,nrow(d)));next}
  rho<-cor(d$def_z,d$gsm,method="spearman")
  tt<-cut(d$def_z,quantile(d$def_z,c(0,1/3,2/3,1)),labels=c("lo","mid","hi"),include.lowest=TRUE)
  gap<-100*(mean(d$good_adj[tt=="hi"])-mean(d$good_adj[tt=="lo"]))
  cat(sprintf("  %-7s %6d %6.1f%% | %16.4f | %+5.1f pp\n",t,nrow(d),100*mean(d$gb),rho,gap)) }

# ── 2. Formal interaction (controlling pitcher quality) ───────────────────────
cat("\n=============== 2. FORMAL def x GB INTERACTION ===============\n")
m<-glm(good~sp_skillz_index+def_z*gb_z,data=base,family=binomial)
co<-summary(m)$coefficients
for(term in c("def_z","gb_z","def_z:gb_z")){ r<-co[term,]
  cat(sprintf("  %-12s coef=%+.4f  se=%.4f  z=%+.2f  p=%.3f %s\n",
      term,r[1],r[2],r[3],r[4], if(r[4]<0.05) "*" else "")) }
cat("  (hypothesis: def_z:gb_z > 0  => defense helps MORE as GB% rises)\n")

# ── 3. Simple contrast: extreme GB vs extreme FB ──────────────────────────────
cat("\n=============== 3. EXTREME GB (>=50%) vs EXTREME FB (<40%) ===============\n")
for(grp in list(c("GB-lean (>=50%)",.50,Inf), c("FB-lean (<40%)",-Inf,.40))){
  d<-base[base$gb>=grp[[2]]&base$gb<grp[[3]],]
  rho<-cor(d$def_z,d$gsm,method="spearman")
  cat(sprintf("  %-16s n=%5d  Spearman(def,GSM)=%+.4f\n",grp[[1]],nrow(d),rho)) }
cat("\nDone.\n")
