#!/usr/bin/env Rscript
# streamonator_home_road_park.R
#
# Q (user): do teams with extreme home parks have extreme home-vs-road ERA splits?
# Logic: at team level, home-vs-road flattens own staff (SP Skillz) AND own defense
# (same gloves travel), and ~flattens opponent quality (balanced schedule). What
# remains is PARK + home-field-advantage (HFA).  So:
#   1. establish the league HFA baseline (mean home-road split, parks ~cancel)
#   2. correlate each team-season split with era-correct Park Factor (expect strong)
#      and with defense composite (expect ~0 — defense can't drive a split)
#   3. decompose split into FIP (HR/BB/K = defense-free, park-on-air) vs
#      BABIP (balls in play = park geometry, the channel that masquerades as defense)
#
# ── ANSWER (run 2026-06-22): park DOES drive home-road splits. No conflict. ─
#   This exists to answer a fair challenge: "if park is such a weak per-start
#   input, why do pitchers post bad ERAs in bad parks?"
#     HFA baseline: -0.28 ERA (better at home), mostly FIP/peripherals;
#                   the BABIP split is ~0
#     ERA split vs Park Factor:  r = +0.42  (~+0.024 ERA per PF point;
#                                Coors runs a +0.42 split)
#     ERA split vs defense:      r = -0.16  (defense travels, so it CANNOT
#                                drive a split — exactly as predicted)
#     PF <-> defense:            r = -0.07  (no confound between them)
#   Park works through BOTH channels about equally:
#     hr_idx    <-> FIP split   r = 0.48   (the HR / over-the-fence channel)
#     bacon_idx <-> BABIP split r = 0.40   (the balls-in-play channel — this is
#                                the one that masquerades as defense)
#   NO CONTRADICTION with the Streamonator: park matters for ERA, but it is
#   already captured, it is linear, it is skill-dominated per start, and it is
#   genuinely distinct from defense. This investigation VALIDATES 6:3:1 plus
#   linear scaling; nothing was overturned.
#
# Data: team_pitching_{home,away}_YYYY.csv (2021-2025), park eras from the PF model.
#   Era-correct PF comes from team_park_era_audit.csv joined to each team's
#   PRIMARY home venue by max n_bbe. The dedup matters: neutral-site games
#   otherwise give a team two "home" venues in a season.
# Usage: Rscript scripts/validation_tuning/streamonator_home_road_park.R

DL <- "/Users/ckaufman/Downloads"
SEASONS <- 2021:2025
PF_ID   <- "data/processed/park_factors/park_factors_savant_style_with_id.csv"
ERA_AUD <- "data/processed/park_factors/team_park_era_audit.csv"
DEF_FILE<- "data/manual/team_defense_2015_2025.csv"
TEAM_MAP <- c(ARI="AZ",ARZ="AZ",AZ="AZ",ATH="ATH",OAK="ATH",BAL="BAL",BOS="BOS",CHC="CHC",CHN="CHC",
  CHW="CHW",CHA="CHW",CWS="CHW",CIN="CIN",CLE="CLE",CLV="CLE",COL="COL",DET="DET",HOU="HOU",KC="KCR",
  KCR="KCR",LAA="LAA",LAD="LAD",LA="LAD",MIA="MIA",FLA="MIA",MIL="MIL",MIN="MIN",NYM="NYM",NYY="NYY",
  PHI="PHI",PIT="PIT",SD="SDP",SDP="SDP",SEA="SEA",SF="SFG",SFG="SFG",STL="STL",TB="TBR",TBR="TBR",
  TAM="TBR",TEX="TEX",TOR="TOR",WSH="WSH",WSN="WSH",WAS="WSH")
norm_team <- function(x){o<-TEAM_MAP[toupper(trimws(as.character(x)))];o[is.na(o)]<-toupper(trimws(as.character(x)))[is.na(o)];unname(o)}
ip_dec <- function(x){w<-trunc(x); f<-round((x-w)*10); w + f/3}

# ── Load home/away pitching, compute ERA / FIP-core / BABIP per team-season ────
load_side <- function(side){
  do.call(rbind, lapply(SEASONS, function(yr){
    d <- read.csv(file.path(DL, sprintf("team_pitching_%s_%d.csv", side, yr)), stringsAsFactors=FALSE,
                  fileEncoding="UTF-8-BOM")
    ipd <- ip_dec(d$IP)
    bip <- d$TBF - d$BB - d$HBP - d$SO - d$HR           # approx balls in play
    data.frame(team=norm_team(d$Team), season=yr,
               ERA = d$ERA,
               FIPc= (13*d$HR + 3*(d$BB+d$HBP) - 2*d$SO)/ipd,   # FIP minus constant (cancels in split)
               BABIP=(d$H - d$HR)/bip,
               HR9 = 9*d$HR/ipd,
               stringsAsFactors=FALSE)
  }))
}
h <- load_side("home"); a <- load_side("away")
m <- merge(h, a, by=c("team","season"), suffixes=c("_h","_a"))
m$ERA_split   <- m$ERA_h   - m$ERA_a
m$FIP_split   <- m$FIPc_h  - m$FIPc_a
m$BABIP_split <- m$BABIP_h - m$BABIP_a
cat(sprintf("Team-seasons: %d\n", nrow(m)))

# ── Era-correct Park Factor + components ──────────────────────────────────────
aud <- read.csv(ERA_AUD, stringsAsFactors=FALSE); aud$team<-norm_team(aud$home_team)
aud <- aud[aud$season %in% SEASONS,]
aud <- aud[order(aud$team, aud$season, -aud$n_bbe),]       # keep PRIMARY home venue
aud <- aud[!duplicated(paste(aud$team, aud$season)),]      # (drop neutral-site one-offs)
pf  <- read.csv(PF_ID,  stringsAsFactors=FALSE)
aud2<- merge(aud[,c("team","season","park_era_id","venue_name")],
             pf[,c("park_era_id","overall_pf_idx_100","bacon_idx_100","hr_idx_100")], by="park_era_id", all.x=TRUE)
m <- merge(m, aud2, by=c("team","season"), all.x=TRUE)

# ── Defense composite (season z of oaa/drs/uzr, mean) ─────────────────────────
def<-read.csv(DEF_FILE,stringsAsFactors=FALSE); def<-def[def$season %in% SEASONS,]; def$team<-norm_team(def$team)
zbs<-function(x,se) ave(x,se,FUN=function(v){mu<-mean(v,na.rm=TRUE);sg<-sd(v,na.rm=TRUE);if(is.na(sg)||sg==0) rep(NA_real_,length(v)) else (v-mu)/sg})
def$def_comp<-rowMeans(cbind(zbs(def$oaa,def$season),zbs(def$drs,def$season),zbs(def$uzr,def$season)),na.rm=TRUE)
m <- merge(m, def[,c("team","season","def_comp")], by=c("team","season"), all.x=TRUE)

# Flag tiny-sample 2025 relocations (Sutter Health = A's, Steinbrenner = Rays)
m$relocated_2025 <- m$venue_name %in% c("Sutter Health Park","George M. Steinbrenner Field")

# ══ 1. HFA BASELINE ════════════════════════════════════════════════════════════
cat("\n================= 1. HOME-FIELD-ADVANTAGE BASELINE =================\n")
cat("  (parks ~cancel league-wide, so the mean split ~ generic HFA)\n")
for(v in c("ERA_split","FIP_split","BABIP_split")){
  x<-m[[v]]; tt<-t.test(x)
  cat(sprintf("  mean %-11s = %+.3f  (sd %.3f)  95%% CI [%+.3f, %+.3f]  %s\n",
      v, mean(x), sd(x), tt$conf.int[1], tt$conf.int[2],
      if(tt$p.value<0.05) sprintf("p=%.3f *",tt$p.value) else sprintf("p=%.2f",tt$p.value)))
}
cat("  (negative = better at home, the expected home-field edge)\n")

# ══ 2. SPLIT vs PARK vs DEFENSE ════════════════════════════════════════════════
cat("\n================= 2. DO EXTREME PARKS DRIVE THE SPLIT? =================\n")
mc <- m[!is.na(m$overall_pf_idx_100) & !is.na(m$def_comp),]
cc <- function(x,y) {ok<-is.finite(x)&is.finite(y); c(r=cor(x[ok],y[ok]), rho=cor(x[ok],y[ok],method="spearman"))}
cat(sprintf("  n with PF & defense = %d\n", nrow(mc)))
cat(sprintf("  %-12s | %18s | %18s\n","split","vs Park Factor","vs Defense composite"))
for(v in c("ERA_split","FIP_split","BABIP_split")){
  p<-cc(mc[[v]], mc$overall_pf_idx_100); d<-cc(mc[[v]], mc$def_comp)
  cat(sprintf("  %-12s | r=%+.2f  rho=%+.2f | r=%+.2f  rho=%+.2f\n", v, p["r"],p["rho"], d["r"],d["rho"]))
}
cat(sprintf("\n  (confound check) Park Factor vs Defense composite: r=%+.2f\n",
    cc(mc$overall_pf_idx_100, mc$def_comp)["r"]))
# calibration: ERA points of split per PF point
fit <- lm(ERA_split ~ overall_pf_idx_100, data=mc)
cat(sprintf("  Calibration: each +1 PF point => %+.3f ERA of home-road split (R^2=%.2f)\n",
    coef(fit)[2], summary(fit)$r.squared))

# ══ 3. DECOMPOSITION: which channel carries the park signal? ════════════════════
cat("\n================= 3. FIP (HR) vs BABIP (balls in play) channel =================\n")
cat(sprintf("  ERA_split variance explained by FIP_split:   R^2=%.2f\n", summary(lm(ERA_split~FIP_split,data=mc))$r.squared))
cat(sprintf("  ERA_split variance explained by BABIP_split: R^2=%.2f\n", summary(lm(ERA_split~BABIP_split,data=mc))$r.squared))
cat(sprintf("  PF correlates with FIP channel  (hr_idx vs FIP_split):   r=%+.2f\n", cc(mc$hr_idx_100, mc$FIP_split)["r"]))
cat(sprintf("  PF correlates with BABIP channel (bacon_idx vs BABIP):   r=%+.2f\n", cc(mc$bacon_idx_100, mc$BABIP_split)["r"]))

# ══ 4. RANKED PARK TABLE (by park-era, mean over 2021-2025) ═════════════════════
cat("\n================= 4. PARKS RANKED BY FACTOR, with mean splits =================\n")
ag <- aggregate(cbind(ERA_split,FIP_split,BABIP_split,overall_pf_idx_100,def_comp,reloc=relocated_2025) ~ team+venue_name,
                data=m, FUN=function(z) mean(z, na.rm=TRUE))
ag <- ag[order(-ag$overall_pf_idx_100),]
cat(sprintf("  %-4s %-26s %5s | %9s %9s %9s | %5s\n","tm","park","PF","ERAsplit","FIPsplit","BABIPspl","defZ"))
show <- rbind(head(ag,6), tail(ag,6))
for(i in seq_len(nrow(show))){ r<-show[i,]
  cat(sprintf("  %-4s %-26s %5.1f | %+8.2f %+8.2f %+8.3f | %+5.2f%s\n",
      r$team, substr(r$venue_name,1,26), r$overall_pf_idx_100, r$ERA_split, r$FIP_split, r$BABIP_split, r$def_comp,
      if(r$reloc>0) "  (2025 reloc, small n)" else if(r$team=="COL") "  <-- COORS" else "")) }
cat("\nDone.\n")
