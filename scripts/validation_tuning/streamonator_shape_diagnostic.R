#!/usr/bin/env Rscript
# streamonator_shape_diagnostic.R
#
# Q: is each component's relationship with GSM linear, or do the TAILS deviate
#    (outlier-emphasis)?  i.e. should Coors / the worst defense pull harder than
#    their linear z-score implies?
#
# Method: characterize the SHAPE of good-start-rate vs each component, measured
# NET OF PITCHER QUALITY (staff-adjust good-rate on SP Skillz first), so a tail
# deviation reflects the park/defense/opponent, not who happened to pitch.
#   - decile table: actual (staff-adj) good-rate vs the linear-fit expectation
#   - quadratic glm: is there statistically visible curvature (controlling SP)?
#   - tail isolation: Coors specifically; worst vs best defense terciles/deciles
#
# Cached; no API.  Usage: Rscript scripts/validation_tuning/streamonator_shape_diagnostic.R

CACHE_DIR <- file.path("data","processed","streamonator_weight_analysis")
DEF_FILE  <- file.path("data","manual","team_defense_2015_2025.csv")
SEASONS   <- 2021:2025
TEAM_MAP <- c(ARI="AZ",ARZ="AZ",AZ="AZ",ATH="ATH",OAK="ATH",BAL="BAL",BOS="BOS",CHC="CHC",CHN="CHC",
  CHW="CHW",CHA="CHW",CWS="CHW",CIN="CIN",CLE="CLE",CLV="CLE",COL="COL",DET="DET",HOU="HOU",KC="KCR",
  KCR="KCR",LAA="LAA",LAD="LAD",LA="LAD",MIA="MIA",FLA="MIA",MIL="MIL",MIN="MIN",NYM="NYM",NYY="NYY",
  PHI="PHI",PIT="PIT",SD="SDP",SDP="SDP",SEA="SEA",SF="SFG",SFG="SFG",STL="STL",TB="TBR",TBR="TBR",
  TAM="TBR",TEX="TEX",TOR="TOR",WSH="WSH",WSN="WSH",WAS="WSH")
norm_team <- function(x){o<-TEAM_MAP[toupper(trimws(as.character(x)))];o[is.na(o)]<-toupper(trimws(as.character(x)))[is.na(o)];unname(o)}

s <- do.call(rbind, lapply(SEASONS, function(yr){d<-read.csv(file.path(CACHE_DIR,sprintf("starts_%d.csv",yr)),stringsAsFactors=FALSE);d$season<-yr;d}))
s$pitcher_team<-norm_team(s$pitcher_team); s$opponent_team<-norm_team(s$opponent_team)
s$whip<-ifelse(!is.na(s$ip)&s$ip>0,(s$h+s$bb)/s$ip,Inf)
ip_ok<-!is.na(s$ip)&s$ip>=5; k_ok<-!is.na(s$k)&!is.na(s$ip)&s$k>=(floor(s$ip)-1)
er_ok<-!is.na(s$er)&!is.na(s$ip)&((s$ip>=6&s$er<=2)|(s$ip>=5&s$ip<6&s$er<=3)|(s$ip>=4&s$ip<5&s$er<=2)|(s$ip<4&s$er<=1))
whip_ok<-!is.na(s$whip)&s$whip<=1.18
s$gsm<-as.integer(ip_ok)+as.integer(k_ok)+as.integer(er_ok)+as.integer(whip_ok)

def<-read.csv(DEF_FILE,stringsAsFactors=FALSE); def<-def[def$season %in% SEASONS,]; def$team_norm<-norm_team(def$team)
zbs<-function(x,se) ave(x,se,FUN=function(v){mu<-mean(v,na.rm=TRUE);sg<-sd(v,na.rm=TRUE);if(is.na(sg)||sg==0) rep(NA_real_,length(v)) else (v-mu)/sg})
def$dc<-rowMeans(cbind(zbs(def$oaa,def$season),zbs(def$drs,def$season),zbs(def$uzr,def$season)),na.rm=TRUE)
def$dc[!is.finite(def$dc)]<-NA_real_
s$dc<-def$dc[match(paste(s$pitcher_team,s$season),paste(def$team_norm,def$season))]

base<-s[!s$spz_placeholder & !is.na(s$gsm) & !is.na(s$park_factor_raw) & !is.na(s$dc),]
base$good<-as.integer(base$gsm>=3)
base$park_team<-ifelse(base$home_away=="H",base$pitcher_team,base$opponent_team)
base$def_index<-100+as.numeric(scale(base$dc))*10
cat(sprintf("Sample N=%d  | overall good-rate=%.1f%%\n",nrow(base),100*mean(base$good)))

# staff-adjusted good (linear-probability residual on SP Skillz, re-centered)
base$good_adj <- residuals(lm(good ~ sp_skillz_index, data=base)) + mean(base$good)

shape_one <- function(x, lab, dir_note) {
  z <- as.numeric(scale(x))
  # quadratic curvature, controlling for pitcher quality
  m <- glm(good ~ sp_skillz_index + z + I(z^2), data=base, family=binomial)
  co <- summary(m)$coefficients
  q  <- co["I(z^2)",]
  # decile table on staff-adjusted good-rate, vs linear-fit expectation
  br <- quantile(x, seq(0,1,0.1), na.rm=TRUE); br[1]<-br[1]-1e-9; br[length(br)]<-br[length(br)]+1e-9
  dec <- cut(x, br, labels=FALSE, include.lowest=TRUE)
  linfit <- lm(good_adj ~ x, data=base); pred <- predict(linfit)
  cat(sprintf("\n=== %s  (%s) ===\n", lab, dir_note))
  cat(sprintf("  Curvature (z^2, controlling SP Skillz): coef=%+.4f  p=%.3f  %s\n",
      q["Estimate"], q["Pr(>|z|)"], if(q["Pr(>|z|)"]<0.05) "<-- non-linear" else "(linear)"))
  cat(sprintf("  %4s %6s %8s | %10s %10s %9s %7s\n","dec","n","mean_X","good_raw%","good_adj%","linfit%","dev"))
  for(d in 1:10){ sel<-dec==d
    gr<-100*mean(base$good[sel]); ga<-100*mean(base$good_adj[sel]); lf<-100*mean(pred[sel])
    cat(sprintf("  %4d %6d %8.1f | %9.1f%% %9.1f%% %8.1f%% %+6.1f\n",
        d,sum(sel),mean(x[sel]),gr,ga,lf,ga-lf)) }
}

shape_one(base$park_factor_raw, "PARK FACTOR", "higher = hitter-friendly = good-rate should fall")
shape_one(base$def_index,       "OWN-TEAM DEFENSE", "higher = better D = good-rate should rise")
shape_one(base$team_rater_raw,  "OPPONENT OFFENSE (Team Rater)", "higher = tougher = good-rate should fall")

# ── Park tail: is COORS special beyond its park-factor value? ──────────────────
cat("\n================= PARK TAIL — is Coors special? =================\n")
pk <- aggregate(cbind(good, good_adj, park_factor_raw) ~ park_team, data=base, FUN=mean)
pk$n <- as.vector(table(base$park_team)[pk$park_team])
pk <- pk[order(-pk$park_factor_raw),]
# linear expectation of staff-adj good-rate from park factor (start-level fit)
plf <- lm(good_adj ~ park_factor_raw, data=base)
pk$exp_adj <- predict(plf, newdata=data.frame(park_factor_raw=pk$park_factor_raw))
cat(sprintf("  %-5s %5s %7s | %9s %9s %9s %7s\n","park","n","PF","good_raw%","good_adj%","exp(lin)%","dev"))
for(i in c(1:5, (nrow(pk)-2):nrow(pk))){ r<-pk[i,]
  cat(sprintf("  %-5s %5d %7.1f | %8.1f%% %8.1f%% %8.1f%% %+6.1f%s\n",
      r$park_team,r$n,r$park_factor_raw,100*r$good,100*r$good_adj,100*r$exp_adj,100*(r$good_adj-r$exp_adj),
      if(r$park_team=="COL") "  <-- COORS" else "")) }

# ── Defense tail: floor asymmetry (bottom vs top) ─────────────────────────────
cat("\n============= DEFENSE TAIL — floor asymmetry? =============\n")
dbr<-quantile(base$def_index, seq(0,1,0.1)); dbr[1]<-dbr[1]-1e-9
ddec<-cut(base$def_index,dbr,labels=FALSE,include.lowest=TRUE)
ov<-mean(base$good_adj)
cat(sprintf("  Overall staff-adj good-rate = %.1f%%\n",100*ov))
cat(sprintf("  Bottom-decile defense: good_adj=%.1f%%  (%+.1f vs overall)\n",
    100*mean(base$good_adj[ddec==1]), 100*(mean(base$good_adj[ddec==1])-ov)))
cat(sprintf("  Top-decile  defense:   good_adj=%.1f%%  (%+.1f vs overall)\n",
    100*mean(base$good_adj[ddec==10]),100*(mean(base$good_adj[ddec==10])-ov)))
cat("  -> if |bottom gap| >> |top gap|, the effect is a downside FLOOR, not symmetric.\n")

# ── Dump tidy tables for visualization ────────────────────────────────────────
write.csv(pk[,c("park_team","park_factor_raw","good","good_adj","exp_adj","n")],
          file.path(CACHE_DIR,"shape_park_level.csv"), row.names=FALSE)
ddf <- data.frame(decile=1:10,
                  def_index = as.vector(tapply(base$def_index, ddec, mean)),
                  good_adj  = as.vector(tapply(base$good_adj,  ddec, mean)),
                  n         = as.vector(table(ddec)))
write.csv(ddf, file.path(CACHE_DIR,"shape_def_decile.csv"), row.names=FALSE)
cat("\nDone.\n")
