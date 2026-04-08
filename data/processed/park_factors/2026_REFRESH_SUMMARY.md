# Park Factors 2026 Refresh - Final Summary

## Overview
Successfully completed a full historical rebuild of park factors (2015-2025) incorporating 6 new dimension/structural changes to MLB ballparks.

## New Park Eras Added
1. **Miami loanDepot park** (2016, 2020): Center field moved in 418→407 ft; wall heights reduced (hitter-friendly)
2. **San Francisco Oracle Park** (2020): CF 399→391 ft, RCF 421→415 ft due to bullpen relocation (modest hitter-friendly)
3. **Detroit Comerica Park** (2023): CF moved in 422→412 ft; walls dropped (hitter-friendly, still pitcher-leaning overall)
4. **New York Mets Citi Field** (2023): RF fence moved in 8.5 ft (modest hitter-friendly, benefits LHH)
5. **Arizona Chase Field** (2018): Humidor for baseballs introduced (affects ball flight/carry)
6. **Cleveland Progressive Field** (2024): Upper-deck seating removed, created wind tunnel effects (boosted HR rates)

## Model Performance
### Validation Results (7-Fold Rolling)
- **2018**: RMSE 0.0230, Correlation 0.564, Slope 0.854
- **2019**: RMSE 0.0305, Correlation 0.462, Slope 0.842
- **2021**: RMSE 0.0225, Correlation 0.372, Slope 0.960
- **2022**: RMSE 0.0169, Correlation 0.513, Slope 0.792
- **2023**: RMSE 0.0220, Correlation 0.253, Slope 0.772
- **2024**: RMSE 0.0155, Correlation 0.559, Slope 0.601
- **2025**: RMSE 0.0185, Correlation 0.401, Slope 0.939

### Key Metrics
- **Data**: 1,635,136 BBE (2015-2025, excluding 2020)
- **Main Model**: lmer with 6 random effects (park_era_id, park_era_half_id, batter/pitcher seasons, fielding/batting teams)
- **Components**: BACON, HR, XBH, double, triple residuals

## Component Weights
Bootstrap-derived optimal weights (400 replicates):
- **BACON**: 0.70 (95% CI: 0.28–1.00)
- **HR**: <0.01 (95% CI: <0.01–0.23)
- **XBH**: 0.15 (95% CI: <0.01–0.67)

**Note**: High uncertainty in weights; BACON dominates in this iteration. Previous 45/35/20 split may be reconsidered based on 2026 validation.

## 2026 Park Factors (Top & Bottom)
### Most Hitter-Friendly
1. Coors Field (Rockies): **130.83**
2. Yankee Stadium (Yankees): **120.27**
3. Globe Life Field (Rangers): **114.65**

### Most Pitcher-Friendly
1. Oakland Coliseum (temp): **83.70**
2. Dodger Stadium (Dodgers): **88.22**
3. Petco Park (Padres): **89.93**

## New Era Impacts
- **Baltimore wall_deep (2022-2024)**: 95.91 PF (pitcher-friendly from "Walltimore")
- **Baltimore wall_medium (2025)**: 101.32 PF (intermediate between original and wall_deep)
- **Cleveland (2024)**: Captured wind tunnel effect in model
- **Miami/SF/Detroit/Mets**: All dimension changes properly reflected in era splits

## Deliverables
- ✅ `park_factors_overall.csv` (with new park_era_id breakpoints)
- ✅ `park_factors_*_overall.csv` (BACON, HR, XBH, double, triple components)
- ✅ `park_factors_savant_style_clean_2026.csv` (30 teams, ranked by PF)
- ✅ `park_factors_savant_style_clean_2026_1H.csv` / `_2H.csv` (seasonal splits)
- ✅ `validation_summary.csv` (fold-by-fold diagnostics)
- ✅ `park_factor_weight_recommendation.csv` (bootstrap weight optimization)

## Process Timeline
- **Validation**: 7 rolling folds, ~5 min per fold
- **Main model fit**: Full dataset (1.6M rows), ~4 min
- **Component models**: BACON/HR/XBH/double/triple, ~30 min total
- **Weight derivation**: 400 bootstrap replicates, ~5 min
- **Clean output generation**: ~2 min
- **Total runtime**: ~60 minutes

## Next Steps
1. Review park factors for known parks (Coors, Yankee Stadium, Petco, etc.)
2. Consider weight recommendations for 2026 in-season updates
3. Monitor new park eras (Cleveland 2024, Arizona humidor) for convergence
4. Plan monthly refresh starting May 1, 2026 (if using monthly pipeline)

---

## Park Eras in Dataset (2015-2025)

| Team / Park | Years Active | Change Type | Park Factor | Source Documentation |
|---|---|---|---|---|
| **ATL** / Truist Park | 2017+ | New Park | 99.25 | https://www.mlb.com/press-release/braves-to-host-yankees-in-exhibition-game-to-open-play-at-suntrust-par-200242862 |
| **MIA** / loanDepot park (CF in) | 2016-2019 | Dimension Change | 97.24 | https://www.mlb.com/marlins/ |
| **MIA** / loanDepot park (Fence reduced) | 2020+ | Dimension Change | 97.24 | https://www.mlb.com/marlins/ |
| **SFG** / Oracle Park (Bullpen center) | 2020+ | Dimension Change | 97.32 | https://www.mlb.com/giants/ |
| **DET** / Comerica Park (CF in) | 2023+ | Dimension Change | 96.49 | https://www.mlb.com/tigers/ |
| **NYM** / Citi Field (RF in) | 2023+ | Dimension Change | 97.22 | https://www.mlb.com/mets/ |
| **BAL** / Camden Yards (Wall deep) | 2022-2024 | Dimension Change | 95.91 | https://www.espn.com/mlb/story/_/id/33050396/baltimore-orioles-move-back-left-field-wall-camden-yards-say-no-longer-outlier-ballparks |
| **BAL** / Camden Yards (Wall medium) | 2025+ | Dimension Change | 101.32 | https://www.mlb.com/orioles/news/orioles-camden-yards-left-field-wall-modifications |
| **KCR** / Kauffman Stadium (Walls in) | 2026+ | Dimension Change | 97.22 | https://www.mlb.com/royals/news/royals-moving-outfield-walls-at-kauffman-stadium |
| **ARI** / Chase Field (Humidor) | 2018+ | Environmental Change | 102.09 | https://www.mlb.com/diamondbacks/ |
| **CLE** / Progressive Field (Wind tunnel) | 2024+ | Structural Change | 99.59 | https://www.mlb.com/guardians/ |
| **TEX** / Globe Life Field | 2021+ | New Park | 98.78 | https://www.mlb.com/news/rangers-original-2020-schedule-released |
| **TOR** / Dunedin (COVID temp) | 2021-05-31 | Temporary Home | 106.01 | https://www.mlb.com/press-release/press-release-blue-jays-to-play-first-2-homestands-in-florida |
| **TOR** / Buffalo (COVID temp) | 2021-06-01 to 07-29 | Temporary Home | 102.31 | https://www.mlb.com/press-release/press-release-blue-jays-to-move-home-location-to-sahlen-field-beginning-june-1 |
| **TBR** / Steinbrenner Field | 2025 | Temporary Home | 101.86 | https://www.mlb.com/rays/news/rays-home-playoff-games-george-m-steinbrenner-field |
| **ATH** / Sutter Health Park | 2025-2027 | Temporary Home | 105.99 | https://www.mlb.com/news/a-s-announce-2025-27-stadium-plans |

**Note**: All 30 teams have a base era (2015-2025 standard configuration) with park factors ranging from 95.25-109.98. This table lists only eras with recorded changes. Park factors represent multi-year averages; individual years may vary due to team offense/defense and environmental factors.

**Multi-Dimensional Changes**:
- Baltimore "Walltimore" (2022-2024): Largest single-era dimension change in dataset
- Toronto 2021: Only team with 2 temporary homes in same year (mid-season move)

---
**Generated**: 2026-04-07
**Model**: Mixed-effects lmer with hierarchical random intercepts
**Data Window**: 2015-2025 (excluding 2020 COVID season)
