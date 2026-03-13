## Park Factor Build Checkpoint

- Timestamp (local): 2026-03-02 08:43 PST
- PID: 480
- Run state: stopped
- Stage: completed
- Log file: `data/processed/park_factors/run_logs/park_factor_build_20260302_071855.log`
- Last validation marker: `Validation fold complete: target=2025 rmse_model=0.018816 rmse_prev=0.018165 corr=0.4546 slope=0.8905`

### Recent log tail

```
Main rolling validation complete.
Fitting final park-factor model on full dataset...
Fitting model with formula: resid ~ half + defense_composite + (1 | park_era_id) + (1 | park_era_half_id) +     (1 | batter_season_id) + (1 | pitcher_season_id) + (1 | fielding_team_season_id) +     (1 | batting_team_season_id)
Rows: 1634138
Final park-factor model fit complete.
Extracting half/overall park-factor tables...
Fitting BACON residual component model...
BACON residual component model fit complete (outcome=bacon_resid).
Fitting HR residual component model...
HR residual component model fit complete (outcome=hr_resid).
Fitting XBH residual component model...
XBH residual component model fit complete (outcome=xbh_resid).
Fitting 2B residual component model...
2B residual component model fit complete (outcome=double_resid).
Fitting 3B residual component model...
3B residual component model fit complete (outcome=triple_resid).
Building Savant-style display table...
Built Savant-style park factor table: data/processed/park_factors/park_factors_savant_style.csv
Park factor build complete.
Output directory: data/processed/park_factors
```
