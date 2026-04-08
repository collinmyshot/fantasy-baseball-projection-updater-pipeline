---
paths:
  - "app.R"
  - "scripts/deploy_shinyapps.R"
---

# Shiny App Context

Interactive fantasy baseball explorer. Runs locally and deploys to shinyapps.io on-demand.

## Files
- `app.R` — full UI + server (~196 KB): ADP exploration, projection comparison by system, position filtering, historical targets, dollar value charts, pitcher view (NFBC 50 / Main Event / Online Championship modes)
- `scripts/deploy_shinyapps.R` — deploys to shinyapps.io via rsconnect (on-demand, not automated)

## Running locally
```bash
Rscript -e "shiny::runApp('/Users/ckaufman/Documents/New project', host='127.0.0.1', port=8080)"
```

## Deploying to shinyapps.io
Run `scripts/deploy_shinyapps.R` — handles rsconnect login and bundle deployment.
Uses `.rscignore` to exclude .git, .github, data/raw, data/manual from the bundle.
Deploy is on-demand only; GitHub Actions does NOT deploy Shiny.

## Notes
- `app.R` and `deploy_shinyapps.R` are git-ignored (kept local per .gitignore)
- rsconnect/ metadata is also git-ignored
- Data consumed by the app comes from `data/processed/` — the app reads CSVs, it does not run the pipeline
- NFBC draft mode switching (50/Main Event/Online Championship) is handled in the server logic
- Packages: shiny, DT, bslib, dplyr, readr, ggplot2
