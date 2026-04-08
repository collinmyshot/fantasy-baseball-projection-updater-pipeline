---
paths:
  - "scripts/fetch_*.R"
  - "scripts/fetch_nfbc_adp.sh"
---

# Data Scraper Context

This workflow covers all external data fetching — projections, ADP, Statcast, park data.

## Scripts
- `fetch_projections.R` — full hitter projection pipeline: fetches BATX/Steamer/OOPSY/ATC from Fangraphs API, calculates weighted per-PA metrics, z-scores, dollar values, merges ADP
- `fetch_nfbc_adp.R` — downloads NFBC ADP from nfc.shgn.com; supports draft types 897/889/890/893; outputs TSV
- `fetch_nfbc_adp.sh` — legacy shell wrapper for fetch_nfbc_adp.R
- `fetch_statcast_bbe.R` — incremental Statcast BBE fetcher with chunk-based caching; skips already-downloaded chunks
- `fetch_team_defense.R` — fetches OAA defensive ratings from Baseball Savant for 2015–2025 (excludes 2020)
- `fetch_mlb_home_parks.R` — fetches 2026 MLB stadium info via MLB Stats API; outputs verified home parks CSV

## Key outputs (data/raw/ and data/processed/)
- `data/raw/ADP.tsv` — latest NFBC ADP
- `data/raw/2026_*_hitters_raw.csv` — per-system projection downloads
- `data/raw/statcast_bbe_store_chunks/` — chunk cache + manifest
- `data/processed/2026_nfbc_adp_clean.csv` — cleaned ADP

## Notes
- NFBC ADP draft type codes: 897 = NFBC 50, 889 = Main Event, 890 = Online Championship, 893 = Draft Champions
- Statcast fetching is incremental — don't re-download existing chunks
- Fangraphs API can return 403; scripts use curl/header fallback
- Name/team normalization happens downstream in fangraphs_projections.R, not here
