#!/usr/bin/env Rscript

required <- c("jsonlite", "yaml", "googlesheets4")

if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos = "https://cloud.r-project.org")
}

if (!file.exists("renv.lock")) {
  renv::init(bare = TRUE)
}

for (pkg in required) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    renv::install(pkg)
  }
}

renv::snapshot(prompt = FALSE)
message("renv initialized/snapshotted.")
