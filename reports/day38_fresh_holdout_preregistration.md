# Day 38 Fresh-Holdout Preregistration

## Purpose

This document freezes the model architecture, threshold-selection rule,
entry rule, cost assumption, and quality protocol before inspecting a new
weekday active-hours holdout batch.

Thursday Day 30 data were not used by the Day 38 threshold-stability script.

## Data used for threshold stability

- Tuesday batch: `weekday_active_tue_day28`
- Wednesday batch: `weekday_active_wed_day29`
- Three expanding chronological calibration folds
- Prediction horizon: 50 events
- MOVE dead zone: 1.0 bps

## Direction stage

- Model: standardized logistic regression
- Features: reduced book set, 8 features
- C: 1.0
- Target: direction conditional on `|future return| > 1.0 bps`

## Primary MOVE stage

- Feature set: `medium_trade_flow`
- Features: 24
- C: 0.03
- MOVE threshold: 0.65
- Direction threshold: 0.65
- Selection source: `base_eligible_fallback`
- Median fold mean net: 0.049265 bps
- Minimum fold mean net: -1.633248 bps
- Positive folds: 2 of 3

## Challenger MOVE stage

- Feature set: `reduced_trade_flow`
- Features: 14
- C: 0.03
- MOVE threshold: 0.75
- Direction threshold: 0.55
- Selection source: `base_eligible_fallback`
- Median fold mean net: 0.165402 bps
- Minimum fold mean net: -2.396057 bps
- Positive folds: 2 of 3

## Deployment rule

- Entry rule: first eligible signal
- Cooldown: 50 events after an entry
- Alternative post-hoc maximum-confidence rules are prohibited
- Round-trip cost assumption: 1.0 bps per signal

## Final fitting before the fresh holdout

- Fit primary and challenger models using strict Tuesday and Wednesday runs
- Do not alter features, C values, thresholds, horizon, dead zone, cooldown,
  or cost after seeing fresh-holdout results
- Thursday remains a development diagnostic and is not used by this
  threshold-selection script

## Required fresh-holdout outputs

1. Strict raw and processed run counts
2. MOVE and direction classification metrics
3. Primary and challenger signal counts
4. Signal-bearing runs
5. Directional precision
6. Mean and median signed return
7. Mean and median net return after 1 bps
8. LONG/SHORT breakdown
9. Per-run results
10. Leave-one-run-out and run-cluster bootstrap
11. Cost sensitivity at 0.5, 1.0, 1.5, and 2.0 bps

## Decision rule

The primary model remains the official confirmatory model. The challenger is
reported separately. The challenger cannot replace the primary based only on
the fresh holdout unless the result is explicitly labeled exploratory and is
confirmed on another independent day.
