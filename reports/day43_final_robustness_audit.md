# Day 43 Final Robustness Audit

## Research status

This audit uses only chronological out-of-sample signals generated on Day 42.
No model was refitted and no threshold was searched.

## Candidate

- Model: `two_stage_sensitivity_0p65_0p65`
- MOVE threshold: `0.65`
- Direction threshold: `0.65`
- Horizon: `50` events
- Cost: `1.0` bps
- Entry rule: first eligible
- Cooldown: `50` events

## Candidate pooled result

- Signals: `225`
- Signal-bearing runs: `68`
- Directional precision: `0.635556`
- Positive-net signal share: `0.480000`
- Mean gross: `1.104361` bps
- Mean net: `0.104361` bps
- Median net: `-0.212278` bps

## Robustness

- Mean net without best signal: `0.057137` bps
- Mean net without best run: `0.034409` bps
- Worst leave-one-fold-out mean net: `-0.125604` bps
- Run-cluster bootstrap 95% CI: `[-0.327366, 0.570912]`
- Bootstrap share of positive means: `0.683900`

## Decision

`FREEZE_PROVISIONAL_CANDIDATE`

A frozen candidate is not a validated profitable strategy. It is only an
eligible specification for one final independent holdout.
