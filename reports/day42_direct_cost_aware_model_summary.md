# Day 42 Direct Cost-Aware Model Development

Tuesday-Friday are development data. This is not a fresh-holdout test.

## Fixed expanding folds

- fold_1_tue_to_wed: train ['weekday_active_tue_day28'] -> validate weekday_active_wed_day29
- fold_2_tue_wed_to_thu: train ['weekday_active_tue_day28', 'weekday_active_wed_day29'] -> validate weekday_active_thu_day30
- fold_3_tue_wed_thu_to_fri: train ['weekday_active_tue_day28', 'weekday_active_wed_day29', 'weekday_active_thu_day30'] -> validate weekday_active_fri_day39

## Compared models

- Two-stage primary: MOVE 0.75 + direction 0.65
- Two-stage sensitivity: MOVE 0.65 + direction 0.65
- Direct multiclass base: SHORT / NO TRADE / LONG
- Direct multiclass temporal: base plus 12 fixed delta features

## Direct target

- SHORT: future h50 return < -1 bps
- NO TRADE: future h50 return between -1 and +1 bps
- LONG: future h50 return > +1 bps

## Interpretation boundary

Day 42 may nominate one candidate for Day 43 robustness analysis.
Any final candidate must be frozen before another independent holdout.

See reports/tables/day42_model_comparison_summary.csv.