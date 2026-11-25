# Volcano Activity Indicator (VAI)

The VAI is a 0-100 score per US volcano that says how active the volcano looks **compared with its own past**. It is a monitoring aid for the emergency-management analyst of Workshop 1, **not an eruption forecast**: official alert levels come from the USGS volcano observatories, and the dashboard says so next to the indicator.

Code: `src/volcano_pipeline/gold/vai_rules.py` (rules) and `src/volcano_pipeline/gold/indicator.py` (PySpark). Parameters: `config/sources.yaml`, block `indicator`. Validation: `notebooks/05_vai_validation.ipynb`.

## Formula

```
VAI = 100 · (w_R·R + w_S·S + w_H·H) / (sum of the weights of the available components)
```

| Component | Input | Rule | Weight |
|---|---|---|---|
| **R**, reported activity | latest USGS HANS alert level (Silver `alert_notices`) | NORMAL 0, ADVISORY 0.4, WATCH 0.7, WARNING 1.0. Not available when the volcano has no HANS notice | `weights.reported` = 0.45 |
| **S**, seismic anomaly | earthquakes M ≥ 1.0 within 30 km (Silver `earthquakes`, `nearest_vnum`) | count in the 7 days before the run (`seismic_window_days`), compared with every complete week since 2010-01-04 (`history_start`), **weeks without earthquakes included**. Mid-rank percentile p = (weeks below + ½ · weeks equal) / weeks; S = max(0, (p − 0.5) / 0.5). Not available below 20 earthquakes in the baseline (`seismic_min_baseline_events`) | `weights.seismic` = 0.35 |
| **H**, eruptive state | GVP eruptions (Silver `eruptions`) | 1.0 while an eruption is going on (GVP marks it with `>` before the end year) or in a week an eruption overlaps; otherwise exp(−Δt / 25) with Δt the years since the last eruption ended (its start when the end is unknown; `eruptive_decay_years`). Not available when the volcano has no dated eruption | `weights.eruptive` = 0.20 |

**Levels** (`levels`): LOW < 25 ≤ MODERATE < 50 ≤ ELEVATED < 75 ≤ HIGH.

A component that is not available is left out and the other weights are shared again (renormalised). The dashboard lists which components each volcano used (user story US8).

## Decisions made while implementing it

| Decision | Why |
|---|---|
| Zero weeks count in the baseline, and the percentile is mid-rank | Most volcanoes are silent most weeks. Without the zero weeks, or with "weeks ≤ now", a volcano with no earthquake at all would get S close to 1. With them, no earthquake now gives p ≤ 0.5 and S = 0 |
| The current week is not in the baseline | It is not over; its count would be too low |
| Continuing = GVP `EndDateYearModifier` `>` | An empty end date usually means "unknown" (6,602 of 11,089 GVP eruptions), not "still going on" |
| Region filter = USGS observatory group, GVP subregion as fallback | US6 asks for Alaska, Hawaii, Cascades...; HANS lists 153 of the 165 volcanoes, the GVP subregion covers the rest (`indicator.regions`) |
| Weekly history from S and H only | USGS HANS keeps no past alert levels, so R cannot be rebuilt for past weeks |
| The dashboard history draws a 4-week average over faint weekly values | One earthquake in a usually silent week already looks unusual, so the weekly value jumps; the average is easier to read and halves the false alarms (validation below) |
| Weekly-report bonus (+0.2 new / +0.1 continuing) not applied | The optional weekly RSS was never ingested (`activity_report` stays empty, see `sql/006`) |

## What Gold holds (storytelling file)

| Section | Rows (2026-11-25) | Content |
|---|---|---|
| `vai_current` | 1,144 (143 volcanoes) | `vai_score`, `level_ord`, `reported`, `seismic`, `eruptive` (null = not available), `components_used`, `earthquakes_window`, `baseline_percentile`; dimension = vnum |
| `vai_weekly` | 338,304 (128 volcanoes × 881 weeks × 3) | `vai_score`, `seismic`, `eruptive` per Monday since 2010-01-04 |
| `volcano_map` | 660 (165 volcanoes) | `latitude`, `longitude`, `alert_level_ord`, `color_code_ord`; dimension = `vnum|name|region|observatory` |
| `eruption_history` | 918 | `vei` (null when unknown) and `continuing` (0/1) at each eruption start since 1900 |

22 of the 165 volcanoes have no VAI: no HANS notice, fewer than 20 earthquakes and no dated eruption.

## Validation (notebook 05, Gold of 2026-11-25)

44 of the 47 US eruptions since 2010 happened at a volcano with a seismic baseline. Threshold: VAI ≥ 50 (ELEVATED).

| Signal | Hit, 4 weeks before or after the start | Early hit, 4 complete weeks before the start | False alarm rate (weeks > 8 weeks from any eruption) |
|---|---|---|---|
| Weekly VAI | 0.70 | 0.52 | 0.14 |
| S alone | 0.68 | 0.52 | 0.17 |
| 4-week average VAI | 0.50 | 0.36 | 0.06 |

* The Workshop 1 hit definition (before *or after*) is partly circular: from the start week on, H = 1. The early hit rate is the honest measure; at the default weights it equals S alone, so the early signal comes from the earthquakes.
* Case studies: **Kilauea** (summit eruption starting 2020-12-20), **Mauna Loa** (2022-11-27, months of rising seismicity before) and **Great Sitkin** (2021-05-25) were ELEVATED or HIGH before the start. **Ahyi** (2025-10-20), a submarine volcano with sparse seismic coverage, was missed (S = 0), as Workshop 1 expected. Kilauea 2018 cannot be tested: GVP counts it as the end of the 1983-2018 eruption, which started before the baseline.
* Weights: when H gets more than half of the weight (S share 0.45 or less), the early hit rate rises to 0.82-0.89, but only because frequently erupting volcanoes keep H near 1 for years and sit at ELEVATED most of the time. An equal split of S and H gives the fewest false alarms (0.12 against 0.14 at the default S share of 0.35 / 0.55 = 0.64) with almost the same early hit rate (0.50 against 0.52); the default is kept as designed in Workshop 1, and the equal split is the alternative to discuss. At the HIGH threshold (75) the false alarm rate stays near 0.05 for any weight.

Figures: `workshop_4/figures/vai_hits_false_alarms.png`, `vai_case_studies.png`, `vai_weight_sensitivity.png`.

## Limitations

* The weekly S is in-sample: each week is compared with the whole 2010-present history, including later weeks. The current S only uses past weeks.
* Earthquakes are attributed to the nearest volcano within 30 km; in clustered fields (Aleutians, Cascades) a swarm under one volcano can raise its neighbour too.
* Volcanoes with sparse networks (submarine, Northern Mariana Islands) rarely reach 20 baseline earthquakes, so their VAI rests on R and H.
* Single-component volcanoes can reach extreme scores: renormalisation gives S the whole weight when R and H are missing, which is why several unmonitored Alaska volcanoes with one or two recent earthquakes top the ranking. The ranking draws those single-input scores lighter, its hover gives the number of inputs and of earthquakes in the last 7 days, and the side panel lists the inputs of the selected volcano.
* R is only today's alert; the weekly-report bonus is not applied.
