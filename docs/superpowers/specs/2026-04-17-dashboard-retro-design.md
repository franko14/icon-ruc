# Dashboard retro-mono redesign

Single-file change to `dashboard.html`. No JS data flow changes; only CSS tokens, font swap, band rendering parameters, and axis label formatting.

## Goals

1. Replace current modern monospace + sans with a clean retro mainframe monospace, while keeping the italic serif headlines as the one elegant counterweight.
2. Make the ensemble spread bands actually readable on the dark panel — currently they exist (p10–p90 outer + p25–p75 inner) but at 0.12/0.28 alpha they vanish against `#0b0d10`.
3. Tighten chart axis label density so the 24h x-axis stops smearing into "28 Oct 10:00 28 Oct 11:00 …".

## Changes

### Fonts

- Drop `JetBrains Mono` and `Manrope` from the Google Fonts `<link>`.
- Add `IBM Plex Mono` weights 400/500/600.
- Update CSS custom properties:
  - `--font-data: "IBM Plex Mono", ui-monospace, Menlo, monospace`
  - `--font-ui:   "IBM Plex Mono", ui-monospace, Menlo, monospace`
  - `--font-display: "Instrument Serif", "Iowan Old Style", serif` *(unchanged)*
- Update inline uPlot axis `font:` strings to `IBM Plex Mono`.

### Spread bands (chart only — sparkline keeps its tiny single band)

- Outer p10–p90 band alpha: `0.12 → 0.22`.
- Inner p25–p75 band alpha: `0.28 → 0.45`.
- Add a 1px dashed stroke on the p10 and p90 series at 0.35 alpha (was `stroke:'transparent'`).
- p50 line: width `1.6 → 2.0`; add a 1px subtle accent-color glow underneath (`shadowColor` + `shadowBlur` in the uPlot draw hook, or stroke twice — preferred: stroke twice with the wider stroke at 0.25 alpha).
- Update the `--precip-3/4` and `--wind-3/4` CSS tokens (and the `legend .sw` swatches) so the legend swatches match the new on-chart alphas.

### Axis labels

- X-axis tick formatter: when range ≤ 24h, show `HH:mm` only; emit a date label (`DD MMM`) at the first tick of each calendar day instead of on every tick.
- Bump y-axis right-padding so the `≥10 m/s` threshold label doesn't clip — adjust uPlot `padding` to `[14, 60, 10, 10]`.

### Run-picker chevron

- Cosmetic: re-color the embedded SVG chevron to match the new mono palette (no structural change).

## Out of scope

- No changes to `api.py`, `pipeline/`, or the JSON schema.
- No new controls; the existing Spread / Time / Threshold segmented buttons stay as-is.
- No CRT scanline / phosphor-glow effects beyond the single-line p50 glow.

## Acceptance

- Open `dashboard.html` against a live `api.py` run; verify in the browser that:
  - Both charts default to "P10–90" and the layered halo is clearly visible (outer + darker inner) on a near-black panel.
  - All non-headline text renders in IBM Plex Mono (inspect computed style on `.sum-label`, `.readout`, axis ticks).
  - Headlines (`Precipitation`, `Wind Gust`, big numeric headlines) still render in italic Instrument Serif.
  - 24h x-axis shows `HH:mm` ticks without overlapping date labels.
  - Threshold label `≥10 m/s` is fully visible at the right edge.
