# Dashboard Retro-Mono Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the static `dashboard.html` to use IBM Plex Mono throughout (with Instrument Serif italic kept for headlines), make the ensemble-spread bands visible on the dark panel, and tighten chart axis labels.

**Architecture:** Single-file change to `dashboard.html`. No JS data flow changes; only the Google Fonts `<link>`, CSS custom properties + selected rule blocks, the band/series config inside `drawChart()`, the uPlot axis `values:` formatter and `padding:`, and one inline SVG (the run-picker chevron). The pipeline (`pipeline/`, `api.py`) is untouched.

**Tech Stack:** Vanilla HTML/CSS/JS, uPlot 1.6.31, Google Fonts (IBM Plex Mono + Instrument Serif).

**Verification approach:** This is a visual-only change. There are no automated unit tests for `dashboard.html`. Verification happens by serving the dashboard with `api.py` and inspecting in a browser at each checkpoint.

**Spec:** `docs/superpowers/specs/2026-04-17-dashboard-retro-design.md`

---

## Pre-flight

### Task 0: Capture baseline

**Files:** none

- [ ] **Step 1: Start the API server in the background**

```bash
.venv/bin/python api.py &
echo $! > /tmp/icon-api.pid
sleep 2
curl -sf http://localhost:5000/api/runs | head -c 200
```

Expected: a JSON array of run IDs, e.g. `["2026-04-17T0700", ...]`.

If the curl fails, stop and investigate before continuing — the rest of the plan assumes the API serves real data.

- [ ] **Step 2: Confirm dashboard loads**

Open `http://localhost:5000/` in a browser. Expected: header reads "Bratislava / 48.15°N 17.11°E", two summary cells (Precipitation, Wind Gust), two chart blocks below, no red error banner.

- [ ] **Step 3: Note current state**

Confirm the precipitation chart's tan band is *barely* visible around the p50 line (this is what we're fixing) and that the font in the readout strip is `JetBrains Mono` (right-click → Inspect → Computed → font-family on `.readout`).

---

## Task 1: Swap fonts to IBM Plex Mono

**Files:**
- Modify: `dashboard.html:9` (Google Fonts `<link>`)
- Modify: `dashboard.html:37-39` (CSS font custom properties)
- Modify: `dashboard.html:606,615,619` (uPlot inline `font:` strings inside `drawChart()`)
- Modify: `dashboard.html:648` (uPlot inline `font:` for threshold label inside `hooks.draw`)

- [ ] **Step 1: Replace the Google Fonts link**

Locate `dashboard.html:9`:

```html
<link href="https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=JetBrains+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700&display=swap" rel="stylesheet">
```

Replace with:

```html
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Instrument+Serif:ital@0;1&display=swap" rel="stylesheet">
```

- [ ] **Step 2: Update the CSS font tokens**

Locate `dashboard.html:37-39`:

```css
  --font-display:"Instrument Serif", "Iowan Old Style", serif;
  --font-data:"JetBrains Mono", ui-monospace, Menlo, monospace;
  --font-ui:"Manrope", ui-sans-serif, system-ui, sans-serif;
```

Replace with:

```css
  --font-display:"Instrument Serif", "Iowan Old Style", serif;
  --font-data:"IBM Plex Mono", ui-monospace, Menlo, monospace;
  --font-ui:"IBM Plex Mono", ui-monospace, Menlo, monospace;
```

(Both `--font-data` and `--font-ui` now resolve to the same family. We keep two tokens so a future change can split them again without touching every rule.)

- [ ] **Step 3: Update uPlot axis font strings**

Search the file for the literal string `"JetBrains Mono", monospace` and replace every occurrence (4 total: x-axis font, y-axis font, y-axis labelFont, threshold-label fillText font) with `"IBM Plex Mono", monospace`. Use this exact replace_all:

- Find:  `'10px "JetBrains Mono", monospace'`
- Replace: `'10px "IBM Plex Mono", monospace'`

- [ ] **Step 4: Reload and verify in the browser**

Hard-reload `http://localhost:5000/` (Cmd+Shift+R). Inspect:

- `.readout` computed font-family → starts with `IBM Plex Mono`
- `.brand-title` computed font-family → still starts with `Instrument Serif`
- `.sum-value` computed font-family → still starts with `Instrument Serif`
- Chart axis ticks visually render in the new monospace (slightly different letterforms vs. JetBrains; the numerals "1", "0", "/" should look distinctly more "IBM mainframe").

If `--font-ui` is being overridden anywhere unexpectedly, grep for `font-family:` inside the `<style>` block and confirm every literal use either references one of the three CSS variables or is intentional.

- [ ] **Step 5: Commit**

```bash
git add dashboard.html
git commit -m "feat(dashboard): swap to IBM Plex Mono for data + UI font"
```

---

## Task 2: Make the spread bands visible

**Files:**
- Modify: `dashboard.html:23-31` (CSS color tokens for precip/wind alphas)
- Modify: `dashboard.html:563-583` (band fill alphas + series stroke for p10/p25/p75/p90 + p50 glow inside `drawChart()`)

- [ ] **Step 1: Bump the CSS swatch alphas to match the new chart values**

Locate `dashboard.html:23-31`:

```css
  --precip:#d4a656;
  --precip-2:rgba(212,166,86,0.55);
  --precip-3:rgba(212,166,86,0.28);
  --precip-4:rgba(212,166,86,0.12);

  --wind:#6b94a8;
  --wind-2:rgba(107,148,168,0.55);
  --wind-3:rgba(107,148,168,0.28);
  --wind-4:rgba(107,148,168,0.12);
```

Replace with:

```css
  --precip:#d4a656;
  --precip-2:rgba(212,166,86,0.55);
  --precip-3:rgba(212,166,86,0.45);
  --precip-4:rgba(212,166,86,0.22);

  --wind:#6b94a8;
  --wind-2:rgba(107,148,168,0.55);
  --wind-3:rgba(107,148,168,0.45);
  --wind-4:rgba(107,148,168,0.22);
```

(Only the `-3` and `-4` rows changed. The legend swatches read from these via the existing `.legend .sw.outer / .sw.inner` rules at `dashboard.html:196-201`, so they update for free.)

- [ ] **Step 2: Update the chart's local alpha constants to match**

Locate `dashboard.html:565-566` inside `drawChart()`:

```js
  const outer4 = meta.accent === 'precip' ? 'rgba(212,166,86,0.12)' : 'rgba(107,148,168,0.12)';
  const inner3 = meta.accent === 'precip' ? 'rgba(212,166,86,0.28)' : 'rgba(107,148,168,0.28)';
```

Replace with:

```js
  const outer4 = meta.accent === 'precip' ? 'rgba(212,166,86,0.22)' : 'rgba(107,148,168,0.22)';
  const inner3 = meta.accent === 'precip' ? 'rgba(212,166,86,0.45)' : 'rgba(107,148,168,0.45)';
  const edge   = meta.accent === 'precip' ? 'rgba(212,166,86,0.35)' : 'rgba(107,148,168,0.35)';
  const glow   = meta.accent === 'precip' ? 'rgba(212,166,86,0.25)' : 'rgba(107,148,168,0.25)';
```

(`edge` is for the dashed p10/p90 strokes; `glow` is the soft halo under the p50 line.)

- [ ] **Step 3: Add visible edge strokes to p10/p25/p75/p90 series and a glow under p50**

Locate `dashboard.html:571-578`:

```js
  const series = [
    {},
    {stroke:'transparent', points:{show:false}, label:'p10', show: cur.spread === 'wide'},
    {stroke:'transparent', points:{show:false}, label:'p90', show: cur.spread === 'wide'},
    {stroke:'transparent', points:{show:false}, label:'p25', show: cur.spread !== 'median'},
    {stroke:'transparent', points:{show:false}, label:'p75', show: cur.spread !== 'median'},
    {stroke:accent, width:1.6, points:{show:false}, label:'p50'},
  ];
```

Replace with:

```js
  const series = [
    {},
    {stroke:edge, width:1, dash:[3,3], points:{show:false}, label:'p10', show: cur.spread === 'wide'},
    {stroke:edge, width:1, dash:[3,3], points:{show:false}, label:'p90', show: cur.spread === 'wide'},
    {stroke:edge, width:1, dash:[2,2], points:{show:false}, label:'p25', show: cur.spread !== 'median'},
    {stroke:edge, width:1, dash:[2,2], points:{show:false}, label:'p75', show: cur.spread !== 'median'},
    {stroke:accent, width:2.0, points:{show:false}, label:'p50'},
  ];
```

- [ ] **Step 4: Add a soft glow under the p50 line via the draw hook**

Locate the `hooks: { draw: [` array around `dashboard.html:631-654`. The existing draw hook draws the threshold dashed line. Add a *second* draw-hook entry that runs first and renders the p50 glow underneath.

Find:

```js
    hooks: {
      draw: [
        (u) => {
          if (thresholdVal == null) return;
```

Replace with:

```js
    hooks: {
      draw: [
        (u) => {
          // p50 glow — wider, lower-alpha stroke under the main line
          const xs = u.data[0];
          const ys = u.data[5];
          if (!xs || !ys || !xs.length) return;
          const ctx = u.ctx;
          ctx.save();
          ctx.beginPath();
          ctx.strokeStyle = glow;
          ctx.lineWidth = 5;
          ctx.lineCap = 'round';
          ctx.lineJoin = 'round';
          let started = false;
          for (let i = 0; i < xs.length; i++){
            if (ys[i] == null) { started = false; continue; }
            const x = u.valToPos(xs[i], 'x', true);
            const y = u.valToPos(ys[i], 'y', true);
            if (!started) { ctx.moveTo(x, y); started = true; }
            else { ctx.lineTo(x, y); }
          }
          ctx.stroke();
          ctx.restore();
        },
        (u) => {
          if (thresholdVal == null) return;
```

(We're prepending a new draw hook function; the existing threshold hook stays as-is. Both `glow` and `accent` are already in scope from Step 2 / the existing code above.)

- [ ] **Step 5: Reload and verify visual band hierarchy**

Hard-reload `http://localhost:5000/`. With "Spread = P10–90" (default) on the precipitation chart:

- The outer p10–p90 region should now be a clearly visible tan haze.
- The inner p25–p75 region should be a distinctly darker/denser tan halo on top.
- The p50 line should sit on a soft tan halo (the glow), with thin dashed edges marking the p10/p25/p75/p90 boundaries.

Toggle Spread → P25–75: only the inner band + dashed p25/p75 edges + p50 (with glow) should remain.
Toggle Spread → P50: only the p50 line + glow should remain.

Repeat the same visual check on the wind gust chart (cool teal palette).

- [ ] **Step 6: Commit**

```bash
git add dashboard.html
git commit -m "feat(dashboard): make ensemble spread bands visible with layered halo + p50 glow"
```

---

## Task 3: Tighten x-axis labels and stop threshold-label clipping

**Files:**
- Modify: `dashboard.html:589-622` (uPlot `padding` and the x-axis `values:` formatter inside `drawChart()`)

- [ ] **Step 1: Increase right padding to fit threshold label**

Locate `dashboard.html:592`:

```js
    padding: [14, 10, 10, 10],
```

Replace with:

```js
    padding: [14, 60, 10, 10],
```

- [ ] **Step 2: Replace the x-axis `values:` formatter**

Locate `dashboard.html:606-609`:

```js
        font:'10px "IBM Plex Mono", monospace',
        values:(u, vals) => vals.map(v => {
          const d = new Date(v*1000);
          return d.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}).replace(',','');
        }),
```

(The font line was updated in Task 1; if you skipped Task 1 it'll still say `JetBrains Mono` — fix that too.)

Replace with:

```js
        font:'10px "IBM Plex Mono", monospace',
        values:(u, vals) => {
          let lastDay = null;
          return vals.map(v => {
            const d = new Date(v*1000);
            const day = d.toLocaleDateString([], {month:'short', day:'numeric'});
            const hm  = d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', hour12:false});
            if (day !== lastDay){ lastDay = day; return `${day} ${hm}`; }
            return hm;
          });
        },
```

(First tick of each day shows `28 Oct 09:00`; subsequent ticks on the same day show only `09:00`. uPlot calls `values()` left-to-right with the visible tick array, so the closure-local `lastDay` works as a running state.)

- [ ] **Step 3: Reload and verify**

Hard-reload `http://localhost:5000/`. With "Time = 24H":

- The first tick should show a date + time (e.g. `28 Oct 09:00`); the next ticks on the same day should show only `HH:00`.
- If the 24h window crosses midnight, the first tick after midnight should show the new date (e.g. `29 Oct 00:00`).
- The threshold label `≥10 m/s` (or `≥0.1 mm/h`) at the right edge of the chart should be fully visible — not clipped or running off the panel.

Toggle Time → 6H, 12H, ALL and confirm labels still render correctly.

- [ ] **Step 4: Commit**

```bash
git add dashboard.html
git commit -m "feat(dashboard): compact x-axis labels (date once per day) and pad right edge"
```

---

## Task 4: Re-color the run-picker chevron

**Files:**
- Modify: `dashboard.html:91` (inline SVG data-URI inside `.run-picker select` background)

- [ ] **Step 1: Update the chevron stroke color**

Locate `dashboard.html:91`:

```css
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 8' fill='none' stroke='%237f8796' stroke-width='1.5'><path d='M2 2l4 4 4-4'/></svg>");
```

The hex `%237f8796` is `--muted` (`#7f8796`), which is fine as-is for the new mono palette — but we want a slightly brighter chevron so it reads as interactive. Replace `%237f8796` with `%23b1b8c5` (`--text-dim`):

```css
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 8' fill='none' stroke='%23b1b8c5' stroke-width='1.5'><path d='M2 2l4 4 4-4'/></svg>");
```

- [ ] **Step 2: Reload and verify**

Hard-reload. The forecast-run dropdown chevron in the header should be slightly brighter / clearly visible against the panel background.

- [ ] **Step 3: Commit**

```bash
git add dashboard.html
git commit -m "style(dashboard): brighten run-picker chevron for new mono palette"
```

---

## Task 5: Final end-to-end verification + cleanup

**Files:** none

- [ ] **Step 1: Stop the background API**

```bash
kill "$(cat /tmp/icon-api.pid)" 2>/dev/null
rm -f /tmp/icon-api.pid
```

- [ ] **Step 2: Restart fresh and walk through every control**

```bash
.venv/bin/python api.py &
echo $! > /tmp/icon-api.pid
sleep 2
```

Open `http://localhost:5000/` and walk through:

1. Both summary cards render with the new mono font and italic serif numbers.
2. Sparkline under each summary card still renders (single tan/teal band — unchanged from baseline).
3. Precipitation chart, default load: layered halo visible, dashed edges visible, p50 glow visible, axis labels compact, threshold label uncropped.
4. Wind Gust chart, default load: same checks in the cool palette.
5. Toggle each chart's Spread (P10–90 / P25–75 / P50) — bands appear/disappear as expected and the legend swatches match.
6. Toggle each chart's Time range — axis labels recompact correctly.
7. Toggle each chart's Threshold — the threshold dashed line + label updates and stays inside the chart area.
8. Hover any chart — the readout strip below the chart shows formatted values in the new font.
9. Resize the browser window — charts redraw correctly, no overlap.

- [ ] **Step 3: Stop the background API**

```bash
kill "$(cat /tmp/icon-api.pid)" 2>/dev/null
rm -f /tmp/icon-api.pid
```

- [ ] **Step 4: Final commit gate**

Run `git status` and confirm a clean working tree (all dashboard changes already committed in Tasks 1–4). If anything is left, commit it now with a meaningful message.

```bash
git log --oneline main..HEAD
```

Expected: 4 feature commits (one per Task 1–4) plus the spec commit, all on `feature/enhance-frontend`.

---

## Done

The feature branch `feature/enhance-frontend` now has the redesigned dashboard. Next step is the user's call: open a PR, merge to `main`, or iterate further. The `superpowers:finishing-a-development-branch` skill can guide that.
