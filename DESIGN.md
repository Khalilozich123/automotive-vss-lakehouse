---
name: COVESA Telemetry Lakehouse
description: Fleet dashboard over the Gold marts, drawn as a technical drawing sheet.
colors:
  paper: "#f3f2ec"
  paper-2: "#e9e7de"
  ink: "#1d1d1b"
  ink-2: "#4a4944"
  rule: "#c9c6ba"
  dimension-blue: "#2f5d8a"
  revision-red: "#c8102e"
  amber-ink: "#9a5b00"
  paper-dim: "#d9d6cb"
  paper-soft: "#e4e1d6"
  dimension-blue-on-ink: "#9db8d3"
  chip-rule: "#6f6e68"
  cell: "#fbfaf6"
  revision-wash: "#fbecec"
  amber-wash: "#f8f0e1"
  slate: "#6f7d8c"
  mauve: "#8a6f9b"
typography:
  display:
    fontFamily: "Archivo, sans-serif"
    fontSize: "2rem"
    fontWeight: 700
    letterSpacing: "-0.01em"
    fontVariation: "'wdth' 75"
  headline:
    fontFamily: "Archivo, sans-serif"
    fontSize: "1.15rem"
    fontWeight: 650
    letterSpacing: "0.05em"
    fontVariation: "'wdth' 75"
  body:
    fontFamily: "Archivo, sans-serif"
    fontSize: "15px"
    fontWeight: 400
    fontFeature: "'tnum' 1"
  label:
    fontFamily: "Archivo, sans-serif"
    fontSize: "0.78rem"
    fontWeight: 600
    letterSpacing: "0.06em"
    fontVariation: "'wdth' 75"
  figure:
    fontFamily: "Archivo, sans-serif"
    fontSize: "1.35rem"
    fontWeight: 650
    fontFeature: "'tnum' 1"
rounded:
  none: "0"
  callout: "50%"
spacing:
  cell: "0.45rem 0.5rem"
  section: "1.5rem"
components:
  tag-action:
    textColor: "{colors.revision-red}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.05rem 0.4rem"
  tag-watch:
    textColor: "{colors.amber-ink}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.05rem 0.4rem"
  tag-info:
    textColor: "{colors.dimension-blue}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.05rem 0.4rem"
  chip:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper-soft}"
    rounded: "{rounded.none}"
    padding: "0.1rem 0.5rem"
  chip-fail:
    backgroundColor: "{colors.revision-red}"
    textColor: "{colors.paper}"
    rounded: "{rounded.none}"
    padding: "0.1rem 0.5rem"
  top-bar:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    padding: "0.55rem 0"
  fleet-cell:
    backgroundColor: "{colors.cell}"
    textColor: "{colors.ink}"
    rounded: "{rounded.none}"
    padding: "0.3rem 0.4rem"
  fleet-cell-action:
    backgroundColor: "{colors.revision-wash}"
    textColor: "{colors.revision-red}"
  fleet-cell-watch:
    backgroundColor: "{colors.amber-wash}"
    textColor: "{colors.amber-ink}"
---

# Design System: COVESA Telemetry Lakehouse

## Overview

**Creative North Star: "The Technical Drawing Sheet"**

The dashboard reads like an engineering drawing of the fleet: drafting paper, ink linework, a title block that records the run, and dimensions that say what "in specification" means. A fleet engineer scans it for what is out of tolerance; a data engineer reads the same sheet as proof the pipeline works, because every KPI is shown next to the specification it was checked against.

It is dense and quiet. Colour is a signal, never decoration: ink for structure, dimension blue for data and limits, revision red only where someone has to act. Explanations are one line under the thing they explain. The data is synthetic and VINs are pseudonymized, and the sheet says so in its own chrome rather than in a disclaimer.

**Key Characteristics:**
- Paper ground, ink hairlines, square corners, no shadows.
- One condensed lettering voice (Archivo at 75 % width) for headings and labels, normal width for reading, tabular figures everywhere.
- Every chart draws its specification as a tolerance band between dashed limit lines.
- Numbered circular callouts tie rows in a table to the vehicles they describe.
- State words in outlined uppercase tags: ACTION, WATCH, IN SPEC, INFO.

## Colors

Four inks on drafting paper; each has one job.

### Primary
- **Dimension Blue** (#2f5d8a): data series, tolerance bands (at 10 % opacity), limit lines and their labels, links, focus rings, the Streamlit primary colour. On the ink top bar it is lifted to **Dimension Blue on Ink** (#9db8d3) for the current page and passing gates.

### Secondary
- **Revision Red** (#c8102e): ACTION state only. Tags, deviations that need action, the failed-gate chip, fault onset markers. Its wash (#fbecec) fills fleet cells in action.
- **Amber Ink** (#9a5b00): WATCH state only (5.6:1 on paper). Its wash (#f8f0e1) fills fleet cells under watch.

### Neutral
- **Drafting Paper** (#f3f2ec): the page. **Paper 2** (#e9e7de) for input wells and secondary surfaces; **Cell** (#fbfaf6) for in-spec fleet cells.
- **Ink** (#1d1d1b): text, table header rules, the top bar, frames. **Ink 2** (#4a4944, 8.6:1 on paper): secondary text, column labels, notes.
- **Rule** (#c9c6ba): row dividers and cell borders.
- On the ink bar: **Paper Dim** (#d9d6cb) nav labels at rest, **Paper Soft** (#e4e1d6) chip text, **Chip Rule** (#6f6e68) chip outlines.
- Extra chart series after the four inks: **Slate** (#6f7d8c), **Mauve** (#8a6f9b).

### Named Rules
**The One Signal Rule.** Red means act, amber means watch, and nothing else may use either. A red that isn't a call to action teaches people to ignore the real ones.

**The Token Rule.** Colours live in `ui.py` as constants, are exposed as `:root` custom properties, and are mirrored in `.streamlit/config.toml`. Markup uses `var(--…)`, never a literal hex.

## Typography

**Display / Label Font:** Archivo variable, condensed to 75 % width (self-hosted woff2, OFL)
**Body Font:** Archivo at normal width

**Character:** One family in two widths, like drawing lettering and the notes beside it: condensed uppercase for anything that labels, normal width for anything you read.

### Hierarchy
- **Display** (700, 2rem, 75 % width): page title, once per page.
- **Headline** (650, 1.15rem, uppercase, 0.05em, 75 % width, 1px ink underline): section titles; the underline is the sheet's section rule.
- **Body** (400, 15px, tabular figures): table cells and prose; notes cap at 75ch.
- **Label** (600, 0.72–0.78rem, uppercase, 0.06–0.07em, 75 % width, Ink 2): column headers, title-block keys, legends.
- **Figure** (650, 1.35rem, tabular): the headline numbers in key-value rows.

### Named Rules
**The Tabular Rule.** All figures are tabular (`font-variant-numeric: tabular-nums`) and right-aligned in tables, so columns of numbers compare at a glance.

**The Word-Spacing Rule.** Condensed uppercase gets extra word spacing (0.18–0.22em); without it, "OUT OF TOLERANCE" reads as one word.

## Layout

A wide single sheet (max 1480px) under a full-bleed ink top bar: brand, page links, then status chips (period, VSS release, gate verdict, synthetic data). The overview splits 3 : 2, with the attention table on the left and proof (validation results, title block, fleet strip) on the right. Other pages stack full-width sections, each opened by a headline rule. Small multiples sit in equal columns on the same y scale.

Below 760px the bar wraps, chips left-align, the display drops to 1.6rem, and tables hide their `hide-sm` columns and switch from fixed to automatic layout. Fixed column widths would crush the free text columns, so on phones words wrap at word boundaries and only dotted VSS paths may break anywhere.

## Elevation & Depth

Flat. There are no shadows anywhere. Depth comes from linework: a 1px ink frame for blocks that hold a record (title block, data frames), 1px rule dividers between rows, and a dashed outline for empty states.

**The Flat Sheet Rule.** Nothing floats above the paper. If something needs emphasis, give it a frame or an ink, not a shadow.

## Shapes

Square corners everywhere (`baseRadius = "none"`, inputs and selects forced to 0). The one round form is the numbered callout (a 1.35rem circle, 1px ink), borrowed from drawing balloons. Borders are 1px; the only 2px line is the current-page underline in the top bar.

## Components

### Tags
Outlined uppercase state words: label type, 1px `currentColor` border, no fill, square. ACTION in revision red, WATCH in amber ink, IN SPEC in Ink 2, INFO / PASS in dimension blue.

### Status chips (top bar)
- **Style:** 1px chip-rule outline on ink, Paper Soft text, condensed uppercase, 0.8rem.
- **States:** passing gates get a Dimension Blue on Ink outline; a failed gate fills revision red; the synthetic-data chip is dashed.

### Sheet tables
The primary component. Column headers in label type over a 1px ink rule; rows divided by 1px rule; numbers right-aligned and tabular. Values are HTML-escaped unless a column is marked raw. Superseded rows render struck through in Ink 2. `fixed` tables take explicit column widths on desktop only.

### Title block (`.revblock`)
A two-column grid in a 1px ink frame, with condensed uppercase keys above values: the run's record (standard, contracts, quality checks, vehicles, as-of date, data provenance).

### Fleet strip
A grid of small cells (min 84px), one per vehicle: code in bold, pseudonym below in Ink 2. In-spec cells are Cell on paper; action and watch cells take their wash and ink border.

### Navigation
Page links in the ink bar: Paper Dim condensed uppercase, turning Paper on hover. The current page is Paper with a 2px Dimension Blue on Ink underline. Focus rings are 2px dimension blue, offset 2px.

### Tolerance band (signature)
Every chart draws its specification first: a Dimension Blue rect at 10 % opacity between the limits, dashed 1px limit rules, and left-aligned dimension labels set just above each limit. Data is drawn on top, so in-spec versus out-of-spec is read from position, not from a legend.

## Do's and Don'ts

### Do:
- **Do** draw the specification behind every chart with `ui.tolerance(lo, hi, …)`, with plausibility bounds, gate thresholds or a vehicle's normal range as the limits.
- **Do** keep every new colour in `ui.py`, `:root` and `config.toml` together, and reference it as `var(--…)`.
- **Do** show a measured deviation next to every out-of-tolerance item; rows without one rank below rows with one.
- **Do** say plainly when data is synthetic or a signal is not reported by a platform ("precursor not reported by this platform"), rather than hiding the row.
- **Do** render an empty state (dashed frame, one line saying what to run) when a table doesn't exist yet; a fresh stack must never crash a page.

### Don't:
- **Don't** use red or amber for anything but ACTION and WATCH.
- **Don't** add shadows, rounded cards, gradients or the stock Streamlit look.
- **Don't** write explanation paragraphs; one note line under the element, 75ch max.
- **Don't** show a raw VIN anywhere. Show the vehicle code and the pseudonym's first and last four characters.
- **Don't** overclaim: a KPI shown as validated must come from `ops.validation_results`, with its sample size beside it.
