"""Visual system of the dashboard: a technical drawing sheet.

Drafting-paper ground, ink linework, dimension blue for data and tolerances, revision red for
action, amber ink for watch. Square corners, 1px hairlines, no shadows. Archivo (variable) narrow
for lettering and labels, normal width for reading, tabular figures everywhere.

The signature move is the **tolerance band**: every chart draws its specification (plausibility
bounds, a gate threshold, a vehicle's normal range) as a light dimension-blue band between dashed
limit lines, labelled like a dimension, so whether a value is in spec is visible at a glance.
"""

from __future__ import annotations

import html
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

import altair as alt
import pandas as pd
import streamlit as st

PAPER = "#f3f2ec"
PAPER_2 = "#e9e7de"
INK = "#1d1d1b"
INK_2 = "#4a4944"  # secondary text: 8.6:1 on paper
RULE = "#c9c6ba"
DIM = "#2f5d8a"  # dimension blue: data series, tolerances, links
REV = "#c8102e"  # revision red: action
WATCH = "#9a5b00"  # amber ink: watch (5.6:1 on paper)
# On the ink top bar
PAPER_DIM = "#d9d6cb"  # nav labels at rest (12:1 on ink)
PAPER_SOFT = "#e4e1d6"  # chip text
DIM_ON_INK = "#9db8d3"  # dimension blue lifted for ink: current page, passing gates
CHIP_RULE = "#6f6e68"  # chip outline
# Washes behind fleet cells
CELL = "#fbfaf6"
# Extra chart series, after the four semantic inks (also in .streamlit/config.toml)
SLATE = "#6f7d8c"
MAUVE = "#8a6f9b"
REV_WASH = "#fbecec"
WATCH_WASH = "#f8f0e1"
FONT = "Archivo"

STATE_COLOR = {"action": REV, "watch": WATCH, "ok": INK_2, "info": DIM}
STATE_LABEL = {"action": "ACTION", "watch": "WATCH", "ok": "IN SPEC", "info": "INFO"}

CSS = f"""
<style>
:root {{
  --paper:{PAPER}; --paper-2:{PAPER_2}; --ink:{INK}; --ink-2:{INK_2}; --rule:{RULE};
  --dim:{DIM}; --rev:{REV}; --watch:{WATCH};
  --paper-dim:{PAPER_DIM}; --paper-soft:{PAPER_SOFT}; --dim-on-ink:{DIM_ON_INK}; --chip-rule:{CHIP_RULE};
  --cell:{CELL}; --rev-wash:{REV_WASH}; --watch-wash:{WATCH_WASH};
}}
html, body, [class*="st-"] {{ font-variant-numeric: tabular-nums; }}
::selection {{ background: var(--dim); color: var(--paper); }}
* {{ scrollbar-color: var(--ink-2) var(--paper-2); scrollbar-width: thin; }}
:focus-visible {{ outline: 2px solid var(--dim) !important; outline-offset: 2px; }}
a {{ text-underline-offset: 3px; }}
header[data-testid="stHeader"] {{ display: none; }}
.block-container {{ padding-top: 0 !important; padding-bottom: 3rem; max-width: 1480px; }}
h1, h2, h3, h4 {{ font-stretch: 75%; letter-spacing: -0.01em; word-spacing: .22em; color: var(--ink); }}
.brand, .chip, table.sheet th, .tag, .legend, .kv div b, .revblock b {{ word-spacing: .18em; }}
h1 {{ font-weight: 700; font-size: 2rem !important; padding: .25rem 0 0 !important; }}
h2 {{ font-weight: 650; font-size: 1.15rem !important; text-transform: uppercase;
     letter-spacing: .05em; border-bottom: 1px solid var(--ink); padding: 1.5rem 0 .35rem !important;
     margin-bottom: .6rem !important; }}

/* shell: ink top bar */
/* full-bleed ink bar: border-image paints the bleed, so the content box keeps its real width */
.st-key-topbar {{ background-color: var(--ink);  /* the bar itself, even where border-image is not drawn */
  border-image: linear-gradient(var(--ink) 0 0) fill 0 / / 0 100vmax;
  color: var(--paper); padding: .55rem 0; margin-bottom: 1.25rem; flex-wrap: wrap; row-gap: .35rem; }}
.st-key-topbar > div:last-child {{ margin-left: auto; }}
.brand {{ font-stretch: 75%; font-weight: 750; font-size: 1.2rem; letter-spacing: .04em;
  text-transform: uppercase; color: var(--paper); white-space: nowrap; }}
.brand span {{ color: var(--dim-on-ink); font-weight: 500; }}
.st-key-topbar a[data-testid="stPageLink-NavLink"] {{ background: transparent; border-radius: 0;
  padding: .35rem .2rem; border-bottom: 2px solid transparent; }}
.st-key-topbar a[data-testid="stPageLink-NavLink"] p {{ color: var(--paper-dim); font-stretch: 75%;
  text-transform: uppercase; letter-spacing: .07em; font-weight: 600; font-size: .95rem; }}
.st-key-topbar a[data-testid="stPageLink-NavLink"]:hover p {{ color: var(--paper); }}
.st-key-topbar a[data-testid="stPageLink-NavLink"][aria-current="page"] {{ border-bottom-color: var(--dim-on-ink); }}
.st-key-topbar a[data-testid="stPageLink-NavLink"][aria-current="page"] p {{ color: var(--paper); }}
.status {{ display: flex; gap: .5rem; justify-content: flex-end; flex-wrap: wrap; }}
.chip {{ border: 1px solid var(--chip-rule); color: var(--paper-soft); padding: .1rem .5rem; font-size: .8rem;
  font-stretch: 75%; letter-spacing: .06em; text-transform: uppercase; white-space: nowrap; }}
.chip.pass {{ border-color: var(--dim-on-ink); color: var(--paper); }}
.chip.fail {{ background: var(--rev); border-color: var(--rev); color: var(--paper); }}
.chip.syn {{ border-style: dashed; }}

/* sheet tables */
table.sheet {{ width: 100%; border-collapse: collapse; font-size: .93rem; }}
table.sheet th {{ text-align: left; font-stretch: 75%; font-weight: 600; text-transform: uppercase;
  letter-spacing: .06em; font-size: .78rem; color: var(--ink-2); padding: .4rem .5rem;
  border-bottom: 1px solid var(--ink); }}
table.sheet td {{ padding: .45rem .5rem; border-bottom: 1px solid var(--rule); vertical-align: top; }}
table.sheet td.num, table.sheet th.num {{ text-align: right; white-space: nowrap; }}
table.sheet.fixed {{ table-layout: fixed; }}
table.sheet.fixed td {{ overflow-wrap: anywhere; }}
table.sheet.fixed td.num {{ overflow-wrap: normal; }}
table.sheet td.path {{ overflow-wrap: anywhere; }}
.code {{ white-space: nowrap; }}
table.sheet tr.void td {{ color: var(--ink-2); text-decoration: line-through; }}
.code {{ font-weight: 700; }}
.pseudo {{ color: var(--ink-2); font-size: .85rem; }}
.tag {{ display: inline-block; font-stretch: 75%; font-weight: 700; font-size: .75rem;
  letter-spacing: .07em; padding: .05rem .4rem; border: 1px solid currentColor; white-space: nowrap; }}
.tag.action {{ color: var(--rev); }} .tag.watch {{ color: var(--watch); }}
.tag.ok {{ color: var(--ink-2); }} .tag.info {{ color: var(--dim); }}
.dev.action {{ color: var(--rev); font-weight: 700; }} .dev.watch {{ color: var(--watch); font-weight: 700; }}
.call {{ display: inline-block; width: 1.35rem; height: 1.35rem; border: 1px solid var(--ink);
  border-radius: 50%; text-align: center; line-height: 1.25rem; font-size: .75rem; font-weight: 600; }}

/* revision block (the drawing's title block, used as the run's record) */
.revblock {{ display: grid; grid-template-columns: 1fr 1fr; border: 1px solid var(--ink);
  margin-top: .9rem; }}
.revblock div {{ padding: .35rem .55rem; border-right: 1px solid var(--rule);
  border-bottom: 1px solid var(--rule); font-size: .9rem; }}
.revblock b {{ display: block; font-stretch: 75%; font-size: .72rem; font-weight: 600;
  text-transform: uppercase; letter-spacing: .07em; color: var(--ink-2); }}

/* fleet strip */
.fleet {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(84px, 1fr)); gap: 4px; }}
.cell {{ border: 1px solid var(--rule); background: var(--cell); padding: .3rem .4rem; line-height: 1.15; }}
.cell b {{ display: block; font-size: .85rem; }}
.cell small {{ color: var(--ink-2); font-size: .72rem; }}
.cell.action {{ border-color: var(--rev); background: var(--rev-wash); }}
.cell.action b {{ color: var(--rev); }}
.cell.watch {{ border-color: var(--watch); background: var(--watch-wash); }}
.cell.watch b {{ color: var(--watch); }}
.legend {{ display: flex; gap: 1.1rem; flex-wrap: wrap; font-size: .8rem; color: var(--ink-2);
  margin-top: .5rem; font-stretch: 75%; text-transform: uppercase; letter-spacing: .06em; }}
.legend i {{ display: inline-block; width: .7rem; height: .7rem; margin-right: .35rem;
  vertical-align: -1px; border: 1px solid var(--ink-2); }}

.note {{ color: var(--ink-2); font-size: .88rem; max-width: 75ch; }}
.empty {{ border: 1px dashed var(--ink-2); padding: 1rem; color: var(--ink-2); }}
.kv {{ display: flex; gap: 2.2rem; flex-wrap: wrap; margin: .2rem 0 .4rem; }}
.kv div b {{ display: block; font-stretch: 75%; font-size: .72rem; text-transform: uppercase;
  letter-spacing: .07em; color: var(--ink-2); font-weight: 600; }}
.kv div span {{ font-size: 1.35rem; font-weight: 650; }}
[data-testid="stDataFrame"] {{ border: 1px solid var(--ink); }}
div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {{ border-radius: 0; }}
@media (max-width: 760px) {{
  .st-key-topbar {{ padding-top: .4rem; padding-bottom: .4rem; }}
  .status {{ justify-content: flex-start; }}
  h1 {{ font-size: 1.6rem !important; }}
  table.sheet {{ font-size: .85rem; }}
  table.sheet .hide-sm {{ display: none; }}
  /* Too narrow for fixed widths: let content size the columns, and wrap at word boundaries. */
  table.sheet.fixed {{ table-layout: auto; }}
  table.sheet.fixed th {{ width: auto !important; }}
  table.sheet.fixed td {{ overflow-wrap: break-word; }}
}}
</style>
"""


def inject_css() -> None:
    st.html(CSS)


def esc(value: object) -> str:
    return html.escape(
        "" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value)
    )


@dataclass(frozen=True)
class Col:
    label: str
    key: str
    num: bool = False
    raw: bool = False  # value is trusted HTML built by this module
    hide_small: bool = False
    path: bool = False  # a dotted VSS path: may break at any character on narrow screens
    width: str | None = None  # CSS width for fixed-layout tables


def sheet_table(
    rows: Iterable[dict[str, object]],
    cols: Sequence[Col],
    void_key: str | None = None,
    fixed: bool = False,
) -> str:
    """A drawing-sheet table. Values are escaped unless the column is marked ``raw``.

    ``fixed`` sets the column widths from ``Col.width`` and lets the free columns share the rest,
    so long text wraps inside its column instead of widening the table past its container.
    """

    def cls(c: Col) -> str:
        names = (
            (["num"] if c.num else [])
            + (["hide-sm"] if c.hide_small else [])
            + (["path"] if c.path else [])
        )
        return f' class="{" ".join(names)}"' if names else ""

    def style(c: Col) -> str:
        return f' style="width:{c.width}"' if fixed and c.width else ""

    head = "".join(f"<th{cls(c)}{style(c)}>{esc(c.label)}</th>" for c in cols)
    body = []
    for r in rows:
        row_cls = ' class="void"' if void_key and r.get(void_key) else ""
        cells = "".join(
            f"<td{cls(c)}>{r.get(c.key, '') if c.raw else esc(r.get(c.key))}</td>" for c in cols
        )
        body.append(f"<tr{row_cls}>{cells}</tr>")
    table_cls = "sheet fixed" if fixed else "sheet"
    return f'<table class="{table_cls}"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'


def tag(state: str, text: str | None = None) -> str:
    return f'<span class="tag {state}">{esc(text or STATE_LABEL[state])}</span>'


def vehicle(code: str, pseudo: str) -> str:
    return f'<span class="code">{esc(code)}</span><br><span class="pseudo">{esc(pseudo)}</span>'


def empty(message: str) -> None:
    st.html(f'<div class="empty">{esc(message)}</div>')


def note(text: str) -> None:
    st.html(f'<p class="note">{esc(text)}</p>')


def kv(items: Sequence[tuple[str, str]]) -> None:
    st.html(
        '<div class="kv">'
        + "".join(f"<div><b>{esc(k)}</b><span>{esc(v)}</span></div>" for k, v in items)
        + "</div>"
    )


# ---------------------------------------------------------------------- charts
def _theme() -> alt.theme.ThemeConfig:
    label = {
        "labelFont": FONT,
        "labelColor": INK_2,
        "labelFontSize": 12,
        "titleFont": FONT,
        "titleColor": INK_2,
        "titleFontSize": 12,
        "titleFontWeight": 600,
    }
    return cast("alt.theme.ThemeConfig", {
        "config": {
            "background": PAPER,
            "font": FONT,
            "view": {"stroke": None},
            "axis": {**label, "domainColor": INK, "tickColor": INK, "gridColor": RULE,
                     "gridDash": [2, 3], "labelPadding": 4},
            "legend": {**label, "orient": "top", "symbolStrokeWidth": 2},
            "title": {"font": FONT, "color": INK, "fontSize": 14, "fontWeight": 650, "anchor": "start"},
            "range": {"category": [DIM, INK, REV, WATCH, SLATE, MAUVE]},
            "line": {"strokeWidth": 1.6},
            "point": {"filled": True, "size": 36},
        }
    })  # fmt: skip


alt.theme.register("drawing_sheet", enable=True)(_theme)


def tolerance(
    lo: float | None, hi: float | None, label_lo: str | None = None, label_hi: str | None = None
) -> alt.LayerChart:
    """The signature move: a specification drawn as a dimensioned tolerance band."""
    band = pd.DataFrame([{"lo": lo, "hi": hi}])
    # zero=False: the band must not pull the data's axis down to a zero baseline.
    y = alt.Scale(zero=False)
    layers: list[alt.Chart] = []
    if lo is not None and hi is not None:
        layers.append(
            alt.Chart(band)
            .mark_rect(color=DIM, opacity=0.1)
            .encode(y=alt.Y("lo:Q", scale=y), y2="hi:Q")
        )
    for value, text in ((lo, label_lo), (hi, label_hi)):
        if value is None:
            continue
        rule = pd.DataFrame([{"v": value, "t": text or ""}])
        layers.append(
            alt.Chart(rule)
            .mark_rule(color=DIM, strokeDash=[5, 3], strokeWidth=1)
            .encode(y=alt.Y("v:Q", scale=y))
        )
        if text:  # dimension label at the left edge, set just above its limit line
            layers.append(
                alt.Chart(rule)
                .mark_text(
                    color=DIM, align="left", dx=6, dy=-6, fontSize=11, font=FONT, fontWeight=600
                )
                .encode(y=alt.Y("v:Q", scale=y), x=alt.value(0), text="t:N")
            )
    return alt.LayerChart(layer=layers)


def chart(c: alt.Chart | alt.LayerChart, height: int = 260) -> None:
    st.altair_chart(c.properties(height=height), width="stretch")
