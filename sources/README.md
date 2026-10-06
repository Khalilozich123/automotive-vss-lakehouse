# Upstream sources

Everything the platform contracts are generated from. The upstream files are vendored
(committed) so that builds and CI never depend on GitHub being reachable. They are pinned in
`src/covesa_lakehouse/data_generator/fetch_sources.py`.

| Path | Source | Pin | License |
|---|---|---|---|
| `dbc/*.dbc` | [commaai/opendbc](https://github.com/commaai/opendbc), `opendbc/dbc/` | commit `a7da786` | MIT, © comma.ai |
| `vss/vss.json`, `vss/units.yaml` | [COVESA Vehicle Signal Specification](https://github.com/COVESA/vehicle_signal_specification) release assets | `v6.1` | MPL-2.0, © COVESA |
| `dbc/synthetic/renault_cmf_ev.dbc` | **Hand-written for this project** | — | project license |
| `mappings/*.yaml` | Hand-curated DBC signal → VSS path mappings | — | project license |
| `mappings/_bounds.yaml` | Physical plausibility bounds per VSS path | — | project license |
| `dtc/dtc_catalog.yaml` | Fault codes used by the simulator; generic SAE J2012 code ranges, paraphrased titles | — | project license |

The vendored files are unmodified. Some opendbc files break the DBC grammar, so they are repaired
**at load time** by `data_generator/dbc_loader.py`, never edited on disk:

* `psa_aee2010_r3.dbc`: two signal names start with a digit (`0_COUNTER`). They are prefixed with `_`.
* `fca_giorgio.dbc`: a 29-bit frame id is missing the extended-frame flag. The flag is added.

> **Synthetic data disclaimer.** `renault_cmf_ev.dbc` is not a Renault document and does not
> describe real Renault CAN traffic. opendbc has no Renault database. Its signal names and
> scalings follow the project specification's example contract, and its message ids and layouts
> are invented.

To bump a pin, edit `fetch_sources.py` and run:

```bash
uv run python -m covesa_lakehouse.data_generator.fetch_sources
uv run python -m covesa_lakehouse.data_generator.dbc_parser
```
