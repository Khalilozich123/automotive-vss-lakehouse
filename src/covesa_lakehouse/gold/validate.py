"""Validate the Gold KPIs against the simulator's hidden ground truth.

The simulator knows what the marts can only infer: each vehicle's true battery State of Health
and driver aggressiveness, and which fault scenario caused each DTC. This report checks that the
marts recover them:

* SoH:        estimated SoH (charging-session Coulomb counting) vs true SoH, per vehicle
* eco score:  rank correlation between harsh-acceleration rate and true aggressiveness
* precursors: the top-ranked signal per DTC is the one its fault scenario actually perturbs

    uv run python -m covesa_lakehouse.gold.validate --truth 'data/backfill/truth-*.jsonl'
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import sys
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession

from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_hex, key_from_env
from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark

# DTC -> VSS paths its fault scenario perturbs (see data_generator/faults.py)
EXPECTED_PRECURSORS = {
    "P0A7E": {
        "Vehicle.Powertrain.TractionBattery.Temperature.Max",
        "Vehicle.Powertrain.TractionBattery.Temperature.Min",
    },
    "P0A80": {
        "Vehicle.Powertrain.TractionBattery.CellVoltage.Max",
        "Vehicle.Powertrain.TractionBattery.CellVoltage.Min",
        "Derived.TractionBattery.CellVoltageSpread",
    },
    "P0217": {"Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature"},
    "P0562": {"Vehicle.LowVoltageBattery.CurrentVoltage"},
    "P0300": {"Vehicle.Powertrain.CombustionEngine.Speed"},
    # any powertrain signal, or one derived from them (they all drop out)
    "U0100": {"*Vehicle.Powertrain.*", "*Derived.TractionBattery.*"},
}


def spearman(xs: list[float], ys: list[float]) -> float:
    def ranks(v: list[float]) -> list[float]:
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        for rank, i in enumerate(order):
            r[i] = float(rank)
        return r

    rx, ry = ranks(xs), ranks(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry, strict=True))
    sx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    sy = math.sqrt(sum((b - my) ** 2 for b in ry))
    return cov / (sx * sy) if sx and sy else float("nan")


def _matches(path: str, expected: set[str]) -> bool:
    return any(path.startswith(e[1:-1]) if e.startswith("*") else path == e for e in expected)


def validate(spark: SparkSession, truth_glob: str) -> dict[str, Any]:
    profiles = {}
    for name in glob.glob(truth_glob):
        with open(name, encoding="utf-8") as fh:
            for line in fh:
                r = json.loads(line)
                if r["event"] == "vehicle_profile":
                    profiles[r["vin"]] = r
    if not profiles:
        raise SystemExit(f"no vehicle_profile records in {truth_glob}")

    # The lakehouse holds pseudonymized VINs: map them with the same keyed hash.
    key, _ = key_from_env()
    by_pseudo = {hmac_sha256_hex(key, vin): p for vin, p in profiles.items()}
    report: dict[str, Any] = {"vehicles": len(profiles)}

    soh_rows = spark.sql(
        "SELECT vin, platform_id, percentile_approx(soh_est_pct, 0.5) AS soh, count(*) AS n "
        "FROM lakehouse.gold.battery_charging_sessions WHERE soh_est_pct IS NOT NULL "
        "GROUP BY vin, platform_id"
    ).collect()
    soh = [
        (r.platform_id, r.soh, by_pseudo[r.vin]["state_of_health"] * 100, r.n)
        for r in soh_rows
        if r.vin in by_pseudo
    ]
    if soh:
        errors = [abs(est - true) for _, est, true, _ in soh]
        report["soh"] = {
            "vehicles": len(soh),
            "sessions": sum(n for *_, n in soh),
            "mean_abs_error_pct_points": round(sum(errors) / len(errors), 2),
            "max_abs_error_pct_points": round(max(errors), 2),
            "spearman": round(spearman([e for _, e, _, _ in soh], [t for _, _, t, _ in soh]), 3),
            "detail": [
                {"platform": p, "estimated": round(e, 1), "true": round(t, 1), "sessions": n}
                for p, e, t, n in sorted(soh, key=lambda x: x[2])
            ],
        }

    eco_rows = spark.sql(
        "SELECT vin, sum(harsh_accel_events) / sum(distance_km) * 100 AS harsh_per_100km "
        "FROM lakehouse.gold.eco_driving_trips WHERE harsh_accel_events IS NOT NULL "
        "GROUP BY vin HAVING sum(distance_km) > 50"
    ).collect()
    eco = [
        (r.harsh_per_100km, by_pseudo[r.vin]["aggressiveness"])
        for r in eco_rows
        if r.vin in by_pseudo
    ]
    if eco:
        report["eco"] = {
            "vehicles": len(eco),
            "spearman_harsh_rate_vs_aggressiveness": round(
                spearman(*map(list, zip(*eco, strict=True))), 3
            ),
        }

    top = spark.sql(
        "SELECT dtc_code, vss_path, onsets, median_score, typical_driver, rank "
        "FROM lakehouse.gold.dtc_precursor_ranking WHERE rank <= 3 ORDER BY dtc_code, rank"
    ).collect()
    precursors = {}
    for code, expected in EXPECTED_PRECURSORS.items():
        ranked = [r for r in top if r.dtc_code == code]
        if not ranked:
            precursors[code] = {"found": False}
            continue
        first = ranked[0]
        precursors[code] = {
            "onsets": first.onsets,
            "top_signal": first.vss_path,
            "driver": first.typical_driver,
            "median_score_sigma": round(first.median_score, 1),
            "correct": _matches(first.vss_path, expected),
        }
    report["precursors"] = precursors
    return report


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--truth", default="data/backfill/truth-*.jsonl")
    p.add_argument("--no-write", action="store_true", help=f"do not record to {RESULTS_TABLE}")
    args = p.parse_args(argv)
    spark = get_spark(LakehouseConfig.from_env(app_name="gold-validate"))
    spark.sparkContext.setLogLevel("ERROR")
    report = validate(spark, args.truth)
    if not args.no_write:
        write_results(spark, report)
    print(json.dumps(report, indent=2), file=sys.stdout)
    spark.stop()


RESULTS_TABLE = "lakehouse.ops.validation_results"


def result_rows(report: dict[str, Any]) -> list[tuple[str, str, float, str, bool, int]]:
    """Flatten the report into (check, label, value, unit, passed, sample_size) rows."""
    rows = []
    if "soh" in report:
        soh = report["soh"]
        rows.append(
            (
                "soh_error",
                "Battery SoH estimate vs true SoH",
                soh["mean_abs_error_pct_points"],
                "pts",
                soh["mean_abs_error_pct_points"] <= 2.0,
                soh["sessions"],
            )
        )
    if "eco" in report:
        rho = report["eco"]["spearman_harsh_rate_vs_aggressiveness"]
        rows.append(
            (
                "eco_rank",
                "Harsh-driving rate vs true aggressiveness",
                rho,
                "rho",
                rho >= 0.6,
                report["eco"]["vehicles"],
            )
        )
    precursors = report.get("precursors", {})
    if precursors:
        found = sum(1 for p in precursors.values() if p.get("correct"))
        rows.append(
            (
                "dtc_precursors",
                "Correct top precursor per fault code",
                float(found),
                f"of {len(precursors)}",
                found == len(precursors),
                len(precursors),
            )
        )
    return rows  # fmt: skip


def write_results(spark: SparkSession, report: dict[str, Any]) -> None:
    """Persist the verdicts so downstream consumers (the dashboard) never need the truth files."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    schema = (
        "check STRING, label STRING, value DOUBLE, unit STRING, passed BOOLEAN,"
        " sample_size BIGINT, validated_at TIMESTAMP"
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [(*r, now) for r in result_rows(report)]
    spark.createDataFrame(rows, schema).writeTo(RESULTS_TABLE).createOrReplace()


if __name__ == "__main__":
    main()
