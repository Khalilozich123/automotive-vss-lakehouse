"""Gold Mart 2: Diagnostic Trouble Code (DTC) predictive triage.

For every DTC **onset** (first report of a code after at least 30 min of silence), each VSS signal
of that vehicle is examined in the 10 minutes before the onset (the *precursor window*), against
two references:

* the **local baseline**: the 60 minutes before the window, and
* the vehicle's **normal range**: its p0.5-p99.5 for that signal over the last 14 days, excluding
  every fault episode (from 70 min before onset to 10 min after the last report).

Four kinds of change are scored, each scaled to read roughly as "sigmas":

=============  ====================================================================  ==========
change         measure                                                               score
=============  ====================================================================  ==========
``level``      window mean vs the baseline's *linear trend extrapolated* into the     |z|
               window, in residual standard deviations (normal drifts such as fuel
               or SoC draining are predicted, not flagged)
``excursion``  share of window readings outside the vehicle's normal range           10 x share
``volatility`` *increase* in spread or in sample-to-sample roughness (jitter)        3 x ln ratio
``dropout``    readings missing compared with the baseline rate                      20 x missing
=============  ====================================================================  ==========

``gold.dtc_precursor_features`` keeps one row per (onset, signal); ``gold.dtc_precursor_ranking``
aggregates per (DTC, signal) with medians, so one odd episode cannot dominate. The signals that
reliably move before a code are its precursors: the input for turning fault codes into early
warnings.

The mart is fully generic: it runs on the long Silver table, so every VSS signal of every
platform is a candidate precursor without naming any of them in code.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from covesa_lakehouse.gold.common import ensure_table, in_days
from covesa_lakehouse.silver.vehicle_state import SIGNALS_TABLE, STATE_TABLE, day_filter

DTC_TABLE = "lakehouse.silver.dtc_events"
FEATURES_TABLE = "lakehouse.gold.dtc_precursor_features"
RANKING_TABLE = "lakehouse.gold.dtc_precursor_ranking"

ONSET_SILENCE_S = 30 * 60
WINDOW_S = 10 * 60
BASELINE_S = 60 * 60
EPISODE_TAIL_S = 10 * 60
REFERENCE_DAYS = 14
MIN_ONSETS = 5
MIN_REFERENCE_READINGS = 100
CELL_SPREAD_PATH = "Derived.TractionBattery.CellVoltageSpread"

FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    onset_id STRING, vin STRING, platform_id STRING, dtc_code STRING, onset_ts TIMESTAMP,
    vss_path STRING,
    w_n BIGINT, w_mean DOUBLE, w_std DOUBLE, w_slope_per_min DOUBLE,
    b_n BIGINT, b_mean DOUBLE, b_std DOUBLE, b_trend_pred DOUBLE,
    ref_lo DOUBLE COMMENT 'vehicle normal range p0.5 (fault episodes excluded)',
    ref_hi DOUBLE COMMENT 'vehicle normal range p99.5',
    level_z DOUBLE COMMENT 'window mean vs extrapolated baseline trend, residual sigmas',
    exceed_share DOUBLE COMMENT 'share of window readings outside [ref_lo, ref_hi]',
    volatility_ratio DOUBLE COMMENT 'window std / baseline std (noise-floor smoothed)',
    roughness_ratio DOUBLE COMMENT 'same for sample-to-sample differences (jitter)',
    completeness DOUBLE COMMENT 'window reading rate / baseline reading rate',
    score DOUBLE COMMENT 'max of the four change scores (approx. sigmas)',
    driver STRING COMMENT 'level | excursion | volatility | dropout'
) USING iceberg PARTITIONED BY (days(onset_ts))
TBLPROPERTIES ('format-version'='2')
"""


def onsets(dtc_events: DataFrame) -> DataFrame:
    """One row per fault episode: onset and last report of a code (30 min silence splits)."""
    w = Window.partitionBy("vin", "dtc_code").orderBy("event_ts")
    prev = F.lag("event_ts").over(w)
    is_onset = prev.isNull() | (
        F.col("event_ts").cast("double") - prev.cast("double") > ONSET_SILENCE_S
    )
    episodes = dtc_events.withColumn("_new", is_onset.cast("int")).withColumn(
        "_episode", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )
    return (
        episodes.groupBy("vin", "platform_id", "dtc_code", "_episode")
        .agg(F.min("event_ts").alias("onset_ts"), F.max("event_ts").alias("episode_end"))
        .select(
            F.concat_ws(
                "|", "vin", "dtc_code", F.date_format("onset_ts", "yyyyMMdd'T'HHmmss")
            ).alias("onset_id"),
            "vin",
            "platform_id",
            "dtc_code",
            "onset_ts",
            "episode_end",
        )
    )


def reference_ranges(signals: DataFrame, episodes: DataFrame) -> DataFrame:
    """Per (vehicle, signal): normal range from fault-free history."""
    s = signals.select("vin", "event_ts", "vss_path", "value").alias("s")
    e = episodes.select("vin", "onset_ts", "episode_end").alias("e")
    excluded = (F.col("s.vin") == F.col("e.vin")) & F.col("s.event_ts").between(
        F.col("e.onset_ts") - F.expr(f"INTERVAL {WINDOW_S + BASELINE_S} SECONDS"),
        F.col("e.episode_end") + F.expr(f"INTERVAL {EPISODE_TAIL_S} SECONDS"),
    )
    clean = s.join(F.broadcast(e), excluded, "left_anti")
    return (
        clean.groupBy("vin", "vss_path")
        .agg(
            F.percentile_approx("value", [0.005, 0.995], 1000).alias("_q"),
            F.count(F.lit(1)).alias("ref_n"),
        )
        .filter(F.col("ref_n") >= MIN_REFERENCE_READINGS)
        .select("vin", "vss_path", F.col("_q")[0].alias("ref_lo"), F.col("_q")[1].alias("ref_hi"))
    )


def _change_scores(features: DataFrame) -> DataFrame:
    level = F.coalesce(F.abs(F.col("level_z")), F.lit(0.0))
    excursion = 10 * F.coalesce(F.col("exceed_share"), F.lit(0.0))

    # One-sided: only *more* spread / jitter is a symptom. Calmer windows mostly reflect a regime
    # change (e.g. engine warm-up in the baseline), not a fault. Explicit null checks: Spark's
    # greatest() skips nulls, so a missing ratio must score 0.
    def log_ratio(c: str) -> Column:
        return F.when(F.col(c).isNotNull(), F.log(F.greatest(F.col(c), F.lit(1.0))))

    vol = 3 * F.coalesce(
        F.greatest(log_ratio("volatility_ratio"), log_ratio("roughness_ratio")), F.lit(0.0)
    )
    drop = 20 * (1 - F.col("completeness"))
    best = F.greatest(level, excursion, vol, drop)
    return features.withColumn("score", best).withColumn(
        "driver",
        F.when(drop == best, "dropout")
        .when(excursion == best, "excursion")
        .when(vol == best, "volatility")
        .otherwise("level"),
    )


def precursor_features(
    signals: DataFrame, onset_rows: DataFrame, ranges: DataFrame | None = None
) -> DataFrame:
    s = signals.select("vin", "event_ts", "vss_path", "value").alias("s")
    o = onset_rows.select("onset_id", "vin", "platform_id", "dtc_code", "onset_ts").alias("o")
    back = F.col("o.onset_ts").cast("double") - F.col("s.event_ts").cast("double")
    joined = s.join(
        F.broadcast(o),
        (F.col("s.vin") == F.col("o.vin")) & (back > 0) & (back <= WINDOW_S + BASELINE_S),
    ).select(
        "o.onset_id",
        "o.vin",
        "o.platform_id",
        "o.dtc_code",
        "o.onset_ts",
        "s.vss_path",
        "s.value",
        (-back / 60.0).alias("t_min"),  # minutes relative to onset (negative)
        (back <= WINDOW_S).alias("in_window"),
    )
    if ranges is None:
        ranges = joined.select("vin", "vss_path").distinct().select(
            "vin", "vss_path", F.lit(None).cast("double").alias("ref_lo"),
            F.lit(None).cast("double").alias("ref_hi"),
        )  # fmt: skip
    joined = joined.join(F.broadcast(ranges), ["vin", "vss_path"], "left")
    step = Window.partitionBy("onset_id", "vss_path", "in_window").orderBy("t_min")
    joined = joined.withColumn("diff", F.col("value") - F.lag("value").over(step))

    in_w, in_b = F.col("in_window"), ~F.col("in_window")

    def w_(c: Column) -> Column:
        return F.when(in_w, c)

    def b_(c: Column) -> Column:
        return F.when(in_b, c)

    v, t = F.col("value"), F.col("t_min")
    outside = (v < F.col("ref_lo")) | (v > F.col("ref_hi"))
    keys = ["onset_id", "vin", "platform_id", "dtc_code", "onset_ts", "vss_path"]
    agg = joined.groupBy(*keys).agg(
        F.count(w_(v)).alias("w_n"),
        F.avg(w_(v)).alias("w_mean"),
        F.stddev_samp(w_(v)).alias("w_std"),
        F.regr_slope(w_(v), w_(t)).alias("w_slope_per_min"),
        F.avg(w_(t)).alias("w_t_mean"),
        F.stddev_samp(w_(F.col("diff"))).alias("w_dstd"),
        F.count(F.when(in_w & outside, 1)).alias("w_out"),
        F.count(b_(v)).alias("b_n"),
        F.avg(b_(v)).alias("b_mean"),
        F.stddev_samp(b_(v)).alias("b_std"),
        F.stddev_samp(b_(F.col("diff"))).alias("b_dstd"),
        F.regr_slope(b_(v), b_(t)).alias("b_slope"),
        F.regr_intercept(b_(v), b_(t)).alias("b_intercept"),
        F.regr_sxx(b_(v), b_(t)).alias("b_sxx"),
        F.regr_syy(b_(v), b_(t)).alias("b_syy"),
        F.regr_sxy(b_(v), b_(t)).alias("b_sxy"),
        F.first("ref_lo").alias("ref_lo"),
        F.first("ref_hi").alias("ref_hi"),
    )

    has_trend = (F.col("b_n") >= 10) & (F.col("b_sxx") > 0)
    predicted = F.when(
        has_trend, F.col("b_intercept") + F.col("b_slope") * F.col("w_t_mean")
    ).otherwise(F.col("b_mean"))
    residual_var = F.greatest(F.col("b_syy") - F.col("b_sxy") ** 2 / F.col("b_sxx"), F.lit(0.0)) / (
        F.col("b_n") - 2
    )
    resid_std = F.when(has_trend, F.sqrt(residual_var)).otherwise(F.col("b_std"))
    small = F.greatest(F.abs(F.col("b_mean")) * 0.01, F.lit(1e-3))  # resolution-scale floor
    level_floor = F.greatest(F.coalesce(resid_std, F.lit(0.0)), small)
    rate_ratio = (F.col("w_n") / WINDOW_S) / (F.col("b_n") / BASELINE_S)

    features = (
        agg.filter(F.col("b_n") >= 3)  # a baseline is needed to talk about change
        .withColumn("b_trend_pred", predicted)
        .withColumn(
            "level_z", F.when(F.col("w_n") > 0, (F.col("w_mean") - predicted) / level_floor)
        )
        .withColumn("exceed_share", F.when(F.col("w_n") > 0, F.col("w_out") / F.col("w_n")))
        .withColumn(
            # Smoothed both sides: constant -> constant is 1 (no change), a freeze or new
            # jitter still stands out.
            "volatility_ratio",
            F.when(
                F.col("w_n") >= 3,
                (F.coalesce("w_std", F.lit(0.0)) + small)
                / (F.coalesce("b_std", F.lit(0.0)) + small),
            ),
        )
        .withColumn(
            "roughness_ratio",
            F.when(
                F.col("w_n") >= 4,
                (F.coalesce("w_dstd", F.lit(0.0)) + small)
                / (F.coalesce("b_dstd", F.lit(0.0)) + small),
            ),
        )
        .withColumn("completeness", F.least(rate_ratio, F.lit(1.0)))
    )
    return _change_scores(features)


def ranking(features: DataFrame) -> DataFrame:
    """Per (DTC, signal): how consistently the signal changes before the code."""
    by = features.groupBy("dtc_code", "vss_path").agg(
        F.countDistinct("onset_id").alias("onsets"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.avg((F.col("score") >= 3).cast("double")).alias("share_above_3_sigma"),
        F.percentile_approx("level_z", 0.5).alias("median_level_z"),
        F.percentile_approx("exceed_share", 0.5).alias("median_exceed_share"),
        F.percentile_approx("volatility_ratio", 0.5).alias("median_volatility_ratio"),
        F.percentile_approx("roughness_ratio", 0.5).alias("median_roughness_ratio"),
        F.percentile_approx("completeness", 0.5).alias("median_completeness"),
        F.mode("driver").alias("typical_driver"),
    )
    w = Window.partitionBy("dtc_code").orderBy(
        F.desc("median_score"), F.desc("share_above_3_sigma")
    )
    return by.filter(F.col("onsets") >= MIN_ONSETS).withColumn("rank", F.row_number().over(w))


def derived_signals(state: DataFrame) -> DataFrame:
    """Domain-derived candidates that VSS does not define as leaves.

    Cell imbalance shows in the *spread* between the highest and lowest cell voltage (mV); each
    cell voltage alone mostly tracks current and SoC. Battery engineers monitor exactly this.
    """
    return state.filter(F.col("cell_v_max").isNotNull() & F.col("cell_v_min").isNotNull()).select(
        "vin",
        "event_ts",
        F.lit(CELL_SPREAD_PATH).alias("vss_path"),
        ((F.col("cell_v_max") - F.col("cell_v_min")) * 1000.0).alias("value"),
    )


def build(spark: SparkSession, start: date, end: date) -> None:
    ensure_table(spark, FEATURES_DDL, FEATURES_TABLE)
    ref_start = start - timedelta(days=REFERENCE_DAYS)
    events = spark.table(DTC_TABLE).filter(day_filter("event_ts", ref_start, end))
    episodes = onsets(events)  # every episode in the reference period is excluded from it
    onset_rows = episodes.filter(in_days("onset_ts", start, end))
    signals = (
        spark.table(SIGNALS_TABLE)
        .filter(day_filter("event_ts", ref_start, end))
        .select("vin", "event_ts", "vss_path", "value")
        .unionByName(
            derived_signals(spark.table(STATE_TABLE).filter(day_filter("event_ts", ref_start, end)))
        )
    )
    ranges = reference_ranges(signals, episodes)
    features = precursor_features(signals, onset_rows, ranges)
    features.select(*spark.table(FEATURES_TABLE).columns).writeTo(
        FEATURES_TABLE
    ).overwritePartitions()
    # The ranking is small and fleet-wide: recompute it from all stored features.
    ranking(spark.table(FEATURES_TABLE)).writeTo(RANKING_TABLE).createOrReplace()
