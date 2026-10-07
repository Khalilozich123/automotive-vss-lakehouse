"""Data-quality rules and Dead Letter Queue routing for the Silver layer.

Message-level errors quarantine the whole message (with its raw payload, for replay). Signal-level
errors quarantine only the offending signal: the other signals of the same message stay in Silver,
because one glitching sensor must not discard a dozen good readings.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# message level: nothing in the message can be trusted or routed
PARSE_ERROR = "PARSE_ERROR"  # not a valid telemetry envelope (corrupt / truncated)
UNKNOWN_PLATFORM = "UNKNOWN_PLATFORM"  # no valid contract for the platform
ENCODING_MISMATCH = "ENCODING_MISMATCH"  # e.g. firmware switched JSON -> CAN frames
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"  # event time missing or unparseable

# signal level: only this reading is quarantined
MALFORMED_FRAME = "MALFORMED_FRAME"  # CAN frame with bad hex or wrong length
INVALID_RAW_VALUE = "INVALID_RAW_VALUE"  # JSON raw value that is not an integer
PHYSICAL_RANGE_VIOLATION = "PHYSICAL_RANGE_VIOLATION"  # decoded value outside plausibility bounds

MESSAGE_LEVEL = (PARSE_ERROR, UNKNOWN_PLATFORM, ENCODING_MISMATCH, INVALID_TIMESTAMP)
SIGNAL_LEVEL = (MALFORMED_FRAME, INVALID_RAW_VALUE, PHYSICAL_RANGE_VIOLATION)

_INTEGER = r"^-?[0-9]{1,18}$"


def is_complete_json(payload: Column) -> Column:
    """Strict check that the whole document parses.

    ``from_json`` alone is not enough: for some malformed documents it returns a *partial* struct
    (fields read before the error, e.g. ``msg_id`` and ``ts`` of a truncated payload), and whether
    it does depends on the execution plan. Extracting the root (``$``) forces a full parse.
    """
    return F.get_json_object(payload, "$").isNotNull()


def message_error(
    payload: Column,
    envelope: Column,
    contract_version: Column,
    contract_encoding: Column,
    event_ts: Column,
) -> Column:
    """First failing message-level rule, or null if the message can be decoded."""
    return (
        F.when(
            ~is_complete_json(payload) | envelope.isNull() | envelope["msg_id"].isNull(),
            PARSE_ERROR,
        )
        .when(contract_version.isNull(), UNKNOWN_PLATFORM)
        .when(~envelope["encoding"].eqNullSafe(contract_encoding), ENCODING_MISMATCH)
        .when(event_ts.isNull(), INVALID_TIMESTAMP)
    )


def message_error_detail(
    code: Column, envelope: Column, platform_id: Column, contract_encoding: Column
) -> Column:
    return (
        F.when(code == PARSE_ERROR, F.lit("payload is not a valid telemetry envelope"))
        .when(
            code == UNKNOWN_PLATFORM,
            F.concat(
                F.lit("no valid contract for platform "), F.coalesce(platform_id, F.lit("<none>"))
            ),
        )
        .when(
            code == ENCODING_MISMATCH,
            F.format_string(
                "envelope encoding %s, contract expects %s", envelope["encoding"], contract_encoding
            ),
        )
        .when(
            code == INVALID_TIMESTAMP,
            F.concat(
                F.lit("unparseable event time: "), F.coalesce(envelope["ts"], F.lit("<none>"))
            ),
        )
    )


def is_integer_text(raw_text: Column) -> Column:
    return F.coalesce(raw_text.rlike(_INTEGER), F.lit(False))


def in_range(value: Column, min_val: Column, max_val: Column) -> Column:
    return F.coalesce(value.between(min_val, max_val), F.lit(False))


def range_detail(
    vss_path: Column, value: Column, min_val: Column, max_val: Column, unit: Column
) -> Column:
    return F.format_string("%s = %.4f %s outside [%s, %s]", vss_path, value, unit, min_val, max_val)


def quarantine_key(dedup_id: Column, signal_name: Column, code: Column) -> Column:
    """Stable key: a defect seen twice (duplicate delivery, replayed batch) is stored once."""
    return F.sha2(F.concat_ws("|", dedup_id, F.coalesce(signal_name, F.lit("")), code), 256)
