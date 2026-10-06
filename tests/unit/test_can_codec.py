import random
from pathlib import Path

import pytest
from cantools.database.errors import DecodeError

from covesa_lakehouse.common import can_codec
from covesa_lakehouse.data_generator.dbc_loader import load_dbc

DBC_FILES = sorted(Path("sources/dbc").rglob("*.dbc"))


def test_little_endian_known_layout() -> None:
    # 16-bit Intel signal starting at bit 8: byte 1 is the LSB, byte 2 the MSB.
    data = bytes([0x00, 0x34, 0x12, 0x00])
    assert can_codec.extract(data, 8, 16, "little_endian", False) == 0x1234


def test_big_endian_known_layout() -> None:
    # 16-bit Motorola signal with MSB at sawtooth bit 7: bytes 0..1 read big-endian.
    data = bytes([0x12, 0x34, 0x00, 0x00])
    assert can_codec.extract(data, 7, 16, "big_endian", False) == 0x1234


def test_signed_values_use_twos_complement() -> None:
    buf = bytearray(2)
    can_codec.insert(buf, -3, 0, 12, "little_endian", True)
    assert can_codec.extract(bytes(buf), 0, 12, "little_endian", True) == -3
    assert can_codec.extract(bytes(buf), 0, 12, "little_endian", False) == 4096 - 3


def test_insert_rejects_values_that_do_not_fit() -> None:
    with pytest.raises(ValueError, match="does not fit"):
        can_codec.insert(bytearray(1), 256, 0, 8, "little_endian", False)


def test_layout_overflowing_the_frame_is_rejected() -> None:
    with pytest.raises(ValueError, match="overflows"):
        can_codec.extract(bytes(2), 12, 8, "little_endian", False)


@pytest.mark.parametrize("dbc_path", DBC_FILES, ids=lambda p: p.stem)
def test_codec_matches_cantools_on_real_dbcs(dbc_path: Path) -> None:
    """Our codec is what Silver will use; cantools is the reference implementation."""
    rng = random.Random(dbc_path.stem)
    db, _ = load_dbc(dbc_path)
    checked = 0
    for message in db.messages:
        if message.is_multiplexed():
            continue
        try:
            message.decode(bytes(message.length), scaling=False)
        except (DecodeError, ValueError):
            continue  # the community DBC itself is inconsistent for this message
        for _ in range(5):
            data = bytes(rng.getrandbits(8) for _ in range(message.length))
            expected = message.decode(data, decode_choices=False, scaling=False)
            rebuilt = bytearray(message.length)
            for s in message.signals:
                order = "little_endian" if s.byte_order == "little_endian" else "big_endian"
                assert can_codec.extract(data, s.start, s.length, order, s.is_signed) == (
                    expected[s.name]
                ), f"{message.name}.{s.name}"
                can_codec.insert(rebuilt, expected[s.name], s.start, s.length, order, s.is_signed)
                checked += 1
            assert bytes(rebuilt) == message.encode(expected, scaling=False, strict=False)
    assert checked > 0
