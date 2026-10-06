"""Pack and unpack raw integer signals in CAN frame payloads using DBC bit semantics.

* little_endian ("Intel"): ``start_bit`` is the LSB; bits run upward through the frame.
* big_endian ("Motorola"): ``start_bit`` is the MSB in DBC sawtooth numbering
  (bit 7 of byte 0 is 7, bit 0 of byte 1 is 8, ...); the signal runs toward later bytes.

Works on raw integers only; scaling to physical values is the contract's job.
"""

from __future__ import annotations

from typing import Literal

ByteOrder = Literal["little_endian", "big_endian"]


def _lsb_shift(start_bit: int, length: int, byte_order: ByteOrder, dlc: int) -> int:
    """Shift that aligns the signal's LSB with bit 0 of the payload read as one integer.

    Little-endian signals use the payload read little-endian; big-endian signals the payload
    read big-endian.
    """
    total = dlc * 8
    if byte_order == "little_endian":
        shift = start_bit
        end = start_bit + length
    else:
        msb_linear = (start_bit // 8) * 8 + (7 - start_bit % 8)
        shift = total - msb_linear - length
        end = msb_linear + length
    if shift < 0 or end > total:
        raise ValueError(
            f"signal (start={start_bit}, len={length}, {byte_order}) overflows a {dlc}-byte frame"
        )
    return shift


def raw_range(length: int, is_signed: bool) -> tuple[int, int]:
    if is_signed:
        return -(1 << (length - 1)), (1 << (length - 1)) - 1
    return 0, (1 << length) - 1


def extract(
    data: bytes,
    start_bit: int,
    length: int,
    byte_order: ByteOrder,
    is_signed: bool,
) -> int:
    shift = _lsb_shift(start_bit, length, byte_order, len(data))
    word = int.from_bytes(data, "little" if byte_order == "little_endian" else "big")
    raw = (word >> shift) & ((1 << length) - 1)
    if is_signed and raw >> (length - 1):
        raw -= 1 << length
    return raw


def insert(
    data: bytearray,
    raw: int,
    start_bit: int,
    length: int,
    byte_order: ByteOrder,
    is_signed: bool,
) -> None:
    lo, hi = raw_range(length, is_signed)
    if not lo <= raw <= hi:
        raise ValueError(f"raw value {raw} does not fit in {length} bits (signed={is_signed})")
    endian: Literal["little", "big"] = "little" if byte_order == "little_endian" else "big"
    shift = _lsb_shift(start_bit, length, byte_order, len(data))
    mask = ((1 << length) - 1) << shift
    word = int.from_bytes(data, endian)
    word = (word & ~mask) | (((raw & ((1 << length) - 1)) << shift) & mask)
    data[:] = word.to_bytes(len(data), endian)
