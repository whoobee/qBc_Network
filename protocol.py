"""
qB Companion — Serial Protocol Definition

Binary protocol between Raspberry Pi (master) and Teensy 4.1 (slave).
Mirrors the Teensy-side appl_protocol.h exactly.

Framing: COBS (Consistent Overhead Byte Stuffing) with 0x00 packet delimiter.
Byte order: little-endian (ARM native).

Wire format:
    COBS_ENCODE(RequestPacket) + 0x00   (Pi -> Teensy, 10 bytes payload)
    COBS_ENCODE(ResponsePacket) + 0x00  (Teensy -> Pi, 11 bytes payload)
"""

import struct

# =====================================================================
#  Packet types
# =====================================================================

PKT_REQUEST   = 0x01  # Pi  -> Teensy
PKT_RESPONSE  = 0x02  # Teensy -> Pi  (reply to a request)
PKT_TELEMETRY = 0x03  # Teensy -> Pi  (unsolicited periodic push)

# =====================================================================
#  Commands
# =====================================================================

CMD_DRIVE     = 0x01  # Control actuators (velocity, position, torque)
CMD_CONFIGURE = 0x02  # Set / get device configuration parameters
CMD_READ      = 0x03  # Read sensor / actuator status

# =====================================================================
#  Read / Write flag
# =====================================================================

RW_READ  = 0x00
RW_WRITE = 0x01

# =====================================================================
#  Device IDs  (mirrors ApplDeviceID in appl_protocol.h)
# =====================================================================

DEV_SYSTEM       = 0x00

DEV_WHEEL_LEFT   = 0x10
DEV_WHEEL_RIGHT  = 0x11

DEV_SERVO_NECK   = 0x20
DEV_SERVO_EAR_L  = 0x21
DEV_SERVO_EAR_R  = 0x22
DEV_SERVO_LEG_FL = 0x23
DEV_SERVO_LEG_FR = 0x24
DEV_SERVO_LEG_BL = 0x25
DEV_SERVO_LEG_BR = 0x26

DEV_TOF_LEFT     = 0x30
DEV_TOF_RIGHT    = 0x31
DEV_TOF_BACK     = 0x32
DEV_TOF_FRONT    = 0x33

DEV_IMU          = 0x40
DEV_LIDAR        = 0x50
DEV_BATTERY      = 0x60

GRP_ALL_WHEELS   = 0xE0
GRP_ALL_SERVOS   = 0xE1

# Joint name -> APPL device ID
JOINT_TO_DEV = {
    "neck":            DEV_SERVO_NECK,
    "left_ear":        DEV_SERVO_EAR_L,
    "right_ear":       DEV_SERVO_EAR_R,
    "left_front_leg":  DEV_SERVO_LEG_FL,
    "right_front_leg": DEV_SERVO_LEG_FR,
    "left_back_leg":   DEV_SERVO_LEG_BL,
    "right_back_leg":  DEV_SERVO_LEG_BR,
}

# Reverse: device ID -> joint name
DEV_TO_JOINT = {v: k for k, v in JOINT_TO_DEV.items()}

# =====================================================================
#  Parameters  (mirrors ApplParam in appl_protocol.h)
# =====================================================================

PARAM_VELOCITY          = 0x01
PARAM_POSITION          = 0x02
PARAM_TORQUE_ENABLE     = 0x03
PARAM_ACCELERATION      = 0x04
PARAM_SPEED             = 0x05

PARAM_TEMPERATURE       = 0x10
PARAM_VOLTAGE           = 0x11
PARAM_CURRENT           = 0x12
PARAM_LOAD              = 0x13
PARAM_DISTANCE_MM       = 0x14
PARAM_ORIENTATION_ROLL  = 0x15
PARAM_ORIENTATION_PITCH = 0x16
PARAM_ORIENTATION_YAW   = 0x17
PARAM_QUATERNION_W      = 0x18
PARAM_QUATERNION_X      = 0x19
PARAM_QUATERNION_Y      = 0x1A
PARAM_QUATERNION_Z      = 0x1B
PARAM_ACCEL_X           = 0x1C
PARAM_ACCEL_Y           = 0x1D
PARAM_ACCEL_Z           = 0x1E

PARAM_ODOM_X            = 0x20
PARAM_ODOM_Y            = 0x21
PARAM_ODOM_HEADING      = 0x22

PARAM_SAFETY_STATUS     = 0x40
PARAM_HEARTBEAT         = 0x41
PARAM_FAULT_CODE        = 0x42

# Lidar polar histogram — 36 bins x 10 deg, param = PARAM_LIDAR_BIN_0 + bin_index
PARAM_LIDAR_BIN_0       = 0x80
PARAM_LIDAR_BIN_35      = 0xA3
LIDAR_BIN_COUNT         = 36
LIDAR_BIN_DEG           = 10.0

# =====================================================================
#  Error codes
# =====================================================================

ERR_OK              = 0x00
ERR_UNKNOWN_CMD     = 0x01
ERR_UNKNOWN_DEVICE  = 0x02
ERR_UNKNOWN_PARAM   = 0x03
ERR_DEVICE_FAULT    = 0x04
ERR_SAFETY_BLOCK    = 0x05
ERR_TIMEOUT         = 0x06
ERR_INVALID_VALUE   = 0x07
ERR_NOT_READY       = 0x08

ERR_NAMES = {
    ERR_OK: "OK", ERR_UNKNOWN_CMD: "UNKNOWN_CMD", ERR_UNKNOWN_DEVICE: "UNKNOWN_DEV",
    ERR_UNKNOWN_PARAM: "UNKNOWN_PARAM", ERR_DEVICE_FAULT: "DEVICE_FAULT",
    ERR_SAFETY_BLOCK: "SAFETY_BLOCK", ERR_TIMEOUT: "TIMEOUT",
    ERR_INVALID_VALUE: "INVALID_VALUE", ERR_NOT_READY: "NOT_READY",
}

# =====================================================================
#  Safety bit definitions (matches Teensy firmware)
# =====================================================================

SAFETY_OBSTACLE_BIT = 1 << 0
SAFETY_TILT_BIT     = 1 << 1
SAFETY_PICKUP_BIT   = 1 << 2
SAFETY_LOWBATT_BIT  = 1 << 3
SAFETY_WATCHDOG_BIT = 1 << 4
SAFETY_OVERTEMP_BIT = 1 << 5

# =====================================================================
#  Packet formats  (little-endian, packed)
# =====================================================================

# RequestPacket:  10 bytes
# [pkt_type:1][sequence:1][command:1][device_id:1][rw_flag:1][parameter:1][value:4]
FMT_REQUEST = "<BBBBBB4s"
REQUEST_SIZE = 10

# ResponsePacket: 11 bytes
# [pkt_type:1][sequence:1][command:1][device_id:1][rw_flag:1][parameter:1][value:4][error:1]
FMT_RESPONSE = "<BBBBBB4sB"
RESPONSE_SIZE = 11

SEQ_TELEMETRY = 0xFF

# =====================================================================
#  Value packing helpers
# =====================================================================

def pack_float(val: float) -> bytes:
    return struct.pack("<f", val)

def unpack_float(data: bytes) -> float:
    return struct.unpack("<f", data[:4])[0]

def pack_uint32(val: int) -> bytes:
    return struct.pack("<I", val)

def unpack_uint32(data: bytes) -> int:
    return struct.unpack("<I", data[:4])[0]

def pack_pos_speed(position: int, speed: int) -> bytes:
    """Pack servo position (uint16) + speed (uint16) into 4 bytes."""
    return struct.pack("<HH", position, speed)

# =====================================================================
#  COBS codec
# =====================================================================

def cobs_encode(data: bytes) -> bytes:
    """Encode data using COBS.  Output contains no 0x00 bytes."""
    data = bytes(data)
    out = bytearray()
    code_idx = 0
    out.append(0)  # placeholder for first code byte
    code = 1
    for byte in data:
        if byte == 0:
            out[code_idx] = code
            code_idx = len(out)
            out.append(0)
            code = 1
        else:
            out.append(byte)
            code += 1
            if code == 0xFF:
                out[code_idx] = code
                code_idx = len(out)
                out.append(0)
                code = 1
    out[code_idx] = code
    return bytes(out)


def cobs_decode(data: bytes) -> bytes:
    """Decode COBS-encoded data.  Input must NOT include the 0x00 delimiter."""
    data = bytes(data)
    out = bytearray()
    idx = 0
    while idx < len(data):
        code = data[idx]
        if code == 0:
            raise ValueError("Unexpected zero byte in COBS data")
        idx += 1
        for _ in range(code - 1):
            if idx >= len(data):
                raise ValueError("COBS decode: truncated packet")
            out.append(data[idx])
            idx += 1
        if code < 0xFF and idx < len(data):
            out.append(0)
    return bytes(out)


# =====================================================================
#  Encode: build RequestPacket (Pi -> Teensy)
# =====================================================================

class SequenceCounter:
    """Thread-safe sequence counter 0-254.  0xFF is reserved for telemetry."""
    def __init__(self):
        self._seq = 0

    def next(self) -> int:
        s = self._seq
        self._seq = (self._seq + 1) % 255  # 0-254
        return s


_seq = SequenceCounter()


def make_request(command: int, device_id: int, rw_flag: int,
                 parameter: int, value: bytes = b"\x00\x00\x00\x00",
                 seq: int | None = None) -> bytes:
    """Build a 10-byte RequestPacket."""
    if seq is None:
        seq = _seq.next()
    return struct.pack(FMT_REQUEST, PKT_REQUEST, seq, command,
                       device_id, rw_flag, parameter, value)


def encode_heartbeat() -> bytes:
    return make_request(CMD_READ, DEV_SYSTEM, RW_WRITE, PARAM_HEARTBEAT)


def encode_wheel_cmd(left_rpm: float, right_rpm: float) -> list[bytes]:
    """Encode wheel velocity as two RequestPackets (one per wheel)."""
    return [
        make_request(CMD_DRIVE, DEV_WHEEL_LEFT,  RW_WRITE, PARAM_VELOCITY, pack_float(left_rpm)),
        make_request(CMD_DRIVE, DEV_WHEEL_RIGHT, RW_WRITE, PARAM_VELOCITY, pack_float(right_rpm)),
    ]


def encode_servo_move(device_id: int, position_raw: int, speed_raw: int) -> bytes:
    """Encode servo position + speed into one RequestPacket.
    value[4] = [position:uint16_LE][speed:uint16_LE]
    """
    return make_request(CMD_DRIVE, device_id, RW_WRITE, PARAM_POSITION,
                        pack_pos_speed(position_raw, speed_raw))


def encode_servo_accel(device_id: int, acceleration: int) -> bytes:
    """Set servo acceleration via CMD_CONFIGURE (send before position command)."""
    return make_request(CMD_CONFIGURE, device_id, RW_WRITE, PARAM_ACCELERATION,
                        pack_uint32(acceleration))


def encode_read(device_id: int, parameter: int) -> bytes:
    """Encode a READ request."""
    return make_request(CMD_READ, device_id, RW_READ, parameter)


# =====================================================================
#  Decode: parse ResponsePacket (Teensy -> Pi)
# =====================================================================

def decode_response(raw: bytes) -> dict | None:
    """Decode an 11-byte ResponsePacket.  Returns dict or None if invalid."""
    if len(raw) != RESPONSE_SIZE:
        return None

    pkt_type, seq, cmd, dev, rw, param, value, error = struct.unpack(FMT_RESPONSE, raw)

    if pkt_type not in (PKT_RESPONSE, PKT_TELEMETRY):
        return None

    return {
        "pkt_type":    pkt_type,
        "sequence":    seq,
        "command":     cmd,
        "device_id":   dev,
        "rw_flag":     rw,
        "parameter":   param,
        "value_raw":   value,
        "value_float": unpack_float(value),
        "error":       error,
        "is_telemetry": pkt_type == PKT_TELEMETRY,
    }


def decode_safety_bits(bits: int) -> dict:
    """Expand safety bitmask into a dict."""
    return {
        "safety_bits": bits,
        "obstacle":    bool(bits & SAFETY_OBSTACLE_BIT),
        "tilt":        bool(bits & SAFETY_TILT_BIT),
        "pickup":      bool(bits & SAFETY_PICKUP_BIT),
        "low_battery": bool(bits & SAFETY_LOWBATT_BIT),
        "watchdog":    bool(bits & SAFETY_WATCHDOG_BIT),
        "overtemp":    bool(bits & SAFETY_OVERTEMP_BIT),
        "status":      "ok" if bits == 0 else "stopped",
    }
