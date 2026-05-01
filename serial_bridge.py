#!/usr/bin/env python3
"""
qBc_Network — Serial Bridge Service

Sole translator between MQTT (Pi services) and the Teensy 4.1 (COBS serial).
No other process on the Pi may talk to the Teensy directly.

Uses the APPL binary protocol (10-byte RequestPacket / 11-byte ResponsePacket)
with COBS framing and 0x00 packet delimiter.

MQTT subscriptions (Pi -> Teensy):
    robot/joints/cmd            Joint move / animate / status requests
    robot/wheels/cmd            Wheel velocity commands

MQTT publications (Teensy -> Pi):
    robot/odometry/pose         20 Hz  pose (from telemetry)
    robot/sensors/tof           20 Hz  3x TOF distances (from telemetry)
    robot/sensors/lidar         ~3 Hz  36-bin polar histogram (from telemetry)
    robot/imu/orientation       20 Hz  euler + quaternion + accel (from telemetry)
    robot/status/battery         ~20 Hz  retained (from telemetry)
    robot/status/motors         ~10 Hz  per-motor full status (round-robin)
    robot/joints/telemetry      ~3 Hz  per-servo full status (round-robin)
    robot/joints/status         on-demand joint status responses
    robot/safety/status         ~20 Hz  retained (from telemetry)
    robot/system/heartbeat/teensy  derived from heartbeat ACK

Usage:
    python serial_bridge.py [--serial-port /dev/ttyAMA0] [--mqtt-broker localhost]
"""

import argparse
import json
import logging
import math
import signal
import threading
import time
from pathlib import Path

import serial
import paho.mqtt.client as mqtt

import protocol as proto

logger = logging.getLogger("qBc_Bridge")

# =====================================================================
#  Constants
# =====================================================================

DEFAULT_SERIAL_PORT = "/dev/ttyAMA0"
DEFAULT_SERIAL_BAUD = 115200
DEFAULT_MQTT_BROKER = "localhost"
DEFAULT_MQTT_PORT = 1883

CALIBRATION_FILE = Path(__file__).parent.parent / "qBc_Servos" / "servo_calibration.json"

STEPS_PER_DEGREE = 4096.0 / 360.0   # ST3215: ~11.378 steps/degree


def _v(val, default=None):
    """Telemetry → JSON-safe scalar.

    The firmware emits IEEE-754 NaN for any sample it deems invalid /
    low-confidence (sensor not present, dropout exceeded the hold+decay
    window, etc.). Convert NaN to ``None`` so the resulting JSON contains
    a clean ``null`` — every downstream consumer can branch on missing
    data without hunting for magic-number sentinels.
    """
    if val is None:
        return default
    try:
        f = float(val)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _vi(val, default=None):
    """Same as :func:`_v` but coerces a finite value to ``int`` (e.g. mm)."""
    f = _v(val, default=None)
    if f is None:
        return default
    return int(f)

# Movement profiles (matches qBc_Servos)
MOVEMENT_PROFILES = {
    "linear":      {"acc": 10,  "speed_mult": 1.0},
    "quadratic":   {"acc": 80,  "speed_mult": 1.5},
    "exponential": {"acc": 180, "speed_mult": 2.0},
}

# MQTT topics — subscribe
TOPIC_JOINTS_CMD = "robot/joints/cmd"
TOPIC_WHEELS_CMD = "robot/wheels/cmd"

# MQTT topics — publish
TOPIC_JOINTS_STATUS   = "robot/joints/status"
TOPIC_JOINTS_STATE    = "robot/joints/current_state"
TOPIC_JOINTS_TELEM    = "robot/joints/telemetry"      # per-servo full status (pos/vel/load/temp/volt/curr)
TOPIC_ODOMETRY        = "robot/odometry/pose"
TOPIC_TOF             = "robot/sensors/tof"
TOPIC_LIDAR           = "robot/sensors/lidar"         # 36-bin polar histogram
TOPIC_IMU             = "robot/imu/orientation"       # full: euler + quat + accel
TOPIC_BATTERY         = "robot/status/battery"
TOPIC_MOTORS          = "robot/status/motors"         # per-motor full status (vel/pos/curr/temp/fault)
TOPIC_SAFETY_STATUS   = "robot/safety/status"
TOPIC_HB_BRIDGE       = "robot/system/heartbeat/bridge"
TOPIC_HB_TEENSY       = "robot/system/heartbeat/teensy"
TOPIC_BRIDGE_STATE    = "robot/bridge/state"

# Motor (wheel) APPL device ID -> canonical name
MOTOR_DEV_TO_NAME = {
    proto.DEV_WHEEL_LEFT:  "left",
    proto.DEV_WHEEL_RIGHT: "right",
}

# Heartbeat interval to Teensy (seconds)
HEARTBEAT_INTERVAL = 1.0
# Teensy heartbeat timeout (seconds) — generous to tolerate telemetry bursts
TEENSY_HB_TIMEOUT = 5.0
# Serial reconnect delay (seconds)
RECONNECT_DELAY = 2.0

# =====================================================================
#  Joint calibration
# =====================================================================

def load_calibration(path: Path) -> dict:
    """Load servo_calibration.json.  Returns {joint_name: {...}}."""
    if not path.exists():
        logger.warning("Calibration file not found: %s — joint mapping disabled", path)
        return {}
    with open(path) as f:
        data = json.load(f)
    joints = {}
    for key, s in data.get("servos", {}).items():
        if key not in proto.JOINT_TO_DEV:
            logger.warning("Calibration joint '%s' not in protocol device map — skipped", key)
            continue
        joints[key] = {
            "name": s["name"],
            "zero_position": s["zero_position"],
            "min_degrees": s["min_degrees"],
            "max_degrees": s["max_degrees"],
            "min_raw": s["min_raw"],
            "max_raw": s["max_raw"],
        }
    logger.info("Loaded %d joints from %s", len(joints), path)
    return joints


def _deg_to_raw(degrees: float, zero: int) -> int:
    return int(zero + degrees * STEPS_PER_DEGREE)


def _raw_to_deg(raw: int, zero: int) -> float:
    return (raw - zero) / STEPS_PER_DEGREE


def _clamp_raw(val: int, joint: dict) -> int:
    return max(joint["min_raw"], min(joint["max_raw"], val))


def _deg_per_sec_to_raw(dps: float) -> int:
    return max(1, int(abs(dps) * STEPS_PER_DEGREE))


# =====================================================================
#  Serial Bridge
# =====================================================================

class SerialBridge:
    def __init__(self, serial_port: str, serial_baud: int,
                 mqtt_broker: str, mqtt_port: int,
                 calibration_path: Path):
        self._serial_port = serial_port
        self._serial_baud = serial_baud
        self._ser = None
        self._ser_lock = threading.Lock()

        self._mqtt_broker = mqtt_broker
        self._mqtt_port = mqtt_port

        self._joints = load_calibration(calibration_path)

        self._running = False
        self._teensy_alive = False
        self._last_teensy_hb = 0.0

        # Telemetry accumulator: (device_id, parameter) -> float
        self._telem = {}

        # Track last commanded servo position for timed-move speed calc
        self._last_servo_pos = {}
        for name, j in self._joints.items():
            self._last_servo_pos[name] = j["zero_position"]

        # MQTT client
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="qbc_bridge",
        )
        self._client.on_connect = self._on_mqtt_connect
        self._client.on_disconnect = self._on_mqtt_disconnect
        self._client.will_set(TOPIC_BRIDGE_STATE,
                              json.dumps({"status": "offline"}),
                              qos=1, retain=True)

    # ------------------------------------------------------------------
    #  MQTT callbacks
    # ------------------------------------------------------------------

    def _on_mqtt_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error("MQTT connection failed: %s", reason_code)
            return
        logger.info("MQTT connected to %s:%d", self._mqtt_broker, self._mqtt_port)
        client.subscribe([(TOPIC_JOINTS_CMD, 1), (TOPIC_WHEELS_CMD, 1)])
        client.message_callback_add(TOPIC_JOINTS_CMD, self._on_joint_cmd)
        client.message_callback_add(TOPIC_WHEELS_CMD, self._on_wheel_cmd)
        self._publish_bridge_state()

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        if reason_code.is_failure:
            logger.warning("MQTT disconnected: %s", reason_code)

    # ------------------------------------------------------------------
    #  MQTT -> Teensy  (command handlers)
    # ------------------------------------------------------------------

    def _on_joint_cmd(self, client, userdata, msg):
        """Handle robot/joints/cmd messages."""
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        msg_type = data.get("type", "")

        if msg_type == "joint_move_request":
            self._handle_joint_move(data)
        elif msg_type == "joint_animate_request":
            self._handle_joint_animate(data)
        elif msg_type == "get_joint_status":
            self._handle_joint_status_req(data)
        else:
            logger.warning("Unknown joint command type: %s", msg_type)

    def _handle_joint_move(self, data: dict):
        """Convert joint_move_request to APPL packets.

        Sends two packets:
          1. CMD_CONFIGURE / PARAM_ACCELERATION  (set accel for this servo)
          2. CMD_DRIVE / PARAM_POSITION           (position + speed packed)
        """
        joint_name = data.get("joint_name")
        joint = self._joints.get(joint_name)
        if not joint:
            self._publish_error(f"Unknown joint: {joint_name}")
            return

        target_deg = data.get("target_position", 0.0)
        speed_dps = data.get("speed", 30.0)
        movement_type = data.get("movement_type", "linear")

        profile = MOVEMENT_PROFILES.get(movement_type)
        if not profile:
            self._publish_error(f"Unknown movement_type: {movement_type}")
            return

        target_deg = max(joint["min_degrees"], min(joint["max_degrees"], target_deg))
        target_raw = _clamp_raw(_deg_to_raw(target_deg, joint["zero_position"]), joint)
        speed_raw = _deg_per_sec_to_raw(speed_dps * profile["speed_mult"])
        accel = profile["acc"]

        dev_id = proto.JOINT_TO_DEV[joint_name]

        # Packet 1: configure acceleration
        self._send_to_teensy(proto.encode_servo_accel(dev_id, accel))
        # Packet 2: drive position + speed
        self._send_to_teensy(proto.encode_servo_move(dev_id, target_raw, speed_raw))

        self._last_servo_pos[joint_name] = target_raw

        logger.debug("MOVE %s -> %.1f deg (raw %d) spd=%d acc=%d [%s]",
                      joint_name, target_deg, target_raw, speed_raw, accel, movement_type)

    def _handle_joint_animate(self, data: dict):
        """Convert joint_animate_request to APPL packets.

        Computes speed from position delta and requested duration.
        Sends two packets (acceleration config + position drive).
        """
        joint_name = data.get("joint_name")
        joint = self._joints.get(joint_name)
        if not joint:
            self._publish_error(f"Unknown joint: {joint_name}")
            return

        target_deg = data.get("target_position", 0.0)
        duration = data.get("duration", 1.0)
        movement_type = data.get("movement_type", "linear")

        profile = MOVEMENT_PROFILES.get(movement_type)
        if not profile:
            self._publish_error(f"Unknown movement_type: {movement_type}")
            return

        if duration <= 0:
            self._publish_error("duration must be > 0")
            return

        target_deg = max(joint["min_degrees"], min(joint["max_degrees"], target_deg))
        target_raw = _clamp_raw(_deg_to_raw(target_deg, joint["zero_position"]), joint)
        accel = profile["acc"]

        # Compute speed from position delta / duration (steps per second)
        current_raw = self._last_servo_pos.get(joint_name, joint["zero_position"])
        delta = abs(target_raw - current_raw)
        if delta == 0:
            return  # already at target
        speed_raw = max(1, int(delta / duration))

        dev_id = proto.JOINT_TO_DEV[joint_name]

        # Packet 1: configure acceleration
        self._send_to_teensy(proto.encode_servo_accel(dev_id, accel))
        # Packet 2: drive position + speed
        self._send_to_teensy(proto.encode_servo_move(dev_id, target_raw, speed_raw))

        self._last_servo_pos[joint_name] = target_raw

        logger.debug("ANIMATE %s -> %.1f deg in %.2fs (raw %d) spd=%d acc=%d [%s]",
                      joint_name, target_deg, duration, target_raw, speed_raw, accel,
                      movement_type)

    def _handle_joint_status_req(self, data: dict):
        """Request current servo position from Teensy."""
        joint_name = data.get("joint_name")
        joint = self._joints.get(joint_name)
        if not joint:
            self._publish_error(f"Unknown joint: {joint_name}")
            return

        dev_id = proto.JOINT_TO_DEV[joint_name]
        self._send_to_teensy(proto.encode_read(dev_id, proto.PARAM_POSITION))

    def _on_wheel_cmd(self, client, userdata, msg):
        """Handle robot/wheels/cmd messages."""
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        left = float(data.get("left_vel", 0.0))
        right = float(data.get("right_vel", 0.0))
        for pkt in proto.encode_wheel_cmd(left, right):
            self._send_to_teensy(pkt)

    # ------------------------------------------------------------------
    #  Teensy -> MQTT  (incoming serial dispatch)
    # ------------------------------------------------------------------

    def _dispatch_response(self, rsp: dict):
        """Route a PKT_RESPONSE to the appropriate handler."""
        dev = rsp["device_id"]
        param = rsp["parameter"]
        error = rsp["error"]

        # Heartbeat ACK
        if dev == proto.DEV_SYSTEM and param == proto.PARAM_HEARTBEAT:
            self._last_teensy_hb = time.monotonic()
            if not self._teensy_alive:
                self._teensy_alive = True
                logger.info("Teensy heartbeat established")
                self._publish_bridge_state()
            self._client.publish(TOPIC_HB_TEENSY, b"1", qos=0)
            return

        # Log errors from Teensy
        if error != proto.ERR_OK:
            err_name = proto.ERR_NAMES.get(error, f"0x{error:02X}")
            logger.warning("Teensy error: dev=0x%02X param=0x%02X err=%s",
                           dev, param, err_name)
            return

        # Joint status response (position read)
        if proto.DEV_SERVO_NECK <= dev <= proto.DEV_SERVO_LEG_BR:
            joint_name = proto.DEV_TO_JOINT.get(dev)
            if joint_name and joint_name in self._joints:
                j = self._joints[joint_name]
                pos_raw = int(rsp["value_float"])
                pos_deg = _raw_to_deg(pos_raw, j["zero_position"])
                self._client.publish(TOPIC_JOINTS_STATUS, json.dumps({
                    "type": "joint_status",
                    "joint_name": joint_name,
                    "position_raw": pos_raw,
                    "position_deg": round(pos_deg, 2),
                }), qos=0)

    def _dispatch_telemetry(self, rsp: dict):
        """Accumulate telemetry values and publish aggregated MQTT when group complete."""
        dev = rsp["device_id"]
        param = rsp["parameter"]
        val = rsp["value_float"]

        # Store latest value
        self._telem[(dev, param)] = val

        # Odometry group — publish after heading (last in TX cycle)
        if dev == proto.DEV_SYSTEM and param == proto.PARAM_ODOM_HEADING:
            self._publish_odometry()

        # IMU full group — firmware sends euler then quat then accel; publish on ACCEL_Z
        elif dev == proto.DEV_IMU and param == proto.PARAM_ACCEL_Z:
            self._publish_imu()

        # TOF group — publish after back sensor (last in TX cycle)
        elif dev == proto.DEV_TOF_BACK and param == proto.PARAM_DISTANCE_MM:
            self._publish_tof()

        # Lidar polar histogram — firmware cycles bins 0..35 in 6-bin slices.
        # Bin 35 arrives once every 6 TX ticks (~300 ms) -> publish then.
        elif dev == proto.DEV_LIDAR and param == proto.PARAM_LIDAR_BIN_35:
            self._publish_lidar()

        # Servo full status — firmware sends pos, vel, temp, volt, curr, load (terminator)
        elif (proto.DEV_SERVO_NECK <= dev <= proto.DEV_SERVO_LEG_BR
              and param == proto.PARAM_LOAD):
            self._publish_servo(dev)

        # Motor (wheel) full status — firmware sends vel, pos, curr, temp, fault (terminator)
        elif dev in MOTOR_DEV_TO_NAME and param == proto.PARAM_FAULT_CODE:
            self._publish_motor(dev)

        # Battery — single value, publish immediately
        elif dev == proto.DEV_BATTERY and param == proto.PARAM_VOLTAGE:
            self._publish_battery()

        # Safety — single value, publish immediately
        elif dev == proto.DEV_SYSTEM and param == proto.PARAM_SAFETY_STATUS:
            self._publish_safety()

    # ------------------------------------------------------------------
    #  Telemetry -> MQTT publishers
    # ------------------------------------------------------------------

    def _publish_odometry(self):
        t = self._telem
        self._client.publish(TOPIC_ODOMETRY, json.dumps({
            "x_mm":        _v(t.get((proto.DEV_SYSTEM, proto.PARAM_ODOM_X))),
            "y_mm":        _v(t.get((proto.DEV_SYSTEM, proto.PARAM_ODOM_Y))),
            "heading_deg": _v(t.get((proto.DEV_SYSTEM, proto.PARAM_ODOM_HEADING))),
        }), qos=0)

    def _publish_imu(self):
        t = self._telem
        self._client.publish(TOPIC_IMU, json.dumps({
            "roll":  _v(t.get((proto.DEV_IMU, proto.PARAM_ORIENTATION_ROLL))),
            "pitch": _v(t.get((proto.DEV_IMU, proto.PARAM_ORIENTATION_PITCH))),
            "yaw":   _v(t.get((proto.DEV_IMU, proto.PARAM_ORIENTATION_YAW))),
            "qw":    _v(t.get((proto.DEV_IMU, proto.PARAM_QUATERNION_W))),
            "qx":    _v(t.get((proto.DEV_IMU, proto.PARAM_QUATERNION_X))),
            "qy":    _v(t.get((proto.DEV_IMU, proto.PARAM_QUATERNION_Y))),
            "qz":    _v(t.get((proto.DEV_IMU, proto.PARAM_QUATERNION_Z))),
            "ax":    _v(t.get((proto.DEV_IMU, proto.PARAM_ACCEL_X))),
            "ay":    _v(t.get((proto.DEV_IMU, proto.PARAM_ACCEL_Y))),
            "az":    _v(t.get((proto.DEV_IMU, proto.PARAM_ACCEL_Z))),
        }), qos=0)

    def _publish_tof(self):
        t = self._telem
        self._client.publish(TOPIC_TOF, json.dumps({
            "left_mm":  _vi(t.get((proto.DEV_TOF_LEFT, proto.PARAM_DISTANCE_MM))),
            "right_mm": _vi(t.get((proto.DEV_TOF_RIGHT, proto.PARAM_DISTANCE_MM))),
            "front_mm": _vi(t.get((proto.DEV_TOF_FRONT, proto.PARAM_DISTANCE_MM))),
            "back_mm":  _vi(t.get((proto.DEV_TOF_BACK, proto.PARAM_DISTANCE_MM))),
        }), qos=0)

    def _publish_lidar(self):
        """Publish the full 36-bin polar histogram (NaN bins → null)."""
        t = self._telem
        bins = [
            _vi(t.get((proto.DEV_LIDAR, proto.PARAM_LIDAR_BIN_0 + i)))
            for i in range(proto.LIDAR_BIN_COUNT)
        ]
        self._client.publish(TOPIC_LIDAR, json.dumps({
            "bins_mm":   bins,
            "bin_count": proto.LIDAR_BIN_COUNT,
            "bin_deg":   proto.LIDAR_BIN_DEG,
        }), qos=0)

    def _publish_servo(self, dev_id: int):
        """Publish a single servo's full telemetry snapshot."""
        joint_name = proto.DEV_TO_JOINT.get(dev_id)
        if joint_name is None:
            return
        j = self._joints.get(joint_name)
        t = self._telem
        pos_raw_v = _vi(t.get((dev_id, proto.PARAM_POSITION)))
        pos_raw = pos_raw_v if pos_raw_v is not None else 0
        pos_deg = _raw_to_deg(pos_raw, j["zero_position"]) if (j and pos_raw_v is not None) else None
        speed_raw_v = _vi(t.get((dev_id, proto.PARAM_VELOCITY)))
        speed_raw = speed_raw_v if speed_raw_v is not None else 0
        # ST3215 speed register is raw steps/s — convert to deg/s for UI readability
        speed_dps = (speed_raw / STEPS_PER_DEGREE) if speed_raw_v is not None else None
        voltage_raw = _v(t.get((dev_id, proto.PARAM_VOLTAGE)))
        self._client.publish(TOPIC_JOINTS_TELEM, json.dumps({
            "joint_name":   joint_name,
            "device_id":    dev_id,
            "position_raw": pos_raw_v,
            "position_deg": round(pos_deg, 2) if pos_deg is not None else None,
            "speed_raw":    speed_raw_v,
            "speed_dps":    round(speed_dps, 2) if speed_dps is not None else None,
            "load":         _v(t.get((dev_id, proto.PARAM_LOAD))),
            "temperature":  _v(t.get((dev_id, proto.PARAM_TEMPERATURE))),
            "voltage":      voltage_raw / 10.0 if voltage_raw is not None else None,  # ST3215 stores V*10
            "current":      _v(t.get((dev_id, proto.PARAM_CURRENT))),
        }), qos=0)

    def _publish_motor(self, dev_id: int):
        """Publish a single wheel motor's full telemetry snapshot."""
        name = MOTOR_DEV_TO_NAME.get(dev_id)
        if name is None:
            return
        t = self._telem
        self._client.publish(TOPIC_MOTORS, json.dumps({
            "motor":       name,
            "device_id":   dev_id,
            "velocity_rpm": _v(t.get((dev_id, proto.PARAM_VELOCITY))),
            "position":    _v(t.get((dev_id, proto.PARAM_POSITION))),
            "current_a":   _v(t.get((dev_id, proto.PARAM_CURRENT))),
            "temperature": _v(t.get((dev_id, proto.PARAM_TEMPERATURE))),
            "fault_code":  _vi(t.get((dev_id, proto.PARAM_FAULT_CODE))),
        }), qos=0)

    def _publish_battery(self):
        t = self._telem
        self._client.publish(TOPIC_BATTERY, json.dumps({
            "voltage": _v(t.get((proto.DEV_BATTERY, proto.PARAM_VOLTAGE))),
        }), qos=1, retain=True)

    def _publish_safety(self):
        t = self._telem
        bits = int(t.get((proto.DEV_SYSTEM, proto.PARAM_SAFETY_STATUS), 0))
        self._client.publish(TOPIC_SAFETY_STATUS,
                             json.dumps(proto.decode_safety_bits(bits)),
                             qos=1, retain=True)

    # ------------------------------------------------------------------
    #  Serial I/O
    # ------------------------------------------------------------------

    def _open_serial(self) -> bool:
        """Open the serial port.  Returns True on success."""
        try:
            self._ser = serial.Serial(
                port=self._serial_port,
                baudrate=self._serial_baud,
                timeout=0.1,
            )
            self._ser.reset_input_buffer()
            logger.info("Serial port opened: %s", self._serial_port)
            return True
        except serial.SerialException as e:
            logger.warning("Cannot open %s: %s", self._serial_port, e)
            self._ser = None
            return False

    def _close_serial(self):
        if self._ser and self._ser.is_open:
            try:
                self._ser.close()
            except Exception:
                pass
        self._ser = None

    def _send_to_teensy(self, raw_packet: bytes):
        """COBS-encode a RequestPacket and write to serial + 0x00 delimiter."""
        with self._ser_lock:
            if not self._ser or not self._ser.is_open:
                return
            try:
                encoded = proto.cobs_encode(raw_packet) + b"\x00"
                self._ser.write(encoded)
            except serial.SerialException as e:
                logger.warning("Serial write error: %s", e)

    def _serial_read_loop(self):
        """Background thread: read COBS frames from Teensy, decode as ResponsePackets."""
        buf = bytearray()

        while self._running:
            if not self._ser or not self._ser.is_open:
                time.sleep(RECONNECT_DELAY)
                if not self._open_serial():
                    continue
                buf.clear()

            try:
                chunk = self._ser.read(256)
            except serial.SerialException:
                logger.warning("Serial read error — reconnecting")
                self._close_serial()
                self._teensy_alive = False
                self._publish_bridge_state()
                continue

            if not chunk:
                continue

            buf.extend(chunk)

            # Extract complete COBS frames (delimited by 0x00)
            while b"\x00" in buf:
                idx = buf.index(0x00)
                frame_data = bytes(buf[:idx])
                del buf[:idx + 1]

                if not frame_data:
                    continue

                try:
                    decoded = proto.cobs_decode(frame_data)
                    rsp = proto.decode_response(decoded)
                    if rsp is None:
                        logger.debug("Invalid response packet (%d bytes)", len(decoded))
                        continue

                    if rsp["is_telemetry"]:
                        self._dispatch_telemetry(rsp)
                    else:
                        self._dispatch_response(rsp)
                except ValueError as e:
                    logger.debug("COBS frame error: %s", e)

    # ------------------------------------------------------------------
    #  State publishing
    # ------------------------------------------------------------------

    def _publish_bridge_state(self):
        state = {
            "status": "online",
            "serial_port": self._serial_port,
            "teensy_alive": self._teensy_alive,
            "joints": list(self._joints.keys()),
        }
        self._client.publish(TOPIC_BRIDGE_STATE, json.dumps(state), qos=1, retain=True)
        self._client.publish(TOPIC_JOINTS_STATE,
                             "ready" if self._teensy_alive else "waiting_for_teensy",
                             qos=1, retain=True)

    def _publish_error(self, message: str):
        self._client.publish(TOPIC_JOINTS_STATUS,
                             json.dumps({"type": "error", "message": message}), qos=0)

    # ------------------------------------------------------------------
    #  Main run loop
    # ------------------------------------------------------------------

    def run(self):
        self._running = True

        # Connect MQTT
        self._client.connect(self._mqtt_broker, self._mqtt_port)
        self._client.loop_start()

        # Start serial reader thread
        reader = threading.Thread(target=self._serial_read_loop, daemon=True)
        reader.start()

        # Open serial (non-blocking — reader will retry if it fails)
        self._open_serial()

        logger.info("Serial bridge running  serial=%s  mqtt=%s:%d",
                     self._serial_port, self._mqtt_broker, self._mqtt_port)

        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())

        while not stop.is_set():
            # Send heartbeat to Teensy
            self._send_to_teensy(proto.encode_heartbeat())
            self._client.publish(TOPIC_HB_BRIDGE, b"1", qos=0)

            # Check Teensy liveness
            if self._teensy_alive:
                elapsed = time.monotonic() - self._last_teensy_hb
                if elapsed > TEENSY_HB_TIMEOUT:
                    logger.warning("Teensy heartbeat lost (%.1fs)", elapsed)
                    self._teensy_alive = False
                    self._publish_bridge_state()

            stop.wait(HEARTBEAT_INTERVAL)

        # Shutdown
        logger.info("Shutting down...")
        self._running = False
        self._client.publish(TOPIC_BRIDGE_STATE,
                             json.dumps({"status": "offline"}), qos=1, retain=True)
        self._client.loop_stop()
        self._client.disconnect()
        self._close_serial()


# =====================================================================
#  CLI entry point
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="qBc_Network Serial Bridge")
    parser.add_argument("--serial-port", default=DEFAULT_SERIAL_PORT,
                        help=f"Teensy serial port (default: {DEFAULT_SERIAL_PORT})")
    parser.add_argument("--serial-baud", type=int, default=DEFAULT_SERIAL_BAUD,
                        help=f"Serial baud rate (default: {DEFAULT_SERIAL_BAUD})")
    parser.add_argument("--mqtt-broker", default=DEFAULT_MQTT_BROKER)
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT)
    parser.add_argument("--calibration", default=str(CALIBRATION_FILE),
                        help=f"Servo calibration JSON (default: {CALIBRATION_FILE})")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    bridge = SerialBridge(
        serial_port=args.serial_port,
        serial_baud=args.serial_baud,
        mqtt_broker=args.mqtt_broker,
        mqtt_port=args.mqtt_port,
        calibration_path=Path(args.calibration),
    )
    bridge.run()


if __name__ == "__main__":
    main()
