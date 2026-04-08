# qBc_Network

Local MQTT Broker service for the qB-Companion robot.

![Python](https://img.shields.io/badge/Python-3.13-blue) ![MQTT](https://img.shields.io/badge/MQTT-Mosquitto-orange)

## Overview

The `qBc_Network` module acts as the central communication nervous system for the robot. It wraps the execution of a local Eclipse Mosquitto MQTT broker inside a Python script for easier lifecycle management by the `qBc_Launcher`.

## Usage

```bash
# Setup
cd qBc_Network
python3 -m venv .venv
source .venv/bin/activate

# Run
python3 main.py
```

## How It Works

1. The script checks if the default MQTT port (`1883`) is already in use.
2. If it is in use, it attempts to kill any existing `mosquitto` processes to ensure a clean start.
3. It spawns `mosquitto -c local_broker.conf` as a subprocess.
4. The Python script stays alive, monitoring the broker process.
5. On `Ctrl+C` or termination signal, it cleanly shuts down the broker process.

## Configuration

The Mosquitto broker is configured using `local_broker.conf`. It typically binds to `localhost:1883` to allow all `qBc_*` services running on the same Raspberry Pi to communicate via publish/subscribe messaging.
