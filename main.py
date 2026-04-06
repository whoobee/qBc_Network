import subprocess
import socket
import time
import sys

BROKER_PORT = 1883


def port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def main():
    # The command to start Mosquitto, using the default configuration file
    command = ["mosquitto", "-c", "local_broker.conf"]

    print("Starting Mosquitto MQTT broker via Python...")

    if port_in_use(BROKER_PORT):
        print(f"Port {BROKER_PORT} already in use. Killing existing mosquitto...")
        subprocess.run(["killall", "mosquitto"], capture_output=True)
        time.sleep(1)
        if port_in_use(BROKER_PORT):
            print(f"Port {BROKER_PORT} still in use after killing mosquitto. Aborting.")
            sys.exit(1)

    try:
        # Popen runs the process asynchronously
        broker_process = subprocess.Popen(command)
        print(f"Broker started with Process ID (PID): {broker_process.pid}")
        print("Press Ctrl+C to stop the broker.")

        # Keep the main Python script alive while the broker is running
        while True:
            # Check if the broker crashed or stopped unexpectedly
            if broker_process.poll() is not None:
                print("\nBroker exited unexpectedly!")
                sys.exit(1)
            time.sleep(1)

    except KeyboardInterrupt:
        # Catch the Ctrl+C signal to shut down gracefully
        print("\nInterrupt received. Stopping Mosquitto...")
        broker_process.terminate()
        broker_process.wait()
        print("Broker stopped successfully.")
        sys.exit(0)

if __name__ == "__main__":
    main()