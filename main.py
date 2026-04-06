import subprocess
import time
import sys

def main():
    # The command to start Mosquitto, using the default configuration file
    command = ["mosquitto", "-c", "local_broker.conf"]
    
    print("Starting Mosquitto MQTT broker via Python...")
    
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
                break
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