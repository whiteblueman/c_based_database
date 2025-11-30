import socket
import time
import subprocess
import sys

def run_test():
    # Start server
    server_process = subprocess.Popen(["./db", "test_server.db", "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1) # Wait for server to start

    try:
        # Connect to server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 8080))
        
        # Helper to send and receive
        def send_cmd(cmd):
            s.sendall((cmd + "\n").encode())
            data = s.recv(4096).decode()
            return data.strip()

        # Test 1: Insert
        print("Testing Insert...")
        response = send_cmd("insert 1 user1 email1")
        if response != "Executed.":
            print(f"FAIL: Expected 'Executed.', got '{response}'")
            return False
            
        # Test 2: Select
        print("Testing Select...")
        response = send_cmd("select")
        if "(1, user1, email1)" not in response:
             print(f"FAIL: Expected '(1, user1, email1)', got '{response}'")
             return False

        # Test 3: Error Handling
        print("Testing Error...")
        response = send_cmd("insert -1 user2 email2")
        if "ID must be positive" not in response:
             print(f"FAIL: Expected 'ID must be positive', got '{response}'")
             return False

        print("All tests passed!")
        s.close()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        server_process.terminate()
        server_process.wait()
        subprocess.run(["rm", "test_server.db"])

if __name__ == "__main__":
    if run_test():
        sys.exit(0)
    else:
        sys.exit(1)
