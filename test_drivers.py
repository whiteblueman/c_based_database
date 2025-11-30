from py_driver import CDBDriver
import subprocess
import time
import sys
import os

def run_test():
    # Start server
    db_file = "test_driver.db"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    server_process = subprocess.Popen(["./db", db_file, "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1) # Wait for server to start

    try:
        db = CDBDriver()
        db.connect('localhost', 8080)
        
        print("Testing Insert...")
        resp = db.execute("INSERT INTO users VALUES (1, 'user1', 'email1')")
        if resp != "Executed.":
            print(f"FAIL: Insert failed, got '{resp}'")
            return False
            
        print("Testing Select...")
        resp = db.execute("SELECT * FROM users WHERE id = 1")
        if "(1, user1, email1)" not in resp:
            print(f"FAIL: Select failed, got '{resp}'")
            return False
            
        db.close()
        print("Python Driver Tests Passed!")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        server_process.terminate()
        server_process.wait()
        stdout, stderr = server_process.communicate()
        print("Server Stdout:", stdout.decode())
        print("Server Stderr:", stderr.decode())
        if os.path.exists(db_file):
            os.remove(db_file)

if __name__ == "__main__":
    if run_test():
        sys.exit(0)
    else:
        sys.exit(1)
