import subprocess
import time
import sys
import os
from py_driver import CDBDriver

def run_test():
    db_file = "test_schema.db"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # Start server
    server_process = subprocess.Popen(["./db", db_file, "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)

    try:
        db = CDBDriver()
        db.connect('localhost', 8080)
        
        print("Testing .schema command...")
        schema_output = db.execute(".schema")
        print("Schema Output:\n", schema_output)
        
        expected_users = "CREATE TABLE users"
        expected_orders = "CREATE TABLE orders"
        
        if expected_users in schema_output and expected_orders in schema_output:
            print("Schema Test Passed!")
            return True
        else:
            print("FAIL: Schema output incorrect")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        db.close()
        server_process.terminate()
        server_process.wait()
        if os.path.exists(db_file):
            os.remove(db_file)

if __name__ == "__main__":
    if run_test():
        sys.exit(0)
    else:
        sys.exit(1)
