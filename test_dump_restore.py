import subprocess
import time
import sys
import os
import json

def run_test():
    db_file = "test_dump.db"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # Start server
    server_process = subprocess.Popen(["./db", db_file, "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)

    try:
        # 1. Insert initial data
        print("Inserting initial data...")
        subprocess.run(["python3", "-c", 
            "from py_driver import CDBDriver; "
            "db = CDBDriver(); "
            "db.connect('localhost', 8080); "
            "db.execute(\"INSERT INTO users VALUES (1, 'user1', 'email1')\"); "
            "print(db.execute(\"INSERT INTO orders VALUES (100, 1, 'apple')\")); "
            "db.close()"
        ], check=True)

        # 2. Dump to JSON
        print("Dumping to JSON...")
        dump_output = subprocess.check_output(["python3", "db_tool.py", "dump", "--format=json"]).decode()
        print("Dump Output:", dump_output)
        
        data = json.loads(dump_output)
        if len(data['users']) != 1 or data['users'][0]['username'] != 'user1':
            print("FAIL: JSON Dump verification failed")
            return False
            
        # 3. Restore from JSON (to same DB, will duplicate key error but that's expected, 
        # actually let's clear DB first? No easy way to clear without restart.
        # Let's restart server with new DB file for restore test)
        
    except Exception as e:
        print(f"Error during dump test: {e}")
        return False
    finally:
        server_process.terminate()
        server_process.wait()
        if os.path.exists(db_file):
            os.remove(db_file)

    # Test Restore
    restore_db_file = "test_restore.db"
    if os.path.exists(restore_db_file):
        os.remove(restore_db_file)
        
    server_process = subprocess.Popen(["./db", restore_db_file, "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    
    try:
        print("Restoring from JSON...")
        json_input = json.dumps({
            "users": [{"id": 2, "username": "user2", "email": "email2"}],
            "orders": [{"id": 200, "user_id": 2, "product_name": "banana"}]
        })
        
        p = subprocess.Popen(["python3", "db_tool.py", "restore", "--format=json"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = p.communicate(input=json_input.encode())
        print("Restore Output:", out.decode())
        
        # Verify data
        verify_output = subprocess.check_output(["python3", "-c", 
            "from py_driver import CDBDriver; "
            "db = CDBDriver(); "
            "db.connect('localhost', 8080); "
            "print(db.execute('SELECT * FROM users')); "
            "db.close()"
        ]).decode()
        
        if "(2, user2, email2)" not in verify_output:
            print("FAIL: Restore verification failed")
            return False
            
        print("Dump/Restore Tests Passed!")
        return True

    except Exception as e:
        print(f"Error during restore test: {e}")
        return False
    finally:
        server_process.terminate()
        server_process.wait()
        if os.path.exists(restore_db_file):
            os.remove(restore_db_file)

if __name__ == "__main__":
    if run_test():
        sys.exit(0)
    else:
        sys.exit(1)
