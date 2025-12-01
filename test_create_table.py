import subprocess
import time
import sys
import os
from py_driver import CDBDriver

def run_test():
    db_file = "test_dynamic.db"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # Start server
    server_process = subprocess.Popen(["./db", db_file, "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)

    try:
        db = CDBDriver()
        db.connect('localhost', 8080)
        
        print("Creating table 'my_users'...")
        # Schema type 0 (User) is inferred because no "product_name"
        print(db.execute("create table my_users (id int, username varchar(32), email varchar(255))"))
        
        print("Inserting into 'my_users'...")
        print(db.execute("insert into my_users values (1, 'dynamic_user', 'dynamic@email.com')"))
        
        print("Selecting from 'my_users'...")
        result = db.execute("select * from my_users")
        print("Result:", result)
        
        if "(1, dynamic_user, dynamic@email.com)" in result:
            print("Dynamic Table Test Passed!")
            return True
        else:
            print("FAIL: Select result incorrect")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        db.close()
        server_process.terminate()
        try:
            outs, errs = server_process.communicate(timeout=2)
            print("Server Stdout:", outs.decode())
            print("Server Stderr:", errs.decode())
        except:
            pass
        if os.path.exists(db_file):
            os.remove(db_file)

if __name__ == "__main__":
    if run_test():
        sys.exit(0)
    else:
        sys.exit(1)
