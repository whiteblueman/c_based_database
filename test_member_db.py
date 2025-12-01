import socket
import time
import os

def connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 8083))
    return s

def send_command(s, cmd):
    s.sendall((cmd + '\n').encode())
    s.settimeout(2.0)
    data = ""
    try:
        while True:
            chunk = s.recv(4096).decode()
            if not chunk:
                break
            data += chunk
            if data.endswith('\ndb > '): # Assuming prompt ends response
                 break
            # For SELECT, we might get many lines.
            # If we don't know the end, we rely on timeout or prompt.
            # The server prints "db > " at the end of loop in main.c?
            # Actually main.c prints prompt.
            # Let's just read with timeout for now or check for prompt.
            if "db > " in data:
                break
    except socket.timeout:
        pass
    return data

def run_test():
    if os.path.exists("member.db"):
        os.remove("member.db")

    # Start server (assuming it's running or we run it manually)
    # For this test script, we assume server is running on port 5432
    # But usually we might want to start it. 
    # Let's assume the user/agent starts it separately or we use existing running server.
    # If we want to be self-contained, we should start it.
    
    import subprocess
    # server_process = subprocess.Popen(["./db", "member.db", "--server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    server_process = None
    
    # Retry connection
    s = None
    for i in range(10):
        try:
            time.sleep(0.5)
            s = connect()
            break
        except ConnectionRefusedError:
            print(f"Connection refused, retrying ({i+1}/10)...")
            continue
    
    if s is None:
        print("Failed to connect to server.")
        if server_process and server_process.poll() is not None:
            print("Server exited prematurely.")
            out, err = server_process.communicate()
            print("Server Stdout:", out.decode())
            print("Server Stderr:", err.decode())
        if server_process:
            server_process.terminate()
        return

    try:
        # Create Table
        print("Creating table 'members'...")
        create_cmd = "create table members (id int, name varchar, age int, class varchar, region varchar, nation varchar, level int)"
        res = send_command(s, create_cmd)
        print(f"Create result: {res.strip()}")
        
        # Insert 100 rows
        print("Inserting 10 rows...")
        for i in range(10):
            username = f"User{i}"
            email = f"user{i}@example.com"
            age = 20 + i
            cls = f"Class{i % 5}"
            region = f"Region{i % 3}"
            nation = f"Nation{i % 2}"
            level = i
            
            insert_cmd = f"insert into members values ({i}, '{username}', {age}, '{cls}', '{region}', '{nation}', {level})"
            response = send_command(s, insert_cmd)
            # print(f"Insert {i}: {response}")
            if "Executed" not in response:
                print(f"Failed to insert row {i}: {response.strip()}")
                break
        
        # Select all
        print("Selecting all rows...")
        select_cmd = "select * from members"
        res = send_command(s, select_cmd)
        lines = res.strip().split('\n')
        # Filter out empty lines
        lines = [l for l in lines if l.strip()]
        print(f"Retrieved {len(lines)} lines (excluding prompt)")
        
        # Verify count (last line might be prompt)
        data_lines = [l for l in lines if l.startswith('(')]
        print(f"Data rows count: {len(data_lines)}")
        
        if len(data_lines) == 10:
            print("SUCCESS: 10 rows inserted and retrieved.")
        else:
            print("FAILURE: Row count mismatch.")
            print("Sample output:", data_lines[:5])

        s.close()
    except Exception as e:
        print(f"Test failed with exception: {e}")
    finally:
        if server_process:
            if server_process.poll() is None:
                server_process.terminate()
            
            out, err = server_process.communicate()
            print("Server Stdout:", out.decode())
            print("Server Stderr:", err.decode())

if __name__ == "__main__":
    run_test()
