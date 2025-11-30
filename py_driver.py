import socket

class CDBDriver:
    def __init__(self):
        self.sock = None

    def connect(self, host, port):
        """Establishes a connection to the database server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def execute(self, sql):
        """Sends a SQL command and returns the response."""
        if not self.sock:
            raise Exception("Not connected to database")
        
        self.sock.sendall((sql + "\n").encode())
        
        # Simple read: read until we get a response. 
        # In a real driver, we'd handle buffering and protocol framing.
        # Here we assume the server sends one response packet per request 
        # or we read a chunk.
        data = self.sock.recv(4096).decode()
        return data.strip()

    def close(self):
        """Closes the connection."""
        if self.sock:
            self.sock.close()
            self.sock = None
