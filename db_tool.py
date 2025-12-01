import argparse
import json
import sys
from py_driver import CDBDriver

def dump_db(host, port, format_type):
    db = CDBDriver()
    try:
        db.connect(host, port)
        
        # Get tables
        tables_resp = db.execute(".tables")
        if not tables_resp:
            print("Error: Could not retrieve tables", file=sys.stderr)
            return

        tables = tables_resp.split('\n')
        dump_data = {}

        for table in tables:
            if not table: continue
            
            # Select all data
            rows_resp = db.execute(f"SELECT * FROM {table}")
            rows = []
            if rows_resp and rows_resp != "Executed.":
                # Parse rows based on table type
                # Format: (1, user1, email1)
                lines = rows_resp.split('\n')
                for line in lines:
                    if not line: continue
                    # Remove parens
                    content = line.strip('()')
                    parts = [p.strip() for p in content.split(',')]
                    
                    if table == "users":
                        if len(parts) >= 3:
                            rows.append({
                                "id": int(parts[0]),
                                "username": parts[1],
                                "email": parts[2]
                            })
                    elif table == "orders":
                        if len(parts) >= 3:
                            rows.append({
                                "id": int(parts[0]),
                                "user_id": int(parts[1]),
                                "product_name": parts[2]
                            })
            
            dump_data[table] = rows

        if format_type == 'json':
            print(json.dumps(dump_data, indent=2))
        elif format_type == 'sql':
            for table, rows in dump_data.items():
                for row in rows:
                    if table == "users":
                        print(f"INSERT INTO users VALUES ({row['id']}, '{row['username']}', '{row['email']}');")
                    elif table == "orders":
                        print(f"INSERT INTO orders VALUES ({row['id']}, {row['user_id']}, '{row['product_name']}');")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        db.close()

def restore_db(host, port, format_type):
    db = CDBDriver()
    try:
        db.connect(host, port)
        
        input_data = sys.stdin.read()
        
        if format_type == 'json':
            data = json.loads(input_data)
            for table, rows in data.items():
                for row in rows:
                    if table == "users":
                        sql = f"INSERT INTO users VALUES ({row['id']}, '{row['username']}', '{row['email']}')"
                        print(db.execute(sql))
                    elif table == "orders":
                        sql = f"INSERT INTO orders VALUES ({row['id']}, {row['user_id']}, '{row['product_name']}')"
                        print(db.execute(sql))
                        
        elif format_type == 'sql':
            commands = input_data.split(';')
            for cmd in commands:
                if cmd.strip():
                    print(db.execute(cmd.strip()))

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='C_DB Dump/Restore Tool')
    subparsers = parser.add_subparsers(dest='command', required=True)

    dump_parser = subparsers.add_parser('dump', help='Dump database to stdout')
    dump_parser.add_argument('--host', default='localhost', help='Database host')
    dump_parser.add_argument('--port', type=int, default=8080, help='Database port')
    dump_parser.add_argument('--format', choices=['json', 'sql'], default='json', help='Output format')

    restore_parser = subparsers.add_parser('restore', help='Restore database from stdin')
    restore_parser.add_argument('--host', default='localhost', help='Database host')
    restore_parser.add_argument('--port', type=int, default=8080, help='Database port')
    restore_parser.add_argument('--format', choices=['json', 'sql'], default='json', help='Input format')

    args = parser.parse_args()

    if args.command == 'dump':
        dump_db(args.host, args.port, args.format)
    elif args.command == 'restore':
        restore_db(args.host, args.port, args.format)
