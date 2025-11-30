# 🚀 C-Based SQL Database Engine

> A high-performance, lightweight SQL database engine written from scratch in C.
> Featuring a B-Tree storage engine, TCP server mode, and full ACID transaction support.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Language](https://img.shields.io/badge/language-C-orange.svg)
![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)

## 🌟 Overview

This project is a fully functional SQL database built to understand the internals of database systems. It implements a complete **B-Tree storage engine**, a **Virtual Machine** for query execution, and a **TCP Server** for remote access. It supports standard ANSI SQL syntax, including complex operations like **Joins**, **Subqueries**, and **Transactions**.

Whether you are learning about database internals or need a lightweight embedded DB, this project demonstrates core concepts in a clean, understandable codebase.

## ✨ Key Features

*   **💾 B-Tree Storage Engine**: Efficient disk-based storage using B-Trees for fast lookups and range scans.
*   **⚡ TCP Network Server**: Run as a standalone server listening on port 8080.
*   **🔌 Client Drivers**: Includes native drivers for **Python** and **Node.js**.
*   **📝 ANSI SQL Support**:
    *   `INSERT INTO table VALUES (...)`
    *   `SELECT * FROM table WHERE ...`
    *   `DELETE FROM table WHERE ...`
*   **🔗 Advanced Queries**: Supports **Nested Loop Joins** and **Subqueries** (`INSERT INTO ... SELECT ...`).
*   **🛡️ ACID Transactions**: Full support for `BEGIN`, `COMMIT`, and `ROLLBACK` with deferred persistence.
*   **🔍 Secondary Indexes**: Fast lookups on non-primary key columns (e.g., username).
*   **🖥️ Interactive REPL**: Built-in command-line interface for direct interaction.

## 🚀 Getting Started

### Prerequisites
*   GCC or Clang compiler
*   Make

### Build
```bash
make
```

### Run (REPL Mode)
Start the database in interactive mode:
```bash
./db my.db
```
```sql
db > INSERT INTO users VALUES (1, 'gorani', 'gorani@example.com');
db > SELECT * FROM users;
(1, gorani, gorani@example.com)
```

### Run (Server Mode)
Start the database as a TCP server:
```bash
./db my.db --server
```
The server will listen on `localhost:8080`.

## 📦 Client Drivers

Connect to your database from your favorite language!

### 🐍 Python Driver
```python
from py_driver import CDBDriver

db = CDBDriver()
db.connect('localhost', 8080)

db.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')")
print(db.execute("SELECT * FROM users WHERE id = 1"))

db.close()
```

### 🟢 Node.js Driver
```javascript
const CDBDriver = require('./node_driver');

async function run() {
    const db = new CDBDriver();
    await db.connect('localhost', 8080);
    
    console.log(await db.execute("SELECT * FROM users"));
    db.close();
}
run();
```

## 🏗️ Architecture

*   **Tokenizer & Parser**: Converts SQL text into an internal Abstract Syntax Tree (AST).
*   **Code Generator**: Compiles AST into bytecode instructions for the VM.
*   **Virtual Machine (VM)**: Executes bytecode, managing control flow and data manipulation.
*   **B-Tree**: The core data structure. Internal nodes contain keys and child pointers; Leaf nodes contain keys and values (rows).
*   **Pager**: Manages raw file I/O, caching pages in memory (Buffer Pool).

## 🤝 Contributing

Contributions are welcome! Feel free to submit a Pull Request.

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
