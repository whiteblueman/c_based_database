const CDBDriver = require('./node_driver');
const { spawn } = require('child_process');
const fs = require('fs');

const dbFile = 'test_node_driver.db';
if (fs.existsSync(dbFile)) {
    fs.unlinkSync(dbFile);
}

// Start server
const server = spawn('./db', [dbFile, '--server']);
let serverStarted = false;

server.stdout.on('data', (data) => {
    // console.log(`Server stdout: ${data}`);
    if (data.toString().includes('Server listening')) {
        serverStarted = true;
    }
});

server.stderr.on('data', (data) => {
    console.error(`Server stderr: ${data}`);
});

async function runTest() {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 1000));

    const db = new CDBDriver();
    try {
        await db.connect('localhost', 8080);

        console.log("Testing Insert...");
        let resp = await db.execute("INSERT INTO users VALUES (1, 'user1', 'email1')");
        if (resp !== "Executed.") {
            console.error(`FAIL: Insert failed, got '${resp}'`);
            process.exit(1);
        }

        console.log("Testing Select...");
        resp = await db.execute("SELECT * FROM users WHERE id = 1");
        if (!resp.includes("(1, user1, email1)")) {
            console.error(`FAIL: Select failed, got '${resp}'`);
            process.exit(1);
        }

        db.close();
        console.log("Node.js Driver Tests Passed!");
    } catch (err) {
        console.error("Error:", err);
        process.exit(1);
    } finally {
        server.kill();
        if (fs.existsSync(dbFile)) {
            fs.unlinkSync(dbFile);
        }
    }
}

runTest();
