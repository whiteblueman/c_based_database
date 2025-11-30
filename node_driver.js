const net = require('net');

class CDBDriver {
    constructor() {
        this.client = new net.Socket();
        this.connected = false;
    }

    connect(host, port) {
        return new Promise((resolve, reject) => {
            this.client.connect(port, host, () => {
                this.connected = true;
                resolve();
            });
            this.client.on('error', (err) => {
                reject(err);
            });
        });
    }

    execute(sql) {
        return new Promise((resolve, reject) => {
            if (!this.connected) {
                reject(new Error("Not connected to database"));
                return;
            }

            this.client.write(sql + '\n');

            this.client.once('data', (data) => {
                resolve(data.toString().trim());
            });

            this.client.once('error', (err) => {
                reject(err);
            });
        });
    }

    close() {
        if (this.connected) {
            this.client.destroy();
            this.connected = false;
        }
    }
}

module.exports = CDBDriver;
