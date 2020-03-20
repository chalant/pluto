"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const messageReader_1 = require("vscode-jsonrpc/lib/messageReader");
const electron_1 = require("electron");
class RemoteElectronIPCMessageReader extends messageReader_1.AbstractMessageReader {
    constructor() {
        super();
        electron_1.remote.ipcMain.on('error', (error) => this.fireError(error));
        electron_1.remote.ipcMain.on('close', () => this.fireClose());
    }
    listen(callback) {
        electron_1.remote.ipcMain.on('message', callback);
    }
}
exports.RemoteElectronIPCMessageReader = RemoteElectronIPCMessageReader;
//# sourceMappingURL=ipcreader.js.map