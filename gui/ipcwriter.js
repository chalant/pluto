define(["require", "exports", "vscode-jsonrpc/lib/messageWriter", "electron"], function (require, exports, messageWriter_1, electron_1) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    class ElectronIPCMessageWriter extends messageWriter_1.AbstractMessageWriter {
        constructor() {
            super();
            this.errorCount = 0;
            this.queue = [];
            this.sending = false;
            electron_1.ipcRenderer.on('error', (error) => this.fireError(error));
            electron_1.ipcRenderer.on('close', () => this.fireClose);
        }
        write(msg) {
            if (!this.sending && this.queue.length === 0) {
                // See https://github.com/nodejs/node/issues/7657
                this.doWriteMessage(msg);
            }
            else {
                this.queue.push(msg);
            }
        }
        doWriteMessage(msg) {
            try {
                if (electron_1.ipcRenderer.send) {
                    this.sending = true;
                    electron_1.ipcRenderer.send(msg, undefined, undefined, (error) => {
                        this.sending = false;
                        if (error) {
                            this.errorCount++;
                            this.fireError(error, msg, this.errorCount);
                        }
                        else {
                            this.errorCount = 0;
                        }
                        if (this.queue.length > 0) {
                            this.doWriteMessage(this.queue.shift());
                        }
                    });
                }
            }
            catch (error) {
                this.errorCount++;
                this.fireError(error, msg, this.errorCount);
            }
        }
    }
    exports.ElectronIPCMessageWriter = ElectronIPCMessageWriter;
});
//# sourceMappingURL=ipcwriter.js.map