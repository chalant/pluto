"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const electron_1 = require("electron");
electron_1.app.on('ready', function () {
    let mainWindow;
    mainWindow = new electron_1.BrowserWindow({ height: 800, width: 1000 });
    mainWindow.loadURL(__dirname);
});
//# sourceMappingURL=server.js.map