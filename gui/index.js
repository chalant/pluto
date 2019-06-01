"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const electron_1 = require("electron");
const url = require("url");
const path = require("path");
// import { AbstractMessageReader, MessageReader, DataCallback } from 'vscode-jsonrpc/lib/messageReader';
// import { ipcMain } from 'electron';
// class ElectronIPCMessageReader extends AbstractMessageReader implements MessageReader {
// 	public constructor() {
//         super();
// 		ipcMain.on('error', (error: any) => this.fireError(error));
//         ipcMain.on('close', () => this.fireClose());
// 	}
// 	public listen(callback: DataCallback): void {
// 		ipcMain.on('message', callback);
// 	}
// }
var state = 'stop';
let mainWindow;
electron_1.app.on('ready', function () {
    // const reader = new ElectronIPCMessageReader()
    mainWindow = new electron_1.BrowserWindow({
        height: 800,
        width: 1400,
        webPreferences: {
            nodeIntegration: true
        },
        backgroundColor: '#282C34'
    });
    mainWindow.loadURL(url.format({
        pathname: path.join(__dirname, 'mainwindow.html'),
        protocol: 'file:',
        slashes: true
    }));
    const mainMenuTemplate = [{
            label: 'File',
            submenu: [
                {
                    label: 'Open',
                    accelerator: 'CmdOrCtrl+O',
                    click: () => {
                        electron_1.dialog.showOpenDialog(mainWindow, {
                            //TODO: generalize path (including other OS)
                            defaultPath: '/home/yves/zipline-gui',
                            properties: ['openFile'],
                            filters: [{ name: 'script', extensions: ['py'] }]
                        }, (file_path) => {
                            if (file_path !== undefined) {
                                webContents.send('open-file', file_path[0]);
                            }
                        });
                    }
                },
                {
                    label: 'Save',
                    accelerator: 'CmdOrCtrl+S',
                    click: () => {
                        electron_1.dialog.showSaveDialog(mainWindow, {}, (path) => {
                            //TODO: generalize paths
                            webContents.send('save-file', '~/zipline-gui/' + path);
                        });
                    }
                }
            ],
        }
    ];
    const webContents = mainWindow.webContents;
    const mainMenu = electron_1.Menu.buildFromTemplate(mainMenuTemplate);
    if (process.platform !== 'darwin') {
        mainWindow.setMenu(mainMenu);
    }
    else {
        electron_1.Menu.setApplicationMenu(mainMenu);
    }
    electron_1.ipcMain.on('output:resume', (e, args) => {
        //resumes the current playing session...
        state = 'playing';
        console.log('Resuming...');
        webContents.send('main:playing');
    });
    electron_1.ipcMain.on('output:play', (e, args) => {
        console.log('Playing...');
        state = 'playing';
        const rand = Math.random() >= 0.2;
        if (rand) {
            webContents.send('main:playing');
            //requests the current file...
            webContents.send('get-file');
        }
        else {
            webContents.send('error');
        }
    });
    electron_1.ipcMain.on('output:stop', (e, args) => {
        if (state !== 'stop') {
            console.log('Stopping...');
            state = 'stop';
            webContents.send('main:stopped');
        }
    });
    electron_1.ipcMain.on('output:pause', (e, args) => {
        //suspends the playing loop
        state = 'pause';
        console.log('Pausing');
        webContents.send('main:paused');
    });
    electron_1.ipcMain.on('editor:file', (e, args) => {
        console.log(args);
    });
    mainWindow.on('closed', () => electron_1.app.quit());
    mainWindow.on('maximize', (e) => webContents.send('maximize'));
    webContents.openDevTools({ mode: 'detach' });
});
//# sourceMappingURL=index.js.map