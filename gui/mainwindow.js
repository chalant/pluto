"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const windowstate_1 = require("./windowstate");
const activitystate_1 = require("./activitystate");
class MainWindow {
    constructor(editor) {
        //TODO: should create the buttons etc. here...
        const stopButton = this.stopButton = document.getElementById('stop');
        const runIcon = this.runIcon = document.getElementById('run-icon');
        //trackers
        const tracker = this.activityStateTracker = new activitystate_1.ActivityStateTracker(runIcon);
        this.windowStateTracker = new windowstate_1.WindowStateTracker(tracker, this);
        this.editor = editor;
        stopButton.onclick = (e) => {
            tracker.update("stop");
        };
        this.frames = 0;
    }
    _resize() {
        window.requestAnimationFrame((now) => {
            this.editor.layout();
        });
    }
    resize(maxFrames) {
        var frame = 0;
        if (frame <= maxFrames) {
            window.requestAnimationFrame((now) => { this._resize(); });
        }
        else {
            frame = 0;
        }
    }
    stopAnimationLoop() {
        this.stopAnLoop = true;
    }
    startAnimationLoop() {
        if (!this.stopAnLoop) {
            this.frames += 1;
            this.editor.layout();
            window.requestAnimationFrame((now) => { this.startAnimationLoop(); });
        }
        else {
            this.frames = 0;
            this.stopAnLoop = false;
        }
    }
}
exports.MainWindow = MainWindow;
//# sourceMappingURL=mainwindow.js.map