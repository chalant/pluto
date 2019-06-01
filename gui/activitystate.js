"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const electron_1 = require("electron");
class ActivityStateTracker {
    constructor(runIcon) {
        this.stoppedActivityState = this.currentActivityState = new StoppedActivityState(this);
        this.pausedActityState = new PausedActivityState(this);
        this.runningActivityState = new RunningActivityState(this);
        this.runIcon = runIcon;
        electron_1.ipcRenderer.on('main:playing', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:playing');
        });
        electron_1.ipcRenderer.on('main:stopped', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:stopped');
        });
        electron_1.ipcRenderer.on('main:paused', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:paused');
        });
    }
    update(event) {
        this.currentActivityState.send(event);
    }
    send(event) {
        electron_1.ipcRenderer.send(event);
    }
}
exports.ActivityStateTracker = ActivityStateTracker;
class ActivityState {
    constructor(activable) {
        this.activable = activable;
    }
    send(event) {
        this._send(event, this.activable);
    }
    nextState(event) {
        if (status !== 'error') {
            return this._nextState(event, this.activable);
        }
        else {
            return this;
        }
    }
}
class RunningActivityState extends ActivityState {
    _send(event, activable) {
        if (event === 'run') {
            //notify the server that it should pause...
            activable.send('output:pause');
        }
        else if (event === 'stop') {
            activable.send('output:stop');
        }
    }
    _nextState(event, activable) {
        activable.runIcon.classList.replace('fa-pause', 'fa-play');
        if (event === 'main:paused') {
            return activable.pausedActityState;
        }
        else if (event === 'main:stopped') {
            return activable.stoppedActivityState;
        }
    }
}
exports.RunningActivityState = RunningActivityState;
class PausedActivityState extends ActivityState {
    _send(event, activable) {
        if (event === 'run') {
            //notify the server to play from the current session...
            activable.send('output:resume');
        }
        else if (event === 'stop') {
            activable.send('output:stop');
        }
    }
    _nextState(event, activable) {
        if (event === 'main:playing') {
            activable.runIcon.classList.replace('fa-play', 'fa-pause');
            return activable.runningActivityState;
        }
        else if (event === 'main:stopped') {
            activable.runIcon.classList.replace('fa-pause', 'fa-play');
            return activable.stoppedActivityState;
        }
    }
}
exports.PausedActivityState = PausedActivityState;
class StoppedActivityState extends ActivityState {
    _send(event, activable) {
        if (event === 'run') {
            //notify the server that it should pause...
            //TODO: check if the the play was successful before changing state
            activable.send('output:play');
        }
    }
    _nextState(event, activable) {
        if (event === 'main:playing') {
            activable.runIcon.classList.replace('fa-play', 'fa-pause');
            return activable.runningActivityState;
        }
    }
}
exports.StoppedActivityState = StoppedActivityState;
//# sourceMappingURL=activitystate.js.map