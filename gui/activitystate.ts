import {ipcRenderer} from 'electron'

export class ActivityStateTracker {
    currentActivityState : ActivityState
    runningActivityState : RunningActivityState
    pausedActityState : PausedActivityState
    stoppedActivityState : StoppedActivityState

    runIcon : HTMLElement

    public constructor(runIcon : HTMLElement){
        this.stoppedActivityState = this.currentActivityState = new StoppedActivityState(this)
        this.pausedActityState = new PausedActivityState(this)
        this.runningActivityState = new RunningActivityState(this)
        this.runIcon = runIcon

        ipcRenderer.on('main:playing', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:playing')
        })
        
        ipcRenderer.on('main:stopped', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:stopped')
        })

        ipcRenderer.on('main:paused', (e, args) => {
            this.currentActivityState = this.currentActivityState.nextState('main:paused')
        })
     }

     public update(event : string){
         this.currentActivityState.send(event)
     }

     public send(event : string){
         ipcRenderer.send(event)
     }
}

abstract class ActivityState {
    private readonly activable : ActivityStateTracker
    public constructor(activable : ActivityStateTracker){
        this.activable = activable
    }
    public send(event : string) : void {
        this._send(event, this.activable)
    }

    public nextState(event: string){
        if (status !== 'error'){
            return this._nextState(event, this.activable)
        }

        else{
            return this
        }
    }

    protected abstract _nextState(event : string, tracker : ActivityStateTracker) : ActivityState

    protected abstract _send(event: string, activable : ActivityStateTracker) : void
}

export class RunningActivityState extends ActivityState{
    protected _send(event: string, activable : ActivityStateTracker) : void{
        if (event === 'run'){
            //notify the server that it should pause...
            activable.send('output:pause')
        }

        else if (event === 'stop'){
            activable.send('output:stop')
        }
    }

    protected _nextState(event : string, activable : ActivityStateTracker) : ActivityState{
        activable.runIcon.classList.replace('fa-pause', 'fa-play')
        if (event === 'main:paused'){
            return activable.pausedActityState
        }
        else if (event === 'main:stopped'){
            return activable.stoppedActivityState
        }
    }
}

export class PausedActivityState extends ActivityState{

    protected _send(event: string, activable : ActivityStateTracker):void{
        if (event === 'run'){
            //notify the server to play from the current session...
            activable.send('output:resume')
    }

        else if (event === 'stop'){
            activable.send('output:stop')
        }
    }

    protected _nextState(event : string, activable : ActivityStateTracker) : ActivityState{
        if (event === 'main:playing'){
            activable.runIcon.classList.replace('fa-play', 'fa-pause')
            return activable.runningActivityState
        }
        else if (event === 'main:stopped'){
            activable.runIcon.classList.replace('fa-pause', 'fa-play')
            return activable.stoppedActivityState
        }
    }
}

export class StoppedActivityState extends ActivityState{
    protected _send(event: string, activable : ActivityStateTracker) : void{
        if (event === 'run'){
            //notify the server that it should pause...
            //TODO: check if the the play was successful before changing state
            activable.send('output:play')
        }
    }

    protected _nextState(event : string, activable : ActivityStateTracker) : ActivityState{
        if (event === 'main:playing'){
            activable.runIcon.classList.replace('fa-play', 'fa-pause')
            return activable.runningActivityState
        }
    }
}