import {WindowStateTracker} from './windowstate'
import {ActivityStateTracker} from './activitystate'
import hub = require('./hubclient');

export class MainWindow {
    private windowStateTracker : WindowStateTracker
    private activityStateTracker : ActivityStateTracker
    private client : hub.HubClient

    runButton : HTMLButtonElement
    stopButton : HTMLButtonElement

    runIcon : HTMLElement

    private editor : any
    private stopAnLoop : boolean
    frames : number

    public constructor(editor){
        //TODO: should create the buttons etc. here...
        const stopButton = this.stopButton = document.getElementById('stop') as (HTMLButtonElement)
        const runIcon = this.runIcon = document.getElementById('run-icon')

        //trackers
        const tracker = this.activityStateTracker = new ActivityStateTracker(runIcon)
        this.windowStateTracker = new WindowStateTracker(tracker, this)
        this.editor = editor

        stopButton.onclick = (e) => {
            tracker.update("stop")
        }

        this.frames = 0
        // this.client = new hub.HubClient(editor)
    }

    private _resize(){
        window.requestAnimationFrame((now) => {
            this.editor.layout()
        })
    }

    public resize(maxFrames : number) {
        var frame = 0 
        if (frame <= maxFrames){
            window.requestAnimationFrame((now) => { this._resize()})}
        else{
            frame = 0
        }
    }

    public stopAnimationLoop(){
        this.stopAnLoop = true
    }

    public startAnimationLoop(){
        if (!this.stopAnLoop){
            this.frames += 1
            this.editor.layout()
            window.requestAnimationFrame((now) => { this.startAnimationLoop()})
        }
        else {
            this.frames = 0
            this.stopAnLoop = false
        }
    }
}