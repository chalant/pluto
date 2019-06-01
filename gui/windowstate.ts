import { ActivityStateTracker } from "./activitystate";
import { MainWindow } from "./mainwindow";

abstract class WindowState{
    private windowStateTracker : WindowStateTracker

    public constructor(windowStateTracker : WindowStateTracker) {
        this.windowStateTracker = windowStateTracker
    }

    public update(event : String) : WindowState {
        return this._update(event, this.windowStateTracker)
    }

    public handleAnimationEnd(event : AnimationEvent):void {
        var tracker = this.windowStateTracker
        this._handleAnimationEnd(event, tracker)
        tracker.mainWindow.stopAnimationLoop()
    }

    public handleAnimationStart(event : AnimationEvent){
        this.windowStateTracker.mainWindow.startAnimationLoop()
        this._handleAnimationStart(event, this.windowStateTracker)
    }

    protected _handleAnimationStart(event : AnimationEvent, windowStateTracker : WindowStateTracker) : void {

    }

    protected abstract _handleAnimationEnd(event : AnimationEvent, windowStateTracker : WindowStateTracker) : void

    protected abstract _update(event : String, windowStateTracker : WindowStateTracker) : WindowState

}


class InitialWindowState extends WindowState{
    titleBar : HTMLDivElement
    outputWindow : HTMLDivElement

    protected _handleAnimationEnd(event: AnimationEvent, tracker: WindowStateTracker){
        // tracker.body.removeChild(tracker.outputWindow)
        // tracker.body.removeChild(this.titleBar)

    }

    protected _update(event : String, tracker : WindowStateTracker) : WindowState{
        if (event === 'run'){
            const outputtitleBar = this.titleBar = document.createElement('div')
            //buttons
            // const play = tracker.playButton = document.createElement('button')
            const quit = tracker.quitButton = document.createElement('button')
            const shrink = tracker.shrinkIcon = document.createElement('button')
            
            //icons
            // const playIcon = tracker.playIcon = document.createElement('i')
            const quitIcon = tracker.quitIcon = document.createElement('i')
            const shrinkIcon = tracker.shrinkIcon = document.createElement('i')
            const body = tracker.body
            const output = tracker.outputWindow = document.createElement('div')
            
            //play
            // play.appendChild(playIcon)
            // play.id = "play"
            // playIcon.id = "play-icon"
            // playIcon.setAttribute('class', 'fas fa-play')

            //quit
            quit.appendChild(quitIcon)
            quit.id = "quit"
            quit.setAttribute('class' ,'output-buttons')
            quitIcon.id = "quit-icon"
            quitIcon.setAttribute('class', 'fas fa-times output-buttons')

            //shrink
            shrink.appendChild(shrinkIcon)
            shrink.setAttribute('class' ,'output-buttons')
            shrink.id = "shrink"
            shrinkIcon.id = "shrink-icon"
            shrinkIcon.setAttribute('class', 'fas fa-window-minimize')
            
            outputtitleBar.appendChild(quit)
            outputtitleBar.appendChild(shrink)
            
            outputtitleBar.id = 'output-titlebar'
            body.appendChild(outputtitleBar)
            body.appendChild(output)

            //register to button click events that affect the current state
            shrink.onclick = () => {
                tracker.currentState = tracker.currentState.update('shrink')
            }

            quit.onclick = () => {
                tracker.currentState = tracker.currentState.update('quit')
            }

            function animationEndHandler(ev : AnimationEvent) {
                tracker.currentState.handleAnimationEnd(ev)
            }

            function animationStartHandler(ev : AnimationEvent) {
                tracker.currentState.handleAnimationStart(ev)
            }
            
            const container = tracker.container
            container.addEventListener('animationend', animationEndHandler)
            container.addEventListener('animationstart' , animationStartHandler)
            return tracker.shrunkOuputWindow.update("run")

        }

        else if (event === 'quit') {
            //stop activity before quitting the window...
            tracker.body.removeChild(tracker.outputWindow)
            tracker.body.removeChild(this.titleBar)
            return this
        }

        else  if (event === 'shrink'){
            //nothing happens on the shrink event when in initial state...
            return this
        }

        else {
            return this
        }
    }

}

class StandbyOuputWindowState extends WindowState {
    protected _handleAnimationEnd(event : AnimationEvent, tracker: WindowStateTracker){
    }

    protected _update(event : String, tracker : WindowStateTracker){
        if (event === 'run'){
            return tracker.shrunkOuputWindow.update("run")
        }

        else if (event === 'quit'){
            tracker.activityStateTracker.update('stop')
            return this
        }
    }
}

class ExpandedOutputWindowState extends WindowState {
    protected _handleAnimationEnd(event : AnimationEvent, tracker : WindowStateTracker){
        tracker.shrinkIcon.classList.remove('fa-window-restore')
        tracker.shrinkIcon.classList.add('fa-window-minimize')
    }

    protected _inner_update(event : String, tracker : WindowStateTracker) : WindowState {
        const container = tracker.container
        if (event === 'shrink'){
            container.style.animation = "container-tran-expand 300ms ease-out"
            container.classList.remove("container-min-dims")
            container.classList.add("container-tran-dims")
            tracker.outputWindow.classList.replace('output-max-dims', 'output-min-dims')
            return tracker.shrunkOuputWindow
        }
        
        else if (event === 'quit'){
            container.style.animation = "container-expand 300ms ease-out"
            container.classList.remove("container-min-dims")
            container.classList.add("container-max-dims")
            tracker.outputWindow.classList.remove('output-max-dims')
            return tracker.standbyOutputWindowState.update('quit')
        }

        else if (event === 'run') {
            tracker.activityStateTracker.update('run')
            return this
        }

        else {
            return this
        }
    }

    protected _update(event: String, tracker: WindowStateTracker) : WindowState {
        return this._inner_update(event, tracker)
    }
}

class ShrunkOutputWindowState extends WindowState {
    protected _handleAnimationEnd(event : AnimationEvent, tracker : WindowStateTracker){
        tracker.shrinkIcon.classList.remove('fa-window-minimize')
        tracker.shrinkIcon.classList.add('fa-window-restore')

    }

    protected _inner_update(event:String, tracker: WindowStateTracker) : WindowState {
        const output = tracker.outputWindow
        const container = tracker.container

        if (event === 'shrink'){
            container.style.animation = "container-tran-shrink 300ms ease-out"
            container.classList.remove("container-tran-dims")
            container.classList.add("container-min-dims")
            tracker.outputWindow.classList.replace('output-min-dims', 'output-max-dims')
            return tracker.expandedOutputWindow
        }

        else if (event === 'run'){
            //creates and expands the output window
            output.id = 'output'

            container.style.animation = "container-shrink 300ms ease-out"
            container.classList.remove("container-max-dims")
            container.classList.add("container-min-dims")
            
            tracker.outputWindow.classList.add('output-max-dims')
            tracker.activityStateTracker.update("run")
            return tracker.expandedOutputWindow
            
        }

        else if (event === 'quit'){
            container.style.animation = "container-tran-max-expand 300ms ease-out"
            container.classList.remove("container-tran-dims")
            container.classList.add("container-max-dims")
            output.classList.remove('output-min-dims')
            return tracker.standbyOutputWindowState.update('quit')
        }

        else {
            return this
        }
    }
    protected _update(event:String, tracker: WindowStateTracker) : WindowState {
        return this._inner_update(event, tracker)
    }
}

export class WindowStateTracker{
    currentState : WindowState
    runButton : HTMLElement
    body : HTMLBodyElement
    container : HTMLDivElement
    shrinkButton : HTMLButtonElement
    quitButton : HTMLButtonElement
    playButton : HTMLButtonElement

    shrinkIcon : Element
    quitIcon : Element
    playIcon : Element

    titleBar : Element
    outputWindow : HTMLDivElement

    expandedOutputWindow : ExpandedOutputWindowState
    shrunkOuputWindow : ShrunkOutputWindowState
    initialWindowState : InitialWindowState
    standbyOutputWindowState : StandbyOuputWindowState

    readonly activityStateTracker : ActivityStateTracker
    readonly mainWindow : MainWindow

    public constructor(activityStateTracker : ActivityStateTracker , mainWindow : MainWindow){
        //states
        this.initialWindowState = new InitialWindowState(this);
        this.expandedOutputWindow = new ExpandedOutputWindowState(this)
        this.shrunkOuputWindow = new ShrunkOutputWindowState(this)
        this.standbyOutputWindowState =  new StandbyOuputWindowState(this)
        
        this.currentState = this.initialWindowState

        this.activityStateTracker = activityStateTracker

        this.mainWindow = mainWindow

        //TODO: should create the buttons etc. here...
        const runButton = this.runButton = document.getElementById('run')

        
        this.body = document.getElementsByTagName('body')[0]
        this.container =  document.getElementById('container') as HTMLDivElement

        runButton.onclick = () => {
            this.currentState = this.currentState.update("run")
        }
    }
}