const requirejs = require('requirejs')

requirejs.config({
    //Pass the top-level main.js/index.js require
    //function to requirejs so that node modules
    //are loaded relative to the top-level JS file.
    nodeRequire: require
});
(function() {
    const grpc = require('grpc') // this shall be a grpc client.
    const path = require('path');
    const amdLoader = require('./node_modules/monaco-editor/min/vs/loader.js');
    const amdRequire = amdLoader.require;
    const amdDefine = amdLoader.require.define;
    const { remote, ipcRenderer } = require('electron');
    const fs = require('fs');
    const {MainWindow} = require('./mainwindow.js');

    function replaceClass(classList, replace, replacement){
        classList.remove(replace)
        classList.add(replacement)
    }

    // document.getElementById('start').onclick = () => {
    //     const element = document.getElementById("start-icon")
    //     const classList = element.classList
    //     //TODO: encapsulate these as functions, so that they take arbitrary class names
    //     if (element.className === 'fas fa-play'){
    //         replaceClass(classList, 'fa-play', 'fa-pause')
    //         ipcRenderer.send('output:play')
    //     }
    //     else {
    //         replaceClass(classList, 'fa-pause', 'fa-play')
    //         ipcRenderer.send('output:pause')
    //     }
    
    // }
    // document.getElementById('stop').onclick = () => {
    //     ipcRenderer.send('output:stop')
    //     const element = document.getElementById("start-icon")
    //     replaceClass(element.classList, 'fa-pause', 'fa-play')
    // }
    // document.getElementById('quit').onclick = () => {
    //     ipcRenderer.send('output:quit')}

    function uriFromPath(_path) {
        var pathName = path.resolve(_path).replace(/\\/g, '/');
        if (pathName.length > 0 && pathName.charAt(0) !== '/') {
            pathName = '/' + pathName;
        }
        return encodeURI('file://' + pathName);
    }

    function load_file(path) {
        console.log('open file')
    }

    amdRequire.config({
        baseUrl: uriFromPath(path.join(__dirname, './node_modules/monaco-editor/min'))
    });
    // workaround monaco-css not understanding the environment
    self.module = undefined;

    amdRequire(['vs/editor/editor.main'], function() {
        const electronWindow = remote.getCurrentWindow()


        monaco.editor.defineTheme('myTheme', {
            base: 'vs-dark',
            inherit: true,
            colors: {
                "editor.background": '#282C34',
            },
            rules: [{
                token: '',
                background: '#282C34'
            }]
        });
        var editor = monaco.editor.create(document.getElementById('container'), {
            value: [
                'def initialize(context): ',"\t'''Initialization Function'''",
                '\tprint("Hello world!")'
            ].join('\n'),
            language: 'python',
            theme : "myTheme",
        });

        const mainWindow = new MainWindow(editor)

       //handle resize event
        electronWindow.on('maximize', () => mainWindow.resize(1))
        electronWindow.on('unmaximize', () => mainWindow.resize(1))
        electronWindow.on('resize', () => mainWindow.resize(1))
        
        //handle file read and write events...
        ipcRenderer.on('open-file', (e, path)=> {
            fs.readFile(path, 'utf-8', (err, data) => {
                console.log('Opening', path)
                editor.setModel(monaco.editor.createModel(data, 'python'))
            })
        })

        ipcRenderer.on('save-file', (e, path)=> {
            fs.writeFile(path + '.py', editor.getValue(), (e) => {

            })
        });

        ipcRenderer.on('upload-file', (e, path) => {
            console.log('uploading file', path)
        });

        ipcRenderer.on('get-file', (e, args) => {
            ipcRenderer.send('editor:file', editor.getValue())
        })

        ipcRenderer.on('error', (e, args) => {
            console.log("ERROR!!!")
        })
    });
})();