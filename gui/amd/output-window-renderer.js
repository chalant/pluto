const { ipcRenderer } = require('electron')

document.getElementById('quit').onclick = () => {
    console.log('Quitting')
    ipcRenderer.send('output:quit')
}