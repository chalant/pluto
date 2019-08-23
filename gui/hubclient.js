// var PROTO_PATH = __dirname.replace('gui', 'protos/hub.proto');
var grpc = require('grpc');
var protoLoader = require('@grpc/proto-loader');
// Suggested options for similarity to existing grpc.load behavior
var packageDefinition = protoLoader.loadSync('./protos/dev.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: ['protos']
});

var protoDescriptor = grpc.loadPackageDefinition(packageDefinition);

const dev = protoDescriptor.dev

export class DevClient {
    //todo: this class should have a reference to the editor => dynamically
    //displays the strategy in the editor.
    constructor(editor, url, credentials){
        this.stub = new dev.Dev(url, credentials)
        this.currentStr = null //holds the current strategy
        this.editor = editor
    }

    //todo: we can actually load all the strategies on the client side.... we don't need to push back a strategy to the hub
    //each time we want to create a new strategy or modify an existing strategy...
    viewList() {
        //displays a list of strategies (collected from the hub)
        var list = this.stub.List(protoDescriptor.ListRequest()) //request the list of strategies in the hub, and load there
        //names as 
        
        for (var i = 0; i < list; i++) {
            let div = document.createElement('div')
            div.innerHTML = 'blabla'
        }
    }

    //deploying puts the current branch on top of the branches stack
    deploy() {

    }


}