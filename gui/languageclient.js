// import { BaseLanguageClient, MessageTransports, LanguageClientOptions } from 'vscode-languageclient';
// import { RemoteElectronIPCMessageReader } from './ipcreader';
// import { ElectronIPCMessageWriter } from './ipcwriter';
// import { TypeDefinitionFeature } from 'vscode-languageclient/lib/typeDefinition';
// import { ColorProviderFeature } from 'vscode-languageclient/lib/colorProvider';
// import { ImplementationFeature } from 'vscode-languageclient/lib/implementation';
// import { WorkspaceFoldersFeature } from 'vscode-languageclient/lib/workspaceFolders'
// export class LanguageClient extends BaseLanguageClient {
//     constructor(id: string, name: string, clientOptions: LanguageClientOptions){
//         super(id, name, clientOptions);
//     }
//     protected createMessageTransports(encoding: string): Thenable<MessageTransports>{
//         return Promise.resolve(
//             {
//                 reader: new RemoteElectronIPCMessageReader(), 
//                 writer: new ElectronIPCMessageWriter()
//             })
//     }
//     protected registerBuiltinFeatures() {
// 		super.registerBuiltinFeatures();
// 		this.registerFeature(new TypeDefinitionFeature(this));
// 		this.registerFeature(new ImplementationFeature(this));
//         this.registerFeature(new ColorProviderFeature(this));
//         this.registerFeature(new WorkspaceFoldersFeature(this));
//     }
// }
//# sourceMappingURL=languageclient.js.map