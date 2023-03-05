"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const client_lambda_1 = require("@aws-sdk/client-lambda");
class OfflineSQSInvokePlugin {
    constructor(serverless, options, logging) {
        this.serverless = serverless;
        this.options = options;
        this.logging = logging;
        this.queueHandlers = {};
        this.hooks = {
            'offline:start:init': () => this.start()
        };
        this.server = (0, express_1.default)();
        this.lambdaClient = new client_lambda_1.LambdaClient({
            region: 'localhost',
            endpoint: 'http://localhost:3002',
            credentials: {
                accessKeyId: 'key',
                secretAccessKey: 'secret'
            }
        });
    }
    start() {
        var _a, _b, _c;
        this.logging.log.notice('Starting Offline SQS invoke');
        this.startHttp();
        let resources = (_a = this.serverless.service.resources) === null || _a === void 0 ? void 0 : _a.Resources;
        if (!resources) {
            return;
        }
        for (let [name, resource] of Object.entries(resources)) {
            if (resource.Type != 'AWS::SQS::Queue') {
                continue;
            }
            if (!((_b = resource.Properties) === null || _b === void 0 ? void 0 : _b.hasOwnProperty('QueueName'))) {
                this.logging.log.error(`Queue '${name}' is missing QueueName property`);
                continue;
            }
            let queueName = resource.Properties.QueueName;
            if (this.queueHandlers.hasOwnProperty(queueName)) {
                this.logging.log.warning(`Found multiple queues with name: '${queueName}'. Only using first definition.`);
                continue;
            }
            this.queueHandlers[queueName] = null;
        }
        let functions = this.serverless.service.functions;
        for (let [name, definition] of Object.entries(functions)) {
            if (definition.name == undefined) {
                continue;
            }
            for (let event of definition.events) {
                if (!event.hasOwnProperty('sqs')) {
                    continue;
                }
                let sqs = event.sqs;
                let queueName;
                if (typeof sqs.arn == 'string') {
                    queueName = sqs.arn.split(':').at(-1);
                }
                else if (sqs.arn.hasOwnProperty('Fn::GetAtt') && sqs.arn['Fn::GetAtt'].length == 2 && sqs.arn['Fn::GetAtt'][1] == 'Arn') {
                    let resourceName = sqs.arn['Fn::GetAtt'][0];
                    let resource = resources[resourceName];
                    if (!resource) {
                        this.logging.log.error(`Unknown resource: ${resourceName}`);
                        continue;
                    }
                    if (!((_c = resource.Properties) === null || _c === void 0 ? void 0 : _c.hasOwnProperty('QueueName'))) {
                        this.logging.log.error(`Resource '${resourceName}' is missing QueueName property`);
                        continue;
                    }
                    queueName = resource.Properties.QueueName;
                }
                else {
                    this.logging.log.error(`Unknown SQS ARN format: ${JSON.stringify(sqs.arn)}`);
                    continue;
                }
                if (!this.queueHandlers.hasOwnProperty(queueName)) {
                    this.logging.log.warning(`Unknown SQS queue '${queueName}' for function '${name}'`);
                    continue;
                }
                if (this.queueHandlers[queueName] != null) {
                    this.logging.log.warning(`Queue '${queueName}' already has handler configured. Only using ${this.queueHandlers[queueName]}.`);
                    continue;
                }
                this.queueHandlers[queueName] = definition.name;
            }
        }
        this.logging.log.notice('Queues available for local testing:');
        for (let [queue, handler] of Object.entries(this.queueHandlers)) {
            this.logging.log.notice(`           * ${queue}: ${handler}`);
        }
    }
    startHttp() {
        this.server.use(express_1.default.urlencoded({ extended: true }));
        this.server.post('/', (request, response) => __awaiter(this, void 0, void 0, function* () {
            var _a;
            let action = (_a = request.body) === null || _a === void 0 ? void 0 : _a.Action;
            if (!action) {
                this.logging.log.warning(`Received SQS request without Action`);
                response.status(400).send('Missing Action query parameter');
                return;
            }
            if (action != 'SendMessage') {
                this.logging.log.warning(`Received unsupported SQS action: ${action}`);
                response.status(400).send('Only SendMessage actions are supported');
                return;
            }
            let success = yield this.onMessage(request.body);
            if (!success) {
                response.status(400).send();
                return;
            }
            response.set('Content-Type', 'text/xml');
            response.send('<SendMessageResponse></SendMessageResponse>');
        }));
        this.server.listen(3003, () => {
            this.logging.log.notice('Offline SQS invoke listening on http://localhost:3003');
        });
    }
    onMessage(body) {
        return __awaiter(this, void 0, void 0, function* () {
            let queueUrl = body.QueueUrl;
            if (!queueUrl) {
                this.logging.log.warning('Missing QueueURL in SQS payload');
                return false;
            }
            let messageBody = body.MessageBody;
            if (!messageBody) {
                this.logging.log.warning('Missing MessageBody in SQS payload');
                return false;
            }
            let queueName = queueUrl.split('/').at(-1);
            if (!queueName) {
                this.logging.log.warning(`Missing queue name in queue url: ${queueUrl}`);
                return false;
            }
            if (!this.queueHandlers.hasOwnProperty(queueName)) {
                this.logging.log.warning(`Unknown queue name: ${queueName}`);
                return false;
            }
            this.logging.log.notice(`Invoking function '${this.queueHandlers[queueName]}' from queue '${queueName}'`);
            let payload = {
                Records: [
                    {
                        'messageId': '00000000-0000-0000-0000-000000000000',
                        'receiptHandle': '',
                        'body': messageBody,
                        'attributes': {},
                        'messageAttributes': {},
                        'md5OfBody': '',
                        'eventSource': 'aws:sqs',
                        'eventSourceARN': `arn:aws:sqs:localhost:000000000000:${queueName}`,
                        'awsRegion': 'localhost'
                    }
                ]
            };
            let payloadString = JSON.stringify(payload);
            let command = new client_lambda_1.InvokeCommand({
                FunctionName: this.queueHandlers[queueName],
                Payload: new TextEncoder().encode(payloadString)
            });
            let response = yield this.lambdaClient.send(command);
            if (response.StatusCode != 200) {
                this.logging.log.error(`Error while invoking Lambda: ${response}`);
                return false;
            }
            return true;
        });
    }
}
module.exports = OfflineSQSInvokePlugin;
