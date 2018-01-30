"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const Logger = require("bunyan");
const Kafka = require("node-rdkafka");
const child_logger_1 = require("./child-logger");
const pubsub_async_iterator_1 = require("./pubsub-async-iterator");
const defaultLogger = Logger.createLogger({
    level: "info",
    name: "pubsub",
    stream: process.stdout,
});
class KafkaPubSub {
    constructor(options) {
        this.options = options;
        this.subscriptionMap = [];
        this.channelSubscriptions = {};
        this.consumer = this.createConsumer(this.options.topic);
        this.logger = child_logger_1.createChildLogger(this.options.logger || defaultLogger, "KafkaPubSub");
    }
    publish(channel, payload) {
        // only create producer if we actually publish something
        this.producer = this.producer || this.createProducer(this.options.topic);
        return this.producer.write(new Buffer(JSON.stringify({ channel, payload })));
    }
    subscribe(channel, onMessage, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const index = this.subscriptionMap.length;
            this.subscriptionMap.push([channel, onMessage]);
            this.channelSubscriptions[channel] = [
                ...(this.channelSubscriptions[channel] || []),
                index,
            ];
            return index;
        });
    }
    unsubscribe(index) {
        const [channel] = this.subscriptionMap[index];
        this.channelSubscriptions[channel] = this.channelSubscriptions[channel].filter(subId => subId !== index);
    }
    asyncIterator(triggers) {
        return new pubsub_async_iterator_1.PubSubAsyncIterator(this, triggers);
    }
    onMessage(channel, message) {
        const subscriptions = this.channelSubscriptions[channel];
        if (!subscriptions) {
            return;
        } // no subscribers, don't publish msg
        for (const subId of subscriptions) {
            const [cnl, listener] = this.subscriptionMap[subId];
            listener(message);
        }
    }
    createProducer(topic) {
        const producer = Kafka.createWriteStream({
            "metadata.broker.list": this.options.brokerList,
        }, {}, { topic });
        producer.on("error", err => {
            this.logger.error(err, "Error in our kafka stream");
        });
        return producer;
    }
    createConsumer(topic) {
        // Create a group for each instance. The consumer will receive all messages from the topic
        const groupId = this.options.groupId || Math.ceil(Math.random() * 9999);
        const consumer = Kafka.createReadStream({
            "group.id": `kafka-group-${groupId}`,
            "metadata.broker.list": this.options.brokerList,
        }, {}, {
            topics: [topic],
        });
        consumer.on("data", message => {
            const parsedMessage = JSON.parse(message.value.toString());
            // Using channel abstraction
            const { channel, payload } = parsedMessage;
            this.onMessage(parsedMessage.channel, payload);
        });
        return consumer;
    }
}
exports.KafkaPubSub = KafkaPubSub;
//# sourceMappingURL=kafka-pubsub.js.map