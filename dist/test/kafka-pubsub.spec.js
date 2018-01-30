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
const index_1 = require("../index");
const mockWrite = jest.fn(msg => msg);
const mockProducer = jest.fn(() => ({
    write: mockWrite,
}));
const mockConsumer = jest.fn(() => undefined);
const topic = "test-topic";
const brokerList = "localhost:9092";
const pubsub = new index_1.KafkaPubSub({
    brokerList,
    topic,
});
describe("KafkaPubSub", () => {
    it("should create producer/consumers correctly", () => {
        const onMessage = jest.fn();
        const testChannel = "testChannel";
        expect(mockProducer).toBeCalledWith(topic);
        expect(mockConsumer).toBeCalledWith(topic);
    });
    it("should subscribe and publish messages correctly", () => __awaiter(this, void 0, void 0, function* () {
        const channel = "test-channel";
        const onMessage = jest.fn();
        const payload = {
            id: "test",
        };
        const subscription = yield pubsub.subscribe(channel, onMessage);
        pubsub.publish(channel, payload);
        expect(mockWrite).toBeCalled();
        expect(mockWrite).toBeCalledWith(new Buffer(JSON.stringify(payload)));
    }));
});
//# sourceMappingURL=kafka-pubsub.spec.js.map