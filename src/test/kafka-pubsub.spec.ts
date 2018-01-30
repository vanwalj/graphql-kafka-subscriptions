import { KafkaPubSub } from "../index";

const mockWrite = jest.fn(msg => msg);
const mockProducer = jest.fn(() => ({
  write: mockWrite,
}));
const mockConsumer = jest.fn(() => undefined);
const topic = "test-topic";
const brokerList = "localhost:9092";
const pubsub = new KafkaPubSub({
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
  it("should subscribe and publish messages correctly", async () => {
    const channel = "test-channel";
    const onMessage = jest.fn();
    const payload = {
      id: "test",
    };
    const subscription = await pubsub.subscribe(channel, onMessage);
    pubsub.publish(channel, payload);
    expect(mockWrite).toBeCalled();
    expect(mockWrite).toBeCalledWith(new Buffer(JSON.stringify(payload)));
  });
});
