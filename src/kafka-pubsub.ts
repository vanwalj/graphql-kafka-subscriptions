import * as Logger from "bunyan";
import { PubSubEngine } from "graphql-subscriptions";
import * as Kafka from "node-rdkafka";
import { createChildLogger } from "./child-logger";
import { PubSubAsyncIterator } from "./pubsub-async-iterator";

export interface IKafkaOptions {
  topic: string;
  brokerList: string;
  logger?: Logger;
  groupId?: number;
}

export interface IKafkaProducer {
  write: (input: Buffer) => any;
}

export interface IKafkaTopic {
  readStream: any;
  writeStream: any;
}

const defaultLogger = Logger.createLogger({
  level: "info",
  name: "pubsub",
  stream: process.stdout,
});

export class KafkaPubSub<T> implements PubSubEngine {
  protected producer: any;
  protected consumer: any;
  protected options: IKafkaOptions;
  protected subscriptionMap: Array<[string, (message: T) => void]>;
  protected channelSubscriptions: { [channel: string]: number[] };
  protected logger: Logger;

  constructor(options: IKafkaOptions) {
    this.options = options;
    this.subscriptionMap = [];
    this.channelSubscriptions = {};
    this.consumer = this.createConsumer(this.options.topic);
    this.logger = createChildLogger(
      this.options.logger || defaultLogger,
      "KafkaPubSub",
    );
  }

  public publish(channel: string, payload: T) {
    // only create producer if we actually publish something
    this.producer = this.producer || this.createProducer(this.options.topic);
    return this.producer.write(
      new Buffer(JSON.stringify({ channel, payload })),
    );
  }

  public async subscribe(channel: string, onMessage: () => void, options?: {}) {
    const index = this.subscriptionMap.length;
    this.subscriptionMap.push([channel, onMessage]);
    this.channelSubscriptions[channel] = [
      ...(this.channelSubscriptions[channel] || []),
      index,
    ];
    return index;
  }

  public unsubscribe(index: number) {
    const [channel] = this.subscriptionMap[index];
    this.channelSubscriptions[channel] = this.channelSubscriptions[
      channel
    ].filter(subId => subId !== index);
  }

  public asyncIterator(triggers: string | string[]) {
    return new PubSubAsyncIterator(this, triggers);
  }

  private onMessage(channel: string, message: T) {
    const subscriptions = this.channelSubscriptions[channel];
    if (!subscriptions) {
      return;
    } // no subscribers, don't publish msg
    for (const subId of subscriptions) {
      const [cnl, listener] = this.subscriptionMap[subId];
      listener(message);
    }
  }

  private createProducer(topic: string) {
    const producer = Kafka.createWriteStream(
      {
        "metadata.broker.list": this.options.brokerList,
      },
      {},
      { topic },
    );
    producer.on("error", err => {
      this.logger.error(err, "Error in our kafka stream");
    });
    return producer;
  }

  private createConsumer(topic: string) {
    // Create a group for each instance. The consumer will receive all messages from the topic
    const groupId = this.options.groupId || Math.ceil(Math.random() * 9999);
    const consumer = Kafka.createReadStream(
      {
        "group.id": `kafka-group-${groupId}`,
        "metadata.broker.list": this.options.brokerList,
      },
      {},
      {
        topics: [topic],
      },
    );
    consumer.on("data", message => {
      const parsedMessage: { channel: string; payload: T } = JSON.parse(
        message.value.toString(),
      );

      // Using channel abstraction
      const { channel, payload } = parsedMessage;
      this.onMessage(parsedMessage.channel, payload);
    });
    return consumer;
  }
}
