/// <reference types="bunyan" />
/// <reference types="node" />
import * as Logger from "bunyan";
import { PubSubEngine } from "graphql-subscriptions";
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
export declare class KafkaPubSub<T> implements PubSubEngine {
    protected producer: any;
    protected consumer: any;
    protected options: IKafkaOptions;
    protected subscriptionMap: Array<[string, (message: T) => void]>;
    protected channelSubscriptions: {
        [channel: string]: number[];
    };
    protected logger: Logger;
    constructor(options: IKafkaOptions);
    publish(channel: string, payload: T): any;
    subscribe(channel: string, onMessage: () => void, options?: {}): Promise<number>;
    unsubscribe(index: number): void;
    asyncIterator(triggers: string | string[]): any;
    private onMessage(channel, message);
    private createProducer(topic);
    private createConsumer(topic);
}
