import "dotenv/config";
import { redisClient as redisClientGlobal } from "./db/redis/index.js";
import { HashSet } from "js-sdsl";
import { sendMessageOnWebSocket } from "./ws/utils/messaging.js";
import WebSocket from "ws";
import type { RedisClientType } from "redis";

type SUBSCRIBED_EVENT = "depth_update_sol_usd" | "depth_update_btc_usd";

class EngineInterface {
  redisClient: RedisClientType;

  eventSubscriptions: Record<SUBSCRIBED_EVENT, HashSet<WebSocket>> = {
    depth_update_btc_usd: new HashSet(),
    depth_update_sol_usd: new HashSet(),
  };

  setupEventSubscriptionHandling = async () => {
    console.log("setup event subsciption handling");
    await this.redisClient.connect();

    console.log("redis client connected ");

    await this.setupEngineUpdatesHandling(); // for now just doing depth updates

    console.log("REDIS SUBSCRIPTION HANDLIKNG SETUP");
  };

  constructor() {
    this.redisClient = redisClientGlobal.duplicate();
  }
  initialize = async () => {
    console.log("INITIALIZING REDIS SUBSCRIPTION HANDLING");
    await this.setupEventSubscriptionHandling();
  };

  handleDepthUpdateEvents = async () => {
    const streamsReadResponse = await this.redisClient.xReadGroup(
      process.env.REDIS_ENGINE_UPDATES_GROUP!,
      "worker1", // coz there is only one worker per group , so no .env needed
      [
        { id: "0", key: process.env.REDIS_ENGINE_UPDATES_STREAM_NAME! }, // is is where u wanna start reading from
      ],
      {
        BLOCK: 0,
        COUNT: 100,
      },
    );

    console.log("streamsReadResponse", streamsReadResponse);
    // {
    //   name: string;
    //   messages: {
    //       id: string;
    //       message: {
    //           [x: string]: string;
    //       };
    //   }[]

    for (let streamReadResponse of streamsReadResponse as any) {
      for (const { id, message } of streamReadResponse.messages) {
        let subscriptions =
          this.eventSubscriptions[streamReadResponse.name as SUBSCRIBED_EVENT];

        if (!subscriptions.empty()) {
          const {
            offset,
            data,
          }: {
            offset: number;
            data: { price: number; qty: number }[];
          } = message;

          subscriptions.forEach((ws) => {
            sendMessageOnWebSocket(ws, {
              payload: { offset, data },
              type: streamReadResponse.name as SUBSCRIBED_EVENT,
            });
          });
        }

        // ack redis for messagie
        await this.redisClient.xAck(
          streamReadResponse.name, // would be stream name
          process.env.REDIS_ENGINE_UPDATES_CONSUMER_GROUP_NAME!,
          id,
        );
      }
    }

    this.handleDepthUpdateEvents();
  };

  private async createConsumerGroup() {
    // create the consumer group  first
    try {
      await this.redisClient.xGroupCreate(
        process.env.REDIS_ENGINE_UPDATES_STREAM_NAME!, // in which stream u wanna make consumer group
        process.env.REDIS_ENGINE_UPDATES_CONSUMER_GROUP_NAME!, // consumer group name you wanna make
        "0", // from where in stream to start reading
        { MKSTREAM: true },
      );
    } catch (err: any) {
      if (!(err.message as string).includes("BUSYGROUP")) throw err; // means already exists
    }
  }
  private async routeStreamMessages() {
    // start reading all responses from straem
    const streamsReadResponse = await this.redisClient.xReadGroup(
      process.env.REDIS_ENGINE_UPDATES_GROUP!,
      "worker1", // coz there is only one worker per group , so no .env needed
      [
        { id: "0", key: process.env.REDIS_ENGINE_UPDATES_STREAM_NAME! }, // id is where u wanna start reading from
      ],
      {
        BLOCK: 0,
        COUNT: 100,
      },
    );

    for (let streamReadResponse of streamsReadResponse as any) {
      for (const { id, message } of streamReadResponse.messages) {
        let subscriptions =
          this.eventSubscriptions[streamReadResponse.name as SUBSCRIBED_EVENT];

        if (!subscriptions.empty()) {
          const {
            offset,
            data,
          }: {
            offset: number;
            data: { price: number; qty: number }[];
          } = message;

          subscriptions.forEach((ws) => {
            sendMessageOnWebSocket(ws, {
              payload: { offset, data },
              type: streamReadResponse.name as SUBSCRIBED_EVENT,
            });
          });
        }

        // ack redis for messagie
        await this.redisClient.xAck(
          streamReadResponse.name, // would be stream name
          process.env.REDIS_ENGINE_UPDATES_CONSUMER_GROUP_NAME!,
          id,
        );
      }
    }
  }

  setupEngineUpdatesHandling = async () => {
    await this.createConsumerGroup();
    await this.routeStreamMessages();
    await this.handleDepthUpdateEvents();
  };

  getEngineResponse = async (requestId: string) => {
    let res = await this.redisClient.blPop(`engine_response_${requestId}`, 0);
    if (res && res.element) {
      return JSON.parse(res.element);
    }
    throw new Error("ERROR IN GETTING ENGINE RESPONSE");
  };

  // returns request id of request to look for in response
  sendEngineRequest = async (type: string, payload: any): Promise<string> => {
    let id: string = crypto.randomUUID();

    await this.redisClient.rPush(
      "engine_request",
      JSON.stringify({ requestId: id, type, payload }),
    );

    return id;
  };

  getEngineResponseForRequest = async (type: string, payload: any) => {
    let id = await this.sendEngineRequest(type, payload);
    return await this.getEngineResponse(id);
  };
}

export default EngineInterface;
