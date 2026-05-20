import "dotenv/config";
import { redisClient as redisClientGlobal } from "./db/redis/index.js";
import { HashSet } from "js-sdsl";
import type { RedisClientType } from "redis";

type SUBSCRIBED_EVENT = "depth_update_sol_usd" | "depth_update_btc_usd";

class EngineInterface {
  redisClient: RedisClientType;

  eventSubscriptions: Record<SUBSCRIBED_EVENT, HashSet<WebSocket>> = {
    depth_update_btc_usd: new HashSet(),
    depth_update_sol_usd: new HashSet(),
  };

  // saving resolve, reject functions of promise
  pendingRequests: Record<string, [(data: any) => void, (data: any) => void]> =
    {};

  private setupEventSubscriptionHandling = async () => {
    console.log("setup event subsciption handling");
    this.redisClient.on("error", (err) => {
      console.log("redis error : ", err);
    });

    await this.redisClient.connect();

    console.log("redis client connected ");

    let dupClient = this.redisClient.duplicate();
    await dupClient.connect();
    this.handleEngineMessages(dupClient); // for now just doing depth updates

    console.log("REDIS SUBSCRIPTION HANDLIKNG SETUP");
  };

  constructor() {
    this.redisClient = redisClientGlobal.duplicate();
  }

  initialize = async () => {
    console.log("INITIALIZING REDIS SUBSCRIPTION HANDLING");
    await this.setupEventSubscriptionHandling();
  };

  async handleEngineMessages(redisClient: RedisClientType) {
    let xreadRes = await redisClient.xRead(
      [{ id: "$", key: process.env.REDIS_ENGINE_RECEIVE_STREAM_NAME! }],
      { BLOCK: 0, COUNT: 100 },
    );

    console.log("xreadRes ", xreadRes);

    // for (let streamReadResponse of xreadRes as any) {
    //   for (const { id, message } of streamReadResponse.messages) {
    //     let subscriptions =
    //       this.eventSubscriptions[streamReadResponse.name as SUBSCRIBED_EVENT];

    //     if (!subscriptions.empty()) {
    //       const {
    //         offset,
    //         data,
    //       }: {
    //         offset: number;
    //         data: { price: number; qty: number }[];
    //       } = message;

    //       subscriptions.forEach((ws) => {
    //         sendMessageOnWebSocket(ws, {
    //           payload: { offset, data },
    //           type: streamReadResponse.name as SUBSCRIBED_EVENT,
    //         });
    //       });
    //     }

    //     // ack redis for messagie
    //     await this.redisClient.xAck(
    //       streamReadResponse.name, // would be stream name
    //       process.env.REDIS_ENGINE_UPDATES_CONSUMER_GROUP_NAME!,
    //       id,
    //     );
    //   }
    // }

    // here resolve the requests
    // maybe TODO :maybe even timeout the resolver after some minutes, reject after 5 min of waiting maybe

    this.handleEngineMessages(redisClient);
  }

  private sendEngineRequest = async (
    requestId: string,
    type: string,
    payload: any,
  ) => {
    let res = await this.redisClient.xAdd(
      process.env.REDIS_ENGINE_SEND_STREAM_NAME!,
      "*",
      {
        data: JSON.stringify({
          requestId,
          stream: process.env.REDIS_ENGINE_RECEIVE_STREAM_NAME!,
          type,
          payload,
        }),
      },
    );
    console.log("res", res);
  };

  getEngineResponseForRequest = async (type: string, payload: any) => {
    let requestId = crypto.randomUUID();
    // wait for it before you send request

    let promiseToReturn = new Promise<{ type: string; payload: any }>(
      (res, rej) => {
        this.pendingRequests[requestId] = [res, rej];
      },
    );

    await this.sendEngineRequest(requestId, type, payload);
    return promiseToReturn;
  };
}

export default EngineInterface;
