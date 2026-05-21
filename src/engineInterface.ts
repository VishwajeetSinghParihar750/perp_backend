import "dotenv/config";
import WebSocket from "ws";
import { redisClient as redisClientGlobal } from "./db/redis/index.js";
import type { RedisClientType } from "redis";

type SUBSCRIBED_EVENT = "depth.updated.sol_usd" | "depth.updated.btc_usd";

class EngineInterface {
  redisClient: RedisClientType;

  engineSubscriptions: Set<SUBSCRIBED_EVENT> = new Set();
  eventSubscriptions: Record<SUBSCRIBED_EVENT, Set<WebSocket>> = {
    "depth.updated.btc_usd": new Set(),
    "depth.updated.sol_usd": new Set(),
  };

  // saving resolve, reject functions of promise
  pendingRequests: Record<string, [(data: any) => void, (data: any) => void]> =
    {};

  async subscribeEvent(eventType: SUBSCRIBED_EVENT, ws: WebSocket) {
    let res = await this.getEngineResponseForRequest("subscribe_event", {
      event: eventType,
      stream: process.env.REDIS_ENGINE_RECEIVE_STREAM_NAME!,
    });

    if (res.type == "error") throw Error();

    this.eventSubscriptions[eventType].add(ws);
  }
  async unsubscribeEvent(eventType: SUBSCRIBED_EVENT, ws: WebSocket) {
    let res = await this.getEngineResponseForRequest("unsubscribe_event", {
      event: eventType,
      stream: process.env.REDIS_ENGINE_RECEIVE_STREAM_NAME!,
    });

    if (res.type == "error") throw Error();

    this.eventSubscriptions[eventType].delete(ws);
  }

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

  async handleEngineMessages(
    redisClient: RedisClientType,
    lastRedisMessageId = "$",
  ) {
    let xreadRes = await redisClient.xRead(
      [
        {
          id: lastRedisMessageId,
          key: process.env.REDIS_ENGINE_RECEIVE_STREAM_NAME!,
        },
      ],
      { BLOCK: 0, COUNT: 100 },
    );
    if (xreadRes)
      for (let streamReadResponse of xreadRes) {
        for (const { id, message } of streamReadResponse.messages) {
          // it has a request id , means it was personal
          let gotRequestId = "";
          try {
            let { type, payload, requestId } = JSON.parse(message.data!);
            gotRequestId = requestId;

            if (requestId) {
              console.log(type, payload, requestId);

              this.pendingRequests[requestId]?.[0]?.({ type, payload });
              delete this.pendingRequests[requestId];
            } else {
              // TODO : else broadcast one, subbed event maybe
            }
          } catch (error) {
            console.log("error in parsing engine message", error);
            this.pendingRequests[gotRequestId]?.[1]?.(error);
          }
          lastRedisMessageId = id;
        }
      }

    // here resolve the requests
    // maybe TODO :maybe even timeout the resolver after some minutes, reject after 5 min of waiting maybe

    this.handleEngineMessages(redisClient, lastRedisMessageId);
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
