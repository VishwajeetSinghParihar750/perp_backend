import type { ENGINE_EVENT } from "./event.js";
const CURRENCY_SYMBOL_ARRAY = ["USD", "SOLUSD", "ETHUSD", "BTCUSD"] as const;
type CURRENCY_SYMBOL = (typeof CURRENCY_SYMBOL_ARRAY)[number];

type LIQUIDATION_EVENT =
  | "markprice.updated"
  | "liquidation.started"
  | "liquidation.completed";
interface markPriceUpdated extends ENGINE_EVENT {
  type: "markprice.updated";
  data: any;
}
interface liquidationStarted extends ENGINE_EVENT {
  type: "liquidation.started";
  data: {
    userId: string;
    symbol: CURRENCY_SYMBOL;
  };
}
interface liquidationCompleted extends ENGINE_EVENT {
  type: "liquidation.completed";
  data: {
    userId: string;
    symbol: CURRENCY_SYMBOL;
    pnl: number;
  };
}

export type {
  markPriceUpdated,
  liquidationStarted,
  liquidationCompleted,
  LIQUIDATION_EVENT,
  CURRENCY_SYMBOL,
  CURRENCY_SYMBOL_ARRAY,
};
