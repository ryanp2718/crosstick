import { wsBroadcasts, wsSlowDrops } from "./metrics.js";

// The minimal slice of ws.WebSocket the fan-out depends on, so the logic is
// unit-testable with plain fakes instead of real sockets.
export interface WsClient {
  readyState: number;
  bufferedAmount: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

const OPEN = 1; // ws.WebSocket.OPEN

// Fans self-contained messages out to WS clients with a close+resync
// backpressure policy (HANDOFF defended decision): a client whose socket buffer
// grows past maxBufferedBytes is closed and dropped rather than fed stale data
// - it reconnects and gets a fresh BBO. Dropping would be safe too (each
// message is self-contained), but closing keeps memory strictly bounded and
// matches the project-wide policy.
export class Broadcaster {
  private readonly clients = new Set<WsClient>();

  constructor(private readonly maxBufferedBytes: number) {}

  add(c: WsClient): void {
    this.clients.add(c);
  }

  remove(c: WsClient): void {
    this.clients.delete(c);
  }

  get size(): number {
    return this.clients.size;
  }

  // `data` is an already-serialized frame: the caller stringifies once and shares
  // the string between the Kafka value and this WS send (G1). Returns before the
  // fan-out loop when no client is connected (G2).
  broadcast(data: string): void {
    wsBroadcasts.inc();
    if (this.clients.size === 0) return;
    for (const c of this.clients) {
      if (c.readyState !== OPEN) {
        this.clients.delete(c);
        continue;
      }
      if (c.bufferedAmount > this.maxBufferedBytes) {
        c.close(1013, "slow consumer"); // 1013 = Try Again Later
        this.clients.delete(c);
        wsSlowDrops.inc();
        continue;
      }
      c.send(data);
    }
  }
}
