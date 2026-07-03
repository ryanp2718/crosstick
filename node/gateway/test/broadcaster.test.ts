import { describe, expect, it, vi } from "vitest";

import { Broadcaster, type WsClient } from "../src/broadcaster.js";

function client(over: Partial<WsClient> = {}): WsClient {
  return { readyState: 1, bufferedAmount: 0, send: vi.fn(), close: vi.fn(), ...over };
}

describe("Broadcaster", () => {
  it("sends the prepared frame to open clients", () => {
    const b = new Broadcaster(1000);
    const c = client();
    b.add(c);
    b.broadcast('{"t":"bbo","bid_px":"100"}');
    expect(c.send).toHaveBeenCalledWith('{"t":"bbo","bid_px":"100"}');
  });

  it("closes and drops a client whose buffer exceeds the limit", () => {
    const b = new Broadcaster(1000);
    const slow = client({ bufferedAmount: 2000 });
    b.add(slow);
    b.broadcast('{"a":1}');
    expect(slow.close).toHaveBeenCalled();
    expect(slow.send).not.toHaveBeenCalled();
    expect(b.size).toBe(0);
  });

  it("drops clients that are no longer open", () => {
    const b = new Broadcaster(1000);
    const gone = client({ readyState: 3 }); // CLOSED
    b.add(gone);
    b.broadcast('{"a":1}');
    expect(gone.send).not.toHaveBeenCalled();
    expect(b.size).toBe(0);
  });

  it("delivers the same frame to every healthy client", () => {
    const b = new Broadcaster(1000);
    const a = client();
    const c = client();
    b.add(a);
    b.add(c);
    b.broadcast('{"n":7}');
    expect(a.send).toHaveBeenCalledWith('{"n":7}');
    expect(c.send).toHaveBeenCalledWith('{"n":7}');
    expect(b.size).toBe(2);
  });

  it("is a no-op with no clients", () => {
    const b = new Broadcaster(1000);
    expect(() => b.broadcast('{"x":1}')).not.toThrow();
    expect(b.size).toBe(0);
  });
});
