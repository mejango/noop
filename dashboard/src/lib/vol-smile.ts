// Pure volatility-smile math shared by /api/smile and the VolSmile chart.

export type SmilePoint = {
  name: string;
  strike: number;
  type: 'P' | 'C';
  delta: number;
  iv: number;           // mark IV, vol points (38.5 = 38.5%)
  bidIv: number | null;
  askIv: number | null;
  oi: number;
};

export type SmileExpiry = { expiry: number; dte: number; forward: number; points: SmilePoint[] };

export type SmileStats = {
  atm: number | null;
  put25: number | null; call25: number | null; rr25: number | null;
  put10: number | null; call10: number | null; rr10: number | null;
};

type RawTicker = { I?: string; option_pricing?: Record<string, string | null> | null; stats?: { oi?: string } | null };

const num = (v: unknown) => { const n = Number(v); return Number.isFinite(n) ? n : null; };

// Signed delta axis: 10Δ put = -0.4, ATM = 0, 10Δ call = +0.4. Lines every expiry up on the same moneyness scale.
export function deltaX(p: Pick<SmilePoint, 'type' | 'delta'>) {
  const d = Math.min(Math.abs(p.delta), 0.5);
  return p.type === 'P' ? d - 0.5 : 0.5 - d;
}

// OTM-only smile from one expiry's get_tickers map; deep wings (<2Δ) are dropped as noise.
export function buildExpiry(expiry: number, tickers: Record<string, RawTicker>, now = Date.now()): SmileExpiry | null {
  const points: SmilePoint[] = [];
  const forwards: number[] = [];
  for (const [name, t] of Object.entries(tickers)) {
    const op = t.option_pricing;
    const f = num(op?.f) ?? num(t.I);
    if (f && f > 0) forwards.push(f);
    const [, , strikeStr, typeStr] = name.split('-');
    const strike = num(strikeStr), delta = num(op?.d), iv = num(op?.i);
    if (!strike || delta == null || !iv || iv <= 0 || !f) continue;
    const type = typeStr === 'P' ? 'P' : 'C';
    if ((type === 'P') !== (strike < f) || Math.abs(delta) < 0.02) continue;
    const bi = num(op?.bi), ai = num(op?.ai);
    points.push({
      name, strike, type, delta, iv: iv * 100,
      bidIv: bi && bi > 0 ? bi * 100 : null, askIv: ai && ai > 0 ? ai * 100 : null,
      oi: num(t.stats?.oi) ?? 0,
    });
  }
  if (points.length < 4 || !forwards.length) return null;
  forwards.sort((a, b) => a - b);
  points.sort((a, b) => a.strike - b.strike);
  return {
    expiry,
    dte: Math.max(0, (expiry * 1000 - now) / 86_400_000),
    forward: forwards[Math.floor(forwards.length / 2)],
    points,
  };
}

// Linear interpolation of IV at |delta| = target on one side of the smile; null outside the quoted range.
export function ivAtDelta(points: Pick<SmilePoint, 'type' | 'delta' | 'iv'>[], type: 'P' | 'C', target: number): number | null {
  const side = points.filter(p => p.type === type).map(p => ({ d: Math.abs(p.delta), iv: p.iv })).sort((a, b) => a.d - b.d);
  for (let i = 0; i < side.length; i++) {
    if (side[i].d === target) return side[i].iv;
    if (i > 0 && side[i - 1].d < target && side[i].d > target) {
      const w = (target - side[i - 1].d) / (side[i].d - side[i - 1].d);
      return side[i - 1].iv + w * (side[i].iv - side[i - 1].iv);
    }
  }
  return null;
}

// ATM = mean of the put and call nearest the forward (each side's closest-to-50Δ quote).
export function expiryStats(points: SmilePoint[]): SmileStats {
  const nearest = (type: 'P' | 'C') => points.filter(p => p.type === type)
    .reduce<SmilePoint | null>((a, b) => (!a || Math.abs(b.delta) > Math.abs(a.delta) ? b : a), null);
  const atmSides = [nearest('P'), nearest('C')].filter((p): p is SmilePoint => p != null && Math.abs(p.delta) > 0.3);
  const atm = atmSides.length ? atmSides.reduce((s, p) => s + p.iv, 0) / atmSides.length : null;
  const put25 = ivAtDelta(points, 'P', 0.25), call25 = ivAtDelta(points, 'C', 0.25);
  const put10 = ivAtDelta(points, 'P', 0.10), call10 = ivAtDelta(points, 'C', 0.10);
  return {
    atm, put25, call25, put10, call10,
    rr25: put25 != null && call25 != null ? call25 - put25 : null,
    rr10: put10 != null && call10 != null ? call10 - put10 : null,
  };
}
