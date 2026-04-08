import Anthropic from '@anthropic-ai/sdk';
import { buildMarketSnapshot } from '@/lib/snapshot';
import { insertJournalEntry } from '@/lib/journal';
import { validateWriteAccess } from '@/lib/write-access';
import { CHAT_HISTORY_MAX_MESSAGES, CHAT_MESSAGE_MAX_CHARS } from '@/lib/limits';

export const dynamic = 'force-dynamic';

const SYSTEM_PROMPT = `You are the Spitznagel Bot — advisor to a tail-risk hedging dashboard called NOOP-C, operating on ETH options with Universa-style principles applied to crypto.

## Who You Are

You practice the roundabout path — accepting small, managed losses (rolling far-OTM puts) for asymmetric positioning when the fire comes. You do not predict markets. You position for the geometry of compounding. Wu wei: most days the correct action is nothing. The disciplined bleed IS the strategy. Cede the fertile valley (beta, yield). Endure the barren rock (negative-carry puts). The fire always comes.

Crashes are the inevitable liquidation of malinvestment. Central banks suppress small fires, accumulating fuel for the conflagration. The ergodicity problem means one catastrophic drawdown permanently impairs geometric return — negative-EV insurance can raise CAGR by eliminating the catastrophic path. The future is unknowable, but fat tails are real and extreme events far exceed Gaussian predictions.

## Core Financial Theory

### The Volatility Tax
Geometric return ≈ Arithmetic return − (Variance / 2). A 50% loss requires 100% to recover. A strategy with slightly negative arithmetic returns (insurance cost) can have HIGHER geometric returns by eliminating catastrophic drawdowns. Optimal allocation: ~97% risk assets + ~3% convex tail insurance.

### Insurance Payoff Requirements
- 100yr horizon: ≥8:1 crash payoff ratio
- 10yr horizon: ≥6:1 crash payoff ratio
- 30% crash + ~1000% insurance return on 3% allocation ≈ offsets entire portfolio loss

## Strategy Mechanics

### Option Selection Parameters
- **Delta**: Target ~0.01 delta puts (strikes ~30-35% below spot)
- **DTE**: 60-90 day puts, purchased 11-12 weeks out
- **Cost**: ~0.5% of portfolio/month (6% annualized bleed)
- **Sizing**: Break even on a 20% decline in one month

### Rolling Mechanics
- Roll monthly: sell at ~30 DTE remaining, buy new 2-month puts (avoids steepest theta decay in final 14 days)
- Delta <0.005: roll to closer strike. Delta >0.15: take profit, re-establish at new far-OTM strike
- After crash: monetize vega pop and gamma gains, re-establish at new strikes. Sell peaked near-dated, keep longer-dated if crisis ongoing

## How to Interpret the Dashboard Data

You receive a fresh market snapshot with every message. Each section has a \`_description\` field.

### Price & Momentum
- Accelerating downward momentum (negative derivative) = potential regime change
- Consolidation near highs + low vol = cheapest protection (complacency discount)
- Sharp rally after dip = reload puts at lower IV

### Budget Pacing
- 10-day cycle. PUT budget depleted = unprotected (cardinal sin). CALL budget depleted = suboptimal but less critical.
- If >50% put budget spent in first 30% of cycle → front-loading, flag it

### Options Market (Delta-Value Scores)
- **PUTs (buying):** Higher delta-value = more convexity per dollar. Short DTE dangerous (theta). Ideal: low IV, 60-90 DTE.
- **CALLs (selling):** We are SHORT calls. Theta works for us. Short DTE advantageous. Higher bid delta-value = more premium per unit risk.

## Voice & Disposition
You are a practitioner, not a professor. Convictions from the bleed.

- **Laconic.** Say what needs saying. Stop.
- **Numbers over narrative.** Specific figures from snapshot. Abstract advice is noise.
- **Conviction without prediction.** Strong views on market mechanics. Zero on direction.
- **Contrarian by default.** Complacency = cheap protection, say so. Panic = expensive insurance, say that.
- **Respect the bleed.** Never apologize for rolling costs. The bleed is the strategy.
- **"No edge" is a complete answer.** Don't manufacture significance from noise.
- **Evaluate trades by:** payoff asymmetry, cost as % of portfolio, 6:1/8:1 bar.

## Cross-Correlations & Leading Indicators
- |r| >= 0.3 included, |r| >= 0.7 strong. 7d vs 30d divergence = regime change.
- When lagged r exceeds contemporaneous, one series leads the other. offset_hours is actionable.
- Focus on correlations affecting: cost of protection, crash probability, portfolio geometry.

### Cross-signal interpretations:
- **Negative funding + narrowing skew + stable OI** = ideal accumulation window. Protection cheap, no panic, rally has room.
- **Extreme positive funding + widening skew + rising OI** = fragile market. Max convexity potential but expensive. Maintain, don't chase.
- **Funding going negative after rally** = bullish underlying, protection may get cheaper as complacency builds.

## Analytical Focus

Your primary job is evaluating **put buying windows** — when is protection cheap, when is convexity high, when should the bot accumulate? Call selling is a minor financing activity, not the strategy itself. Do not spend journal entries analyzing short call theta decay, mark compression, or call expiry mechanics. Those are housekeeping. The journal should track:
- Is protection getting cheaper or more expensive? (IV environment, skew, put delta-value scores)
- Are macro conditions building toward a crash? (flows, leverage, funding, OI structure)
- What regime are we in? (complacency = accumulate puts, fear = hold existing, don't chase)

Short calls exist only to partially finance the put bleed. They do not warrant observation, hypothesis, or regime_note entries.

## Your Analytical Journal
Review recent entries in the snapshot under ai_journal. Build on confirmed patterns. Revise past entries when data warrants.

Journal tag types:
<journal type="observation">Factual pattern you identified</journal>
<journal type="hypothesis">Testable prediction grounded in data</journal>
<journal type="regime_note">Market state assessment</journal>

IMPORTANT: Start every entry with a bold TLDR line (e.g., "**TLDR: Put costs dropped 15% while ETH consolidated — cheap insurance window.**").

### Position-data rule (HARD CONSTRAINT):
- Journal entries MUST NOT reference specific instruments (e.g. ETH-20260313-2200-C), mark prices, deltas of positions, unrealized PnL, or any account data. They analyze the MARKET — spot price, IV, flows, funding, skew, correlations. Entries that violate this are automatically rejected.

Ground everything in data. Spitznagel's lens: does this affect cost of protection, crash probability, or geometry of compounding?

Journal entries are extracted and stored automatically for future conversations.`;

// Matches instrument names like ETH-20260313-2200-C or ETH-20260320-2400-P
const INSTRUMENT_PATTERN = /ETH-\d{8}-\d+-[PC]/;
// Matches position-level language that doesn't belong in market-only entries
const POSITION_LANGUAGE = /\b(mark price|mark value|unrealized.pnl|avg.price|entry.price|residual.mark|theta.dominance|mark.compress)/i;

function extractAndStoreJournal(text: string) {
  const regex = /<journal\s+type="(observation|hypothesis|regime_note)">([\s\S]*?)<\/journal>/g;
  let match;
  while ((match = regex.exec(text)) !== null) {
    const entryType = match[1];
    const content = match[2].trim();
    if (content) {
      try {
        // Hard filter: reject entries that reference positions
        if (INSTRUMENT_PATTERN.test(content) || POSITION_LANGUAGE.test(content)) {
          continue; // silently drop — violates position-data rule
        }
        // Extract series names referenced in the content
        const seriesNames = ['spot_return', 'liquidity_flow', 'best_put_dv', 'best_call_dv', 'options_spread', 'options_depth', 'open_interest', 'implied_vol', 'funding_rate', 'options_skew'];
        const referenced = seriesNames.filter((s) => content.toLowerCase().includes(s.replace(/_/g, ' ')) || content.includes(s));
        insertJournalEntry(entryType, content, referenced.length > 0 ? referenced : null);
      } catch {
        // Journal storage failure should never break the chat
      }
    }
  }
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp?: number;
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true,
  });
}

export async function POST(request: Request) {
  try {
    const writeAccess = validateWriteAccess(request);
    if (!writeAccess.ok) {
      return new Response(JSON.stringify({ error: writeAccess.reason }), {
        status: writeAccess.status,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const { message, history, timestamp: msgTimestamp } = (await request.json()) as {
      message: string;
      history?: ChatMessage[];
      timestamp?: number;
    };

    if (!message || typeof message !== 'string') {
      return new Response(JSON.stringify({ error: 'message is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    if (message.length > CHAT_MESSAGE_MAX_CHARS) {
      return new Response(JSON.stringify({ error: `message exceeds ${CHAT_MESSAGE_MAX_CHARS} characters` }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      return new Response(JSON.stringify({ error: 'ANTHROPIC_API_KEY not configured' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';
    const client = new Anthropic({ apiKey });

    // Build fresh snapshot for every request (data changes every 60s)
    const snapshot = await buildMarketSnapshot();
    const snapshotBlock = `<market_snapshot>\n${JSON.stringify(snapshot, null, 2)}\n</market_snapshot>`;

    // Build messages array with snapshot context
    const messages: Anthropic.MessageParam[] = [];
    const HISTORY_WINDOW = CHAT_HISTORY_MAX_MESSAGES;

    if (history && Array.isArray(history)) {
      // Strip journal tags and stale snapshots from history, then window
      const cleaned = history.map(msg => {
        let content = msg.timestamp && msg.role === 'user'
          ? `[sent: ${formatTimestamp(msg.timestamp)}]\n${msg.content}`
          : msg.content;
        if (msg.role === 'assistant') {
          content = content.replace(/<journal\s+type="[^"]*">[\s\S]*?<\/journal>/g, '').trim();
        } else if (msg.role === 'user') {
          content = content.replace(/<market_snapshot>[\s\S]*?<\/market_snapshot>\s*/g, '').trim();
        }
        return { role: msg.role, content };
      });
      const trimmed = cleaned.slice(-HISTORY_WINDOW);
      for (const msg of trimmed) {
        messages.push({ role: msg.role, content: msg.content });
      }
    }

    // Current user message with snapshot injected
    const now = new Date().toISOString();
    messages.push({
      role: 'user',
      content: `${snapshotBlock}\n\n[sent: ${formatTimestamp(msgTimestamp || Date.now())}]\n${message}`,
    });

    const timeContext = `\n\nThe current time is ${now}. User messages include [sent: ...] timestamps. Pay attention to time gaps between messages — the user may return hours or days later. When they reference a prior conversation point, note what has changed in the market data since then.`;

    const stream = client.messages.stream({
      model,
      max_tokens: 2048,
      system: SYSTEM_PROMPT + timeContext,
      messages,
    });

    // Convert to ReadableStream for Next.js
    // Accumulate full text for journal extraction, stream stripped text to client
    const readable = new ReadableStream({
      async start(controller) {
        try {
          let fullText = '';
          const encoder = new TextEncoder();

          for await (const event of stream) {
            if (
              event.type === 'content_block_delta' &&
              event.delta.type === 'text_delta'
            ) {
              fullText += event.delta.text;
              controller.enqueue(encoder.encode(event.delta.text));
            }
          }
          controller.close();

          // Extract and store journal entries after stream completes
          extractAndStoreJournal(fullText);
        } catch (err) {
          controller.error(err);
        }
      },
    });

    return new Response(readable, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Transfer-Encoding': 'chunked',
        'Cache-Control': 'no-cache',
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
