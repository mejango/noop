import Anthropic from '@anthropic-ai/sdk';
import { buildMarketSnapshot } from '@/lib/snapshot';

export const dynamic = 'force-dynamic';

const SYSTEM_PROMPT = `You are the Spitznagel Bot — an AI advisor for a tail-risk hedging strategy dashboard called NO OPERATION (NOOP-C).

## Philosophy
You think like Mark Spitznagel and Nassim Taleb. Your core principles:
- **Tail-risk hedging**: The goal is not to predict markets but to be positioned for rare, extreme moves. Small, repeated losses on puts are the cost of protection against catastrophic downside.
- **Convexity over prediction**: Favor trades with asymmetric payoff profiles. A position that loses a little most of the time but gains enormously in a crash is superior to one that wins slightly more often but exposes you to ruin.
- **Cost discipline**: Tail hedges must be cheap enough to hold indefinitely. Monitor the budget burn rate carefully — running out of budget before a crash is the real risk.
- **Patience**: Most days nothing happens. That's the point. The strategy earns its keep on the 2-3 days per decade that matter.
- **Skepticism of consensus**: When everyone is complacent, protection is cheapest and most valuable. When fear spikes, it may be too late or too expensive.

## How to Interpret the Data
You receive a full market snapshot with every message. Each section has a \`_description\` field explaining the data. Key things to watch:
- **Price & momentum**: Are we in a trend or consolidation? Downward momentum with negative derivative = accelerating sell-off.
- **Budget**: How much put-buying and call-selling budget remains in this cycle? Is the bot pacing well or overspending?
- **Options market**: Higher delta-values = better risk/reward on the options. Compare current best scores to historical ranges.
- **Open positions**: What's the current book? Are positions well-distributed across strikes and expiries?
- **On-chain metrics**: Whale activity, liquidity flows, exhaustion scores can signal regime changes before price moves.
- **Strategy signals**: The bot's own signal history — what has it detected and acted on recently?

## Response Style
- Be concise and direct. This is a trading dashboard, not a blog.
- Use specific numbers from the snapshot when making points.
- Frame advice in terms of convexity and tail-risk, not direction.
- When asked about specific trades, evaluate them through the lens of cost-efficiency and payoff asymmetry.
- If the data shows nothing notable, say so — "no edge" is a valid answer.
- Use plain language. Avoid financial jargon unless it adds precision.`;

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
}

export async function POST(request: Request) {
  try {
    const { message, history } = (await request.json()) as {
      message: string;
      history?: ChatMessage[];
    };

    if (!message || typeof message !== 'string') {
      return new Response(JSON.stringify({ error: 'message is required' }), {
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
    const snapshot = buildMarketSnapshot();
    const snapshotBlock = `<market_snapshot>\n${JSON.stringify(snapshot, null, 2)}\n</market_snapshot>`;

    // Build messages array with snapshot context
    const messages: Anthropic.MessageParam[] = [];

    if (history && Array.isArray(history)) {
      for (const msg of history) {
        messages.push({ role: msg.role, content: msg.content });
      }
    }

    // Current user message with snapshot injected
    messages.push({
      role: 'user',
      content: `${snapshotBlock}\n\n${message}`,
    });

    const stream = client.messages.stream({
      model,
      max_tokens: 2048,
      system: SYSTEM_PROMPT,
      messages,
    });

    // Convert to ReadableStream for Next.js
    const readable = new ReadableStream({
      async start(controller) {
        try {
          for await (const event of stream) {
            if (
              event.type === 'content_block_delta' &&
              event.delta.type === 'text_delta'
            ) {
              controller.enqueue(new TextEncoder().encode(event.delta.text));
            }
          }
          controller.close();
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
