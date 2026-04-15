import Anthropic from '@anthropic-ai/sdk';
import { buildMarketSnapshot } from '@/lib/snapshot';
import { CHAT_HISTORY_MAX_MESSAGES, CHAT_MESSAGE_MAX_CHARS } from '@/lib/limits';

export const dynamic = 'force-dynamic';

const SYSTEM_PROMPT = `You are the Spitznagel Bot in read-only research mode for a tail-risk hedging dashboard called NOOP-C.

You answer questions about:
- the wiki
- the analytical journal
- current market/account state in the snapshot
- how the strategy works
- what the recent observations imply

Hard constraints:
- You are READ ONLY.
- Never propose or initiate execution.
- Never create, modify, or confirm trades, rules, pending actions, or policy changes.
- Never emit <journal> tags.
- Never suggest that your response will be ingested into the journal or wiki.
- Never frame your answer as an instruction to the bot to act.

Response style:
- Answer directly and concretely.
- Use figures from the snapshot when helpful.
- If the user asks about why something happened, distinguish between market facts, stored journal observations, and your inference.
- If the answer is uncertain, say so plainly.

You may summarize and interpret the wiki and journal, but this conversation has no write path and no operational authority.`;

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
    const snapshot = await buildMarketSnapshot();
    const snapshotBlock = `<market_snapshot>\n${JSON.stringify(snapshot, null, 2)}\n</market_snapshot>`;

    const messages: Anthropic.MessageParam[] = [];
    if (history && Array.isArray(history)) {
      const trimmed = history.slice(-CHAT_HISTORY_MAX_MESSAGES);
      for (const msg of trimmed) {
        const content = msg.role === 'user'
          ? msg.content.replace(/<market_snapshot>[\s\S]*?<\/market_snapshot>\s*/g, '').trim()
          : msg.content.trim();
        messages.push({ role: msg.role, content });
      }
    }

    messages.push({
      role: 'user',
      content: `${snapshotBlock}\n\n[sent: ${formatTimestamp(msgTimestamp || Date.now())}]\n${message}`,
    });

    const now = new Date().toISOString();
    const timeContext = `\n\nThe current time is ${now}. User messages include [sent: ...] timestamps. Use that to reason about changes over time, but remain read-only.`;

    const stream = client.messages.stream({
      model,
      max_tokens: 2048,
      system: SYSTEM_PROMPT + timeContext,
      messages,
    });

    const readable = new ReadableStream({
      async start(controller) {
        try {
          const encoder = new TextEncoder();
          for await (const event of stream) {
            if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
              controller.enqueue(encoder.encode(event.delta.text));
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
