'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

interface JournalEntry {
  id: number;
  timestamp: string;
  entry_type: string;
  content: string;
  series_referenced: string | null;
  created_at: string;
}

function stripJournalTags(text: string): string {
  return text.replace(/<journal\s+type="[^"]*">[\s\S]*?<\/journal>/g, '').replace(/\n{3,}/g, '\n\n').trim();
}

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

const TYPE_STYLES: Record<string, { label: string; color: string }> = {
  observation: { label: 'OBS', color: 'bg-blue-500/20 text-blue-400' },
  hypothesis: { label: 'HYP', color: 'bg-amber-500/20 text-amber-400' },
  regime_note: { label: 'REG', color: 'bg-purple-500/20 text-purple-400' },
};

const STARTERS = [
  'How is the tail-hedge portfolio positioned right now?',
  'Is the bot pacing its budget well this cycle?',
  'What do the current options scores tell us?',
  'Synthesize your journal — what patterns are forming and what do you expect next?',
];

export default function AdvisorDrawer() {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<'chat' | 'journal'>('chat');
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [journalEntries, setJournalEntries] = useState<JournalEntry[]>([]);
  const [journalLoading, setJournalLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const scrollToBottom = useCallback(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, []);

  const fetchJournal = useCallback(async () => {
    setJournalLoading(true);
    try {
      const res = await fetch('/api/ai/journal');
      if (res.ok) {
        const data = await res.json();
        setJournalEntries(data.entries || []);
      }
    } catch { /* silent */ }
    setJournalLoading(false);
  }, []);

  useEffect(() => {
    if (open && tab === 'journal') {
      fetchJournal();
    }
  }, [open, tab, fetchJournal]);

  useEffect(() => {
    if (open && tab === 'chat' && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open, tab]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || streaming) return;

    const userMsg: Message = { role: 'user', content: text.trim() };
    const history = [...messages, userMsg];
    setMessages(history);
    setInput('');
    setStreaming(true);

    // Add empty assistant message to stream into
    setMessages(prev => [...prev, { role: 'assistant', content: '' }]);

    // Scroll to show the user's message, then let them read at their own pace
    setTimeout(scrollToBottom, 50);

    try {
      const res = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: text.trim(),
          history: messages, // prior messages (before this user msg)
        }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        setMessages(prev => {
          const updated = [...prev];
          updated[updated.length - 1] = {
            role: 'assistant',
            content: `Error: ${err.error || res.statusText}`,
          };
          return updated;
        });
        setStreaming(false);
        return;
      }

      const reader = res.body?.getReader();
      if (!reader) throw new Error('No response stream');

      const decoder = new TextDecoder();
      let accumulated = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        accumulated += decoder.decode(value, { stream: true });
        const text = accumulated;
        setMessages(prev => {
          const updated = [...prev];
          updated[updated.length - 1] = { role: 'assistant', content: text };
          return updated;
        });
      }
    } catch (err) {
      setMessages(prev => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          role: 'assistant',
          content: `Error: ${err instanceof Error ? err.message : 'Unknown error'}`,
        };
        return updated;
      });
    } finally {
      setStreaming(false);
    }
  }, [messages, streaming, scrollToBottom]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  return (
    <>
      {/* Toggle tab */}
      <button
        onClick={() => setOpen(o => !o)}
        className={`fixed top-1/2 -translate-y-1/2 z-50 transition-all duration-300 ${
          open ? 'right-[400px] md:right-[480px]' : 'right-0'
        }`}
        style={{ writingMode: 'vertical-rl' }}
      >
        <span className="bg-juice-orange/90 hover:bg-juice-orange text-black text-xs font-bold px-1.5 py-3 tracking-wider cursor-pointer select-none">
          {open ? 'CLOSE' : 'SPITZ'}
        </span>
      </button>

      {/* Backdrop */}
      {open && (
        <div
          className="fixed inset-0 bg-black/40 z-40 md:hidden"
          onClick={() => setOpen(false)}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed top-0 right-0 h-full z-40 bg-[#111] border-l border-white/10 flex flex-col transition-transform duration-300 w-[400px] md:w-[480px] ${
          open ? 'translate-x-0' : 'translate-x-full'
        }`}
      >
        {/* Header */}
        <div className="px-4 py-3 border-b border-white/10 shrink-0">
          <div className="flex items-center justify-between">
            <span className="text-sm font-bold text-juice-orange tracking-wide">SPITZNAGEL BOT</span>
            <button
              onClick={() => {
                setMessages([]);
                setInput('');
              }}
              className="text-xs text-gray-500 hover:text-white transition-colors"
            >
              Clear
            </button>
          </div>
          <div className="flex gap-1 mt-2">
            <button
              onClick={() => setTab('chat')}
              className={`text-xs px-3 py-1 transition-colors ${
                tab === 'chat'
                  ? 'bg-white/10 text-white'
                  : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Chat
            </button>
            <button
              onClick={() => setTab('journal')}
              className={`text-xs px-3 py-1 transition-colors ${
                tab === 'journal'
                  ? 'bg-white/10 text-white'
                  : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Journal{journalEntries.length > 0 ? ` (${journalEntries.length})` : ''}
            </button>
          </div>
        </div>

        {/* Journal View */}
        {tab === 'journal' && (
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
            {journalLoading && (
              <p className="text-gray-500 text-xs">Loading journal...</p>
            )}
            {!journalLoading && journalEntries.length === 0 && (
              <div className="pt-4">
                <p className="text-gray-500 text-xs">No journal entries yet. Chat with the bot to generate observations, hypotheses, and regime notes.</p>
              </div>
            )}
            {journalEntries.map((entry) => {
              const style = TYPE_STYLES[entry.entry_type] || { label: entry.entry_type.toUpperCase(), color: 'bg-white/10 text-gray-400' };
              return (
                <div key={entry.id} className="border border-white/5 px-3 py-2.5 space-y-1.5">
                  <div className="flex items-center justify-between">
                    <span className={`text-[10px] font-bold px-1.5 py-0.5 ${style.color}`}>
                      {style.label}
                    </span>
                    <span className="text-[10px] text-gray-600">{timeAgo(entry.timestamp)}</span>
                  </div>
                  <p className="text-xs text-gray-300 leading-relaxed">{entry.content}</p>
                  {entry.series_referenced && (
                    <div className="flex gap-1 flex-wrap">
                      {(JSON.parse(entry.series_referenced) as string[]).map((s) => (
                        <span key={s} className="text-[10px] text-gray-600 bg-white/5 px-1.5 py-0.5">{s}</span>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* Chat Messages */}
        {tab === 'chat' && (
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-4">
            {messages.length === 0 && (
              <div className="space-y-3 pt-4">
                <p className="text-gray-500 text-xs">Ask about the portfolio, market conditions, or strategy.</p>
                <div className="space-y-2">
                  {STARTERS.map((s) => (
                    <button
                      key={s}
                      onClick={() => sendMessage(s)}
                      className="block w-full text-left text-xs text-gray-400 hover:text-white border border-white/10 hover:border-white/20 px-3 py-2 transition-colors"
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={msg.role === 'user' ? 'flex justify-end' : ''}>
                <div
                  className={`text-sm max-w-[90%] ${
                    msg.role === 'user'
                      ? 'bg-white/10 text-white px-3 py-2'
                      : 'text-gray-300'
                  }`}
                >
                  {msg.role === 'user' ? (
                    <span className="text-xs break-words">{msg.content}</span>
                  ) : (
                    <div className="prose prose-invert prose-xs max-w-none break-words [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_h2]:text-sm [&_h2]:font-bold [&_h2]:text-juice-orange [&_h2]:mt-4 [&_h2]:mb-2 [&_h3]:text-xs [&_h3]:font-bold [&_h3]:text-white [&_h3]:mt-3 [&_h3]:mb-1 [&_p]:text-xs [&_p]:leading-relaxed [&_p]:my-1.5 [&_strong]:text-white [&_ul]:text-xs [&_ul]:my-1 [&_ul]:pl-4 [&_li]:my-0.5 [&_hr]:border-white/10 [&_hr]:my-3 [&_code]:text-juice-orange [&_code]:text-[11px] [&_code]:bg-white/5 [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded [&_table]:w-full [&_table]:text-xs [&_table]:my-2 [&_table]:border-collapse [&_th]:text-left [&_th]:text-gray-400 [&_th]:font-medium [&_th]:border-b [&_th]:border-white/10 [&_th]:px-2 [&_th]:py-1 [&_td]:text-gray-300 [&_td]:border-b [&_td]:border-white/5 [&_td]:px-2 [&_td]:py-1">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>{stripJournalTags(msg.content)}</ReactMarkdown>
                      {streaming && i === messages.length - 1 && (
                        <span className="inline-block w-1.5 h-3.5 bg-juice-orange ml-0.5 animate-pulse" />
                      )}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Input — only show in chat tab */}
        {tab === 'chat' && (
          <div className="px-4 py-3 border-t border-white/10 shrink-0">
            <div className="flex gap-2">
              <textarea
                ref={inputRef}
                value={input}
                onChange={e => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask the bot..."
                rows={1}
                className="flex-1 bg-white/5 border border-white/10 text-white text-sm px-3 py-2 resize-none focus:outline-none focus:border-juice-orange/50 placeholder-gray-600"
              />
              <button
                onClick={() => sendMessage(input)}
                disabled={streaming || !input.trim()}
                className="bg-juice-orange hover:bg-juice-orange/80 disabled:bg-gray-700 disabled:text-gray-500 text-black font-bold text-sm px-4 py-2 transition-colors shrink-0"
              >
                Send
              </button>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
