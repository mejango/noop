'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

const STARTERS = [
  'How is the tail-hedge portfolio positioned right now?',
  'Is the bot pacing its budget well this cycle?',
  'What do the current options scores tell us?',
  'Any on-chain signals worth watching?',
];

export default function AdvisorDrawer() {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const scrollToBottom = useCallback(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || streaming) return;

    const userMsg: Message = { role: 'user', content: text.trim() };
    const history = [...messages, userMsg];
    setMessages(history);
    setInput('');
    setStreaming(true);

    // Add empty assistant message to stream into
    setMessages(prev => [...prev, { role: 'assistant', content: '' }]);

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
  }, [messages, streaming]);

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
        <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between shrink-0">
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

        {/* Messages */}
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
                  <div className="prose prose-invert prose-xs max-w-none break-words [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_h2]:text-sm [&_h2]:font-bold [&_h2]:text-juice-orange [&_h2]:mt-4 [&_h2]:mb-2 [&_h3]:text-xs [&_h3]:font-bold [&_h3]:text-white [&_h3]:mt-3 [&_h3]:mb-1 [&_p]:text-xs [&_p]:leading-relaxed [&_p]:my-1.5 [&_strong]:text-white [&_ul]:text-xs [&_ul]:my-1 [&_ul]:pl-4 [&_li]:my-0.5 [&_hr]:border-white/10 [&_hr]:my-3 [&_code]:text-juice-orange [&_code]:text-[11px] [&_code]:bg-white/5 [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded">
                    <ReactMarkdown>{msg.content}</ReactMarkdown>
                    {streaming && i === messages.length - 1 && (
                      <span className="inline-block w-1.5 h-3.5 bg-juice-orange ml-0.5 animate-pulse" />
                    )}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>

        {/* Input */}
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
      </div>
    </>
  );
}
