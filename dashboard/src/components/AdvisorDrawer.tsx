'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import WikiBrowser from './WikiBrowser';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
}

interface Chat {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;
  updatedAt: number;
}

const STORAGE_KEY = 'advisor-chats';

function loadChats(): Chat[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveChats(chats: Chat[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(chats));
  } catch { /* quota exceeded — silent */ }
}

function chatTimeAgo(ts: number): string {
  const diff = Date.now() - ts;
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

interface JournalEntry {
  id: number;
  timestamp: string;
  entry_type: string;
  content: string;
  series_referenced: string | null;
  created_at: string;
  prediction_deadline: string | null;
  outcome_status: string | null;
  outcome_verdict: string | null;
  outcome_confidence: number | null;
  trade_pnl_attribution: number | null;
  trades_in_window: string | null;
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
  observation: { label: 'Observation', color: 'bg-blue-500/20 text-blue-400' },
  hypothesis: { label: 'Hypothesis', color: 'bg-amber-500/20 text-amber-400' },
  regime_note: { label: 'Regime', color: 'bg-purple-500/20 text-purple-400' },
};

const VERDICT_STYLES: Record<string, { label: string; color: string }> = {
  confirmed_convex: { label: 'Convex Win', color: 'bg-green-500/20 text-green-400' },
  confirmed_linear: { label: 'Linear Win', color: 'bg-blue-500/20 text-blue-400' },
  partially_confirmed: { label: 'Partial', color: 'bg-blue-500/20 text-blue-400' },
  disproven_bounded: { label: 'Bounded Loss', color: 'bg-gray-500/20 text-gray-400' },
  disproven_costly: { label: 'Costly Miss', color: 'bg-red-500/20 text-red-400' },
};

function timeUntil(ts: string): string {
  const diff = new Date(ts).getTime() - Date.now();
  if (diff <= 0) return 'reviewing...';
  const hrs = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);
  if (hrs > 0) return `verdict in ${hrs}h ${mins}m`;
  return `verdict in ${mins}m`;
}

const STARTERS = [
  'How is the tail-hedge portfolio positioned right now?',
  'Is the bot pacing its budget well this cycle?',
  'What do the current options scores tell us?',
  'Synthesize your journal — what patterns are forming and what do you expect next?',
];

export default function AdvisorDrawer() {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<'chat' | 'journal' | 'wiki'>('chat');
  const [chats, setChats] = useState<Chat[]>([]);
  const [activeChatId, setActiveChatId] = useState<string | null>(null);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [journalEntries, setJournalEntries] = useState<JournalEntry[]>([]);
  const [journalLoading, setJournalLoading] = useState(false);
  const [analyticsTab, setAnalyticsTab] = useState(false);
  const [journalFilter, setJournalFilter] = useState<string | null>(null);
  const [hypStats, setHypStats] = useState<{
    total: number; reviewed: number; pending: number;
    confirmed_convex: number; confirmed_linear: number;
    disproven_bounded: number; disproven_costly: number;
    partially_confirmed: number;
    convexPostureRate: number; costlyRate: number;
  } | null>(null);
  const [lessons, setLessons] = useState<{ id: number; lesson: string; evidence_count: number; created_at: string }[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Load chats from localStorage on mount
  useEffect(() => {
    const saved = loadChats();
    if (saved.length > 0) {
      setChats(saved);
      setActiveChatId(saved[0].id); // most recent first
    }
  }, []);

  // Save chats to localStorage whenever they change
  const chatsRef = useRef(chats);
  chatsRef.current = chats;
  const saveChatsDebounced = useCallback((updated: Chat[]) => {
    saveChats(updated);
  }, []);

  // Derived: active chat's messages
  const activeChat = chats.find(c => c.id === activeChatId) ?? null;
  const messages = activeChat?.messages ?? [];

  const createNewChat = useCallback(() => {
    const newChat: Chat = {
      id: crypto.randomUUID(),
      title: 'New chat',
      messages: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    setChats(prev => {
      const updated = [newChat, ...prev];
      saveChatsDebounced(updated);
      return updated;
    });
    setActiveChatId(newChat.id);
    return newChat.id;
  }, [saveChatsDebounced]);

  const deleteChat = useCallback((chatId: string) => {
    setChats(prev => {
      const updated = prev.filter(c => c.id !== chatId);
      saveChatsDebounced(updated);
      if (activeChatId === chatId) {
        setActiveChatId(updated.length > 0 ? updated[0].id : null);
      }
      return updated;
    });
  }, [activeChatId, saveChatsDebounced]);

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

  const fetchAnalytics = useCallback(async () => {
    try {
      const [statsRes, lessonsRes] = await Promise.all([
        fetch('/api/ai/hypothesis-stats'),
        fetch('/api/ai/hypothesis-lessons'),
      ]);
      if (statsRes.ok) {
        const data = await statsRes.json();
        setHypStats(data.stats);
      }
      if (lessonsRes.ok) {
        const data = await lessonsRes.json();
        setLessons(data.lessons || []);
      }
    } catch { /* silent */ }
  }, []);

  useEffect(() => {
    if (open && tab === 'journal') {
      fetchJournal();
    }
  }, [open, tab, fetchJournal]);

  useEffect(() => {
    if (open && tab === 'journal' && analyticsTab) {
      fetchAnalytics();
    }
  }, [open, tab, analyticsTab, fetchAnalytics]);

  useEffect(() => {
    if (open && tab === 'chat' && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open, tab]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || streaming) return;

    // Ensure we have an active chat
    let chatId = activeChatId;
    if (!chatId) {
      chatId = createNewChat();
    }

    const now = Date.now();
    const userMsg: Message = { role: 'user', content: text.trim(), timestamp: now };
    const isFirstMessage = (chatsRef.current.find(c => c.id === chatId)?.messages.length ?? 0) === 0;

    // Add user message + empty assistant placeholder to active chat
    setChats(prev => {
      const updated = prev.map(c => {
        if (c.id !== chatId) return c;
        return {
          ...c,
          title: isFirstMessage ? text.trim().slice(0, 50) : c.title,
          messages: [...c.messages, userMsg, { role: 'assistant' as const, content: '', timestamp: now }],
          updatedAt: now,
        };
      });
      saveChatsDebounced(updated);
      return updated;
    });

    setInput('');
    setStreaming(true);
    setTimeout(scrollToBottom, 50);

    // History = messages before this user message (from current chats ref)
    const priorMessages = chatsRef.current.find(c => c.id === chatId)?.messages ?? [];

    try {
      const res = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: text.trim(),
          timestamp: now,
          history: priorMessages.map(m => ({ role: m.role, content: m.content, timestamp: m.timestamp })),
        }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        setChats(prev => {
          const updated = prev.map(c => {
            if (c.id !== chatId) return c;
            const msgs = [...c.messages];
            msgs[msgs.length - 1] = { role: 'assistant', content: `Error: ${err.error || res.statusText}`, timestamp: Date.now() };
            return { ...c, messages: msgs };
          });
          saveChatsDebounced(updated);
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
        const streamedText = accumulated;
        setChats(prev =>
          prev.map(c => {
            if (c.id !== chatId) return c;
            const msgs = [...c.messages];
            msgs[msgs.length - 1] = { role: 'assistant', content: streamedText, timestamp: msgs[msgs.length - 1].timestamp };
            return { ...c, messages: msgs };
          })
        );
      }

      // Final save after stream completes
      setChats(prev => {
        const updated = prev.map(c => {
          if (c.id !== chatId) return c;
          return { ...c, updatedAt: Date.now() };
        });
        saveChatsDebounced(updated);
        return updated;
      });
    } catch (err) {
      setChats(prev => {
        const updated = prev.map(c => {
          if (c.id !== chatId) return c;
          const msgs = [...c.messages];
          msgs[msgs.length - 1] = { role: 'assistant', content: `Error: ${err instanceof Error ? err.message : 'Unknown error'}`, timestamp: Date.now() };
          return { ...c, messages: msgs };
        });
        saveChatsDebounced(updated);
        return updated;
      });
    } finally {
      setStreaming(false);
    }
  }, [activeChatId, createNewChat, streaming, scrollToBottom, saveChatsDebounced]);

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
              onClick={createNewChat}
              className="text-xs text-gray-500 hover:text-white transition-colors"
            >
              + New Chat
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
            <button
              onClick={() => setTab('wiki')}
              className={`text-xs px-3 py-1 transition-colors ${
                tab === 'wiki'
                  ? 'bg-white/10 text-white'
                  : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Wiki
            </button>
          </div>
          {/* Chat list chips */}
          {tab === 'chat' && chats.length > 0 && (
            <div className="flex gap-1.5 mt-2 overflow-x-auto scrollbar-none pb-0.5">
              {chats.map(chat => (
                <button
                  key={chat.id}
                  onClick={() => setActiveChatId(chat.id)}
                  className={`group flex items-center gap-1 shrink-0 text-[11px] px-2 py-1 transition-colors ${
                    chat.id === activeChatId
                      ? 'bg-juice-orange/20 text-juice-orange border border-juice-orange/30'
                      : 'bg-white/5 text-gray-400 hover:text-white border border-white/5 hover:border-white/20'
                  }`}
                >
                  <span className="truncate max-w-[120px]">{chat.title}</span>
                  <span className="text-[9px] text-gray-600 shrink-0">{chatTimeAgo(chat.updatedAt)}</span>
                  <span
                    onClick={(e) => { e.stopPropagation(); deleteChat(chat.id); }}
                    className="text-gray-600 hover:text-red-400 ml-0.5 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer shrink-0"
                  >
                    ×
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Journal View */}
        {tab === 'journal' && (
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
            {/* Sub-toggle: Entries / Analytics */}
            <div className="flex gap-1 border-b border-white/5 pb-2">
              <button
                onClick={() => setAnalyticsTab(false)}
                className={`text-[11px] px-2.5 py-1 transition-colors ${
                  !analyticsTab ? 'bg-white/10 text-white' : 'text-gray-500 hover:text-gray-300'
                }`}
              >
                Entries
              </button>
              <button
                onClick={() => setAnalyticsTab(true)}
                className={`text-[11px] px-2.5 py-1 transition-colors ${
                  analyticsTab ? 'bg-white/10 text-white' : 'text-gray-500 hover:text-gray-300'
                }`}
              >
                Analytics
              </button>
            </div>

            {/* Analytics View */}
            {analyticsTab && (
              <div className="space-y-4">
                {!hypStats ? (
                  <p className="text-gray-500 text-xs">Loading analytics...</p>
                ) : (
                  <>
                    {/* Convex Posture Rate */}
                    <div className="text-center py-3">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">Convex Posture Rate</p>
                      <p className="text-3xl font-bold text-juice-orange">
                        {(hypStats.convexPostureRate * 100).toFixed(0)}%
                      </p>
                      <p className="text-[10px] text-gray-600 mt-1">
                        {hypStats.reviewed} reviewed / {hypStats.total} total ({hypStats.pending} pending)
                      </p>
                    </div>

                    {/* Outcome Breakdown */}
                    <div className="space-y-2">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wider">Outcome Breakdown</p>
                      {[
                        { key: 'confirmed_convex', label: 'Convex Win', count: hypStats.confirmed_convex, dot: 'bg-green-400' },
                        { key: 'confirmed_linear', label: 'Linear Win', count: hypStats.confirmed_linear, dot: 'bg-blue-400' },
                        { key: 'partially_confirmed', label: 'Partial', count: hypStats.partially_confirmed, dot: 'bg-blue-300' },
                        { key: 'disproven_bounded', label: 'Bounded Loss', count: hypStats.disproven_bounded, dot: 'bg-gray-400' },
                        { key: 'disproven_costly', label: 'Costly Miss', count: hypStats.disproven_costly, dot: 'bg-red-400' },
                      ].map((row) => (
                        <div key={row.key} className="flex items-center gap-2">
                          <span className={`w-2 h-2 rounded-full ${row.dot} shrink-0`} />
                          <span className="text-xs text-gray-400 flex-1">{row.label}</span>
                          <span className="text-xs text-gray-300 font-mono">{row.count}</span>
                          <div className="w-20 h-1.5 bg-white/5 rounded-full overflow-hidden">
                            <div
                              className={`h-full ${row.dot} rounded-full`}
                              style={{ width: hypStats.reviewed > 0 ? `${(row.count / hypStats.reviewed) * 100}%` : '0%' }}
                            />
                          </div>
                        </div>
                      ))}
                    </div>

                    {/* Costly Miss Warning */}
                    {hypStats.costlyRate > 0.2 && (
                      <div className="bg-red-500/10 border border-red-500/20 px-3 py-2">
                        <p className="text-xs text-red-400 font-bold">High Costly Miss Rate</p>
                        <p className="text-[11px] text-red-400/70 mt-0.5">
                          {(hypStats.costlyRate * 100).toFixed(0)}% of reviewed hypotheses resulted in costly misses.
                          Consider tightening entry criteria.
                        </p>
                      </div>
                    )}

                    {/* Active Lessons */}
                    {lessons.length > 0 && (
                      <div className="space-y-2">
                        <p className="text-[10px] text-gray-500 uppercase tracking-wider">Active Lessons</p>
                        {lessons.map((l) => (
                          <div key={l.id} className="border border-white/5 px-3 py-2">
                            <p className="text-xs text-gray-300">{l.lesson}</p>
                            <p className="text-[10px] text-gray-600 mt-1">
                              {l.evidence_count} evidence point{l.evidence_count !== 1 ? 's' : ''} &middot; {timeAgo(l.created_at)}
                            </p>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                )}
              </div>
            )}

            {/* Entries View */}
            {!analyticsTab && (
              <>
                {/* Type filter */}
                <div className="flex gap-1 flex-wrap">
                  <button
                    onClick={() => setJournalFilter(null)}
                    className={`text-[10px] px-2 py-0.5 transition-colors ${
                      journalFilter === null ? 'bg-white/15 text-white' : 'bg-white/5 text-gray-500 hover:text-gray-300'
                    }`}
                  >
                    All
                  </button>
                  {Object.entries(TYPE_STYLES).map(([key, style]) => (
                    <button
                      key={key}
                      onClick={() => setJournalFilter(journalFilter === key ? null : key)}
                      className={`text-[10px] px-2 py-0.5 transition-colors ${
                        journalFilter === key ? style.color : 'bg-white/5 text-gray-500 hover:text-gray-300'
                      }`}
                    >
                      {style.label}
                    </button>
                  ))}
                </div>
                {journalLoading && (
                  <p className="text-gray-500 text-xs">Loading journal...</p>
                )}
                {!journalLoading && journalEntries.length === 0 && (
                  <div className="pt-4">
                    <p className="text-gray-500 text-xs">No journal entries yet. Chat with the bot to generate observations, hypotheses, and regime notes.</p>
                  </div>
                )}
                {journalEntries.filter(e => !journalFilter || e.entry_type === journalFilter).map((entry) => {
                  const style = TYPE_STYLES[entry.entry_type] || { label: entry.entry_type.toUpperCase(), color: 'bg-white/10 text-gray-400' };
                  return (
                    <div key={entry.id} className="border border-white/5 px-3 py-2.5 space-y-1.5">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-1.5">
                          <span className={`text-[10px] font-bold px-1.5 py-0.5 ${style.color}`}>
                            {style.label}
                          </span>
                          <span className="text-[10px] text-gray-500 font-mono">#{entry.id}</span>
                          {entry.entry_type === 'hypothesis' && entry.outcome_status && entry.outcome_status !== 'pending' && (
                            <span className={`text-[10px] font-bold px-1.5 py-0.5 ${VERDICT_STYLES[entry.outcome_status]?.color || 'bg-white/10 text-gray-400'}`}>
                              {VERDICT_STYLES[entry.outcome_status]?.label || entry.outcome_status}
                            </span>
                          )}
                          {entry.entry_type === 'hypothesis' && (!entry.outcome_status || entry.outcome_status === 'pending') && entry.prediction_deadline && (
                            <span className="text-[10px] text-gray-500 italic">
                              {timeUntil(entry.prediction_deadline)}
                            </span>
                          )}
                        </div>
                        <span className="text-[10px] text-gray-600">{timeAgo(entry.timestamp)}</span>
                      </div>
                      <div className="prose prose-invert prose-xs max-w-none break-words [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_h2]:text-sm [&_h2]:font-bold [&_h2]:text-white [&_h2]:mt-3 [&_h2]:mb-1 [&_h3]:text-xs [&_h3]:font-bold [&_h3]:text-white [&_h3]:mt-2 [&_h3]:mb-1 [&_p]:text-xs [&_p]:leading-relaxed [&_p]:my-1 [&_strong]:text-white [&_ul]:text-xs [&_ul]:my-1 [&_ul]:pl-4 [&_ol]:text-xs [&_ol]:my-1 [&_ol]:pl-4 [&_li]:my-0.5 [&_hr]:border-white/10 [&_hr]:my-2 [&_code]:text-juice-orange [&_code]:text-[11px] [&_code]:bg-white/5 [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded [&_table]:w-full [&_table]:text-xs [&_table]:my-2 [&_table]:border-collapse [&_th]:text-left [&_th]:text-gray-400 [&_th]:font-medium [&_th]:border-b [&_th]:border-white/10 [&_th]:px-2 [&_th]:py-1 [&_td]:text-gray-300 [&_td]:border-b [&_td]:border-white/5 [&_td]:px-2 [&_td]:py-1">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>{entry.content}</ReactMarkdown>
                      </div>
                      {entry.entry_type === 'hypothesis' && entry.outcome_verdict && (
                        <details className="text-xs text-gray-400 border-t border-white/5 pt-1.5 mt-1.5">
                          <summary className="cursor-pointer hover:text-gray-300 transition-colors">
                            Verdict ({entry.outcome_confidence ? (entry.outcome_confidence * 100).toFixed(0) : '?'}% confidence)
                            {entry.trade_pnl_attribution != null && (
                              <span className={entry.trade_pnl_attribution >= 0 ? 'text-green-400 ml-2' : 'text-red-400 ml-2'}>
                                P&amp;L: {entry.trade_pnl_attribution >= 0 ? '+' : ''}{entry.trade_pnl_attribution.toFixed(4)}
                              </span>
                            )}
                          </summary>
                          <p className="mt-1 text-gray-500 leading-relaxed">{entry.outcome_verdict}</p>
                        </details>
                      )}
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
              </>
            )}
          </div>
        )}

        {/* Wiki View */}
        {tab === 'wiki' && (
          <div className="flex-1 overflow-y-auto">
            <WikiBrowser />
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
                onChange={e => {
                  setInput(e.target.value);
                  // Auto-resize: reset then grow to content, capped at 38vh
                  const el = e.target;
                  el.style.height = 'auto';
                  el.style.height = Math.min(el.scrollHeight, window.innerHeight * 0.38) + 'px';
                }}
                onKeyDown={handleKeyDown}
                placeholder="Ask the bot..."
                rows={1}
                className="flex-1 bg-white/5 border border-white/10 text-white text-sm px-3 py-2 resize-none focus:outline-none focus:border-juice-orange/50 placeholder-gray-600 overflow-y-auto"
                style={{ maxHeight: '38vh' }}
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
