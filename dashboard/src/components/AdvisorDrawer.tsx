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

// ─── Ops Types ──────────────────────────────────────────────────────────────

interface OpsStats {
  active_rules: number; pending_count: number; confirmed_count: number;
  executed_count: number; rejected_count: number; failed_count: number;
  orders_24h: number; current_advisory_id: string | null;
  advisory_created_at: string | null;
}

interface TradingRule {
  id: number; rule_type: string; action: string; instrument_name: string | null;
  criteria: string; budget_limit: number | null; priority: string;
  reasoning: string | null; created_at: string; advisory_id: string | null;
}

interface PendingAction {
  id: number; rule_id: number | null; action: string; instrument_name: string;
  amount: number | null; price: number | null; trigger_details: string | null;
  status: string; retries: number; triggered_at: string;
  confirmation_reasoning: string | null; confirmed_at: string | null;
  executed_at: string | null; execution_result: string | null;
  rule_reasoning: string | null; rule_priority: string | null; rule_criteria: string | null;
}

interface Order {
  id: number; timestamp: string; action: string; success: number;
  reason: string | null; instrument_name: string | null;
  strike: number | null; expiry: string | null; delta: number | null;
  price: number | null; intended_amount: number | null; filled_amount: number | null;
  fill_price: number | null; total_value: number | null; spot_price: number | null;
}

interface OpsAssessment {
  content: string;
  timestamp: string;
}

interface PortfolioSnapshot {
  timestamp: string; spot_price: number; usdc_balance: number; eth_balance: number;
  total_unrealized_pnl: number; total_realized_pnl: number; portfolio_value_usd: number;
}

interface RealizedPnL {
  net_realized_pnl: number; total_put_cost: number; total_put_revenue: number;
  total_call_revenue: number; total_call_cost: number;
  successful_orders: number; total_orders: number;
}

interface OpsData {
  stats: OpsStats | null;
  rules: TradingRule[];
  actions: PendingAction[];
  orders: Order[];
  assessment: OpsAssessment | null;
  portfolio?: PortfolioSnapshot | null;
  pnl?: RealizedPnL | null;
}

const ACTION_STYLES: Record<string, { label: string; color: string }> = {
  buy_put: { label: 'BUY PUT', color: 'bg-red-500/20 text-red-400' },
  sell_put: { label: 'SELL PUT', color: 'bg-green-500/20 text-green-400' },
  sell_call: { label: 'SELL CALL', color: 'bg-amber-500/20 text-amber-400' },
  buyback_call: { label: 'BUY CALL', color: 'bg-blue-500/20 text-blue-400' },
};

const STATUS_STYLES: Record<string, { color: string }> = {
  pending: { color: 'bg-yellow-500/20 text-yellow-400' },
  confirmed: { color: 'bg-blue-500/20 text-blue-400' },
  executed: { color: 'bg-green-500/20 text-green-400' },
  rejected: { color: 'bg-gray-500/20 text-gray-400' },
  failed: { color: 'bg-red-500/20 text-red-400' },
};

const PRIORITY_STYLES: Record<string, { color: string }> = {
  urgent: { color: 'text-red-400' },
  high: { color: 'text-amber-400' },
  medium: { color: 'text-gray-400' },
  low: { color: 'text-gray-600' },
};

export default function AdvisorDrawer() {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<'chat' | 'journal' | 'wiki' | 'ops'>('journal');
  const [chats, setChats] = useState<Chat[]>([]);
  const [activeChatId, setActiveChatId] = useState<string | null>(null);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [journalEntries, setJournalEntries] = useState<JournalEntry[]>([]);
  const [journalLoading, setJournalLoading] = useState(false);
  const [analyticsTab, setAnalyticsTab] = useState(false);
  const [journalFilter, setJournalFilter] = useState<string | null>(null);
  const [opsData, setOpsData] = useState<OpsData>({ stats: null, rules: [], actions: [], orders: [], assessment: null });
  const [opsLoading, setOpsLoading] = useState(false);
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

  const fetchOps = useCallback(async () => {
    setOpsLoading(true);
    try {
      const res = await fetch('/api/ops');
      if (res.ok) {
        const data = await res.json();
        setOpsData(data);
      }
    } catch { /* silent */ }
    setOpsLoading(false);
  }, []);

  useEffect(() => {
    if (open && tab === 'ops') {
      fetchOps();
      const id = setInterval(fetchOps, 30_000);
      return () => clearInterval(id);
    }
  }, [open, tab, fetchOps]);

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
      const res = await fetch('/api/ai/research-chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
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
  }, [activeChatId, createNewChat, saveChatsDebounced, scrollToBottom, streaming]);

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
              onClick={() => setTab('ops')}
              className={`text-xs px-3 py-1 transition-colors ${
                tab === 'ops'
                  ? 'bg-white/10 text-white'
                  : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Ops{opsData.stats?.pending_count ? ` (${opsData.stats.pending_count})` : ''}
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
                    <p className="text-gray-500 text-xs">No journal entries yet.</p>
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

        {/* Ops View */}
        {tab === 'ops' && (
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
            <div className="flex items-center border-b border-white/5 pb-2">
              <span className="text-[10px] text-gray-500 uppercase tracking-wider">Trading Operations</span>
              <button
                onClick={fetchOps}
                className="text-[11px] px-2.5 py-1 text-gray-600 hover:text-gray-300 transition-colors ml-auto"
              >
                refresh
              </button>
            </div>

            {opsLoading && !opsData.stats && (
              <p className="text-gray-500 text-xs">Loading ops data...</p>
            )}

            {opsData.stats && (
              <div className="space-y-3">
                {/* Status Banner */}
                <div className="bg-white/5 border border-white/10 px-3 py-2.5">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-[10px] text-gray-500 uppercase tracking-wider">LLM Brain Status</span>
                    <span className="text-[10px] text-gray-600">
                      advisory {opsData.stats.advisory_created_at ? timeAgo(opsData.stats.advisory_created_at) : 'never'}
                      {opsData.stats.current_advisory_id && (
                        <span className="text-gray-700 ml-1">({opsData.stats.current_advisory_id.slice(0, 8)})</span>
                      )}
                    </span>
                  </div>
                  <div className="grid grid-cols-3 gap-2 text-center">
                    <div>
                      <p className="text-lg font-bold text-white">{opsData.stats.active_rules}</p>
                      <p className="text-[10px] text-gray-500">rules</p>
                    </div>
                    <div>
                      <p className="text-lg font-bold text-yellow-400">{opsData.stats.pending_count}</p>
                      <p className="text-[10px] text-gray-500">pending</p>
                    </div>
                    <div>
                      <p className="text-lg font-bold text-green-400">{opsData.stats.orders_24h}</p>
                      <p className="text-[10px] text-gray-500">orders 24h</p>
                    </div>
                  </div>
                  <div className="flex gap-3 mt-2 pt-2 border-t border-white/5 text-[10px]">
                    <span className="text-green-400">{opsData.stats.executed_count} executed</span>
                    <span className="text-blue-400">{opsData.stats.confirmed_count} confirmed</span>
                    <span className="text-gray-400">{opsData.stats.rejected_count} rejected</span>
                    <span className="text-red-400">{opsData.stats.failed_count} failed</span>
                  </div>
                </div>

                {/* Portfolio P&L */}
                {(opsData.portfolio || opsData.pnl) && (
                  <div className="bg-white/5 border border-white/10 px-3 py-2.5">
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-2">Portfolio P&L</p>
                    {opsData.portfolio && (
                      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] mb-2">
                        <div className="flex justify-between">
                          <span className="text-gray-500">Portfolio Value</span>
                          <span className="text-white font-mono">${Number(opsData.portfolio.portfolio_value_usd).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Unrealized P&L</span>
                          <span className={`font-mono ${Number(opsData.portfolio.total_unrealized_pnl) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                            {Number(opsData.portfolio.total_unrealized_pnl) >= 0 ? '+' : ''}${Number(opsData.portfolio.total_unrealized_pnl).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </span>
                        </div>
                      </div>
                    )}
                    {opsData.pnl && (
                      <div className="space-y-1 text-[11px]">
                        <div className="flex justify-between">
                          <span className="text-gray-500">Realized P&L</span>
                          <span className={`font-mono ${Number(opsData.pnl.net_realized_pnl) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                            {Number(opsData.pnl.net_realized_pnl) >= 0 ? '+' : ''}${Number(opsData.pnl.net_realized_pnl).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </span>
                        </div>
                        <div className="flex justify-between text-[10px]">
                          <span className="text-gray-600">Puts: cost ${Number(opsData.pnl.total_put_cost).toFixed(2)} / rev ${Number(opsData.pnl.total_put_revenue).toFixed(2)}</span>
                        </div>
                        <div className="flex justify-between text-[10px]">
                          <span className="text-gray-600">Calls: rev ${Number(opsData.pnl.total_call_revenue).toFixed(2)} / cost ${Number(opsData.pnl.total_call_cost).toFixed(2)}</span>
                        </div>
                        <div className="flex justify-between text-[10px] pt-1 border-t border-white/5">
                          <span className="text-gray-600">{opsData.pnl.successful_orders}/{opsData.pnl.total_orders} orders successful</span>
                        </div>
                      </div>
                    )}
                    {opsData.portfolio && (
                      <p className="text-[9px] text-gray-700 mt-1">snapshot {timeAgo(opsData.portfolio.timestamp)}</p>
                    )}
                  </div>
                )}

                {/* Advisory Assessment */}
                {opsData.assessment && (
                  <div className="bg-white/5 border border-white/10 px-3 py-2.5">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[10px] text-gray-500 uppercase tracking-wider">Advisory Assessment</span>
                      <span className="text-[10px] text-gray-600">{timeAgo(opsData.assessment.timestamp)}</span>
                    </div>
                    <p className="text-[11px] text-gray-300 leading-relaxed">{opsData.assessment.content}</p>
                  </div>
                )}

                {/* Trading Rules */}
                {opsData.rules.length > 0 && (
                  <div>
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">Active Rules ({opsData.rules.length})</p>
                    {opsData.rules.map((rule) => {
                      const actionStyle = ACTION_STYLES[rule.action] || { label: rule.action, color: 'bg-white/10 text-gray-400' };
                      const priorityStyle = PRIORITY_STYLES[rule.priority] || { color: 'text-gray-500' };
                      let criteriaDisplay: string = '';
                      let criteriaIsJson = false;
                      if (rule.criteria) {
                        try {
                          const parsed = JSON.parse(rule.criteria);
                          if (typeof parsed === 'object' && parsed !== null && Object.keys(parsed).length > 0) {
                            criteriaDisplay = JSON.stringify(parsed, null, 2);
                            criteriaIsJson = true;
                          } else if (typeof parsed === 'string' && parsed.trim()) {
                            criteriaDisplay = parsed;
                          }
                        } catch {
                          // Not JSON — treat as plain text criteria
                          criteriaDisplay = rule.criteria;
                        }
                      }
                      // Summary snippet: first 80 chars of criteria for collapsed view
                      const criteriaSnippet = criteriaDisplay
                        ? (criteriaIsJson ? '' : criteriaDisplay.slice(0, 80) + (criteriaDisplay.length > 80 ? '...' : ''))
                        : '';
                      return (
                        <details key={rule.id} className="border border-white/5 mb-1.5">
                          <summary className="px-3 py-2 cursor-pointer hover:bg-white/5 transition-colors">
                            <div className="inline-flex items-center gap-1.5 flex-wrap">
                              <span className={`text-[10px] font-bold px-1.5 py-0.5 ${actionStyle.color}`}>
                                {actionStyle.label}
                              </span>
                              <span className={`text-[10px] ${priorityStyle.color}`}>{rule.priority}</span>
                              <span className="text-[10px] text-gray-600">{rule.rule_type}</span>
                              {rule.instrument_name && (
                                <span className="text-[10px] text-gray-500 font-mono">{rule.instrument_name}</span>
                              )}
                              {rule.budget_limit && (
                                <span className="text-[10px] text-gray-500">${rule.budget_limit.toFixed(0)} limit</span>
                              )}
                            </div>
                            {criteriaSnippet && (
                              <p className="text-[10px] text-gray-600 mt-0.5 ml-1 truncate">{criteriaSnippet}</p>
                            )}
                          </summary>
                          <div className="px-3 pb-2.5 space-y-1.5 border-t border-white/5">
                            {rule.reasoning && (
                              <div className="mt-1.5">
                                <p className="text-[10px] text-gray-600 uppercase">Reasoning</p>
                                <p className="text-[11px] text-gray-300 leading-relaxed">{rule.reasoning}</p>
                              </div>
                            )}
                            {criteriaDisplay && (
                              <div>
                                <p className="text-[10px] text-gray-600 uppercase">Criteria</p>
                                {criteriaIsJson ? (
                                  <pre className="text-[10px] text-gray-500 font-mono bg-black/30 px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
                                    {criteriaDisplay}
                                  </pre>
                                ) : (
                                  <p className="text-[11px] text-gray-400 leading-relaxed">{criteriaDisplay}</p>
                                )}
                              </div>
                            )}
                            <p className="text-[10px] text-gray-600">Rule #{rule.id} &middot; Created {rule.created_at}</p>
                          </div>
                        </details>
                      );
                    })}
                  </div>
                )}

                {/* Action Pipeline */}
                {opsData.actions.length > 0 && (
                  <div>
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">Action Pipeline ({opsData.actions.length})</p>
                    {opsData.actions.map((a) => {
                      const actionStyle = ACTION_STYLES[a.action] || { label: a.action, color: 'bg-white/10 text-gray-400' };
                      const statusStyle = STATUS_STYLES[a.status] || { color: 'bg-white/10 text-gray-400' };
                      let triggerDetails: Record<string, unknown> = {};
                      try { if (a.trigger_details) triggerDetails = JSON.parse(a.trigger_details); } catch { /* skip */ }
                      let confirmReasoning: Record<string, unknown> | string = '';
                      try { if (a.confirmation_reasoning) confirmReasoning = JSON.parse(a.confirmation_reasoning); } catch { confirmReasoning = a.confirmation_reasoning || ''; }
                      let execResult: Record<string, unknown> = {};
                      try { if (a.execution_result) execResult = JSON.parse(a.execution_result); } catch { /* skip */ }
                      return (
                        <details key={a.id} className="border border-white/5 mb-1.5">
                          <summary className="px-3 py-2 cursor-pointer hover:bg-white/5 transition-colors">
                            <div className="inline-flex items-center gap-1.5 flex-wrap">
                              <span className={`text-[10px] font-bold px-1.5 py-0.5 ${actionStyle.color}`}>
                                {actionStyle.label}
                              </span>
                              <span className={`text-[10px] px-1.5 py-0.5 ${statusStyle.color}`}>
                                {a.status}
                              </span>
                              <span className="text-[10px] text-gray-500 font-mono truncate max-w-[160px]">{a.instrument_name}</span>
                              {a.price && <span className="text-[10px] text-gray-500">@ ${a.price.toFixed(2)}</span>}
                              {a.amount && <span className="text-[10px] text-gray-500">x{a.amount}</span>}
                              {a.retries > 0 && <span className="text-[10px] text-red-400">retry {a.retries}</span>}
                              <span className="text-[10px] text-gray-600 ml-auto">{timeAgo(a.triggered_at)}</span>
                            </div>
                          </summary>
                          <div className="px-3 pb-2.5 space-y-1.5 border-t border-white/5">
                            {/* Timeline */}
                            <div className="mt-1.5 flex gap-3 flex-wrap text-[10px] text-gray-500">
                              <span>triggered {a.triggered_at}</span>
                              {a.confirmed_at && <span>confirmed {a.confirmed_at}</span>}
                              {a.executed_at && <span>executed {a.executed_at}</span>}
                            </div>
                            {/* Rule context */}
                            {a.rule_reasoning && (
                              <div>
                                <p className="text-[10px] text-gray-600 uppercase">Rule Reasoning</p>
                                <p className="text-[11px] text-gray-400 leading-relaxed">{a.rule_reasoning}</p>
                              </div>
                            )}
                            {/* Trigger details */}
                            {Object.keys(triggerDetails).length > 0 && (
                              <div>
                                <p className="text-[10px] text-gray-600 uppercase">Trigger Conditions</p>
                                <pre className="text-[10px] text-gray-500 font-mono bg-black/30 px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
                                  {JSON.stringify(triggerDetails, null, 2)}
                                </pre>
                              </div>
                            )}
                            {/* Confirmation reasoning */}
                            {confirmReasoning && (
                              <div>
                                <p className="text-[10px] text-gray-600 uppercase">Confirmation Votes</p>
                                {typeof confirmReasoning === 'string' ? (
                                  <p className="text-[11px] text-gray-400 leading-relaxed">{confirmReasoning}</p>
                                ) : (
                                  <pre className="text-[10px] text-gray-500 font-mono bg-black/30 px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
                                    {JSON.stringify(confirmReasoning, null, 2)}
                                  </pre>
                                )}
                              </div>
                            )}
                            {/* Execution result */}
                            {Object.keys(execResult).length > 0 && (
                              <div>
                                <p className="text-[10px] text-gray-600 uppercase">Execution Result</p>
                                <pre className="text-[10px] text-gray-500 font-mono bg-black/30 px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
                                  {JSON.stringify(execResult, null, 2)}
                                </pre>
                              </div>
                            )}
                          </div>
                        </details>
                      );
                    })}
                  </div>
                )}

                {/* Orders */}
                {opsData.orders.length > 0 && (
                  <div>
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">Recent Orders ({opsData.orders.length})</p>
                    {opsData.orders.map((o) => (
                      <details key={o.id} className="border border-white/5 mb-1.5">
                        <summary className="px-3 py-2 cursor-pointer hover:bg-white/5 transition-colors">
                          <div className="inline-flex items-center gap-1.5">
                            <span className={`text-[10px] font-bold px-1.5 py-0.5 ${o.success ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
                              {o.success ? 'FILLED' : 'FAILED'}
                            </span>
                            <span className={`text-[10px] font-bold px-1.5 py-0.5 ${ACTION_STYLES[o.action]?.color || 'bg-white/10 text-gray-400'}`}>
                              {ACTION_STYLES[o.action]?.label || o.action}
                            </span>
                            <span className="text-[10px] text-gray-500 font-mono truncate max-w-[140px]">{o.instrument_name}</span>
                            {o.total_value != null && <span className="text-[10px] text-white">${o.total_value.toFixed(2)}</span>}
                            <span className="text-[10px] text-gray-600">{timeAgo(o.timestamp)}</span>
                          </div>
                        </summary>
                        <div className="px-3 pb-2.5 border-t border-white/5 mt-0">
                          <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[10px] text-gray-500">
                            <span>Time: {o.timestamp}</span>
                            {o.spot_price != null && <span>Spot: ${o.spot_price.toFixed(2)}</span>}
                            {o.strike != null && <span>Strike: {o.strike}</span>}
                            {o.delta != null && <span>Delta: {o.delta.toFixed(4)}</span>}
                            {o.intended_amount != null && <span>Intended: {o.intended_amount}</span>}
                            {o.filled_amount != null && <span>Filled: {o.filled_amount}</span>}
                            {o.price != null && <span>Limit: ${o.price.toFixed(2)}</span>}
                            {o.fill_price != null && <span>Fill: ${o.fill_price.toFixed(2)}</span>}
                          </div>
                          {o.reason && <p className="text-[10px] text-gray-500 mt-1">{o.reason}</p>}
                        </div>
                      </details>
                    ))}
                  </div>
                )}

                {opsData.rules.length === 0 && opsData.actions.length === 0 && opsData.orders.length === 0 && (
                  <div className="pt-4 text-center">
                    <p className="text-gray-500 text-xs">No trading activity yet.</p>
                    <p className="text-gray-600 text-[10px] mt-1">The advisory council generates rules every 8h, or on first boot if none exist.</p>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Chat Messages */}
        {tab === 'chat' && (
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-4">
            {messages.length === 0 && (
              <div className="space-y-3 pt-4">
                <p className="text-gray-500 text-xs">Ask about the wiki, journals, market state, or how the strategy works. This chat is read-only.</p>
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
            <p className="mb-3 text-xs text-gray-500">Read-only research chat. No executions, no journal writes, no wiki ingest.</p>
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
