'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

type WikiPageStatus = 'current' | 'stale' | 'needs_attention' | 'unreviewed' | 'missing';

interface WikiPageMeta {
  path: string;
  title: string;
  category: string;
  wordCount: number;
  lastModified: string;
  lastReviewed: string | null;
  lastEvidenceAt: string | null;
  lastCheckedAt: string | null;
  summary: string;
  status: WikiPageStatus;
  issues: string[];
  changeSummary: string | null;
  evidenceCount: number;
  freshnessDays: number;
  owner: 'wiki' | 'learning';
  briefing: boolean;
}

interface WikiPageDetail {
  path: string;
  title: string;
  content: string;
  wordCount: number;
  lastModified: string;
  history: { timestamp: string; size: number }[];
}

interface SearchResult {
  path: string;
  title: string;
  snippets: string[];
}

interface WikiSummary {
  current: number;
  stale: number;
  needsAttention: number;
  unreviewed: number;
  missing: number;
  lastLint: string | null;
  lastLintAttempt: string | null;
  lastLintError: string | null;
  nextLintAt: string | null;
  manualReviewPending: number;
  lastIngest: string | null;
}

const EMPTY_SUMMARY: WikiSummary = {
  current: 0,
  stale: 0,
  needsAttention: 0,
  unreviewed: 0,
  missing: 0,
  lastLint: null,
  lastLintAttempt: null,
  lastLintError: null,
  nextLintAt: null,
  manualReviewPending: 0,
  lastIngest: null,
};

const CATEGORY_LABELS: Record<string, { label: string; color: string }> = {
  regimes: { label: 'Regimes', color: 'text-purple-400' },
  protection: { label: 'Protection', color: 'text-blue-400' },
  revenue: { label: 'Revenue', color: 'text-emerald-400' },
  indicators: { label: 'Indicators', color: 'text-amber-400' },
  strategy: { label: 'Strategy', color: 'text-green-400' },
};

const STATUS_META: Record<WikiPageStatus, { label: string; className: string; dot: string }> = {
  current: { label: 'current', className: 'text-emerald-300 border-emerald-500/25 bg-emerald-500/10', dot: 'bg-emerald-400' },
  stale: { label: 'stale', className: 'text-amber-300 border-amber-500/25 bg-amber-500/10', dot: 'bg-amber-400' },
  needs_attention: { label: 'attention', className: 'text-red-300 border-red-500/25 bg-red-500/10', dot: 'bg-red-400' },
  unreviewed: { label: 'unreviewed', className: 'text-gray-400 border-white/10 bg-white/5', dot: 'bg-gray-500' },
  missing: { label: 'missing', className: 'text-red-300 border-red-500/25 bg-red-500/10', dot: 'bg-red-400' },
};

function timeAgo(ts: string): string {
  const parsed = new Date(ts).getTime();
  if (!Number.isFinite(parsed) || parsed <= 0) return 'never';
  const diff = Math.max(0, Date.now() - parsed);
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function formatRelative(ts: string | null | undefined): string {
  return ts ? timeAgo(ts) : 'never';
}

function timeUntil(ts: string | null | undefined): string {
  if (!ts) return 'soon';
  const parsed = new Date(ts).getTime();
  if (!Number.isFinite(parsed)) return 'soon';
  const diff = parsed - Date.now();
  if (diff <= 0) return 'now';
  const mins = Math.ceil(diff / 60000);
  if (mins < 60) return `in ${mins}m`;
  const hrs = Math.ceil(mins / 60);
  return hrs < 24 ? `in ${hrs}h` : `in ${Math.ceil(hrs / 24)}d`;
}

function StatusPill({ status }: { status: WikiPageStatus }) {
  const meta = STATUS_META[status];
  return (
    <span className={`inline-flex items-center gap-1 border px-1.5 py-0.5 text-[9px] uppercase tracking-wider ${meta.className}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
      {meta.label}
    </span>
  );
}

function PageOwnership({ owner }: { owner: WikiPageMeta['owner'] }) {
  return owner === 'learning' ? (
    <span className="border border-blue-500/20 bg-blue-500/5 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-blue-300">
      Learning-derived
    </span>
  ) : (
    <span className="border border-white/10 bg-white/[0.03] px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-gray-500">
      Wiki research
    </span>
  );
}

export default function WikiBrowser() {
  const [pages, setPages] = useState<WikiPageMeta[]>([]);
  const [briefing, setBriefing] = useState<WikiPageMeta[]>([]);
  const [attention, setAttention] = useState<WikiPageMeta[]>([]);
  const [recentChanges, setRecentChanges] = useState<WikiPageMeta[]>([]);
  const [summary, setSummary] = useState<WikiSummary>(EMPTY_SUMMARY);
  const [view, setView] = useState<'briefing' | 'library'>('briefing');
  const [selectedPage, setSelectedPage] = useState<string | null>(null);
  const [pageDetail, setPageDetail] = useState<WikiPageDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[] | null>(null);
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    fetch('/api/wiki/pages')
      .then((response) => response.json())
      .then((data) => {
        setPages(data.pages || []);
        setBriefing(data.briefing || []);
        setAttention(data.attention || []);
        setRecentChanges(data.recentChanges || []);
        setSummary(data.summary || EMPTY_SUMMARY);
      })
      .catch(() => {});
  }, []);

  const selectedMeta = useMemo(
    () => pages.find((page) => page.path === selectedPage) || null,
    [pages, selectedPage],
  );

  const categories = useMemo(() => pages.reduce<Record<string, WikiPageMeta[]>>((acc, page) => {
    if (!acc[page.category]) acc[page.category] = [];
    acc[page.category].push(page);
    return acc;
  }, {}), [pages]);

  const loadPage = useCallback(async (pagePath: string) => {
    setSelectedPage(pagePath);
    setSearchResults(null);
    setPageDetail(null);
    setPageError(null);
    setLoading(true);
    try {
      const slug = pagePath.replace('.md', '');
      const response = await fetch(`/api/wiki/${slug}`);
      if (!response.ok) {
        const data = await response.json().catch(() => ({ error: 'Failed to load page' }));
        setPageError(data.error || `Failed to load ${pagePath}`);
      } else {
        setPageDetail(await response.json());
      }
    } catch {
      setPageError(`Failed to load ${pagePath}`);
    }
    setLoading(false);
  }, []);

  const handleSearch = useCallback(async () => {
    if (searchQuery.trim().length < 2) return;
    setSearching(true);
    setSelectedPage(null);
    setPageDetail(null);
    setPageError(null);
    try {
      const response = await fetch(`/api/wiki/search?q=${encodeURIComponent(searchQuery.trim())}`);
      if (response.ok) {
        const data = await response.json();
        setSearchResults(data.results || []);
      }
    } catch { /* non-fatal */ }
    setSearching(false);
  }, [searchQuery]);

  const returnHome = () => {
    setSelectedPage(null);
    setPageDetail(null);
    setPageError(null);
    setSearchResults(null);
  };

  return (
    <div className="flex h-full flex-col">
      <div className="sticky top-0 z-10 border-b border-white/5 bg-[#111]/95 px-3 py-2 backdrop-blur">
        <div className="flex gap-1.5">
          <input
            type="text"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            onKeyDown={(event) => event.key === 'Enter' && handleSearch()}
            placeholder="Search evidence, regimes, rules..."
            className="flex-1 border border-white/10 bg-white/5 px-2 py-1.5 text-[11px] text-white placeholder-gray-600 focus:border-juice-orange/50 focus:outline-none"
          />
          <button
            onClick={handleSearch}
            disabled={searchQuery.trim().length < 2}
            className="bg-white/10 px-2 py-1 text-[10px] text-gray-400 transition-colors hover:text-white disabled:text-gray-600"
          >
            Search
          </button>
        </div>
        {!selectedPage && searchResults === null && (
          <div className="mt-2 flex gap-1">
            {(['briefing', 'library'] as const).map((item) => (
              <button
                key={item}
                onClick={() => setView(item)}
                className={`px-2 py-1 text-[10px] uppercase tracking-wider transition-colors ${view === item ? 'bg-white/10 text-white' : 'text-gray-600 hover:text-gray-300'}`}
              >
                {item}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="flex-1 overflow-y-auto">
        {searchResults !== null && (
          <div className="space-y-2 px-3 py-3">
            <div className="flex items-center justify-between">
              <p className="text-[10px] text-gray-500">
                {searching ? 'Searching…' : `${searchResults.length} result${searchResults.length === 1 ? '' : 's'} for “${searchQuery}”`}
              </p>
              <button onClick={() => { setSearchResults(null); setSearchQuery(''); }} className="text-[10px] text-gray-600 hover:text-gray-300">Clear</button>
            </div>
            {searchResults.map((result) => (
              <button key={result.path} onClick={() => loadPage(result.path)} className="block w-full border border-white/5 px-3 py-2 text-left transition-colors hover:border-white/20 hover:bg-white/[0.02]">
                <p className="text-xs font-medium text-white">{result.title}</p>
                {result.snippets.map((snippet, index) => (
                  <p key={index} className="mt-1 line-clamp-2 text-[10px] leading-relaxed text-gray-400">{snippet}</p>
                ))}
              </button>
            ))}
          </div>
        )}

        {selectedPage && searchResults === null && (
          <div className="space-y-3 px-3 py-3">
            <button onClick={returnHome} className="text-[10px] text-gray-500 transition-colors hover:text-gray-300">← Back to Wiki</button>
            {selectedMeta && (
              <div className="border border-white/10 bg-white/[0.02] p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="mr-auto text-sm font-bold text-white">{selectedMeta.title}</p>
                  <StatusPill status={selectedMeta.status} />
                  <PageOwnership owner={selectedMeta.owner} />
                </div>
                <p className="mt-2 text-xs leading-relaxed text-gray-300">{selectedMeta.summary}</p>
                <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-[10px] text-gray-500 sm:grid-cols-4">
                  <span>evidence {selectedMeta.evidenceCount}</span>
                  <span>changed {formatRelative(selectedMeta.lastModified)}</span>
                  <span>validated {formatRelative(selectedMeta.lastReviewed)}</span>
                  <span>freshness {selectedMeta.freshnessDays}d</span>
                </div>
                {selectedMeta.changeSummary && <p className="mt-2 border-t border-white/5 pt-2 text-[10px] text-gray-400">Latest change: {selectedMeta.changeSummary}</p>}
                {selectedMeta.issues.length > 0 && (
                  <div className="mt-2 border border-red-500/20 bg-red-500/5 p-2">
                    <p className="text-[10px] uppercase tracking-wider text-red-300">Validation issues</p>
                    {selectedMeta.issues.map((issue) => <p key={issue} className="mt-1 text-[10px] text-red-200/80">{issue}</p>)}
                  </div>
                )}
              </div>
            )}

            {loading && <p className="text-[10px] text-gray-500">Loading page…</p>}
            {!loading && pageError && <div className="border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs text-red-400">{pageError}</div>}
            {pageDetail && (
              <>
                {pageDetail.history.length > 0 && (
                  <details className="border border-white/5 px-3 py-2 text-[10px] text-gray-500">
                    <summary className="cursor-pointer transition-colors hover:text-gray-300">{pageDetail.history.length} previous version{pageDetail.history.length === 1 ? '' : 's'}</summary>
                    <div className="mt-2 space-y-1">
                      {pageDetail.history.map((version) => (
                        <p key={`${version.timestamp}-${version.size}`} className="text-gray-600">{new Date(version.timestamp).toLocaleString()} · {version.size} bytes</p>
                      ))}
                    </div>
                  </details>
                )}
                <div className="prose prose-invert prose-xs max-w-none break-words border border-white/5 bg-black/10 p-3 [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_h1]:mb-2 [&_h1]:mt-3 [&_h1]:text-sm [&_h1]:font-bold [&_h1]:text-juice-orange [&_h2]:mb-1 [&_h2]:mt-3 [&_h2]:text-xs [&_h2]:font-bold [&_h2]:text-white [&_h3]:mb-1 [&_h3]:mt-2 [&_h3]:text-xs [&_h3]:font-bold [&_h3]:text-gray-300 [&_li]:my-0.5 [&_ol]:my-1 [&_ol]:pl-4 [&_p]:my-1 [&_p]:text-xs [&_p]:leading-relaxed [&_strong]:text-white [&_table]:my-2 [&_table]:w-full [&_table]:border-collapse [&_table]:text-xs [&_td]:border-b [&_td]:border-white/5 [&_td]:px-2 [&_td]:py-1 [&_th]:border-b [&_th]:border-white/10 [&_th]:px-2 [&_th]:py-1 [&_th]:text-left [&_th]:font-medium [&_th]:text-gray-400 [&_ul]:my-1 [&_ul]:pl-4">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{pageDetail.content}</ReactMarkdown>
                </div>
              </>
            )}
          </div>
        )}

        {!selectedPage && searchResults === null && view === 'briefing' && (
          <div className="space-y-4 px-3 py-3">
            <section>
              <div className="flex items-end justify-between gap-3">
                <div>
                  <p className="text-[10px] uppercase tracking-[0.18em] text-juice-orange">Knowledge briefing</p>
                  <p className="mt-1 text-xs text-gray-400">What the system currently believes—and how trustworthy it is.</p>
                </div>
                <p className="text-right text-[9px] leading-relaxed text-gray-600">ingest {formatRelative(summary.lastIngest)}<br />lint {formatRelative(summary.lastLint)}</p>
              </div>
              <div className="mt-3 grid grid-cols-4 gap-1.5">
                {[
                  ['current', summary.current, 'text-emerald-300'],
                  ['stale', summary.stale, 'text-amber-300'],
                  ['attention', summary.needsAttention, 'text-red-300'],
                  ['unreviewed', summary.unreviewed + summary.missing, 'text-gray-400'],
                ].map(([label, value, color]) => (
                  <div key={String(label)} className="border border-white/5 bg-white/[0.02] px-2 py-2">
                    <p className={`text-lg leading-none ${color}`}>{value}</p>
                    <p className="mt-1 text-[9px] uppercase tracking-wider text-gray-600">{label}</p>
                  </div>
                ))}
              </div>
              {summary.lastLintError && (
                <div className="mt-2 border border-red-500/20 bg-red-500/5 px-3 py-2 text-[10px] leading-relaxed text-red-200/80">
                  Wiki validation failed {formatRelative(summary.lastLintAttempt)}: {summary.lastLintError}. Next daily validation {timeUntil(summary.nextLintAt)}.
                </div>
              )}
              {!summary.lastLintError && summary.unreviewed > 0 && (
                <div className="mt-2 border border-blue-500/15 bg-blue-500/5 px-3 py-2 text-[10px] leading-relaxed text-blue-200/70">
                  {summary.unreviewed} page{summary.unreviewed === 1 ? '' : 's'} awaiting page-level validation. Next daily validation {timeUntil(summary.nextLintAt)}.
                </div>
              )}
              {summary.manualReviewPending > 0 && (
                <div className="mt-2 border border-amber-500/15 bg-amber-500/5 px-3 py-2 text-[10px] leading-relaxed text-amber-100/70">
                  Manual review: {summary.manualReviewPending} flagged page{summary.manualReviewPending === 1 ? '' : 's'}. Automatic repairs are disabled; findings remain visible until a maintainer edits the page and a daily validation passes.
                </div>
              )}
            </section>

            <section>
              <p className="mb-2 text-[10px] uppercase tracking-[0.18em] text-gray-500">Current view</p>
              <div className="grid gap-2 sm:grid-cols-2">
                {briefing.map((page) => (
                  <button key={page.path} onClick={() => loadPage(page.path)} className="border border-white/[0.07] bg-white/[0.025] p-3 text-left transition-colors hover:border-white/20 hover:bg-white/[0.04]">
                    <div className="flex items-center gap-2">
                      <p className={`text-[9px] uppercase tracking-wider ${CATEGORY_LABELS[page.category]?.color || 'text-gray-500'}`}>{page.title}</p>
                      <span className="ml-auto"><StatusPill status={page.status} /></span>
                    </div>
                    <p className="mt-2 line-clamp-4 text-xs leading-relaxed text-gray-200">{page.summary}</p>
                    <div className="mt-2 flex items-center gap-2 text-[9px] text-gray-600">
                      <span>{page.evidenceCount} evidence</span>
                      <span>·</span>
                      <span>{formatRelative(page.lastEvidenceAt || page.lastModified)}</span>
                    </div>
                  </button>
                ))}
              </div>
            </section>

            <section>
              <div className="mb-2 flex items-center justify-between">
                <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Needs attention</p>
                <button onClick={() => setView('library')} className="text-[9px] text-gray-600 hover:text-gray-300">View library →</button>
              </div>
              {attention.length === 0 ? (
                <div className="border border-emerald-500/10 bg-emerald-500/5 px-3 py-2 text-[10px] text-emerald-300">All Wiki pages are current and validated.</div>
              ) : (
                <div className="divide-y divide-white/5 border border-white/5">
                  {attention.slice(0, 6).map((page) => (
                    <button key={page.path} onClick={() => loadPage(page.path)} className="flex w-full items-center gap-2 px-3 py-2 text-left hover:bg-white/[0.03]">
                      <StatusPill status={page.status} />
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-[11px] text-gray-300">{page.title}</p>
                        <p className="truncate text-[9px] text-gray-600">{page.issues[0] || (page.status === 'stale' ? `Evidence is older than its ${page.freshnessDays}d freshness window` : 'Awaiting page-level validation')}</p>
                      </div>
                      <span className="text-[9px] text-gray-600">{formatRelative(page.lastModified)}</span>
                    </button>
                  ))}
                </div>
              )}
            </section>

            {recentChanges.length > 0 && (
              <section>
                <p className="mb-2 text-[10px] uppercase tracking-[0.18em] text-gray-500">What changed</p>
                <div className="space-y-1">
                  {recentChanges.map((page) => (
                    <button key={page.path} onClick={() => loadPage(page.path)} className="block w-full border-l border-white/10 py-1 pl-3 text-left hover:border-juice-orange/50">
                      <p className="text-[10px] text-gray-300">{page.title} <span className="text-gray-600">· {formatRelative(page.lastModified)}</span></p>
                      <p className="mt-0.5 line-clamp-2 text-[10px] text-gray-500">{page.changeSummary}</p>
                    </button>
                  ))}
                </div>
              </section>
            )}
          </div>
        )}

        {!selectedPage && searchResults === null && view === 'library' && (
          <div className="space-y-4 px-3 py-3">
            <div className="border border-blue-500/10 bg-blue-500/5 px-3 py-2 text-[10px] leading-relaxed text-blue-200/70">
              Market research lives in Wiki. Strategy Lessons, Mistakes, and Playbook are Learning-derived views; canonical execution rules remain in Learning.
            </div>
            {Object.entries(categories).map(([category, categoryPages]) => {
              const categoryMeta = CATEGORY_LABELS[category] || { label: category, color: 'text-gray-400' };
              return (
                <section key={category}>
                  <p className={`mb-1.5 text-[10px] uppercase tracking-[0.18em] ${categoryMeta.color}`}>{categoryMeta.label}</p>
                  <div className="divide-y divide-white/5 border border-white/5">
                    {categoryPages.map((page) => (
                      <button key={page.path} onClick={() => loadPage(page.path)} className="block w-full px-3 py-2.5 text-left transition-colors hover:bg-white/[0.03]">
                        <div className="flex items-center gap-2">
                          <p className="text-xs text-gray-200">{page.title}</p>
                          {page.owner === 'learning' && <PageOwnership owner={page.owner} />}
                          <span className="ml-auto"><StatusPill status={page.status} /></span>
                        </div>
                        <p className="mt-1 line-clamp-2 text-[10px] leading-relaxed text-gray-500">{page.summary}</p>
                        <div className="mt-1.5 flex gap-2 text-[9px] text-gray-600">
                          <span>{page.evidenceCount} evidence</span>
                          <span>changed {formatRelative(page.lastModified)}</span>
                          <span>validated {formatRelative(page.lastReviewed)}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </section>
              );
            })}
            {pages.length === 0 && <p className="py-4 text-center text-xs text-gray-500">No Wiki pages found.</p>}
          </div>
        )}
      </div>
    </div>
  );
}
