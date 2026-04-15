'use client';

import { useState, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface WikiPageMeta {
  path: string;
  title: string;
  category: string;
  wordCount: number;
  lastModified: string;
  lastReviewed: string | null;
}

interface WikiPageDetail {
  path: string;
  title: string;
  content: string;
  wordCount: number;
  lastModified: string;
  lastReviewed?: string | null;
  history: { timestamp: string; size: number }[];
}

interface SearchResult {
  path: string;
  title: string;
  snippets: string[];
}

const CATEGORY_LABELS: Record<string, { label: string; color: string }> = {
  regimes: { label: 'Regimes', color: 'text-purple-400' },
  protection: { label: 'Protection', color: 'text-blue-400' },
  revenue: { label: 'Revenue', color: 'text-emerald-400' },
  indicators: { label: 'Indicators', color: 'text-amber-400' },
  strategy: { label: 'Strategy', color: 'text-green-400' },
};

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function formatRelative(ts: string | null | undefined): string {
  return ts ? timeAgo(ts) : 'never';
}

export default function WikiBrowser() {
  const [pages, setPages] = useState<WikiPageMeta[]>([]);
  const [selectedPage, setSelectedPage] = useState<string | null>(null);
  const [pageDetail, setPageDetail] = useState<WikiPageDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[] | null>(null);
  const [searching, setSearching] = useState(false);

  // Fetch page list
  useEffect(() => {
    fetch('/api/wiki/pages')
      .then(r => r.json())
      .then(data => setPages(data.pages || []))
      .catch(() => {});
  }, []);

  // Fetch page content when selected
  const loadPage = useCallback(async (pagePath: string) => {
    setSelectedPage(pagePath);
    setSearchResults(null);
    setPageDetail(null);
    setPageError(null);
    setLoading(true);
    try {
      const slug = pagePath.replace('.md', '');
      const res = await fetch(`/api/wiki/${slug}`);
      if (res.ok) {
        const data = await res.json();
        const pageMeta = pages.find((page) => page.path === pagePath);
        setPageDetail({ ...data, lastReviewed: pageMeta?.lastReviewed ?? null });
      } else {
        const data = await res.json().catch(() => ({ error: 'Failed to load page' }));
        setPageError(data.error || `Failed to load ${pagePath}`);
      }
    } catch {
      setPageError(`Failed to load ${pagePath}`);
    }
    setLoading(false);
  }, [pages]);

  // Search
  const handleSearch = useCallback(async () => {
    if (searchQuery.length < 2) return;
    setSearching(true);
    setSelectedPage(null);
    setPageDetail(null);
    setPageError(null);
    try {
      const res = await fetch(`/api/wiki/search?q=${encodeURIComponent(searchQuery)}`);
      if (res.ok) {
        const data = await res.json();
        setSearchResults(data.results || []);
      }
    } catch { /* silent */ }
    setSearching(false);
  }, [searchQuery]);

  // Group pages by category
  const categories = pages.reduce<Record<string, WikiPageMeta[]>>((acc, page) => {
    const cat = page.category;
    if (!acc[cat]) acc[cat] = [];
    acc[cat].push(page);
    return acc;
  }, {});

  return (
    <div className="flex flex-col h-full">
      {/* Search */}
      <div className="px-3 py-2 border-b border-white/5">
        <div className="flex gap-1.5">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="Search wiki..."
            className="flex-1 bg-white/5 border border-white/10 text-white text-[11px] px-2 py-1.5 focus:outline-none focus:border-juice-orange/50 placeholder-gray-600"
          />
          <button
            onClick={handleSearch}
            disabled={searchQuery.length < 2}
            className="text-[10px] px-2 py-1 bg-white/10 text-gray-400 hover:text-white disabled:text-gray-600 transition-colors"
          >
            Search
          </button>
        </div>
      </div>

      {/* Content area */}
      <div className="flex-1 overflow-y-auto">
        {/* Search results */}
        {searchResults !== null && (
          <div className="px-3 py-2 space-y-2">
            <div className="flex items-center justify-between">
              <p className="text-[10px] text-gray-500">{searchResults.length} result{searchResults.length !== 1 ? 's' : ''} for &ldquo;{searchQuery}&rdquo;</p>
              <button
                onClick={() => { setSearchResults(null); setSearchQuery(''); }}
                className="text-[10px] text-gray-600 hover:text-gray-400"
              >
                Clear
              </button>
            </div>
            {searching && <p className="text-[10px] text-gray-500">Searching...</p>}
            {searchResults.map((result) => (
              <button
                key={result.path}
                onClick={() => loadPage(result.path)}
                className="block w-full text-left border border-white/5 px-3 py-2 hover:border-white/20 transition-colors"
              >
                <p className="text-xs text-white font-medium">{result.title}</p>
                <p className="text-[10px] text-gray-500">{result.path}</p>
                {result.snippets.map((s, i) => (
                  <p key={i} className="text-[10px] text-gray-400 mt-1 line-clamp-2">{s}</p>
                ))}
              </button>
            ))}
          </div>
        )}

        {/* Page detail view */}
        {selectedPage && !searchResults && (
          <div className="px-3 py-2 space-y-2">
            <button
              onClick={() => { setSelectedPage(null); setPageDetail(null); setPageError(null); }}
              className="text-[10px] text-gray-500 hover:text-gray-300 transition-colors"
            >
              &larr; Back to pages
            </button>
            {pageDetail && (
              <>
                <div className="flex items-center justify-between">
                  <p className="text-xs text-white font-bold">{pageDetail.title}</p>
                  <div className="flex items-center gap-2">
                    <span className="text-[10px] text-gray-600">{pageDetail.wordCount} words</span>
                    <span className="text-[10px] text-gray-500">updated {formatRelative(pageDetail.lastModified)}</span>
                    <span className="text-[10px] text-gray-600">reviewed {formatRelative(pageDetail.lastReviewed)}</span>
                  </div>
                </div>

                {/* History dropdown */}
                {pageDetail.history.length > 0 && (
                  <details className="text-[10px] text-gray-500">
                    <summary className="cursor-pointer hover:text-gray-300 transition-colors">
                      {pageDetail.history.length} previous version{pageDetail.history.length !== 1 ? 's' : ''}
                    </summary>
                    <div className="mt-1 space-y-0.5 pl-2">
                      {pageDetail.history.map((v, i) => (
                        <p key={i} className="text-gray-600">
                          {new Date(v.timestamp).toLocaleDateString()} {new Date(v.timestamp).toLocaleTimeString()} ({v.size} bytes)
                        </p>
                      ))}
                    </div>
                  </details>
                )}

                {/* Page content rendered as markdown */}
                {loading ? (
                  <p className="text-[10px] text-gray-500">Loading...</p>
                ) : (
                  <div className="prose prose-invert prose-xs max-w-none break-words [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_h1]:text-sm [&_h1]:font-bold [&_h1]:text-juice-orange [&_h1]:mt-3 [&_h1]:mb-2 [&_h2]:text-xs [&_h2]:font-bold [&_h2]:text-white [&_h2]:mt-3 [&_h2]:mb-1 [&_h3]:text-xs [&_h3]:font-bold [&_h3]:text-gray-300 [&_h3]:mt-2 [&_h3]:mb-1 [&_p]:text-xs [&_p]:leading-relaxed [&_p]:my-1 [&_strong]:text-white [&_ul]:text-xs [&_ul]:my-1 [&_ul]:pl-4 [&_ol]:text-xs [&_ol]:my-1 [&_ol]:pl-4 [&_li]:my-0.5 [&_hr]:border-white/10 [&_hr]:my-2 [&_code]:text-juice-orange [&_code]:text-[11px] [&_code]:bg-white/5 [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded [&_table]:w-full [&_table]:text-xs [&_table]:my-2 [&_table]:border-collapse [&_th]:text-left [&_th]:text-gray-400 [&_th]:font-medium [&_th]:border-b [&_th]:border-white/10 [&_th]:px-2 [&_th]:py-1 [&_td]:text-gray-300 [&_td]:border-b [&_td]:border-white/5 [&_td]:px-2 [&_td]:py-1">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{pageDetail.content}</ReactMarkdown>
                  </div>
                )}
              </>
            )}

            {!loading && pageError && (
              <div className="border border-red-500/20 bg-red-500/5 px-3 py-2">
                <p className="text-xs text-red-400">{pageError}</p>
              </div>
            )}

            {loading && !pageDetail && !pageError && (
              <p className="text-[10px] text-gray-500">Loading...</p>
            )}
          </div>
        )}

        {/* Page tree (default view) */}
        {!selectedPage && !searchResults && (
          <div className="px-3 py-2 space-y-3">
            {Object.entries(categories).map(([cat, catPages]) => {
              const catMeta = CATEGORY_LABELS[cat] || { label: cat, color: 'text-gray-400' };
              return (
                <div key={cat}>
                  <p className={`text-[10px] uppercase tracking-wider mb-1 ${catMeta.color}`}>
                    {catMeta.label}
                  </p>
                  <div className="space-y-0.5">
                    {catPages.map((page) => (
                      <button
                        key={page.path}
                        onClick={() => loadPage(page.path)}
                        className="w-full flex items-center justify-between px-2.5 py-1.5 text-left hover:bg-white/5 transition-colors group"
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="text-xs text-gray-300 group-hover:text-white truncate">
                            {page.title}
                          </span>
                        </div>
                        <div className="flex items-center gap-2 shrink-0 ml-2">
                          <span className="text-[10px] text-gray-600">{page.wordCount}w</span>
                          <span className="text-[10px] text-gray-500">upd {formatRelative(page.lastModified)}</span>
                          <span className="text-[10px] text-gray-600">rev {formatRelative(page.lastReviewed)}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              );
            })}

            {pages.length === 0 && (
              <p className="text-xs text-gray-500 py-4 text-center">
                No wiki pages found. Run <code className="text-juice-orange">node bot/seed-wiki.js</code> to bootstrap.
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
