'use client';

import { usePathname, useRouter } from 'next/navigation';
import { usePolling, useLiveTimeAgo, useCountdown } from '@/lib/hooks';
import { formatUSD } from '@/lib/format';
import { ASSETS } from '@/lib/market-assets';

const CYCLE_DAYS = 15;

interface Budget {
  putTotalBudget: number;
  putSpent: number;
  putRemaining: number;
  putDaysLeft: number;
  cycleDays: number;
}

interface NavStats {
  last_price: number;
  last_price_time: string;
  lyra_spot: number | null;
  margin_usage_pct: number | null;
  budget: Budget;
}

interface Collateral {
  asset_name: string;
  amount: number;
}

interface AccountData {
  collaterals: Collateral[];
}

const emptyBudget: Budget = { putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0, cycleDays: CYCLE_DAYS };
const emptyStats: NavStats = { last_price: 0, last_price_time: '', lyra_spot: null, margin_usage_pct: null, budget: emptyBudget };
const emptyAccount: AccountData = { collaterals: [] };

function EthStats() {
  const STATS_INTERVAL = 60_000;
  const { data: stats, fetchTick, dataUrl: statsDataUrl } = usePolling<NavStats>('/api/stats', emptyStats, STATS_INTERVAL);
  const { data: account } = usePolling<AccountData>('/api/lyra/account', emptyAccount, 60_000);
  const b = stats.budget || emptyBudget;
  const liveAgo = useLiveTimeAgo(stats.last_price_time);
  const nextTick = useCountdown(STATS_INTERVAL, [fetchTick]);

  const usdc = account.collaterals.find(c => c.asset_name === 'USDC');
  const eth = account.collaterals.find(c => c.asset_name === 'ETH');

  // The API owns the budget, including its spot-based fallback (bot/put-budget.js).
  const { putTotalBudget, putRemaining } = b;

  return (
    <div className="flex items-center gap-2 md:gap-4 text-xs md:text-sm flex-wrap justify-end">
          <span className="font-semibold">
            <span className="text-gray-400">ETH</span>{' '}
            <span className="text-juice-orange">{statsDataUrl ? formatUSD(stats.last_price) : '—'}</span>
            {stats.lyra_spot != null && stats.lyra_spot > 0 && (
              <><span className="text-gray-500 mx-0.5">/</span><span className="text-white">{formatUSD(stats.lyra_spot)}</span></>
            )}
          </span>
          <span className="text-gray-500 text-xs hidden sm:inline leading-tight">
            <span className="block">{liveAgo}</span>
            <span className="block text-gray-600" style={{ fontSize: '0.65rem' }}>next tick {nextTick}</span>
          </span>
          {(usdc || eth) && (
            <>
              <span className="text-gray-600">|</span>
              <span className="tabular-nums">
                {usdc && <><span className="text-gray-400 hidden sm:inline">USDC </span><span className="text-white">{Number(usdc.amount).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span></>}
                {usdc && eth && <span className="text-gray-600 mx-1">|</span>}
                {eth && <><span className="text-gray-400 hidden sm:inline">ETH </span><span className="text-white">{Number(eth.amount).toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 })}</span></>}
              </span>
            </>
          )}
          <span className="text-gray-600 hidden md:inline">|</span>
          {stats.margin_usage_pct != null && (
            <>
              <span className="text-gray-400 hidden md:inline">MARGIN <span className="text-white">{stats.margin_usage_pct.toFixed(1)}%</span></span>
              <span className="text-gray-600 hidden md:inline">|</span>
            </>
          )}
          <span className="text-gray-400 hidden md:inline">PUT <span className="text-white">{statsDataUrl ? formatUSD(putRemaining) : '—'}</span>/<span className="text-gray-500">{statsDataUrl ? formatUSD(putTotalBudget) : '—'}</span></span>
          <span className="text-gray-500 text-xs hidden md:inline">{!statsDataUrl ? 'Loading…' : b.putDaysLeft > 0 ? `${b.putDaysLeft}d left` : 'cycle ended'}</span>
    </div>
  );
}

function AssetStats({ asset }: { asset: string }) {
  const { data, dataUrl } = usePolling<{ stats: { last_price: number; last_price_time: string } }>(
    `/api/assets/${asset.toLowerCase()}/overview?range=1h`,
    { stats: { last_price: 0, last_price_time: '' } },
    60_000,
  );
  const liveAgo = useLiveTimeAgo(data.stats.last_price_time);
  return <div className="text-xs md:text-sm text-gray-400"><span>{asset} <span className="text-juice-orange">{dataUrl && data.stats.last_price > 0 ? formatUSD(data.stats.last_price) : '—'}</span></span><span className="ml-2 text-gray-500">{dataUrl ? liveAgo : 'Loading…'}</span></div>;
}

export default function Nav() {
  const pathname = usePathname();
  const router = useRouter();
  const marketPage = pathname.startsWith('/assets/');
  const activeAsset = marketPage ? pathname.split('/')[2]?.toUpperCase() : 'ETH';
  return (
    <nav className="border-b border-white/25 bg-black py-3">
      <div className="w-full px-3 md:px-6 xl:px-8 2xl:px-10 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-lg md:text-2xl font-bold tracking-tight text-white shrink-0">🥱 NO OPERATION</span>
          <span className="group relative inline-flex items-center ml-3">
            <select
              aria-label="Asset"
              value={activeAsset}
              onChange={event => router.push(event.target.value === 'ETH' ? '/' : `/assets/${event.target.value.toLowerCase()}`)}
              className="appearance-none bg-transparent text-gray-300 group-hover:text-juice-orange text-sm pl-0 pr-6 py-1 cursor-pointer focus:outline-none focus-visible:outline focus-visible:outline-1 focus-visible:outline-offset-4 focus-visible:outline-juice-orange"
            >
              {ASSETS.map(asset => <option key={asset} value={asset}>{asset}</option>)}
            </select>
            <svg aria-hidden="true" viewBox="0 0 12 8" width="12" height="8" className="pointer-events-none absolute right-1 text-gray-500 group-hover:text-juice-orange">
              <path d="m1 1 5 5 5-5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </span>
        </div>
        {marketPage ? <AssetStats key={activeAsset} asset={activeAsset} /> : <EthStats />}
      </div>
    </nav>
  );
}
