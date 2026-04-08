'use client';

import { usePolling, useLiveTimeAgo, useCountdown } from '@/lib/hooks';
import { formatUSD } from '@/lib/format';

interface Budget {
  putTotalBudget: number;
  putSpent: number;
  putRemaining: number;
  putDaysLeft: number;
  callTotalBudget: number;
  callSpent: number;
  callRemaining: number;
  callDaysLeft: number;
  cycleDays: number;
}

interface NavStats {
  last_price: number;
  last_price_time: string;
  lyra_spot: number | null;
  budget: Budget;
}

interface Collateral {
  asset_name: string;
  amount: number;
}

interface AccountData {
  collaterals: Collateral[];
}

const emptyBudget: Budget = {
  putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
  callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0, cycleDays: 10,
};

const emptyStats: NavStats = {
  last_price: 0, last_price_time: '', lyra_spot: null, budget: emptyBudget,
};

const emptyAccount: AccountData = { collaterals: [] };

export default function Nav() {
  const STATS_INTERVAL = 60_000;
  const { data: stats, fetchTick } = usePolling<NavStats>('/api/stats', emptyStats, STATS_INTERVAL);
  const { data: account } = usePolling<AccountData>('/api/lyra/account', emptyAccount, 60_000);
  const rawBudget = stats.budget || emptyBudget;
  const liveAgo = useLiveTimeAgo(stats.last_price_time);
  const nextTick = useCountdown(STATS_INTERVAL, [fetchTick]);

  const usdc = account.collaterals.find(c => c.asset_name === 'USDC');
  const eth = account.collaterals.find(c => c.asset_name === 'ETH');

  // Compute budget client-side if API returns 0 (DB column not yet populated)
  const PUT_ANNUAL_RATE = 0.0333;
  const CYCLE_DAYS = rawBudget.cycleDays || 15;
  let b = rawBudget;
  if (b.putTotalBudget === 0 && stats.last_price > 0) {
    const portfolioValue = Number(eth?.amount || 0) * stats.last_price + Number(usdc?.amount || 0);
    if (portfolioValue > 0) {
      const cyclesPerYear = 365 / CYCLE_DAYS;
      const computed = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
      b = { ...b, putTotalBudget: computed, putRemaining: computed };
    }
  }

  return (
    <nav className="border-b border-white/10 bg-juice-dark/80 backdrop-blur-md py-3">
      <div className="max-w-7xl mx-auto px-3 md:px-6 flex items-center justify-between gap-3">
        <span className="text-lg md:text-2xl font-bold tracking-tight text-white shrink-0">🥱 NO OPERATION</span>
        <div className="flex items-center gap-2 md:gap-4 text-xs md:text-sm flex-wrap justify-end">
          <span className="font-semibold">
            <span className="text-gray-400">ETH</span>{' '}
            <span className="text-juice-orange">{formatUSD(stats.last_price)}</span>
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
          <span className="text-gray-400 hidden md:inline">PUT <span className="text-white">{formatUSD(b.putRemaining)}</span>/<span className="text-gray-500">{formatUSD(b.putTotalBudget)}</span></span>
          <span className="text-gray-500 text-xs hidden md:inline">{b.putDaysLeft > 0 ? `${b.putDaysLeft}d left` : 'cycle ended'}</span>
        </div>
      </div>
    </nav>
  );
}
