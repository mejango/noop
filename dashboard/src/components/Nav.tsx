'use client';

import { usePolling } from '@/lib/hooks';
import { formatUSD, timeAgo } from '@/lib/format';

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
  budget: Budget;
}

const emptyBudget: Budget = {
  putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
  callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0, cycleDays: 10,
};

const emptyStats: NavStats = {
  last_price: 0, last_price_time: '', budget: emptyBudget,
};

export default function Nav() {
  const { data: stats } = usePolling<NavStats>('/api/stats', emptyStats);
  const b = stats.budget || emptyBudget;

  return (
    <nav className="border-b border-white/10 bg-juice-dark/80 backdrop-blur-md py-3">
      <div className="max-w-7xl mx-auto px-6 flex items-center justify-between">
        <span className="text-2xl font-bold tracking-tight text-white">NO OPERATION</span>
        <div className="flex items-center gap-4 text-sm">
          <span className="text-juice-orange font-semibold">ETH {formatUSD(stats.last_price)}</span>
          <span className="text-gray-500 text-xs">{timeAgo(stats.last_price_time)}</span>
          <span className="text-gray-600">|</span>
          <span className="text-gray-400">PUT <span className="text-white">{formatUSD(b.putRemaining)}</span>/<span className="text-gray-500">{formatUSD(b.putTotalBudget)}</span></span>
          <span className="text-gray-400">CALL <span className="text-white">{formatUSD(b.callRemaining)}</span>/<span className="text-gray-500">{formatUSD(b.callTotalBudget)}</span></span>
          <span className="text-gray-500 text-xs">{b.putDaysLeft > 0 ? `${b.putDaysLeft}d left` : 'cycle ended'}</span>
        </div>
      </div>
    </nav>
  );
}
