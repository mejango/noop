import { notFound } from 'next/navigation';
import { isMarketAsset } from '@/lib/market-assets';
import { getAssetMarketData } from '@/lib/db';
import { ASSET_DEFAULT_RANGE, dashboardRangeMs } from '@/lib/dashboard-ranges';
import AssetMarketDashboard from '@/components/AssetMarketDashboard';

export const dynamic = 'force-dynamic';

export default function AssetPage({ params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) notFound();
  const rangeMs = dashboardRangeMs(ASSET_DEFAULT_RANGE, ASSET_DEFAULT_RANGE);
  const since = new Date(Date.now() - rangeMs).toISOString();
  try {
    const initialData = { asset, range: ASSET_DEFAULT_RANGE, ...getAssetMarketData(asset, since, rangeMs) };
    return <AssetMarketDashboard asset={asset} initialData={initialData} />;
  } catch {
    const initialData = { asset, range: ASSET_DEFAULT_RANGE, ready: false, latest: null,
      history: [], quotes: [], observation_count: 0 };
    return <AssetMarketDashboard asset={asset} initialData={initialData} initialError="Market data temporarily unavailable." />;
  }
}
