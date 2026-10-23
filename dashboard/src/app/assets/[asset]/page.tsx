import { notFound } from 'next/navigation';
import { isMarketAsset } from '@/lib/market-assets';
import { ASSET_DEFAULT_RANGE } from '@/lib/dashboard-ranges';
import { getAssetOverview } from '@/lib/asset-overview';
import { MarketOverview, type AssetOverviewData } from '@/app/page';

export const dynamic = 'force-dynamic';

export default function AssetPage({ params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset) || asset === 'ETH') notFound();
  try {
    const initialAssetOverview = getAssetOverview(asset, ASSET_DEFAULT_RANGE) as unknown as AssetOverviewData;
    return <MarketOverview asset={asset} initialAssetOverview={initialAssetOverview} />;
  } catch {
    return <MarketOverview asset={asset} initialAssetError="Market data temporarily unavailable." />;
  }
}
