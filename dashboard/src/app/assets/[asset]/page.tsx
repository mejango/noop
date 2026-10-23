import { notFound } from 'next/navigation';
import { isMarketAsset } from '@/lib/market-assets';
import AssetMarketDashboard from '@/components/AssetMarketDashboard';

export default function AssetPage({ params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) notFound();
  return <AssetMarketDashboard asset={asset} />;
}
