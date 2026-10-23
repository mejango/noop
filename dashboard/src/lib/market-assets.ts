import assets from '../../../bot/market-assets.json';

export const ASSETS: string[] = assets;

export function isMarketAsset(asset: string): boolean {
  return asset !== 'ETH' && ASSETS.includes(asset);
}
