/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  // Concurrent V3 testnet development must not overwrite the V2 build cache.
  distDir: process.env.NOOP_VENUE === 'v3-testnet' ? '.next-v3-testnet' : '.next',
  compress: true,
  async headers() {
    return [
      {
        source: '/',
        headers: [{
          key: 'Cache-Control',
          value: 'public, max-age=300, s-maxage=3600, stale-while-revalidate=86400',
        }],
      },
      {
        source: '/pnl',
        headers: [{
          key: 'Cache-Control',
          value: 'public, max-age=300, s-maxage=3600, stale-while-revalidate=86400',
        }],
      },
    ];
  },
};

export default nextConfig;
