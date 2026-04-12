export const CHAT_MESSAGE_MAX_CHARS = 4_000;
export const CHAT_HISTORY_MAX_MESSAGES = 16;

export const CHART_ROW_LIMITS: Record<string, { prices: number; heatmap: number }> = {
  '1h': { prices: 1_000, heatmap: 3_000 },
  '6h': { prices: 2_000, heatmap: 6_000 },
  '24h': { prices: 5_000, heatmap: 12_000 },
  '3d': { prices: 8_000, heatmap: 15_000 },
  '6.2d': { prices: 12_000, heatmap: 18_000 },
  '7d': { prices: 12_000, heatmap: 18_000 },
  '14d': { prices: 16_000, heatmap: 18_000 },
  '30d': { prices: 20_000, heatmap: 15_000 },
  '90d': { prices: 30_000, heatmap: 18_000 },
  '365d': { prices: 12_000, heatmap: 30_000 },
  'all': { prices: 40_000, heatmap: 20_000 },
};
