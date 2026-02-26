// Shared chart theme aligned with juice design system

export const chartColors = {
  primary: '#F5A623',    // juice-orange — ETH price, primary series
  secondary: '#5CEBDF',  // juice-cyan — secondary series
  tertiary: '#10b981',   // emerald — call scores, success
  quaternary: '#f59e0b', // amber — warnings
  red: '#f87171',        // red — put scores, exhaustion
  blue: '#3b82f6',       // blue — liquidity
  trade: '#facc15',      // yellow — trade markers
  refHigh: '#10b981',    // emerald — reference high
  refLow: '#ef4444',     // red — reference low
} as const;

export const chartAxis = {
  stroke: '#666666',
  tick: { fill: '#999999', fontSize: 11 },
  tickSecondary: { fill: '#666666', fontSize: 10 },
} as const;

export const chartTooltip = {
  contentStyle: {
    backgroundColor: '#1a1a1a',
    border: '1px solid rgba(255, 255, 255, 0.15)',
    borderRadius: 8,
    fontSize: 12,
  },
  isAnimationActive: false,
} as const;
