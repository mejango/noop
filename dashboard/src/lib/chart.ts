// Shared chart theme, btop-style terminal palette

export const chartColors = {
  primary: '#d7c46c',    // khaki — ETH price, primary series
  secondary: '#5f9ad7',  // btop temp blue — secondary series
  tertiary: '#77ca9b',   // green — call scores, success
  quaternary: '#cbc06c', // amber — warnings
  red: '#dc4c4c',        // red — put scores, exhaustion
  blue: '#9a7bd8',       // purple — liquidity
  trade: '#f0f0f0',      // white — trade markers
  refHigh: '#77ca9b',    // green — reference high
  refLow: '#dc4c4c',     // red — reference low
} as const;

export const chartAxis = {
  stroke: '#4a4a4a',
  tick: { fill: '#8a8a8a', fontSize: 11 },
  tickSecondary: { fill: '#5a5a5a', fontSize: 10 },
} as const;

export const chartTooltip = {
  contentStyle: {
    backgroundColor: '#000000',
    border: '1px solid #4a4a4a',
    borderRadius: 0,
    fontSize: 12,
  },
  // Legends render later in the DOM; keep the tooltip above them.
  wrapperStyle: { zIndex: 20 },
  isAnimationActive: false,
} as const;
