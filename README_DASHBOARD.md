# 📈 ETH Options Trading Dashboard

A real-time visualization tool for monitoring your ETH options trading bot performance.

## 🚀 Quick Start

### Option 1: With Auto-Refresh Server (Recommended)

**Quick Start:**
```bash
# On macOS/Linux:
./start_dashboard.sh

# On Windows:
start_dashboard.bat

# Or manually:
npm install && npm start
```

Then open: **http://localhost:3000/trading_dashboard.html**

**🔄 Automatic Data Loading:**
- The dashboard automatically reads from your `./archive/` directory
- No manual file selection needed
- Data refreshes every minute automatically
- Works with your existing trading bot data

### Option 2: Manual File Upload

1. **Open the HTML file directly:**
   ```bash
   open trading_dashboard.html
   ```

2. **Select data files manually** from the `./archive/` directory

## 📊 Features

### Real-time Metrics
- **ETH Spot Price** - Current market price with trend visualization
- **Best Put/Call Prices** - Optimal option prices from your bot's analysis
- **Liquidity Score** - Market liquidity health indicator
- **Trading Activity** - Buy/sell order frequency
- **Whale Activity** - Large transaction monitoring

### Interactive Chart
- **Multi-line overlay** showing all key metrics over time
- **Time range filtering** (1 hour to 1 month)
- **Auto-refresh** with configurable intervals
- **Export functionality** to save charts as images

### Data Sources
The dashboard automatically reads from:
- `./archive/YYYY-MM-DD.json` - Daily trading data
- `./archive/YYYY-MM_spot_momentum.txt` - Price and momentum data
- `./archive/YYYY-MM-DD_trading_decisions.json` - Trading decisions log

## 🎛️ Controls

### Time Range
- **Last Hour** - Recent activity
- **Last 6 Hours** - Short-term trends
- **Last 24 Hours** - Daily performance (default)
- **Last Week** - Weekly patterns
- **Last Month** - Monthly analysis
- **All Data** - Complete history

### Auto Refresh
- **Manual Only** - Refresh when you click the button
- **30 seconds** - High-frequency updates
- **1 minute** - Standard refresh (default)
- **5 minutes** - Low-frequency updates

## 📈 Chart Legend

- 🔴 **Red Line** - ETH Spot Price
- 🔵 **Blue Line** - Best Put Price
- 🟢 **Green Line** - Best Call Price
- 🟡 **Yellow Line** - Liquidity Score
- 🟣 **Purple Line** - Buy Orders
- 🟠 **Orange Line** - Sell Orders

## 🔧 Technical Details

### Data Processing
- Automatically parses multiple file formats
- Handles missing data gracefully
- Sorts data chronologically
- Filters by time range

### Performance
- Efficient data loading and processing
- Smooth chart animations
- Responsive design for all screen sizes
- Minimal memory footprint

## 🛠️ Troubleshooting

### No Data Showing
1. Check that your bot is running and saving data to `./archive/`
2. Verify file permissions in the archive directory
3. Try refreshing the page or restarting the server

### Server Won't Start
1. Make sure you have Node.js installed
2. Run `npm install` to install dependencies
3. Check that port 3000 is available

### Chart Not Updating
1. Check the auto-refresh setting
2. Verify your bot is actively writing new data
3. Try manually refreshing with the button

## 📁 File Structure

```
noop/
├── trading_dashboard.html    # Main dashboard interface
├── dashboard_server.js       # Auto-refresh server
├── package.json             # Server dependencies
├── archive/                 # Data directory
│   ├── 2025-08-04.json     # Daily trading data
│   ├── 2025-08_spot_momentum.txt  # Price/momentum data
│   └── 2025-08-04_trading_decisions.json  # Trading log
└── README_DASHBOARD.md      # This file
```

## 🎯 Use Cases

### Daily Monitoring
- Track your bot's performance in real-time
- Identify optimal trading windows
- Monitor market conditions and liquidity

### Performance Analysis
- Analyze trading patterns over time
- Compare put vs call performance
- Identify correlation between price and activity

### Risk Management
- Monitor whale activity for market impact
- Track liquidity changes
- Spot unusual trading patterns

---

**Happy Trading! 📈**
