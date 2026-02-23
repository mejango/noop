const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const app = express();
const PORT = 3000;

// Simple queue to prevent multiple simultaneous file processing
let isProcessingFiles = false;

// Enable CORS for all routes
app.use(cors());

// Serve static files
app.use(express.static('.'));

// API endpoint to get all available data files
app.get('/api/data-files', (req, res) => {
    try {
        const archiveDir = './archive';
        const files = [];
        
        if (fs.existsSync(archiveDir)) {
            const fileList = fs.readdirSync(archiveDir);
            
            fileList.forEach(file => {
                const filePath = path.join(archiveDir, file);
                const stats = fs.statSync(filePath);
                
                if (stats.isFile()) {
                    files.push({
                        name: file,
                        path: filePath,
                        size: stats.size,
                        modified: stats.mtime,
                        type: getFileType(file)
                    });
                }
            });
        }
        
        // Sort by modification time (newest first)
        files.sort((a, b) => b.modified - a.modified);
        
        res.json(files);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to get specific data file content
app.get('/api/data/:filename', (req, res) => {
    try {
        const filename = req.params.filename;
        const filePath = path.join('./archive', filename);
        
        if (!fs.existsSync(filePath)) {
            return res.status(404).json({ error: 'File not found' });
        }
        
        const content = fs.readFileSync(filePath, 'utf-8');
        const fileType = getFileType(filename);
        
        if (fileType === 'json') {
            res.json(JSON.parse(content));
        } else {
            res.send(content);
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to get the most recent file data (fast initial load)
app.get('/api/recent-data', async (req, res) => {
    try {
        const archiveDir = './archive';
        
        if (!fs.existsSync(archiveDir)) {
            return res.json([]);
        }
        
        const files = fs.readdirSync(archiveDir);
        
        // Get the most recent files, prioritizing data-rich files
        const sortedFiles = files
            .map(file => ({
                name: file,
                path: path.join(archiveDir, file),
                stats: fs.statSync(path.join(archiveDir, file))
            }))
            .filter(file => file.stats.isFile())
            .sort((a, b) => b.stats.mtime - a.stats.mtime);
        
        if (sortedFiles.length === 0) {
            return res.json([]);
        }
        
        // Find the most recent data-rich files (prioritize spot_momentum, options_data, etc.)
        const dataRichFiles = sortedFiles.filter(file => 
            file.name.includes('spot_momentum') || 
            file.name.includes('options_data') || 
            file.name.includes('onchain_analysis') ||
            file.name.includes('trading_decisions')
        );
        
        const filesToLoad = dataRichFiles.length > 0 ? dataRichFiles.slice(0, 3) : sortedFiles.slice(0, 1);
        
        console.log(`Loading ${filesToLoad.length} most recent data-rich files`);
        
        let allData = [];
        for (const fileInfo of filesToLoad) {
            console.log(`Loading file: ${fileInfo.name}`);
            const content = fs.readFileSync(fileInfo.path, 'utf-8');
            const data = parseFileContent(content, fileInfo.name);
            allData = allData.concat(data);
        }
        
        // Sort by timestamp
        allData.sort((a, b) => a.timestamp - b.timestamp);
        
        // Merge data points with same timestamp
        const mergedData = mergeDataPoints(allData);
        
        // Filter to only show data from October 8, 2025 onwards
        const startDate = new Date('2025-10-08T00:00:00Z');
        const filteredData = mergedData.filter(point => point.timestamp >= startDate);
        
        res.json(filteredData);
    } catch (error) {
        console.error('Error processing recent data:', error);
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to get latest data automatically (legacy - now loads all recent files)
app.get('/api/latest-data', async (req, res) => {
    // Set a timeout to prevent hanging
    const timeout = setTimeout(() => {
        if (!res.headersSent) {
            res.status(500).json({ error: 'Request timeout - data processing took too long' });
        }
    }, 30000); // 30 second timeout
    
    try {
        const archiveDir = './archive';
        const allData = [];
        
        if (!fs.existsSync(archiveDir)) {
            return res.json([]);
        }
        
        const files = fs.readdirSync(archiveDir);
        
        // Sort files by modification time and process most recent first
        const sortedFiles = files
            .map(file => ({
                name: file,
                path: path.join(archiveDir, file),
                stats: fs.statSync(path.join(archiveDir, file))
            }))
            .filter(file => file.stats.isFile())
            .sort((a, b) => b.stats.mtime - a.stats.mtime);
        
        // Start with most recent 10 days of data
        const tenDaysAgo = new Date(Date.now() - (10 * 24 * 60 * 60 * 1000));
        const recentFiles = sortedFiles.filter(file => file.stats.mtime >= tenDaysAgo);
        
        console.log(`Processing ${recentFiles.length} files from last 10 days...`);
        
        // Process different file types
        for (const fileInfo of recentFiles) {
            const file = fileInfo.name;
            const filePath = path.join(archiveDir, file);
            const content = fs.readFileSync(filePath, 'utf-8');
            
            try {
                const parsedData = parseFileContent(content, file);
                allData.push(...parsedData);
            } catch (parseError) {
                console.warn(`Error parsing file ${file}:`, parseError.message);
            }
        }
        
        // Sort by timestamp
        allData.sort((a, b) => a.timestamp - b.timestamp);
        
        // Merge data points with same timestamp
        const mergedData = mergeDataPoints(allData);
        
        // Filter to only show data from October 8, 2025 onwards
        const startDate = new Date('2025-10-08T00:00:00Z');
        const filteredData = mergedData.filter(point => point.timestamp >= startDate);
        
        clearTimeout(timeout);
        res.json(filteredData);
    } catch (error) {
        clearTimeout(timeout);
        console.error('Error processing data:', error);
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to get more historical data (for progressive loading)
app.get('/api/historical-data', async (req, res) => {
    const daysBack = parseInt(req.query.days) || 30; // Default to 30 days back
    const offset = parseInt(req.query.offset) || 0; // Days to skip from most recent
    
    try {
        const archiveDir = './archive';
        const allData = [];
        
        if (!fs.existsSync(archiveDir)) {
            return res.json([]);
        }
        
        const files = fs.readdirSync(archiveDir);
        
        // Sort files by modification time
        const sortedFiles = files
            .map(file => ({
                name: file,
                path: path.join(archiveDir, file),
                stats: fs.statSync(path.join(archiveDir, file))
            }))
            .filter(file => file.stats.isFile())
            .sort((a, b) => b.stats.mtime - a.stats.mtime);
        
        // Calculate date range for this batch
        const endDate = new Date(Date.now() - (offset * 24 * 60 * 60 * 1000));
        const startDate = new Date(endDate.getTime() - (daysBack * 24 * 60 * 60 * 1000));
        
        const batchFiles = sortedFiles.filter(file => 
            file.stats.mtime >= startDate && file.stats.mtime < endDate
        );
        
        console.log(`Loading historical data: ${daysBack} days back, offset ${offset} days (${batchFiles.length} files)`);
        
        // Process files in this batch
        for (const fileInfo of batchFiles) {
            const file = fileInfo.name;
            const filePath = path.join(archiveDir, file);
            
            try {
                const content = fs.readFileSync(filePath, 'utf-8');
                const parsedData = parseFileContent(content, file);
                allData.push(...parsedData);
            } catch (parseError) {
                console.warn(`Error parsing file ${file}:`, parseError.message);
            }
        }
        
        // Sort by timestamp
        allData.sort((a, b) => a.timestamp - b.timestamp);
        
        // Merge data points with same timestamp
        const mergedData = mergeDataPoints(allData);
        
        // Filter to only show data from October 8, 2025 onwards
        const startDateFilter = new Date('2025-10-08T00:00:00Z');
        const filteredData = mergedData.filter(point => point.timestamp >= startDateFilter);
        
        res.json(filteredData);
    } catch (error) {
        console.error('Error processing historical data:', error);
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to load files one by one (for progressive loading)
app.get('/api/load-file', async (req, res) => {
    const fileIndex = parseInt(req.query.index) || 0;
    const daysBack = parseInt(req.query.days) || 10; // Default to 10 days
    
    // Prevent multiple simultaneous requests
    if (isProcessingFiles) {
        return res.status(429).json({ error: 'File processing in progress, please wait' });
    }
    
    isProcessingFiles = true;
    
    try {
        const archiveDir = './archive';
        
        if (!fs.existsSync(archiveDir)) {
            return res.json([]);
        }
        
        const files = fs.readdirSync(archiveDir);
        
        // Sort files by modification time (most recent first)
        const sortedFiles = files
            .map(file => ({
                name: file,
                path: path.join(archiveDir, file),
                stats: fs.statSync(path.join(archiveDir, file))
            }))
            .filter(file => file.stats.isFile())
            .sort((a, b) => b.stats.mtime - a.stats.mtime);
        
        // Filter to only include files from the specified number of days back
        const cutoffDate = new Date(Date.now() - (daysBack * 24 * 60 * 60 * 1000));
        const recentFiles = sortedFiles.filter(file => file.stats.mtime >= cutoffDate);
        
        // Prioritize data-rich files
        const dataRichFiles = recentFiles.filter(file => 
            file.name.includes('spot_momentum') || 
            file.name.includes('options_data') || 
            file.name.includes('onchain_analysis') ||
            file.name.includes('trading_decisions')
        );
        
        // Combine data-rich files first, then other files
        const prioritizedFiles = [...dataRichFiles, ...recentFiles.filter(file => !dataRichFiles.includes(file))];
        
        if (fileIndex >= prioritizedFiles.length) {
            return res.json({ done: true, message: `All files from last ${daysBack} days loaded` });
        }
        
        const fileInfo = prioritizedFiles[fileIndex];
        console.log(`Loading file ${fileIndex + 1}/${prioritizedFiles.length}: ${fileInfo.name}`);
        
        const content = fs.readFileSync(fileInfo.path, 'utf-8');
        const data = parseFileContent(content, fileInfo.name);
        
        // Sort by timestamp
        data.sort((a, b) => a.timestamp - b.timestamp);
        
        // Filter to only show data from October 8, 2025 onwards
        const startDate = new Date('2025-10-08T00:00:00Z');
        const filteredData = data.filter(point => point.timestamp >= startDate);
        
        res.json({
            data: filteredData,
            fileIndex: fileIndex,
            fileName: fileInfo.name,
            totalFiles: prioritizedFiles.length,
            done: false
        });
    } catch (error) {
        console.error('Error loading file:', error);
        res.status(500).json({ error: error.message });
    } finally {
        isProcessingFiles = false;
    }
});

// Helper function to parse file content (extracted from main endpoint)
function parseFileContent(content, file) {
    const data = [];
    
    try {
        if (file.includes('spot_momentum')) {
            // Parse spot momentum text file
            const lines = content.split('\n').filter(line => line.trim());
            lines.forEach(line => {
                const parts = line.split('::');
                if (parts.length >= 4) {
                    const pricePart = parts[0];
                    const timestamp = parts[3];
                    const price = parseFloat(pricePart.split(' ')[0]);
                    
                    if (!isNaN(price) && timestamp) {
                        data.push({
                            timestamp: new Date(timestamp),
                            spotPrice: price,
                            shortMomentum: parts[1],
                            mediumMomentum: parts[2],
                            source: file
                        });
                    }
                }
            });
        } else if (file.includes('trading_decisions') && file.endsWith('.json')) {
            // Parse trading decisions JSON
            const jsonData = JSON.parse(content);
            if (jsonData.decisions) {
                jsonData.decisions.forEach(decision => {
                    if (decision.timestamp) {
                        data.push({
                            timestamp: new Date(decision.timestamp),
                            action: decision.action,
                            success: decision.success,
                            spotPrice: decision.spotPrice,
                            optionPrice: decision.option?.askPrice || decision.option?.bidPrice,
                            reason: decision.reason,
                            source: file
                        });
                    }
                });
            }
        } else if (file.includes('options_data') && file.endsWith('.json')) {
            // Parse options data JSON
            const jsonData = JSON.parse(content);
            if (jsonData.options) {
                jsonData.options.forEach(option => {
                    if (option.timestamp) {
                        const optionData = {
                            timestamp: new Date(option.timestamp),
                            source: file
                        };
                        
                        // Extract spot price
                        if (option.spotPrice) {
                            optionData.spotPrice = parseFloat(option.spotPrice);
                        }
                        
                        // Extract best put score as put value
                        if (option.bestPutScore) {
                            optionData.bestPutValue = parseFloat(option.bestPutScore);
                        }
                        
                        data.push(optionData);
                    }
                });
            }
        } else if (file.includes('onchain_analysis') && file.endsWith('.json')) {
            // Parse onchain analysis JSON
            const jsonData = JSON.parse(content);
            if (jsonData.analysis) {
                jsonData.analysis.forEach(analysis => {
                    if (analysis.timestamp) {
                        const analysisData = {
                            timestamp: new Date(analysis.timestamp),
                            source: file
                        };
                        
                        // Extract liquidity delta from onchain analysis
                        if (analysis.dexLiquidity && analysis.dexLiquidity.flowAnalysis) {
                            const flow = analysis.dexLiquidity.flowAnalysis;
                            if (flow.weightedChange !== undefined) {
                                analysisData.liquidityDelta = flow.weightedChange * 100; // Convert to percentage
                            }
                        }
                        
                        data.push(analysisData);
                    }
                });
            }
        } else if (file.endsWith('.json') && !file.includes('whale_') && !file.includes('performance_') && !file.includes('options_data') && !file.includes('onchain_analysis')) {
            // Parse general JSON data (main data files)
            const jsonData = JSON.parse(content);
            if (jsonData.ticks) {
                jsonData.ticks.forEach(tick => {
                    if (tick.timestamp) {
                        const tickData = {
                            timestamp: new Date(tick.timestamp),
                            source: file
                        };
                        
                        // Extract spot price from instruments
                        if (tick.instruments && tick.instruments.length > 0) {
                            const spotInstrument = tick.instruments.find(inst => 
                                inst.instrument_name === 'ETH-USD' || 
                                inst.instrument_name.includes('ETH')
                            );
                            if (spotInstrument && spotInstrument.last_price) {
                                tickData.spotPrice = parseFloat(spotInstrument.last_price);
                            }
                        }
                        
                        data.push(tickData);
                    }
                });
            }
        }
    } catch (error) {
        console.error('Error parsing file:', file, error);
    }
    
    return data;
}

// Helper function to merge data points with same timestamp
function mergeDataPoints(data) {
    const merged = new Map();
    
    data.forEach(point => {
        const key = point.timestamp.getTime();
        if (merged.has(key)) {
            const existing = merged.get(key);
            // Merge properties, preferring non-null values
            Object.keys(point).forEach(key => {
                if (point[key] !== null && point[key] !== undefined) {
                    existing[key] = point[key];
                }
            });
        } else {
            merged.set(key, { ...point });
        }
    });
    
    return Array.from(merged.values()).sort((a, b) => a.timestamp - b.timestamp);
}

// Helper function to determine file type
function getFileType(filename) {
    if (filename.endsWith('.json')) return 'json';
    if (filename.endsWith('.txt')) return 'text';
    return 'unknown';
}

// Start server
app.listen(PORT, () => {
    console.log(`🚀 Trading Dashboard Server running at http://localhost:${PORT}`);
    console.log(`📊 Dashboard available at http://localhost:${PORT}/trading_dashboard.html`);
    console.log(`📁 Archive directory: ./archive`);
    console.log(`🔄 Auto-refresh enabled - data updates automatically`);
});
