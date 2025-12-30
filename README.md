# AIS Vessel Streaming Pipeline

A real-time data pipeline that ingests live maritime vessel data, aggregates it by type, and visualizes traffic patterns over time.

## What This Does

- **Streams live AIS data** from vessels in the US East Coast region
- **Aggregates vessel counts** by type every minute (true streaming, not batch)
- **Stores aggregated data** in SQLite (only ~1MB per day)
- **Visualizes in real-time** with an auto-updating stacked area chart

## Example Output

The visualization shows how vessel traffic composition changes over time:
- **Cargo** (blue) - Container ships, bulk carriers
- **Tanker** (purple) - Oil tankers, chemical tankers 
- **Passenger** (orange) - Cruise ships, ferries
- **Fishing** (red) - Fishing vessels
- **Other** (gray) - Tugs, pilot boats, military
- **Unknown** (light gray) - Vessels without metadata

## Quick Start

### 1. Setup
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure
Edit `config.py` and add your AISStream API key:
```python
AISSTREAM_API_KEY = "your_api_key_here"
```

Get a free API key at [aisstream.io](https://aisstream.io)

### 3. Run
```bash
# Terminal 1 - Start data collection
python3 main.py

# Terminal 2 - Start visualization (after a minute or two)
python3 visualize.py
```

## Project Structure

```
Level1/
├── config.py              # Configuration (API key, region, settings)
├── main.py                # Main pipeline coordinator
├── ais_client.py          # WebSocket client for AISStream
├── aggregator.py          # Real-time streaming aggregation
├── database.py            # SQLite operations
├── visualize.py           # Real-time dashboard
├── requirements.txt       # Python dependencies
├── check_status.py        # Database diagnostic tool
├── test_setup.py          # Verify installation
└── ais_vessel_data.db     # SQLite database (created on first run)
```

## How It Works

### Architecture
```
AISStream.io → WebSocket → Aggregator → SQLite → Visualization
                  ↓            ↓
              Messages    Time Windows
```

### Data Flow
1. **WebSocket** receives AIS messages (position reports + vessel metadata)
2. **Aggregator** caches vessel types and tracks unique vessels per minute
3. **Database** stores aggregated counts (not raw messages)
4. **Visualization** queries database every 5 seconds and updates chart

### Why This Design?
- **Memory efficient**: Only current window in RAM (~20MB)
- **Disk efficient**: 1MB/day vs 1.5GB/day for raw messages
- **True streaming**: Event-driven window closing, not batch processing
- **Scalable**: Can handle 100+ messages/second on a laptop

## Understanding the Data

### Vessel Categories
| Category  | AIS Codes | Typical Count |
|-----------|-----------|---------------|
| Cargo     | 70-79     | 40-60         |
| Tanker    | 80-89     | 20-30         |
| Passenger | 60-69     | 5-15          |
| Fishing   | 30-40     | 10-20         |
| Other     | Various   | 30-50         |
| Unknown   | Missing   | 10-30 (decreases over time) |

### Expected Behavior
- **First 60 seconds**: No windows closed yet, database empty
- **After 1-2 minutes**: First window closes, data appears in visualization
- **After 10 minutes**: "Unknown" category shrinks as metadata is cached
- **Normal operation**: ~200-300 unique vessels per minute in US East Coast

## Common Tasks

### Check Pipeline Status
```bash
python3 check_status.py
```

### Test Setup
```bash
python3 test_setup.py
```

### Query Database Directly
```bash
sqlite3 ais_vessel_data.db "SELECT * FROM vessel_counts ORDER BY timestamp DESC LIMIT 10;"
```

### Change Region
Edit `config.py`:
```python
BOUNDING_BOX = [
    [lat_sw, lon_sw],  # Southwest corner
    [lat_ne, lon_ne]   # Northeast corner
]
```

Example regions:
- **Mediterranean**: `[[30, -6], [46, 37]]`
- **English Channel**: `[[49, -6], [52, 3]]`
- **Singapore Strait**: `[[1, 103], [2, 105]]`

### Change Aggregation Window
Edit `config.py`:
```python
AGGREGATION_WINDOW_SECONDS = 300  # 5 minutes instead of 1
```

### Export Data to CSV
```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('ais_vessel_data.db')
df = pd.read_sql_query('SELECT * FROM vessel_counts', conn)
df.to_csv('export.csv', index=False)
```

## Troubleshooting

### No data appearing
- Wait 60+ seconds for first window to close
- Check API key in `config.py`
- Run `python3 check_status.py` to diagnose

### Visualization is empty
- Make sure `main.py` is running
- Run both scripts from the same directory
- Check database has data: `python3 check_status.py`

### "Unknown" category too high
- Normal at startup (metadata being cached)
- Should decrease after 10-15 minutes
- ShipStaticData messages are less frequent than position reports

### High memory usage
- Normal: ~65MB for entire pipeline
- If much higher, restart `main.py`

## Performance

### Typical Metrics
- **Message rate**: 50-100 messages/second
- **Processing time**: <1ms per message
- **Window close time**: <10ms
- **Database size**: ~1MB/day, ~30MB/month
- **Memory usage**: ~65MB total
- **CPU usage**: <5% on modern laptop

### Tested On
- Ubuntu 24.04 / Linux
- Python 3.11
- 8GB RAM, 2 CPU cores
- Ran continuously for 24+ hours without issues

## 🎓 Learning Opportunities

This project demonstrates:
- **WebSocket communication** - Real-time bidirectional data streaming
- **Event-driven architecture** - Windows close when next message arrives
- **Set-based aggregation** - Tracking unique items efficiently
- **Async Python** - Modern concurrent programming patterns
- **Time-series database design** - Efficient storage and querying
- **Real-time visualization** - Matplotlib animation framework

## Next Steps

### Ideas to Extend
1. **Add geographic heatmap** - Show where vessels are concentrated
2. **Track vessel speeds** - Identify congestion or slow-downs
3. **Anomaly detection** - Alert when traffic patterns change
4. **Multiple regions** - Compare different shipping lanes
5. **Web dashboard** - Replace matplotlib with Plotly Dash or Streamlit
6. **Export to Grafana** - Professional monitoring dashboard
7. **Machine learning** - Predict traffic patterns

### Files to Read
1. **main.py** - See how components connect
2. **aggregator.py** - Understand streaming logic
3. **ais_client.py** - Learn WebSocket patterns
4. **visualize.py** - Matplotlib animation techniques

## Notes

### Key Design Decisions
- **True streaming** vs batch: Lower latency, minimal memory
- **Store aggregates only**: 3000x smaller than raw messages
- **SQLite** vs PostgreSQL: Zero configuration, perfect for this scale
- **1-minute windows**: Fast feedback for learning, easy to change

### Known Limitations
- Late messages assigned to current window (acceptable for 1-min windows)
- No handling of vessel position updates vs static data races
- Visualization redraws entire chart (fine for <1000 windows)
- Single-threaded (sufficient for current message rates)

### Production Considerations
If deploying for real use:
- Add connection retry logic in `ais_client.py`
- Implement database cleanup/archival
- Add proper logging rotation
- Consider PostgreSQL for multi-user access
- Add authentication if exposing visualization
- Implement proper error monitoring

## Credits

- **AIS data**: [aisstream.io](https://aisstream.io)
- **Vessel type codes**: [IMO AIS standard](https://www.imo.org/)
- **Built with**: Python, websockets, pandas, matplotlib, SQLite


