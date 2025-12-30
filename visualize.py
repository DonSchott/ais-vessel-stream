"""
Real-time visualization dashboard for AIS vessel data
"""
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.dates import DateFormatter
from datetime import datetime
import pandas as pd
import logging

from database import VesselDatabase
from config import DATABASE_PATH

logger = logging.getLogger(__name__)


class VesselDashboard:
    """
    Real-time dashboard showing vessel type distribution over time
    """
    
    def __init__(self, db_path: str = DATABASE_PATH, update_interval: int = 5000):
        """
        Initialize dashboard
        
        Args:
            db_path: Path to SQLite database
            update_interval: Update interval in milliseconds
        """
        self.db_path = db_path
        self.update_interval = update_interval
        
        # Setup plot
        self.fig, self.ax = plt.subplots(figsize=(14, 8))
        self.fig.suptitle('AIS Vessel Type Distribution Over Time', fontsize=16, fontweight='bold')
        
        # Color scheme for vessel categories
        self.colors = {
            'Cargo': '#2E86AB',      # Blue
            'Tanker': '#A23B72',     # Purple
            'Passenger': '#F18F01',  # Orange
            'Fishing': '#C73E1D',    # Red
            'Other': '#6C757D',      # Gray
            'Unknown': '#ADB5BD'     # Light gray
        }
        
        self.categories = ['Cargo', 'Tanker', 'Passenger', 'Fishing', 'Other', 'Unknown']
        
    def _fetch_data(self):
        """
        Fetch latest data from database
        
        Returns:
            pandas DataFrame with timestamp, category, and count columns
        """
        db = VesselDatabase(self.db_path)
        db.connect()
        
        try:
            data = db.get_all_data()
            db.close()
            
            if not data:
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=['timestamp', 'vessel_category', 'unique_vessels'])
            
            # Parse timestamps - use format='ISO8601' to handle variations
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', utc=True)
            except Exception as e:
                logger.error(f"Timestamp parsing error: {e}")
                # Try alternative parsing
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
            
            # Remove any rows with invalid timestamps
            before_len = len(df)
            df = df.dropna(subset=['timestamp'])
            if len(df) < before_len:
                logger.warning(f"Dropped {before_len - len(df)} rows with invalid timestamps")
            
            # Sanity check: ensure timestamps are in reasonable range (last 30 days to 1 day in future)
            now = pd.Timestamp.now(tz='UTC')
            valid_range_start = now - pd.Timedelta(days=30)
            valid_range_end = now + pd.Timedelta(days=1)
            
            mask = (df['timestamp'] >= valid_range_start) & (df['timestamp'] <= valid_range_end)
            invalid_count = (~mask).sum()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} timestamps outside valid range, filtering them out")
                df = df[mask]
            
            if df.empty:
                logger.error("All timestamps were invalid")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            db.close()
            return None
    
    def _update_plot(self, frame):
        """
        Update plot with latest data (called by animation)
        """
        df = self._fetch_data()
        
        if df is None or df.empty:
            return
        
        # Clear axis
        self.ax.clear()
        
        # Remove any duplicate timestamp+category combinations (keep last)
        df = df.drop_duplicates(subset=['timestamp', 'vessel_category'], keep='last')
        
        # Convert timestamps to proper datetime objects, removing timezone
        # This prevents matplotlib from getting confused
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_localize(None)
        
        # Pivot data for stacked area chart
        try:
            pivot_df = df.pivot(index='timestamp', columns='vessel_category', values='unique_vessels')
        except Exception as e:
            logger.error(f"Error pivoting data: {e}")
            return
        
        if pivot_df.empty:
            return
        
        # Ensure all categories exist (fill missing with 0)
        for category in self.categories:
            if category not in pivot_df.columns:
                pivot_df[category] = 0
        
        # Reorder columns to match category order
        pivot_df = pivot_df[self.categories]
        
        # Fill NaN with 0
        pivot_df = pivot_df.fillna(0)
        
        # Convert index to matplotlib date format manually
        import matplotlib.dates as mdates
        timestamps_mpl = mdates.date2num(pivot_df.index.to_pydatetime())
        
        # Create stacked area plot manually to avoid date conversion issues
        y_stack = pivot_df.values.T
        colors_list = [self.colors[cat] for cat in self.categories]
        
        self.ax.stackplot(timestamps_mpl, y_stack, 
                         labels=self.categories,
                         colors=colors_list,
                         alpha=0.8)
        
        # Formatting
        self.ax.set_xlabel('Time (UTC)', fontsize=12, fontweight='bold')
        self.ax.set_ylabel('Unique Vessels', fontsize=12, fontweight='bold')
        self.ax.legend(loc='upper left', framealpha=0.9, fontsize=10)
        self.ax.grid(True, alpha=0.3, linestyle='--')
        
        # Format x-axis with proper date formatter
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        self.ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        
        # Rotate labels
        plt.setp(self.ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Add statistics text
        total_vessels = df.groupby('timestamp')['unique_vessels'].sum()
        if not total_vessels.empty:
            latest_total = total_vessels.iloc[-1]
            max_total = total_vessels.max()
            
            stats_text = f'Latest: {latest_total:.0f} vessels | Peak: {max_total:.0f} vessels | Windows: {len(pivot_df)}'
            self.ax.text(
                0.02, 0.98, stats_text,
                transform=self.ax.transAxes,
                fontsize=11,
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8)
            )
        
        plt.tight_layout()
    
    def start(self):
        """
        Start the dashboard with real-time updates
        """
        logger.info("Starting visualization dashboard...")
        logger.info(f"Update interval: {self.update_interval}ms")
        
        # Create animation
        ani = animation.FuncAnimation(
            self.fig,
            self._update_plot,
            interval=self.update_interval,
            cache_frame_data=False
        )
        
        plt.show()


def main():
    """
    Standalone dashboard entry point
    """
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start dashboard
    dashboard = VesselDashboard(update_interval=5000)  # Update every 5 seconds
    
    try:
        dashboard.start()
    except KeyboardInterrupt:
        logger.info("Dashboard closed")


if __name__ == "__main__":
    main()
