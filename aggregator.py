"""
Real-time streaming aggregator for AIS vessel data
"""
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, Set
import logging

from config import AGGREGATION_WINDOW_SECONDS, get_vessel_category
from database import VesselDatabase

logger = logging.getLogger(__name__)


class VesselAggregator:
    """
    Streaming aggregator that maintains sliding windows of vessel data
    """
    
    def __init__(self, db: VesselDatabase, window_seconds: int = AGGREGATION_WINDOW_SECONDS):
        """
        Initialize the aggregator
        
        Args:
            db: Database instance for persisting aggregations
            window_seconds: Size of aggregation window in seconds
        """
        self.db = db
        self.window_seconds = window_seconds
        
        # Cache of vessel metadata (MMSI -> ship_type)
        self.vessel_metadata: Dict[int, int] = {}
        
        # Current window tracking
        self.current_window_start = None
        self.current_window_vessels: Dict[str, Set[int]] = defaultdict(set)
        
        # Statistics
        self.messages_processed = 0
        self.windows_completed = 0
        
    def process_message(self, message: dict):
        """
        Process a single AIS message
        
        Args:
            message: Decoded AIS message from aisstream.io
        """
        try:
            message_type = message.get('MessageType')
            
            if message_type == 'ShipStaticData':
                self._process_static_data(message)
            elif message_type == 'PositionReport':
                self._process_position_report(message)
                
            self.messages_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _process_static_data(self, message: dict):
        """Process ship static data to cache vessel metadata"""
        try:
            metadata = message.get('MetaData', {})
            mmsi = metadata.get('MMSI')
            
            ship_static = message.get('Message', {}).get('ShipStaticData', {})
            ship_type = ship_static.get('Type')
            
            if mmsi and ship_type is not None:
                self.vessel_metadata[mmsi] = ship_type
                logger.debug(f"Cached metadata for MMSI {mmsi}: type {ship_type}")
                
        except Exception as e:
            logger.error(f"Error processing static data: {e}")
    
    def _process_position_report(self, message: dict):
        """Process position report and update current window"""
        try:
            metadata = message.get('MetaData', {})
            mmsi = metadata.get('MMSI')
            timestamp_str = metadata.get('time_utc')
            
            if not mmsi or not timestamp_str:
                return
            
            # Parse timestamp - handle AISStream format with nanoseconds
            # Format: '2025-12-29 15:54:06.743205339 +0000 UTC'
            # Python's strptime only handles microseconds (6 digits), so truncate nanoseconds
            timestamp_str = timestamp_str.replace(' UTC', '').strip()
            
            # Split on space to get date/time and timezone separately
            parts = timestamp_str.rsplit(' ', 1)  # ['2025-12-29 15:54:06.743205339', '+0000']
            datetime_part = parts[0]
            tz_part = parts[1] if len(parts) > 1 else '+0000'
            
            # Split datetime into date+time and fractional seconds
            if '.' in datetime_part:
                base_time, fractional = datetime_part.split('.')
                # Truncate to 6 digits (microseconds) from 9 digits (nanoseconds)
                fractional = fractional[:6]
                datetime_part = f"{base_time}.{fractional}"
            
            # Reconstruct and parse
            timestamp_str = f"{datetime_part} {tz_part}"
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f %z')
            
            # Initialize window if needed
            if self.current_window_start is None:
                self._start_new_window(timestamp)
            
            # Check if we need to close current window and start new one
            window_end = self.current_window_start + timedelta(seconds=self.window_seconds)
            
            if timestamp >= window_end:
                self._close_window(window_end)
                self._start_new_window(timestamp)
            
            # Get vessel category
            ship_type = self.vessel_metadata.get(mmsi)
            category = get_vessel_category(ship_type)
            
            # Add to current window
            self.current_window_vessels[category].add(mmsi)
            
        except Exception as e:
            logger.error(f"Error processing position report: {e}")
    
    def _start_new_window(self, timestamp: datetime):
        """Start a new aggregation window"""
        # Align to minute boundaries for cleaner timestamps
        self.current_window_start = timestamp.replace(second=0, microsecond=0)
        self.current_window_vessels = defaultdict(set)
        logger.info(f"Started new window at {self.current_window_start}")
    
    def _close_window(self, window_end: datetime):
        """Close current window and persist aggregation"""
        # Count unique vessels per category
        vessel_counts = {
            category: len(mmsi_set)
            for category, mmsi_set in self.current_window_vessels.items()
        }
        
        # Ensure all categories are represented (even if zero)
        all_categories = ['Cargo', 'Tanker', 'Passenger', 'Fishing', 'Other', 'Unknown']
        for category in all_categories:
            if category not in vessel_counts:
                vessel_counts[category] = 0
        
        # Persist to database
        self.db.insert_aggregation(
            self.current_window_start,
            window_end,
            vessel_counts
        )
        
        self.windows_completed += 1
        
        logger.info(
            f"Closed window {self.current_window_start} -> {window_end}: "
            f"Total unique vessels = {sum(vessel_counts.values())}"
        )
        logger.debug(f"Category breakdown: {vessel_counts}")
    
    def force_close_current_window(self):
        """Force close the current window (useful for shutdown)"""
        if self.current_window_start:
            window_end = datetime.utcnow()
            self._close_window(window_end)
    
    def get_stats(self) -> dict:
        """Get aggregator statistics"""
        return {
            'messages_processed': self.messages_processed,
            'windows_completed': self.windows_completed,
            'cached_vessels': len(self.vessel_metadata),
            'current_window_vessels': sum(
                len(mmsi_set) for mmsi_set in self.current_window_vessels.values()
            )
        }
