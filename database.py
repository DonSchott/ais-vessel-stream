"""
Database module for storing aggregated vessel counts
"""
import sqlite3
from datetime import datetime
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class VesselDatabase:
    """Handles SQLite database operations for vessel aggregation data"""
    
    def __init__(self, db_path: str):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection and create tables"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
        logger.info(f"Connected to database: {self.db_path}")
        
    def _create_tables(self):
        """Create the vessel_counts table if it doesn't exist"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS vessel_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                vessel_category TEXT NOT NULL,
                unique_vessels INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster queries
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON vessel_counts(timestamp)
        """)
        
        self.conn.commit()
        logger.info("Database tables created/verified")
        
    def insert_aggregation(self, window_start: datetime, window_end: datetime, 
                          vessel_counts: Dict[str, int]):
        """
        Insert aggregated vessel counts for a time window
        
        Args:
            window_start: Start of aggregation window
            window_end: End of aggregation window
            vessel_counts: Dictionary mapping category -> unique vessel count
        """
        timestamp = window_end.isoformat()
        window_start_str = window_start.isoformat()
        window_end_str = window_end.isoformat()
        
        records = [
            (timestamp, window_start_str, window_end_str, category, count)
            for category, count in vessel_counts.items()
        ]
        
        self.cursor.executemany("""
            INSERT INTO vessel_counts 
            (timestamp, window_start, window_end, vessel_category, unique_vessels)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        
        self.conn.commit()
        logger.debug(f"Inserted {len(records)} aggregation records for window ending {timestamp}")
        
    def get_recent_data(self, limit: int = 100):
        """
        Retrieve recent aggregation data for visualization
        
        Args:
            limit: Number of recent time windows to retrieve
            
        Returns:
            List of tuples (timestamp, category, count)
        """
        self.cursor.execute("""
            SELECT timestamp, vessel_category, unique_vessels
            FROM vessel_counts
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit * 6,))  # Multiply by ~6 categories to get enough windows
        
        return self.cursor.fetchall()
    
    def get_all_data(self):
        """
        Retrieve all aggregation data
        
        Returns:
            List of tuples (timestamp, category, count)
        """
        self.cursor.execute("""
            SELECT timestamp, vessel_category, unique_vessels
            FROM vessel_counts
            ORDER BY timestamp ASC
        """)
        
        return self.cursor.fetchall()
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
