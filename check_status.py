#!/usr/bin/env python3
"""
Diagnostic script to check AIS pipeline status
"""
import sqlite3
import sys
from datetime import datetime

def check_database(db_path='ais_vessel_data.db'):
    """Check database contents"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("=" * 60)
        print("AIS Pipeline Database Diagnostics")
        print("=" * 60)
        
        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='vessel_counts'
        """)
        
        if not cursor.fetchone():
            print("❌ vessel_counts table does not exist")
            print("   Run main.py to create the database")
            conn.close()
            return
        
        print("✓ vessel_counts table exists")
        print()
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM vessel_counts")
        total_records = cursor.fetchone()[0]
        print(f"Total records: {total_records}")
        
        if total_records == 0:
            print()
            print("⚠️  No data in database yet")
            print()
            print("This is normal if:")
            print("  • main.py just started (wait 60+ seconds)")
            print("  • No vessels in the region")
            print("  • Windows haven't closed yet")
            print()
            print("The aggregator only saves data when a time window closes.")
            print("Windows close when the first message from the NEXT window arrives.")
            conn.close()
            return
        
        print()
        
        # Count unique windows
        cursor.execute("SELECT COUNT(DISTINCT timestamp) FROM vessel_counts")
        unique_windows = cursor.fetchone()[0]
        print(f"Unique time windows: {unique_windows}")
        
        # Get time range
        cursor.execute("""
            SELECT MIN(timestamp), MAX(timestamp) 
            FROM vessel_counts
        """)
        min_time, max_time = cursor.fetchone()
        print(f"Time range: {min_time} to {max_time}")
        
        print()
        print("-" * 60)
        print("Recent Data (last 5 windows):")
        print("-" * 60)
        
        # Get recent windows with totals
        cursor.execute("""
            SELECT 
                timestamp,
                SUM(unique_vessels) as total_vessels,
                GROUP_CONCAT(vessel_category || ':' || unique_vessels) as breakdown
            FROM vessel_counts
            GROUP BY timestamp
            ORDER BY timestamp DESC
            LIMIT 5
        """)
        
        rows = cursor.fetchall()
        
        if rows:
            for timestamp, total, breakdown in rows:
                print(f"\n{timestamp}")
                print(f"  Total vessels: {total}")
                print(f"  Breakdown: {breakdown}")
        
        print()
        print("-" * 60)
        print("Category Summary (all time):")
        print("-" * 60)
        
        cursor.execute("""
            SELECT 
                vessel_category,
                SUM(unique_vessels) as total,
                AVG(unique_vessels) as average,
                MAX(unique_vessels) as peak
            FROM vessel_counts
            GROUP BY vessel_category
            ORDER BY total DESC
        """)
        
        print(f"{'Category':<12} {'Total':<8} {'Avg':<8} {'Peak':<8}")
        print("-" * 60)
        for category, total, avg, peak in cursor.fetchall():
            print(f"{category:<12} {total:<8} {avg:<8.1f} {peak:<8}")
        
        print()
        print("=" * 60)
        print("✓ Database is healthy and contains data")
        print("  Run visualize.py to see the dashboard")
        print("=" * 60)
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import os
    
    db_path = 'ais_vessel_data.db'
    
    if not os.path.exists(db_path):
        print("❌ Database file not found: ais_vessel_data.db")
        print("   Run main.py to start collecting data")
        sys.exit(1)
    
    check_database(db_path)
