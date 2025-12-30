#!/usr/bin/env python3
"""
Cleanup and maintenance script for AIS pipeline
"""
import os
import sqlite3
from datetime import datetime, timedelta
import argparse


def backup_database(db_path='ais_vessel_data.db'):
    """Create a timestamped backup of the database"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return False
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"ais_vessel_data_backup_{timestamp}.db"
    
    import shutil
    shutil.copy2(db_path, backup_path)
    
    # Get file sizes
    original_size = os.path.getsize(db_path) / 1024  # KB
    backup_size = os.path.getsize(backup_path) / 1024
    
    print(f"✓ Created backup: {backup_path}")
    print(f"  Original: {original_size:.1f} KB")
    print(f"  Backup: {backup_size:.1f} KB")
    
    return True


def remove_duplicates(db_path='ais_vessel_data.db'):
    """Remove duplicate records from database"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check for duplicates
    cursor.execute("""
        SELECT timestamp, vessel_category, COUNT(*) as cnt
        FROM vessel_counts
        GROUP BY timestamp, vessel_category
        HAVING cnt > 1
    """)
    
    dupes = cursor.fetchall()
    
    if not dupes:
        print("✓ No duplicates found")
        conn.close()
        return
    
    print(f"Found {len(dupes)} duplicate timestamp+category combinations")
    
    # Remove duplicates (keep the one with highest id)
    cursor.execute("""
        DELETE FROM vessel_counts
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM vessel_counts
            GROUP BY timestamp, vessel_category
        )
    """)
    
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✓ Removed {deleted} duplicate records")


def clean_old_data(db_path='ais_vessel_data.db', days=30):
    """Remove data older than specified days"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Count records to delete
    cursor.execute("""
        SELECT COUNT(*) FROM vessel_counts
        WHERE timestamp < ?
    """, (cutoff_date,))
    
    count = cursor.fetchone()[0]
    
    if count == 0:
        print(f"✓ No data older than {days} days")
        conn.close()
        return
    
    print(f"Found {count} records older than {days} days")
    
    # Delete old records
    cursor.execute("""
        DELETE FROM vessel_counts
        WHERE timestamp < ?
    """, (cutoff_date,))
    
    conn.commit()
    
    # Vacuum to reclaim space
    print("Vacuuming database...")
    cursor.execute("VACUUM")
    
    conn.close()
    
    print(f"✓ Removed {count} old records")


def optimize_database(db_path='ais_vessel_data.db'):
    """Optimize database for better performance"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Optimizing database...")
    
    # Analyze tables
    cursor.execute("ANALYZE")
    
    # Rebuild indexes
    cursor.execute("REINDEX")
    
    # Vacuum
    cursor.execute("VACUUM")
    
    conn.close()
    
    print("✓ Database optimized")


def show_statistics(db_path='ais_vessel_data.db'):
    """Show database statistics"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("Database Statistics")
    print("=" * 60)
    
    # File size
    size_kb = os.path.getsize(db_path) / 1024
    print(f"File size: {size_kb:.1f} KB")
    
    # Total records
    cursor.execute("SELECT COUNT(*) FROM vessel_counts")
    total = cursor.fetchone()[0]
    print(f"Total records: {total:,}")
    
    # Unique windows
    cursor.execute("SELECT COUNT(DISTINCT timestamp) FROM vessel_counts")
    windows = cursor.fetchone()[0]
    print(f"Time windows: {windows:,}")
    
    # Date range
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM vessel_counts")
    min_ts, max_ts = cursor.fetchone()
    print(f"Date range: {min_ts} to {max_ts}")
    
    # Category breakdown
    print("\nCategory Summary:")
    cursor.execute("""
        SELECT 
            vessel_category,
            SUM(unique_vessels) as total,
            AVG(unique_vessels) as avg,
            MAX(unique_vessels) as peak
        FROM vessel_counts
        GROUP BY vessel_category
        ORDER BY total DESC
    """)
    
    print(f"{'Category':<12} {'Total':<10} {'Average':<10} {'Peak':<10}")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]:<12} {row[1]:<10} {row[2]:<10.1f} {row[3]:<10}")
    
    conn.close()
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='AIS Pipeline Cleanup and Maintenance')
    parser.add_argument('action', choices=['backup', 'duplicates', 'clean', 'optimize', 'stats', 'all'],
                       help='Action to perform')
    parser.add_argument('--db', default='ais_vessel_data.db',
                       help='Database path (default: ais_vessel_data.db)')
    parser.add_argument('--days', type=int, default=30,
                       help='Days to keep for clean action (default: 30)')
    
    args = parser.parse_args()
    
    if args.action == 'backup':
        backup_database(args.db)
    
    elif args.action == 'duplicates':
        remove_duplicates(args.db)
    
    elif args.action == 'clean':
        print(f"Cleaning data older than {args.days} days...")
        clean_old_data(args.db, args.days)
    
    elif args.action == 'optimize':
        optimize_database(args.db)
    
    elif args.action == 'stats':
        show_statistics(args.db)
    
    elif args.action == 'all':
        print("Running full maintenance...")
        print()
        backup_database(args.db)
        print()
        remove_duplicates(args.db)
        print()
        optimize_database(args.db)
        print()
        show_statistics(args.db)


if __name__ == "__main__":
    main()
