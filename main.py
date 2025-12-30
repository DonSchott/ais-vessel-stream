"""
Main application - Coordinates the AIS streaming pipeline
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime

from config import AISSTREAM_API_KEY, DATABASE_PATH
from database import VesselDatabase
from aggregator import VesselAggregator
from ais_client import AISStreamClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ais_pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


class AISPipeline:
    """
    Main pipeline coordinator
    """
    
    def __init__(self):
        self.db = None
        self.aggregator = None
        self.client = None
        self.running = False
        
    async def start(self):
        """
        Start the streaming pipeline
        """
        logger.info("=" * 60)
        logger.info("AIS Vessel Streaming Pipeline Starting")
        logger.info("=" * 60)
        
        # Initialize database
        self.db = VesselDatabase(DATABASE_PATH)
        self.db.connect()
        
        # Initialize aggregator
        self.aggregator = VesselAggregator(self.db)
        
        # Initialize AIS client
        self.client = AISStreamClient(
            api_key=AISSTREAM_API_KEY,
            message_callback=self.aggregator.process_message
        )
        
        # Start statistics reporting task
        stats_task = asyncio.create_task(self._report_stats())
        
        # Start streaming
        self.running = True
        logger.info("Pipeline started. Press Ctrl+C to stop.")
        
        try:
            await self.client.connect_and_stream()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            await self.stop()
            stats_task.cancel()
    
    async def stop(self):
        """
        Stop the pipeline gracefully
        """
        logger.info("Stopping pipeline...")
        self.running = False
        
        # Close current window
        if self.aggregator:
            self.aggregator.force_close_current_window()
            
            # Print final stats
            stats = self.aggregator.get_stats()
            logger.info("=" * 60)
            logger.info("Final Statistics:")
            logger.info(f"  Messages processed: {stats['messages_processed']}")
            logger.info(f"  Windows completed: {stats['windows_completed']}")
            logger.info(f"  Cached vessels: {stats['cached_vessels']}")
            logger.info("=" * 60)
        
        # Stop client
        if self.client:
            await self.client.stop()
        
        # Close database
        if self.db:
            self.db.close()
        
        logger.info("Pipeline stopped")
    
    async def _report_stats(self):
        """
        Periodically report statistics
        """
        while self.running:
            await asyncio.sleep(30)  # Report every 30 seconds
            
            if self.aggregator:
                stats = self.aggregator.get_stats()
                logger.info(
                    f"Stats: {stats['messages_processed']} messages | "
                    f"{stats['windows_completed']} windows | "
                    f"{stats['cached_vessels']} vessels cached | "
                    f"{stats['current_window_vessels']} in current window"
                )


def main():
    """
    Main entry point
    """
    # Check API key
    if AISSTREAM_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("Please set your AIS Stream API key in config.py")
        sys.exit(1)
    
    # Create and run pipeline
    pipeline = AISPipeline()
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        raise KeyboardInterrupt
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the pipeline
    try:
        asyncio.run(pipeline.start())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
