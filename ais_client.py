"""
AIS Stream Client - Connects to aisstream.io and receives live vessel data
"""
import asyncio
import json
import logging
import websockets
from typing import Callable

from config import AISSTREAM_API_KEY, AISSTREAM_URL, BOUNDING_BOX, MESSAGE_TYPES

logger = logging.getLogger(__name__)


class AISStreamClient:
    """
    WebSocket client for aisstream.io
    """
    
    def __init__(self, api_key: str, message_callback: Callable):
        """
        Initialize AIS stream client
        
        Args:
            api_key: Your aisstream.io API key
            message_callback: Function to call with each received message
        """
        self.api_key = api_key
        self.message_callback = message_callback
        self.websocket = None
        self.running = False
        
    async def connect_and_stream(self):
        """
        Connect to AIS stream and process messages
        """
        subscription_message = {
            "APIKey": self.api_key,
            "BoundingBoxes": [BOUNDING_BOX],
            "FilterMessageTypes": MESSAGE_TYPES
        }
        
        logger.info("Connecting to AIS Stream...")
        logger.info(f"Bounding box: {BOUNDING_BOX}")
        logger.info(f"Message types: {MESSAGE_TYPES}")
        
        try:
            async with websockets.connect(AISSTREAM_URL) as websocket:
                self.websocket = websocket
                self.running = True
                
                # Send subscription message
                await websocket.send(json.dumps(subscription_message))
                logger.info("Subscription sent, waiting for messages...")
                
                # Receive and process messages
                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                        
                        # Call the callback (aggregator) with the message
                        self.message_callback(message)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                    except Exception as e:
                        logger.error(f"Error in message callback: {e}")
                        
        except websockets.exceptions.WebSocketException as e:
            logger.error(f"WebSocket error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
        finally:
            self.running = False
            self.websocket = None
            logger.info("AIS Stream connection closed")
    
    async def stop(self):
        """Stop the stream gracefully"""
        self.running = False
        if self.websocket:
            await self.websocket.close()
