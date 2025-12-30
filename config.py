"""
Configuration for AIS Streaming Pipeline
"""

# AIS Stream API Configuration
AISSTREAM_API_KEY = ""  # Replace with your actual API key
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# Geographic bounding box (US East Coast)
# Example regions:
# **Mediterranean**: `[[30, -6], [46, 37]]`
# **English Channel**: `[[49, -6], [52, 3]]`
# **Singapore Strait**: `[[1, 103], [2, 105]]`
#   ** US East-coast: [32, -81], [45, -65] 
BOUNDING_BOX = [
    [1, 103],  # Southwest corner (lat, lon)
    [2, 105]   # Northeast corner (lat, lon)
]

# Time window for aggregation (seconds)
AGGREGATION_WINDOW_SECONDS = 60

# Database configuration
import os
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "ais_vessel_data.db")

# Vessel type mapping (AIS ship type codes to categories)
VESSEL_TYPE_MAPPING = {
    'Cargo': list(range(70, 80)),
    'Tanker': list(range(80, 90)),
    'Passenger': list(range(60, 70)),
    'Fishing': list(range(30, 40)),
}

def get_vessel_category(ship_type):
    """
    Map AIS ship type code to category
    
    Args:
        ship_type: Integer AIS ship type code or None
    
    Returns:
        String category name
    """
    if ship_type is None:
        return 'Unknown'
    
    for category, type_codes in VESSEL_TYPE_MAPPING.items():
        if ship_type in type_codes:
            return category
    
    return 'Other'

# Message types to subscribe to
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]
