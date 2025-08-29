# Routers package

# Import all router modules
from . import (
    demand_forecast,
    usage_tracking,
    rental_summary,
    anomaly_detection,
    dealer_digest,
    overdue_fees,
    security,
    geofence,
    predictive_health
)

# For backward compatibility with existing imports
security_system = security
geofence_control = geofence

__all__ = [
    'demand_forecast',
    'usage_tracking', 
    'rental_summary',
    'anomaly_detection',
    'dealer_digest',
    'overdue_fees',
    'security',
    'geofence',
    'predictive_health',
    'security_system',  # backward compatibility
    'geofence_control'  # backward compatibility
]