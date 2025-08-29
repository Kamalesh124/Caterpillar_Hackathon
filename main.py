from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
import uvicorn
from datetime import datetime, timedelta
import pytz
from typing import List, Optional

from database import get_db, engine
from models import Base
from routers import (
    demand_forecast,
    usage_tracking,
    rental_summary,
    anomaly_detection,
    dealer_digest,
    overdue_fees,
    security_system,
    geofence_control,
    predictive_health
)
from services.data_loader import load_csv_data

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Smart Rental Tracking System",
    description="Dealer-oriented equipment rental tracking with predictive analytics",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(demand_forecast.router, prefix="/api/v1/demand", tags=["Demand Forecasting"])
app.include_router(usage_tracking.router, prefix="/api/v1/usage", tags=["Usage Tracking"])
app.include_router(rental_summary.router, prefix="/api/v1/rental-summary", tags=["Rental Summary"])
app.include_router(anomaly_detection.router, prefix="/api/v1/anomalies", tags=["Anomaly Detection"])
app.include_router(dealer_digest.router, prefix="/api/v1/digest", tags=["Dealer Digest"])
app.include_router(overdue_fees.router, prefix="/api/v1/overdue", tags=["Overdue & Fees"])
app.include_router(security_system.router, prefix="/api/v1/security", tags=["Security System"])
app.include_router(geofence_control.router, prefix="/api/v1/geofence", tags=["Geofence Control"])
app.include_router(predictive_health.router, prefix="/api/v1/predictive", tags=["Predictive Health"])

# Note: Static files mounting removed - frontend not built yet

@app.on_event("startup")
async def startup_event():
    """Initialize data on startup"""
    try:
        # Load initial data from CSV files
        await load_csv_data()
        
        print("✅ Application startup completed successfully")
    except Exception as e:
        print(f"❌ Startup error: {e}")

@app.get("/")
async def root():
    return {
        "message": "Smart Rental Tracking System API",
        "version": "1.0.0",
        "timezone": "Asia/Kolkata",
        "status": "active"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat(),
        "database": "connected",
        "redis": "connected"
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )