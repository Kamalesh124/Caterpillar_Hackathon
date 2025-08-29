import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
import pytz
from database import SessionLocal
from models import (
    MasterEquipment, Rental, UsageDaily, DemandDaily, Event,
    EquipmentStatus, PaymentStatus, EventType, Severity
)
import os

IST = pytz.timezone('Asia/Kolkata')

def parse_datetime(date_str):
    """Parse datetime string with timezone support"""
    if pd.isna(date_str) or date_str == '':
        return None
    
    try:
        # Handle ISO format with timezone
        if '+' in str(date_str) or 'T' in str(date_str):
            return pd.to_datetime(date_str)
        # Handle DD-MM-YYYY format
        else:
            dt = pd.to_datetime(date_str, format='%d-%m-%Y')
            return IST.localize(dt.replace(hour=0, minute=0, second=0))
    except:
        return pd.to_datetime(date_str)

async def load_csv_data():
    """Load all CSV data into database"""
    db = SessionLocal()
    
    try:
        # Check if data already exists
        if db.query(MasterEquipment).count() > 0:
            print("Data already loaded, skipping CSV import")
            return
        
        print("Loading CSV data into database...")
        
        # Load master equipment
        df_equipment = pd.read_csv('master_equipment.csv')
        for _, row in df_equipment.iterrows():
            equipment = MasterEquipment(
                equipment_id=row['equipment_id'],
                equipment_type=row['equipment_type'],
                make=row['make'],
                model=row['model'],
                year=int(row['year']),
                capacity=float(row['capacity']),
                fuel_type=row['fuel_type'],
                branch_id=row['branch_id'],
                branch_name=row['branch_name'],
                site_id=row['site_id'],
                site_name=row['site_name'],
                customer_id=row['customer_id'] if pd.notna(row['customer_id']) else None,
                customer_name=row['customer_name'] if pd.notna(row['customer_name']) else None,
                status=EquipmentStatus(row['status'])
            )
            db.add(equipment)
        
        # Load rentals
        df_rentals = pd.read_csv('rentals.csv')
        for _, row in df_rentals.iterrows():
            rental = Rental(
                rental_id=row['rental_id'],
                equipment_id=row['equipment_id'],
                site_id=row['site_id'],
                customer_id=row['customer_id'],
                contract_start_ts=parse_datetime(row['contract_start_ts']),
                contract_end_ts_planned=parse_datetime(row['contract_end_ts_planned']),
                actual_end_ts=parse_datetime(row['actual_end_ts']) if pd.notna(row['actual_end_ts']) else None,
                billed_days=int(row['billed_days']),
                billed_hours=float(row['billed_hours']),
                rate_day=float(row['rate_day']),
                discount_pct=float(row['discount_pct']),
                revenue=float(row['revenue']),
                payment_status=PaymentStatus(row['payment_status']),
                late_hours=float(row['late_hours']),
                late_days=int(row['late_days']),
                late_fee=float(row['late_fee'])
            )
            db.add(rental)
        
        # Load usage daily
        df_usage = pd.read_csv('usage_daily.csv')
        for _, row in df_usage.iterrows():
            usage = UsageDaily(
                equipment_id=row['equipment_id'],
                date=parse_datetime(row['date']),
                runtime_hours=float(row['runtime_hours']),
                idle_hours=float(row['idle_hours']),
                distance_km=float(row['distance_km']),
                fuel_used_liters=float(row['fuel_used_liters']),
                fuel_eff_lph=float(row['fuel_eff_lph']),
                utilization_pct=float(row['utilization_pct']),
                availability_flag=bool(int(row['availability_flag'])),
                breakdown_hours=float(row['breakdown_hours']),
                last_gps_lat=float(row['last_gps_lat']) if pd.notna(row['last_gps_lat']) else None,
                last_gps_lon=float(row['last_gps_lon']) if pd.notna(row['last_gps_lon']) else None
            )
            db.add(usage)
        
        # Load demand daily
        df_demand = pd.read_csv('demand_daily.csv')
        for _, row in df_demand.iterrows():
            demand = DemandDaily(
                branch_id=row['branch_id'],
                equipment_type=row['equipment_type'],
                date=parse_datetime(row['date']),
                rental_requests=int(row['rental_requests']),
                confirmed_rentals=int(row['confirmed_rentals']),
                cancellations=int(row['cancellations'])
            )
            db.add(demand)
        
        # Load events
        df_events = pd.read_csv('events.csv')
        for _, row in df_events.iterrows():
            # Parse severity if present
            severity = None
            if pd.notna(row.get('severity', None)):
                try:
                    severity = Severity(row['severity'])
                except:
                    severity = None
            
            event = Event(
                event_id=row['event_id'],
                equipment_id=row['equipment_id'],
                ts=parse_datetime(row['ts']),
                event_type=EventType(row['event_type']),
                subtype=row['subtype'] if pd.notna(row['subtype']) else None,
                severity=severity,
                value=float(row['value']) if pd.notna(row['value']) else None,
                details=row['details'] if pd.notna(row['details']) else None
            )
            db.add(event)
        
        db.commit()
        print("CSV data loaded successfully!")
        
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def refresh_data():
    """Refresh data from CSV files"""
    db = SessionLocal()
    try:
        # Clear existing data
        db.query(Event).delete()
        db.query(UsageDaily).delete()
        db.query(DemandDaily).delete()
        db.query(Rental).delete()
        db.query(MasterEquipment).delete()
        db.commit()
        
        # Reload data
        import asyncio
        asyncio.run(load_csv_data())
        
    except Exception as e:
        print(f"Error refreshing data: {e}")
        db.rollback()
        raise
    finally:
        db.close()