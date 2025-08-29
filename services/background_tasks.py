from celery import Celery
from database import CELERY_BROKER_URL, CELERY_RESULT_BACKEND
from datetime import datetime, timedelta
import pytz

# Initialize Celery
celery_app = Celery(
    "smart_rental_tracking",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=[
        'services.background_tasks'
    ]
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    beat_schedule={
        'check-overdue-rentals': {
            'task': 'services.background_tasks.check_overdue_rentals',
            'schedule': 300.0,  # Every 5 minutes
        },
        'generate-daily-digest': {
            'task': 'services.background_tasks.generate_daily_digest',
            'schedule': 3600.0,  # Every hour
        },
        'update-predictive-health': {
            'task': 'services.background_tasks.update_predictive_health',
            'schedule': 1800.0,  # Every 30 minutes
        },
        'detect-anomalies': {
            'task': 'services.background_tasks.detect_anomalies',
            'schedule': 600.0,  # Every 10 minutes
        },
    }
)

IST = pytz.timezone('Asia/Kolkata')

@celery_app.task
def check_overdue_rentals():
    """Check for overdue rentals and update late fees"""
    from database import SessionLocal
    from models import Rental, PaymentStatus
    from sqlalchemy import and_
    
    db = SessionLocal()
    try:
        now = datetime.now(IST)
        
        # Find overdue rentals
        overdue_rentals = db.query(Rental).filter(
            and_(
                Rental.contract_end_ts_planned < now,
                Rental.actual_end_ts.is_(None),
                Rental.payment_status != PaymentStatus.PAID
            )
        ).all()
        
        for rental in overdue_rentals:
            # Calculate overdue hours
            overdue_delta = now - rental.contract_end_ts_planned
            overdue_hours = overdue_delta.total_seconds() / 3600
            
            # Update late hours and fees
            rental.late_hours = overdue_hours
            rental.late_days = int(overdue_hours / 24)
            
            # Calculate late fee (₹300 per hour after grace period)
            grace_hours = rental.grace_minutes / 60 if rental.grace_minutes else 1
            if overdue_hours > grace_hours:
                rental.late_fee = (overdue_hours - grace_hours) * 300
            
            # Update payment status
            if overdue_hours > 24:
                rental.payment_status = PaymentStatus.OVERDUE
        
        db.commit()
        return f"Updated {len(overdue_rentals)} overdue rentals"
        
    except Exception as e:
        db.rollback()
        return f"Error checking overdue rentals: {e}"
    finally:
        db.close()

@celery_app.task
def generate_daily_digest():
    """Generate daily digest for dealers"""
    from database import SessionLocal
    from models import UsageDaily, Event, Rental
    from sqlalchemy import func, and_
    
    db = SessionLocal()
    try:
        now = datetime.now(IST)
        yesterday = now - timedelta(days=1)
        
        # Get usage statistics for last 24h
        usage_stats = db.query(
            func.sum(UsageDaily.runtime_hours).label('total_runtime'),
            func.sum(UsageDaily.idle_hours).label('total_idle'),
            func.sum(UsageDaily.fuel_used_liters).label('total_fuel'),
            func.avg(UsageDaily.utilization_pct).label('avg_utilization')
        ).filter(
            UsageDaily.date >= yesterday
        ).first()
        
        # Get event counts by type
        event_counts = db.query(
            Event.event_type,
            func.count(Event.event_id).label('count')
        ).filter(
            Event.ts >= yesterday
        ).group_by(Event.event_type).all()
        
        # Get overdue rental count
        overdue_count = db.query(Rental).filter(
            and_(
                Rental.contract_end_ts_planned < now,
                Rental.actual_end_ts.is_(None)
            )
        ).count()
        
        digest_data = {
            'date': now.strftime('%Y-%m-%d'),
            'usage_stats': {
                'total_runtime': float(usage_stats.total_runtime or 0),
                'total_idle': float(usage_stats.total_idle or 0),
                'total_fuel': float(usage_stats.total_fuel or 0),
                'avg_utilization': float(usage_stats.avg_utilization or 0)
            },
            'event_counts': {event.event_type.value: event.count for event in event_counts},
            'overdue_rentals': overdue_count
        }
        
        return digest_data
        
    except Exception as e:
        return f"Error generating daily digest: {e}"
    finally:
        db.close()

@celery_app.task
def update_predictive_health():
    """Update predictive health scores for equipment"""
    from database import SessionLocal
    from models import UsageDaily, PredictiveHealth, MasterEquipment
    from sqlalchemy import func, and_
    import numpy as np
    
    db = SessionLocal()
    try:
        now = datetime.now(IST)
        week_ago = now - timedelta(days=7)
        
        # Get equipment with recent usage
        equipment_list = db.query(MasterEquipment.equipment_id).filter(
            MasterEquipment.equipment_id.in_(
                db.query(UsageDaily.equipment_id).filter(
                    UsageDaily.date >= week_ago
                ).distinct()
            )
        ).all()
        
        predictions_added = 0
        
        for equipment in equipment_list:
            equipment_id = equipment.equipment_id
            
            # Get recent usage data
            usage_data = db.query(UsageDaily).filter(
                and_(
                    UsageDaily.equipment_id == equipment_id,
                    UsageDaily.date >= week_ago
                )
            ).all()
            
            if len(usage_data) < 3:  # Need minimum data points
                continue
            
            # Simple predictive model based on usage patterns
            breakdown_hours = sum(u.breakdown_hours for u in usage_data)
            avg_utilization = np.mean([u.utilization_pct for u in usage_data])
            fuel_efficiency_trend = np.mean([u.fuel_eff_lph for u in usage_data if u.fuel_eff_lph > 0])
            
            # Calculate failure probability (simplified model)
            failure_prob = min(0.95, (
                (breakdown_hours * 0.1) +
                (max(0, 80 - avg_utilization) * 0.005) +
                (max(0, 10 - fuel_efficiency_trend) * 0.02)
            ))
            
            # Determine predicted failure type
            failure_type = "Normal Operation"
            recommended_action = "Continue monitoring"
            days_until_failure = None
            
            if failure_prob > 0.7:
                failure_type = "Hydraulic System Issue"
                recommended_action = "Schedule immediate inspection"
                days_until_failure = 2
            elif failure_prob > 0.5:
                failure_type = "Engine Performance Degradation"
                recommended_action = "Schedule preventive maintenance"
                days_until_failure = 7
            elif failure_prob > 0.3:
                failure_type = "Minor Component Wear"
                recommended_action = "Monitor closely, schedule maintenance"
                days_until_failure = 14
            
            # Save prediction
            prediction = PredictiveHealth(
                equipment_id=equipment_id,
                prediction_date=now,
                failure_probability=failure_prob,
                predicted_failure_type=failure_type,
                recommended_action=recommended_action,
                confidence_score=0.75,  # Static confidence for demo
                days_until_failure=days_until_failure
            )
            
            db.add(prediction)
            predictions_added += 1
        
        db.commit()
        return f"Updated predictive health for {predictions_added} equipment"
        
    except Exception as e:
        db.rollback()
        return f"Error updating predictive health: {e}"
    finally:
        db.close()

@celery_app.task
def detect_anomalies():
    """Detect anomalies in equipment usage"""
    from database import SessionLocal
    from models import UsageDaily, Event, EventType, Severity
    from sqlalchemy import and_, func
    import numpy as np
    
    db = SessionLocal()
    try:
        now = datetime.now(IST)
        today = now.date()
        week_ago = now - timedelta(days=7)
        
        # Get today's usage data
        today_usage = db.query(UsageDaily).filter(
            func.date(UsageDaily.date) == today
        ).all()
        
        anomalies_detected = 0
        
        for usage in today_usage:
            equipment_id = usage.equipment_id
            
            # Get historical data for comparison
            historical_data = db.query(UsageDaily).filter(
                and_(
                    UsageDaily.equipment_id == equipment_id,
                    UsageDaily.date >= week_ago,
                    func.date(UsageDaily.date) != today
                )
            ).all()
            
            if len(historical_data) < 3:
                continue
            
            # Calculate baselines
            avg_fuel = np.mean([h.fuel_used_liters for h in historical_data if h.fuel_used_liters > 0])
            avg_idle = np.mean([h.idle_hours for h in historical_data])
            
            # Detect fuel spike anomaly
            if usage.fuel_used_liters > 0 and avg_fuel > 0:
                if usage.fuel_used_liters > avg_fuel * 2:
                    event = Event(
                        event_id=f"ANOM_{equipment_id}_{int(now.timestamp())}",
                        equipment_id=equipment_id,
                        ts=now,
                        event_type=EventType.ANOMALY,
                        subtype="FUEL_SPIKE",
                        severity=Severity.MEDIUM,
                        value=usage.fuel_used_liters,
                        details=f"Fuel usage {usage.fuel_used_liters:.1f}L vs baseline {avg_fuel:.1f}L"
                    )
                    db.add(event)
                    anomalies_detected += 1
            
            # Detect excessive idle anomaly
            if usage.idle_hours > avg_idle * 1.5 and usage.idle_hours > 2:
                event = Event(
                    event_id=f"ANOM_{equipment_id}_{int(now.timestamp())}_IDLE",
                    equipment_id=equipment_id,
                    ts=now,
                    event_type=EventType.ANOMALY,
                    subtype="EXCESS_IDLE",
                    severity=Severity.LOW,
                    value=usage.idle_hours,
                    details=f"Idle time {usage.idle_hours:.1f}h vs baseline {avg_idle:.1f}h"
                )
                db.add(event)
                anomalies_detected += 1
        
        db.commit()
        return f"Detected {anomalies_detected} anomalies"
        
    except Exception as e:
        db.rollback()
        return f"Error detecting anomalies: {e}"
    finally:
        db.close()

def setup_celery_tasks():
    """Setup and start Celery background tasks"""
    print("Celery background tasks configured successfully!")
    return celery_app