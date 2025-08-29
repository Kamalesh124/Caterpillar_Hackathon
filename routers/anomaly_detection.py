from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import Event, UsageDaily, MasterEquipment, EventType, Severity
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import uuid

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Request models
class RealTimeDetectionRequest(BaseModel):
    equipment_id: str
    fuel_level: float
    engine_hours: float
    gps_lat: float
    gps_lon: float
    engine_temp: float
    hydraulic_pressure: float
    is_engine_on: bool
    operator_id: str = None

class TamperDetectionRequest(BaseModel):
    equipment_id: str
    telemetry_gaps: list[dict]  # List of gap periods
    sensor_anomalies: list[dict]  # List of sensor reading anomalies
    unauthorized_access: list[dict] = []  # List of unauthorized access attempts

@router.post("/real-time-detect")
async def real_time_anomaly_detection(
    request: RealTimeDetectionRequest,
    db: Session = Depends(get_db)
):
    """Advanced real-time anomaly detection using machine learning"""
    try:
        now = datetime.now(IST)
        anomalies_detected = []
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get historical data for ML analysis
        historical_data = db.query(UsageDaily).filter(
            and_(
                UsageDaily.equipment_id == request.equipment_id,
                UsageDaily.date >= (now - timedelta(days=30)).date()
            )
        ).all()
        
        # 1. Advanced Fuel Theft Detection
        fuel_anomalies = await _detect_fuel_theft_ml(request, historical_data, equipment)
        anomalies_detected.extend(fuel_anomalies)
        
        # 2. Sophisticated Idle Abuse Detection
        idle_anomalies = await _detect_idle_abuse_advanced(request, historical_data, equipment)
        anomalies_detected.extend(idle_anomalies)
        
        # 3. Engine Performance Anomaly Detection
        performance_anomalies = await _detect_performance_anomalies(request, historical_data, equipment)
        anomalies_detected.extend(performance_anomalies)
        
        # 4. Operational Pattern Anomalies
        pattern_anomalies = await _detect_operational_patterns(request, historical_data, equipment)
        anomalies_detected.extend(pattern_anomalies)
        
        # Log detected anomalies to database
        for anomaly in anomalies_detected:
            await _log_anomaly_event(
                request.equipment_id, anomaly['subtype'], anomaly['severity'],
                anomaly['details'], anomaly['value'], db
            )
        
        return {
            'status': 'success',
            'equipment_id': request.equipment_id,
            'timestamp': now.isoformat(),
            'anomalies_detected': anomalies_detected,
            'total_anomalies': len(anomalies_detected),
            'risk_level': _calculate_risk_level(anomalies_detected)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in real-time detection: {str(e)}")

@router.post("/detect-tampering")
async def detect_tampering(
    request: TamperDetectionRequest,
    db: Session = Depends(get_db)
):
    """Advanced tampering detection based on telemetry and sensor data"""
    try:
        now = datetime.now(IST)
        tampering_indicators = []
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # 1. Analyze telemetry gaps
        for gap in request.telemetry_gaps:
            gap_duration = gap.get('duration_minutes', 0)
            gap_time = gap.get('start_time', '')
            
            if gap_duration > 30:  # Gaps longer than 30 minutes are suspicious
                severity = 'HIGH' if gap_duration > 120 else 'MEDIUM'
                tampering_indicators.append({
                    'type': 'TELEMETRY_GAP',
                    'severity': severity,
                    'details': f"Telemetry gap of {gap_duration} minutes at {gap_time}",
                    'value': gap_duration,
                    'timestamp': gap_time
                })
        
        # 2. Analyze sensor anomalies
        for anomaly in request.sensor_anomalies:
            sensor_type = anomaly.get('sensor_type', 'unknown')
            deviation = anomaly.get('deviation_percentage', 0)
            
            if deviation > 50:  # More than 50% deviation is highly suspicious
                tampering_indicators.append({
                    'type': 'SENSOR_TAMPERING',
                    'severity': 'HIGH',
                    'details': f"{sensor_type} sensor showing {deviation}% deviation from normal",
                    'value': deviation,
                    'timestamp': anomaly.get('timestamp', now.isoformat())
                })
        
        # 3. Analyze unauthorized access patterns
        for access in request.unauthorized_access:
            access_time = access.get('timestamp', '')
            access_type = access.get('type', 'unknown')
            
            tampering_indicators.append({
                'type': 'UNAUTHORIZED_ACCESS',
                'severity': 'HIGH',
                'details': f"Unauthorized {access_type} access detected at {access_time}",
                'value': 1,
                'timestamp': access_time
            })
        
        # Log tampering events
        for indicator in tampering_indicators:
            await _log_anomaly_event(
                request.equipment_id, indicator['type'], indicator['severity'],
                indicator['details'], indicator['value'], db
            )
        
        # Calculate tampering risk score
        risk_score = _calculate_tampering_risk(tampering_indicators)
        
        return {
            'status': 'success',
            'equipment_id': request.equipment_id,
            'timestamp': now.isoformat(),
            'tampering_indicators': tampering_indicators,
            'total_indicators': len(tampering_indicators),
            'risk_score': risk_score,
            'recommendation': _get_tampering_recommendation(risk_score)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in tampering detection: {str(e)}")

@router.get("/detect")
async def detect_anomalies(hours: int = 24, db: Session = Depends(get_db)):
    """Detect anomalies in equipment usage and behavior"""
    try:
        now = datetime.now(IST)
        start_time = now - timedelta(hours=hours)
        
        # Get anomaly events from the last N hours
        anomaly_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Event.event_type == EventType.ANOMALY,
                Event.ts >= start_time
            )
        ).order_by(Event.ts.desc()).all()
        
        # Get usage data for analysis
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= (now - timedelta(days=7)).date()
        ).all()
        
        # Process anomaly events
        anomalies = []
        for event, equipment_type, branch_name, site_name in anomaly_events:
            anomalies.append({
                'event_id': event.event_id,
                'equipment_id': event.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'site_name': site_name,
                'timestamp': event.ts.strftime('%Y-%m-%d %H:%M:%S'),
                'event_type': event.event_type.value,
                'subtype': event.subtype,
                'severity': event.severity.value,
                'value': event.value,
                'details': event.details,
                'hours_ago': round((now - event.ts).total_seconds() / 3600, 1)
            })
        
        # Analyze usage patterns for additional anomalies
        usage_anomalies = await _analyze_usage_patterns(usage_data, db)
        
        # Combine all anomalies
        all_anomalies = anomalies + usage_anomalies
        
        # Save to CSV
        csv_path = 'outputs/anomalies_detected_rules.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if all_anomalies:
            df = pd.DataFrame(all_anomalies)
            df.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        severity_counts = {}
        subtype_counts = {}
        equipment_counts = {}
        
        for anomaly in all_anomalies:
            severity = anomaly['severity']
            subtype = anomaly['subtype']
            equipment_id = anomaly['equipment_id']
            
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1
            equipment_counts[equipment_id] = equipment_counts.get(equipment_id, 0) + 1
        
        # Find equipment with most anomalies
        most_problematic = max(equipment_counts.items(), key=lambda x: x[1]) if equipment_counts else None
        
        return {
            'status': 'success',
            'anomalies': all_anomalies,
            'csv_file': csv_path,
            'summary': {
                'total_anomalies': len(all_anomalies),
                'severity_breakdown': severity_counts,
                'subtype_breakdown': subtype_counts,
                'most_problematic_equipment': {
                    'equipment_id': most_problematic[0],
                    'anomaly_count': most_problematic[1]
                } if most_problematic else None,
                'period': f'Last {hours} hours'
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting anomalies: {str(e)}")


# Advanced ML-based anomaly detection helper functions
async def _detect_fuel_theft_ml(request: RealTimeDetectionRequest, historical_data, equipment):
    """Advanced fuel theft detection using machine learning"""
    anomalies = []
    
    try:
        # Convert historical data to DataFrame for analysis
        if not historical_data:
            return anomalies
            
        df = pd.DataFrame([{
            'fuel_consumed': h.fuel_consumed,
            'runtime_hours': h.runtime_hours,
            'idle_hours': h.idle_hours,
            'date': h.date
        } for h in historical_data])
        
        if len(df) < 7:  # Need at least a week of data
            return anomalies
            
        # Calculate fuel efficiency metrics
        df['fuel_efficiency'] = df['fuel_consumed'] / (df['runtime_hours'] + 0.1)
        df['fuel_per_total_hour'] = df['fuel_consumed'] / (df['runtime_hours'] + df['idle_hours'] + 0.1)
        
        # Use Isolation Forest for anomaly detection
        features = ['fuel_consumed', 'fuel_efficiency', 'fuel_per_total_hour']
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(df[features])
        
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_scores = iso_forest.fit_predict(scaled_features)
        
        # Analyze current fuel consumption
        current_fuel_efficiency = request.fuel_level / (request.engine_hours + 0.1)
        
        # Statistical analysis
        mean_fuel = df['fuel_consumed'].mean()
        std_fuel = df['fuel_consumed'].std()
        mean_efficiency = df['fuel_efficiency'].mean()
        std_efficiency = df['fuel_efficiency'].std()
        
        # Detect sudden fuel spikes (potential theft)
        if request.fuel_level > mean_fuel + 3 * std_fuel:
            anomalies.append({
                'subtype': 'FUEL_THEFT_SPIKE',
                'severity': 'HIGH',
                'details': f'Abnormal fuel consumption: {request.fuel_level:.2f}L vs avg {mean_fuel:.2f}L',
                'value': request.fuel_level,
                'confidence': 0.9
            })
        
        # Detect poor fuel efficiency (potential theft or tampering)
        if current_fuel_efficiency > mean_efficiency + 2.5 * std_efficiency:
            anomalies.append({
                'subtype': 'FUEL_EFFICIENCY_ANOMALY',
                'severity': 'MEDIUM',
                'details': f'Poor fuel efficiency: {current_fuel_efficiency:.2f}L/h vs avg {mean_efficiency:.2f}L/h',
                'value': current_fuel_efficiency,
                'confidence': 0.8
            })
        
        # Detect fuel consumption without corresponding work hours
        if request.fuel_level > 10 and request.engine_hours < 1:
            anomalies.append({
                'subtype': 'FUEL_WITHOUT_WORK',
                'severity': 'HIGH',
                'details': f'High fuel consumption ({request.fuel_level:.2f}L) with minimal runtime ({request.engine_hours:.2f}h)',
                'value': request.fuel_level / (request.engine_hours + 0.1),
                'confidence': 0.95
            })
            
    except Exception as e:
        print(f"Error in fuel theft detection: {e}")
    
    return anomalies

async def _detect_idle_abuse_advanced(request: RealTimeDetectionRequest, historical_data, equipment):
    """Advanced idle abuse detection with pattern analysis"""
    anomalies = []
    
    try:
        # Calculate idle percentage based on engine status
        if not request.is_engine_on and request.engine_hours > 0:
            # Engine is off but has hours - potential idle abuse
            anomalies.append({
                'subtype': 'EXCESSIVE_IDLE',
                'severity': 'MEDIUM',
                'details': f'Engine off with accumulated hours: {request.engine_hours:.2f}h',
                'value': request.engine_hours,
                'confidence': 0.7
            })
        
        # Historical idle analysis
        if historical_data:
            df = pd.DataFrame([{
                'idle_hours': h.idle_hours,
                'runtime_hours': h.runtime_hours,
                'total_hours': h.runtime_hours + h.idle_hours
            } for h in historical_data if h.runtime_hours + h.idle_hours > 0])
            
            if len(df) > 0:
                df['idle_percentage'] = (df['idle_hours'] / df['total_hours']) * 100
                mean_idle_pct = df['idle_percentage'].mean()
                std_idle_pct = df['idle_percentage'].std()
                
                # Detect unusual idle patterns based on engine hours
                if request.engine_hours > 8:  # More than 8 hours of operation
                    idle_ratio = 0.8  # Assume 80% might be idle
                    estimated_idle = request.engine_hours * idle_ratio
                    
                    if estimated_idle > mean_idle_pct + 2 * std_idle_pct:
                        anomalies.append({
                            'subtype': 'IDLE_PATTERN_ANOMALY',
                            'severity': 'MEDIUM',
                            'details': f'Unusual idle pattern detected based on engine hours',
                            'value': estimated_idle,
                            'confidence': 0.75
                        })
            
    except Exception as e:
        print(f"Error in idle abuse detection: {e}")
    
    return anomalies

async def _detect_performance_anomalies(request: RealTimeDetectionRequest, historical_data, equipment):
    """Detect engine and operational performance anomalies"""
    anomalies = []
    
    try:
        # Analyze engine temperature
        if request.engine_temp > 100:  # High temperature threshold
            severity = 'HIGH' if request.engine_temp > 120 else 'MEDIUM'
            anomalies.append({
                'subtype': 'HIGH_ENGINE_TEMP',
                'severity': severity,
                'details': f'High engine temperature: {request.engine_temp:.1f}°C',
                'value': request.engine_temp,
                'confidence': 0.9
            })
        
        # Analyze hydraulic pressure
        if request.hydraulic_pressure < 50:  # Low pressure threshold
            anomalies.append({
                'subtype': 'LOW_HYDRAULIC_PRESSURE',
                'severity': 'MEDIUM',
                'details': f'Low hydraulic pressure: {request.hydraulic_pressure:.1f} PSI',
                'value': request.hydraulic_pressure,
                'confidence': 0.85
            })
        
        # Historical performance comparison
        if historical_data and len(historical_data) >= 7:
            df = pd.DataFrame([{
                'fuel_eff_lph': h.fuel_eff_lph,
                'runtime_hours': h.runtime_hours
            } for h in historical_data if h.fuel_eff_lph > 0])
            
            if len(df) > 0:
                mean_efficiency = df['fuel_eff_lph'].mean()
                std_efficiency = df['fuel_eff_lph'].std()
                
                # Calculate current efficiency estimate
                current_efficiency = request.fuel_level / (request.engine_hours + 0.1)
                
                if current_efficiency > mean_efficiency + 2 * std_efficiency:
                    anomalies.append({
                        'subtype': 'EFFICIENCY_ANOMALY',
                        'severity': 'MEDIUM',
                        'details': f'Unusual efficiency pattern: {current_efficiency:.2f} vs avg {mean_efficiency:.2f}',
                        'value': current_efficiency,
                        'confidence': 0.8
                    })
                
    except Exception as e:
        print(f"Error in performance anomaly detection: {e}")
    
    return anomalies

async def _detect_operational_patterns(request: RealTimeDetectionRequest, historical_data, equipment):
    """Detect unusual operational patterns"""
    anomalies = []
    
    try:
        # Detect unusual operating hours (if timestamp available)
        current_hour = datetime.now(IST).hour
        
        # Night operations (10 PM to 5 AM) might be suspicious
        if current_hour >= 22 or current_hour <= 5:
            if request.is_engine_on and request.engine_hours > 2:  # Significant night operation
                anomalies.append({
                    'subtype': 'NIGHT_OPERATION',
                    'severity': 'MEDIUM',
                    'details': f'Unusual night operation: {request.engine_hours:.2f}h at {current_hour}:00',
                    'value': request.engine_hours,
                    'confidence': 0.6
                })
        
        # Weekend operations (if date available)
        current_weekday = datetime.now(IST).weekday()
        if current_weekday >= 5:  # Saturday or Sunday
            if request.is_engine_on and request.engine_hours > 4:  # Significant weekend operation
                anomalies.append({
                    'subtype': 'WEEKEND_OPERATION',
                    'severity': 'LOW',
                    'details': f'Weekend operation detected: {request.engine_hours:.2f}h',
                    'value': request.engine_hours,
                    'confidence': 0.5
                })
        
        # Detect GPS anomalies (equipment moved without operation)
        if request.gps_lat != 0 and request.gps_lon != 0:
            if not request.is_engine_on and request.engine_hours == 0:
                # Equipment has GPS coordinates but no operation - potential theft
                anomalies.append({
                    'subtype': 'LOCATION_WITHOUT_OPERATION',
                    'severity': 'HIGH',
                    'details': f'Equipment location changed without operation at {request.gps_lat:.6f}, {request.gps_lon:.6f}',
                    'value': 1,
                    'confidence': 0.8
                })
            
    except Exception as e:
        print(f"Error in operational pattern detection: {e}")
    
    return anomalies

async def _log_anomaly_event(equipment_id: str, subtype: str, severity: str, details: str, value: float, db: Session):
    """Log anomaly event to database"""
    try:
        # Convert severity string to enum
        severity_enum = Severity.HIGH if severity == 'HIGH' else (
            Severity.MEDIUM if severity == 'MEDIUM' else Severity.LOW
        )
        
        event = Event(
            event_id=str(uuid.uuid4()),
            equipment_id=equipment_id,
            ts=datetime.now(IST),
            event_type=EventType.ANOMALY,
            subtype=subtype,
            severity=severity_enum,
            value=value,
            details=details
        )
        
        db.add(event)
        db.commit()
        
    except Exception as e:
        print(f"Error logging anomaly event: {e}")
        db.rollback()

def _calculate_risk_level(anomalies):
    """Calculate overall risk level based on detected anomalies"""
    if not anomalies:
        return 'LOW'
    
    high_count = sum(1 for a in anomalies if a['severity'] == 'HIGH')
    medium_count = sum(1 for a in anomalies if a['severity'] == 'MEDIUM')
    
    if high_count >= 2:
        return 'CRITICAL'
    elif high_count >= 1 or medium_count >= 3:
        return 'HIGH'
    elif medium_count >= 1:
        return 'MEDIUM'
    else:
        return 'LOW'

def _calculate_tampering_risk(indicators):
    """Calculate tampering risk score"""
    if not indicators:
        return 0
    
    risk_score = 0
    for indicator in indicators:
        if indicator['severity'] == 'HIGH':
            risk_score += 3
        elif indicator['severity'] == 'MEDIUM':
            risk_score += 2
        else:
            risk_score += 1
    
    return min(risk_score, 10)  # Cap at 10

def _get_tampering_recommendation(risk_score):
    """Get recommendation based on tampering risk score"""
    if risk_score >= 8:
        return "IMMEDIATE ACTION REQUIRED: High risk of tampering detected. Investigate immediately."
    elif risk_score >= 5:
        return "CAUTION: Moderate tampering risk. Schedule inspection within 24 hours."
    elif risk_score >= 2:
        return "MONITOR: Low tampering risk detected. Continue monitoring."
    else:
        return "NORMAL: No significant tampering indicators detected."

@router.post("/batch-process")
async def batch_anomaly_processing(
    equipment_ids: list[str],
    hours_back: int = 24,
    db: Session = Depends(get_db)
):
    """Process anomalies for multiple equipment in batch"""
    try:
        results = []
        now = datetime.now(IST)
        
        for equipment_id in equipment_ids:
            # Get equipment info
            equipment = db.query(MasterEquipment).filter(
                MasterEquipment.equipment_id == equipment_id
            ).first()
            
            if not equipment:
                results.append({
                    'equipment_id': equipment_id,
                    'status': 'error',
                    'message': 'Equipment not found'
                })
                continue
            
            # Get recent usage data
            recent_usage = db.query(UsageDaily).filter(
                and_(
                    UsageDaily.equipment_id == equipment_id,
                    UsageDaily.date >= (now - timedelta(hours=hours_back)).date()
                )
            ).order_by(UsageDaily.date.desc()).first()
            
            if not recent_usage:
                results.append({
                    'equipment_id': equipment_id,
                    'status': 'no_data',
                    'message': 'No recent usage data found'
                })
                continue
            
            # Create a mock request for analysis
            mock_request = RealTimeDetectionRequest(
                equipment_id=equipment_id,
                fuel_level=recent_usage.fuel_consumed,
                engine_hours=recent_usage.runtime_hours,
                engine_temp=85.0,  # Default values
                hydraulic_pressure=150.0,
                is_engine_on=recent_usage.runtime_hours > 0,
                gps_lat=0.0,
                gps_lon=0.0
            )
            
            # Get historical data
            historical_data = db.query(UsageDaily).filter(
                and_(
                    UsageDaily.equipment_id == equipment_id,
                    UsageDaily.date >= (now - timedelta(days=30)).date()
                )
            ).all()
            
            # Run anomaly detection
            anomalies_detected = []
            
            fuel_anomalies = await _detect_fuel_theft_ml(mock_request, historical_data, equipment)
            anomalies_detected.extend(fuel_anomalies)
            
            idle_anomalies = await _detect_idle_abuse_advanced(mock_request, historical_data, equipment)
            anomalies_detected.extend(idle_anomalies)
            
            performance_anomalies = await _detect_performance_anomalies(mock_request, historical_data, equipment)
            anomalies_detected.extend(performance_anomalies)
            
            pattern_anomalies = await _detect_operational_patterns(mock_request, historical_data, equipment)
            anomalies_detected.extend(pattern_anomalies)
            
            # Log anomalies
            for anomaly in anomalies_detected:
                await _log_anomaly_event(
                    equipment_id, anomaly['subtype'], anomaly['severity'],
                    anomaly['details'], anomaly['value'], db
                )
            
            results.append({
                'equipment_id': equipment_id,
                'status': 'success',
                'anomalies_detected': len(anomalies_detected),
                'risk_level': _calculate_risk_level(anomalies_detected),
                'anomalies': anomalies_detected
            })
        
        return {
            'status': 'success',
            'processed_equipment': len(equipment_ids),
            'timestamp': now.isoformat(),
            'results': results
        }
        
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Error in batch processing: {str(e)}")

async def _analyze_usage_patterns(usage_data, db: Session):
    """Analyze usage patterns to detect additional anomalies"""
    anomalies = []
    
    if not usage_data:
        return anomalies
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame([
        {
            'equipment_id': usage.equipment_id,
            'equipment_type': equipment_type,
            'branch_name': branch_name,
            'date': usage.date,
            'runtime_hours': usage.runtime_hours,
            'idle_hours': usage.idle_hours,
            'fuel_used_liters': usage.fuel_used_liters,
            'fuel_eff_lph': usage.fuel_eff_lph,
            'utilization_pct': usage.utilization_pct,
            'breakdown_hours': usage.breakdown_hours
        }
        for usage, equipment_type, branch_name in usage_data
    ])
    
    # 1. Detect fuel consumption anomalies
    for equipment_type in df['equipment_type'].unique():
        type_data = df[df['equipment_type'] == equipment_type]
        
        if len(type_data) > 3:  # Need enough data for statistical analysis
            fuel_mean = type_data['fuel_used_liters'].mean()
            fuel_std = type_data['fuel_used_liters'].std()
            fuel_threshold = fuel_mean + 2 * fuel_std  # 2 standard deviations
            
            fuel_spikes = type_data[type_data['fuel_used_liters'] > fuel_threshold]
            
            for _, row in fuel_spikes.iterrows():
                anomalies.append({
                    'event_id': f"FUEL_SPIKE_{row['equipment_id']}_{row['date']}",
                    'equipment_id': row['equipment_id'],
                    'equipment_type': row['equipment_type'],
                    'branch_name': row['branch_name'],
                    'site_name': 'N/A',
                    'timestamp': row['date'].strftime('%Y-%m-%d 12:00:00'),
                    'event_type': 'ANOMALY',
                    'subtype': 'FUEL_SPIKE',
                    'severity': 'HIGH',
                    'value': row['fuel_used_liters'],
                    'details': f"Fuel consumption {row['fuel_used_liters']:.1f}L exceeds normal by {row['fuel_used_liters'] - fuel_mean:.1f}L",
                    'hours_ago': (datetime.now(IST).date() - row['date']).days * 24
                })
    
    # 2. Detect excessive idle time
    for _, row in df.iterrows():
        total_hours = row['runtime_hours'] + row['idle_hours']
        if total_hours > 0:
            idle_percentage = (row['idle_hours'] / total_hours) * 100
            
            if idle_percentage > 70:  # More than 70% idle time
                anomalies.append({
                    'event_id': f"EXCESS_IDLE_{row['equipment_id']}_{row['date']}",
                    'equipment_id': row['equipment_id'],
                    'equipment_type': row['equipment_type'],
                    'branch_name': row['branch_name'],
                    'site_name': 'N/A',
                    'timestamp': row['date'].strftime('%Y-%m-%d 12:00:00'),
                    'event_type': 'ANOMALY',
                    'subtype': 'EXCESS_IDLE',
                    'severity': 'MEDIUM',
                    'value': idle_percentage,
                    'details': f"Equipment idle {idle_percentage:.1f}% of operational time ({row['idle_hours']:.1f}h idle, {row['runtime_hours']:.1f}h runtime)",
                    'hours_ago': (datetime.now(IST).date() - row['date']).days * 24
                })
    
    # 3. Detect fuel efficiency anomalies
    for equipment_type in df['equipment_type'].unique():
        type_data = df[(df['equipment_type'] == equipment_type) & (df['fuel_eff_lph'] > 0)]
        
        if len(type_data) > 3:
            eff_mean = type_data['fuel_eff_lph'].mean()
            eff_std = type_data['fuel_eff_lph'].std()
            eff_threshold = eff_mean + 2 * eff_std  # Poor efficiency threshold
            
            poor_efficiency = type_data[type_data['fuel_eff_lph'] > eff_threshold]
            
            for _, row in poor_efficiency.iterrows():
                anomalies.append({
                    'event_id': f"POOR_FUEL_EFF_{row['equipment_id']}_{row['date']}",
                    'equipment_id': row['equipment_id'],
                    'equipment_type': row['equipment_type'],
                    'branch_name': row['branch_name'],
                    'site_name': 'N/A',
                    'timestamp': row['date'].strftime('%Y-%m-%d 12:00:00'),
                    'event_type': 'ANOMALY',
                    'subtype': 'POOR_FUEL_EFFICIENCY',
                    'severity': 'MEDIUM',
                    'value': row['fuel_eff_lph'],
                    'details': f"Fuel efficiency {row['fuel_eff_lph']:.2f} L/h is {row['fuel_eff_lph'] - eff_mean:.2f} L/h above average",
                    'hours_ago': (datetime.now(IST).date() - row['date']).days * 24
                })
    
    # 4. Detect excessive breakdown hours
    breakdown_threshold = 4  # More than 4 hours of breakdown per day
    excessive_breakdown = df[df['breakdown_hours'] > breakdown_threshold]
    
    for _, row in excessive_breakdown.iterrows():
        anomalies.append({
            'event_id': f"EXCESS_BREAKDOWN_{row['equipment_id']}_{row['date']}",
            'equipment_id': row['equipment_id'],
            'equipment_type': row['equipment_type'],
            'branch_name': row['branch_name'],
            'site_name': 'N/A',
            'timestamp': row['date'].strftime('%Y-%m-%d 12:00:00'),
            'event_type': 'ANOMALY',
            'subtype': 'EXCESS_BREAKDOWN',
            'severity': 'HIGH',
            'value': row['breakdown_hours'],
            'details': f"Equipment breakdown time {row['breakdown_hours']:.1f}h exceeds threshold of {breakdown_threshold}h",
            'hours_ago': (datetime.now(IST).date() - row['date']).days * 24
        })
    
    return anomalies

@router.get("/charts")
async def generate_anomaly_charts(hours: int = 24, db: Session = Depends(get_db)):
    """Generate anomaly detection charts"""
    try:
        now = datetime.now(IST)
        start_time = now - timedelta(hours=hours)
        
        # Get anomaly events
        anomaly_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Event.event_type == EventType.ANOMALY,
                Event.ts >= start_time
            )
        ).all()
        
        charts = {}
        
        if not anomaly_events:
            return {'status': 'success', 'charts': {}, 'message': 'No anomalies detected in the specified period'}
        
        # 1. Timeline chart - Anomalies per equipment over time
        timeline_data = []
        for event, equipment_type, branch_name in anomaly_events:
            timeline_data.append({
                'equipment_id': event.equipment_id,
                'timestamp': event.ts,
                'subtype': event.subtype,
                'severity': event.severity.value,
                'equipment_type': equipment_type,
                'value': event.value
            })
        
        df_timeline = pd.DataFrame(timeline_data)
        
        # Create timeline scatter plot
        fig_timeline = px.scatter(
            df_timeline,
            x='timestamp',
            y='equipment_id',
            color='subtype',
            size='value',
            hover_data=['severity', 'equipment_type'],
            title='Anomaly Timeline by Equipment'
        )
        
        fig_timeline.update_layout(
            xaxis_title='Time',
            yaxis_title='Equipment ID',
            height=600
        )
        
        charts['timeline'] = json.loads(fig_timeline.to_json())
        
        # 2. Pie chart - Anomalies by severity
        severity_counts = df_timeline['severity'].value_counts()
        
        colors = {'HIGH': '#FF4444', 'MEDIUM': '#FF8800', 'LOW': '#FFBB00'}
        
        fig_severity = go.Figure(data=[go.Pie(
            labels=severity_counts.index,
            values=severity_counts.values,
            marker_colors=[colors.get(severity, '#9E9E9E') for severity in severity_counts.index],
            hole=0.3
        )])
        
        fig_severity.update_layout(
            title='Anomalies by Severity Level',
            annotations=[dict(text=f'{len(anomaly_events)}<br>Total<br>Anomalies', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        charts['severity_distribution'] = json.loads(fig_severity.to_json())
        
        # 3. Bar chart - Anomalies by subtype
        subtype_counts = df_timeline['subtype'].value_counts()
        
        fig_subtype = go.Figure(data=[go.Bar(
            x=subtype_counts.index,
            y=subtype_counts.values,
            marker_color='#FF6B6B'
        )])
        
        fig_subtype.update_layout(
            title='Anomalies by Type',
            xaxis_title='Anomaly Type',
            yaxis_title='Count',
            xaxis_tickangle=-45
        )
        
        charts['subtype_distribution'] = json.loads(fig_subtype.to_json())
        
        # 4. Heatmap - Anomalies by equipment type and hour of day
        df_timeline['hour'] = df_timeline['timestamp'].dt.hour
        heatmap_data = df_timeline.groupby(['equipment_type', 'hour']).size().reset_index(name='count')
        
        if not heatmap_data.empty:
            heatmap_pivot = heatmap_data.pivot(index='equipment_type', columns='hour', values='count').fillna(0)
            
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=heatmap_pivot.values,
                x=heatmap_pivot.columns,
                y=heatmap_pivot.index,
                colorscale='Reds',
                hoverongaps=False
            ))
            
            fig_heatmap.update_layout(
                title='Anomaly Frequency by Equipment Type and Hour',
                xaxis_title='Hour of Day',
                yaxis_title='Equipment Type'
            )
            
            charts['hourly_heatmap'] = json.loads(fig_heatmap.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating anomaly charts: {str(e)}")

@router.get("/insights")
async def generate_anomaly_insights(hours: int = 24, db: Session = Depends(get_db)):
    """Generate text insights for anomaly detection"""
    try:
        now = datetime.now(IST)
        start_time = now - timedelta(hours=hours)
        
        # Get anomaly events
        anomaly_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Event.event_type == EventType.ANOMALY,
                Event.ts >= start_time
            )
        ).order_by(Event.ts.desc()).all()
        
        insights = []
        
        if not anomaly_events:
            insights.append(f"✅ No anomalies detected in the last {hours} hours - all systems operating normally.")
            return {'status': 'success', 'insights': insights}
        
        total_anomalies = len(anomaly_events)
        insights.append(f"🚨 {total_anomalies} anomalies detected in the last {hours} hours.")
        
        # Analyze by severity
        severity_counts = {}
        for event, _, _, _ in anomaly_events:
            severity = event.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        if 'HIGH' in severity_counts:
            insights.append(f"⚠️ {severity_counts['HIGH']} HIGH severity anomalies require immediate attention.")
        
        # Analyze by subtype
        subtype_counts = {}
        recent_critical = []
        
        for event, equipment_type, branch_name, site_name in anomaly_events:
            subtype = event.subtype
            subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1
            
            # Collect recent critical events
            if event.severity == Severity.HIGH and (now - event.ts).total_seconds() < 3600:  # Last hour
                recent_critical.append({
                    'equipment_id': event.equipment_id,
                    'subtype': subtype,
                    'details': event.details,
                    'time': event.ts.strftime('%H:%M'),
                    'site': site_name or 'Unknown'
                })
        
        # Report most common anomaly types
        if subtype_counts:
            most_common = max(subtype_counts, key=subtype_counts.get)
            count = subtype_counts[most_common]
            insights.append(f"📊 Most frequent anomaly: {most_common} ({count} occurrences).")
        
        # Report recent critical events
        for critical in recent_critical[:3]:  # Top 3 most recent
            if critical['subtype'] == 'FUEL_SPIKE':
                insights.append(f"⛽ Fuel spike on {critical['equipment_id']} at {critical['time']} - {critical['details']}.")
            elif critical['subtype'] == 'EXCESS_IDLE':
                insights.append(f"⏰ Excessive idle time on {critical['equipment_id']} at {critical['site']} - investigate operator behavior.")
            elif critical['subtype'] == 'TELEMETRY_GAP':
                insights.append(f"📡 Telemetry gap on {critical['equipment_id']} - possible tampering or connectivity issue.")
            elif critical['subtype'] == 'TAMPER_SUSPECT':
                insights.append(f"🔒 Suspected tampering on {critical['equipment_id']} - security investigation required.")
        
        # Equipment with multiple anomalies
        equipment_counts = {}
        for event, _, _, _ in anomaly_events:
            equipment_id = event.equipment_id
            equipment_counts[equipment_id] = equipment_counts.get(equipment_id, 0) + 1
        
        problematic_equipment = [(eq_id, count) for eq_id, count in equipment_counts.items() if count >= 3]
        
        if problematic_equipment:
            problematic_equipment.sort(key=lambda x: x[1], reverse=True)
            worst_equipment, worst_count = problematic_equipment[0]
            insights.append(f"🔧 Equipment {worst_equipment} has {worst_count} anomalies - schedule immediate inspection.")
        
        # Time-based patterns
        hour_counts = {}
        for event, _, _, _ in anomaly_events:
            hour = event.ts.hour
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
        
        if hour_counts:
            peak_hour = max(hour_counts, key=hour_counts.get)
            peak_count = hour_counts[peak_hour]
            if peak_count >= 3:
                insights.append(f"⏰ Peak anomaly time: {peak_hour:02d}:00 hours ({peak_count} events) - review shift operations.")
        
        # Fuel-related insights
        fuel_events = [event for event, _, _, _ in anomaly_events if 'FUEL' in event.subtype]
        if fuel_events:
            total_fuel_loss = sum(float(event.value) for event in fuel_events if event.value)
            if total_fuel_loss > 0:
                insights.append(f"⛽ Potential fuel loss: {total_fuel_loss:.1f} liters from {len(fuel_events)} incidents.")
        
        return {
            'status': 'success',
            'insights': insights,
            'period': f'Last {hours} hours',
            'total_anomalies': total_anomalies,
            'severity_breakdown': severity_counts,
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating anomaly insights: {str(e)}")

@router.get("/equipment/{equipment_id}")
async def get_equipment_anomalies(equipment_id: str, days: int = 7, db: Session = Depends(get_db)):
    """Get anomaly history for specific equipment"""
    try:
        now = datetime.now(IST)
        start_time = now - timedelta(days=days)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get anomaly events for this equipment
        events = db.query(Event).filter(
            and_(
                Event.equipment_id == equipment_id,
                Event.event_type == EventType.ANOMALY,
                Event.ts >= start_time
            )
        ).order_by(Event.ts.desc()).all()
        
        anomaly_history = []
        for event in events:
            anomaly_history.append({
                'event_id': event.event_id,
                'timestamp': event.ts.strftime('%Y-%m-%d %H:%M:%S'),
                'subtype': event.subtype,
                'severity': event.severity.value,
                'value': event.value,
                'details': event.details,
                'days_ago': (now - event.ts).days
            })
        
        # Calculate anomaly statistics
        severity_counts = {}
        subtype_counts = {}
        
        for event in events:
            severity = event.severity.value
            subtype = event.subtype
            
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1
        
        return {
            'status': 'success',
            'equipment_info': {
                'equipment_id': equipment.equipment_id,
                'equipment_type': equipment.equipment_type,
                'branch_name': equipment.branch_name,
                'site_name': equipment.site_name,
                'status': equipment.status.value
            },
            'anomaly_history': anomaly_history,
            'statistics': {
                'total_anomalies': len(events),
                'severity_breakdown': severity_counts,
                'subtype_breakdown': subtype_counts,
                'period': f'Last {days} days'
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching equipment anomalies: {str(e)}")