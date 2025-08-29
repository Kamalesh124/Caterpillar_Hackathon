from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import MasterEquipment, UsageDaily, Event, EventType, Severity
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import math
import random

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Request models
class PositionCheckRequest(BaseModel):
    equipment_id: str
    current_lat: float
    current_lon: float

# Predefined site geofences (in real implementation, would be stored in database)
SITE_GEOFENCES = {
    'SITE001': {'center_lat': 12.9716, 'center_lon': 77.5946, 'radius_km': 2.0, 'name': 'Bangalore Metro Phase 3'},
    'SITE002': {'center_lat': 19.0760, 'center_lon': 72.8777, 'radius_km': 1.5, 'name': 'Mumbai Metro Line 5'},
    'SITE003': {'center_lat': 28.7041, 'center_lon': 77.1025, 'radius_km': 3.0, 'name': 'Delhi Airport Expansion'},
    'SITE004': {'center_lat': 13.0827, 'center_lon': 80.2707, 'radius_km': 2.5, 'name': 'Chennai Port Development'},
    'SITE005': {'center_lat': 22.5726, 'center_lon': 88.3639, 'radius_km': 1.8, 'name': 'Kolkata Bridge Project'}
}

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS coordinates using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

@router.get("/violations")
async def get_geofence_violations(hours: int = 24, db: Session = Depends(get_db)):
    """Get all geofence violations from the last N hours"""
    try:
        cutoff_time = datetime.now(IST) - timedelta(hours=hours)
        
        # Get geofence-related security events
        geofence_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name,
            MasterEquipment.site_id
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Event.event_type == EventType.SECURITY,
                Event.subtype.in_(['GEOFENCE_BREACH', 'SUSPICIOUS_MOVEMENT']),
                Event.ts >= cutoff_time
            )
        ).order_by(Event.ts.desc()).all()
        
        violations = []
        
        for event, equipment_type, branch_name, site_name, site_id in geofence_events:
            violation_data = {
                'event_id': event.event_id,
                'equipment_id': event.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'site_name': site_name,
                'site_id': site_id,
                'timestamp': event.ts.strftime('%Y-%m-%d %H:%M:%S'),
                'violation_type': event.subtype,
                'severity': event.severity.value,
                'details': event.details,
                'session_id': event.session_id
            }
            
            violations.append(violation_data)
        
        # Also check for potential new violations based on current GPS positions
        current_violations = await _check_current_positions(db)
        
        # Combine historical and current violations
        all_violations = violations + current_violations
        
        # Save to CSV
        csv_path = 'outputs/geofence_violations.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if all_violations:
            df = pd.DataFrame(all_violations)
            df.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        violation_types = {}
        severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        site_violations = {}
        
        for violation in all_violations:
            v_type = violation['violation_type']
            severity = violation['severity']
            site = violation['site_name']
            
            violation_types[v_type] = violation_types.get(v_type, 0) + 1
            severity_counts[severity] += 1
            site_violations[site] = site_violations.get(site, 0) + 1
        
        return {
            'status': 'success',
            'geofence_violations': all_violations,
            'csv_file': csv_path,
            'summary': {
                'total_violations': len(all_violations),
                'historical_violations': len(violations),
                'current_violations': len(current_violations),
                'violation_types': violation_types,
                'severity_breakdown': severity_counts,
                'site_breakdown': site_violations,
                'time_period_hours': hours
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching geofence violations: {str(e)}")

async def _check_current_positions(db: Session):
    """Check current equipment positions for geofence violations"""
    try:
        current_violations = []
        
        # Get latest GPS positions for all equipment
        latest_positions = db.query(
            UsageDaily.equipment_id,
            UsageDaily.last_gps_lat,
            UsageDaily.last_gps_lon,
            UsageDaily.date,
            MasterEquipment.site_id,
            MasterEquipment.site_name,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                UsageDaily.last_gps_lat.isnot(None),
                UsageDaily.last_gps_lon.isnot(None),
                UsageDaily.date >= (datetime.now(IST).date() - timedelta(days=1))
            )
        ).all()
        
        for position in latest_positions:
            equipment_id = position.equipment_id
            current_lat = position.last_gps_lat
            current_lon = position.last_gps_lon
            site_id = position.site_id
            site_name = position.site_name
            equipment_type = position.equipment_type
            branch_name = position.branch_name
            
            # Check if equipment is within its assigned site geofence
            if site_id in SITE_GEOFENCES:
                geofence = SITE_GEOFENCES[site_id]
                distance = calculate_distance(
                    current_lat, current_lon,
                    geofence['center_lat'], geofence['center_lon']
                )
                
                if distance > geofence['radius_km']:
                    # Equipment is outside its geofence
                    violation_data = {
                        'event_id': f'CURRENT_{equipment_id}',
                        'equipment_id': equipment_id,
                        'equipment_type': equipment_type,
                        'branch_name': branch_name,
                        'site_name': site_name,
                        'site_id': site_id,
                        'timestamp': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                        'violation_type': 'GEOFENCE_BREACH',
                        'severity': 'HIGH' if distance > geofence['radius_km'] * 2 else 'MEDIUM',
                        'details': f'Equipment {distance:.2f}km outside {site_name} geofence (limit: {geofence["radius_km"]}km)',
                        'session_id': None,
                        'current_lat': current_lat,
                        'current_lon': current_lon,
                        'geofence_center_lat': geofence['center_lat'],
                        'geofence_center_lon': geofence['center_lon'],
                        'distance_from_center': round(distance, 2)
                    }
                    
                    current_violations.append(violation_data)
        
        return current_violations
        
    except Exception as e:
        print(f"Error checking current positions: {str(e)}")
        return []

@router.post("/check-position")
async def check_equipment_position(
    request: PositionCheckRequest,
    db: Session = Depends(get_db)
):
    """Check if equipment position is within authorized geofence"""
    try:
        now = datetime.now(IST)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()

        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")

        position_check = {
            'equipment_id': request.equipment_id,
            'timestamp': now.isoformat(),
            'current_position': {
                'latitude': request.current_lat,
                'longitude': request.current_lon
            },
            'site_info': {
                'site_id': equipment.site_id,
                'site_name': equipment.site_name
            },
            'is_within_geofence': False,
            'distance_from_center': None,
            'geofence_radius': None,
            'violation_severity': None
        }
        
        # Check against assigned site geofence
        if equipment.site_id in SITE_GEOFENCES:
            geofence = SITE_GEOFENCES[equipment.site_id]
            distance = calculate_distance(
                request.current_lat, request.current_lon,
                geofence['center_lat'], geofence['center_lon']
            )
            
            position_check['distance_from_center'] = round(distance, 2)
            position_check['geofence_radius'] = geofence['radius_km']
            position_check['geofence_center'] = {
                'latitude': geofence['center_lat'],
                'longitude': geofence['center_lon']
            }
            
            if distance <= geofence['radius_km']:
                position_check['is_within_geofence'] = True
                position_check['status'] = 'AUTHORIZED'
            else:
                position_check['is_within_geofence'] = False
                position_check['status'] = 'GEOFENCE_VIOLATION'
                
                # Determine violation severity based on distance
                if distance > geofence['radius_km'] * 3:
                    position_check['violation_severity'] = 'CRITICAL'
                elif distance > geofence['radius_km'] * 2:
                    position_check['violation_severity'] = 'HIGH'
                else:
                    position_check['violation_severity'] = 'MEDIUM'
                
                # Log geofence violation event
                await _log_geofence_event(
                    equipment_id, 'GEOFENCE_BREACH',
                    Severity[position_check['violation_severity']],
                    f'Equipment {distance:.2f}km outside {equipment.site_name} geofence',
                    db
                )
        else:
            position_check['status'] = 'NO_GEOFENCE_DEFINED'
        
        db.commit()
        
        return {
            'status': 'success',
            'position_check': position_check
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error checking equipment position: {str(e)}")

@router.get("/movement-trail/{equipment_id}")
async def get_equipment_movement_trail(equipment_id: str, days: int = 7, db: Session = Depends(get_db)):
    """Get movement trail for specific equipment"""
    try:
        cutoff_date = datetime.now(IST).date() - timedelta(days=days)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get GPS positions from usage data
        positions = db.query(UsageDaily).filter(
            and_(
                UsageDaily.equipment_id == equipment_id,
                UsageDaily.date >= cutoff_date,
                UsageDaily.last_gps_lat.isnot(None),
                UsageDaily.last_gps_lon.isnot(None)
            )
        ).order_by(UsageDaily.date).all()
        
        trail_points = []
        total_distance = 0
        prev_lat, prev_lon = None, None
        
        for position in positions:
            point = {
                'date': position.date.strftime('%Y-%m-%d'),
                'latitude': position.last_gps_lat,
                'longitude': position.last_gps_lon,
                'runtime_hours': position.runtime_hours,
                'distance_km': position.distance_km
            }
            
            # Calculate distance from previous point
            if prev_lat is not None and prev_lon is not None:
                daily_distance = calculate_distance(
                    prev_lat, prev_lon,
                    position.last_gps_lat, position.last_gps_lon
                )
                point['distance_from_previous'] = round(daily_distance, 2)
                total_distance += daily_distance
            else:
                point['distance_from_previous'] = 0
            
            trail_points.append(point)
            prev_lat, prev_lon = position.last_gps_lat, position.last_gps_lon
        
        # Check for suspicious movements
        suspicious_movements = []
        for i, point in enumerate(trail_points):
            if point['distance_from_previous'] > 50:  # More than 50km in a day
                suspicious_movements.append({
                    'date': point['date'],
                    'distance': point['distance_from_previous'],
                    'severity': 'HIGH' if point['distance_from_previous'] > 100 else 'MEDIUM'
                })
        
        # Get geofence info for the assigned site
        geofence_info = None
        if equipment.site_id in SITE_GEOFENCES:
            geofence_info = SITE_GEOFENCES[equipment.site_id]
        
        return {
            'status': 'success',
            'equipment_info': {
                'equipment_id': equipment_id,
                'equipment_type': equipment.equipment_type,
                'site_name': equipment.site_name,
                'site_id': equipment.site_id
            },
            'movement_trail': trail_points,
            'geofence_info': geofence_info,
            'summary': {
                'total_points': len(trail_points),
                'total_distance_km': round(total_distance, 2),
                'days_analyzed': days,
                'suspicious_movements': len(suspicious_movements),
                'avg_daily_distance': round(total_distance / max(1, len(trail_points)), 2)
            },
            'suspicious_movements': suspicious_movements
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching movement trail: {str(e)}")

async def _log_geofence_event(
    equipment_id: str,
    subtype: str,
    severity: Severity,
    details: str,
    db: Session
):
    """Log a geofence-related security event"""
    try:
        event = Event(
            equipment_id=equipment_id,
            ts=datetime.now(IST),
            event_type=EventType.SECURITY,
            subtype=subtype,
            severity=severity,
            value=1.0,
            details=details
        )
        
        db.add(event)
        # Note: commit is handled by calling function
        
    except Exception as e:
        print(f"Error logging geofence event: {str(e)}")

@router.get("/charts")
async def generate_geofence_charts(hours: int = 24, db: Session = Depends(get_db)):
    """Generate geofence violation charts"""
    try:
        # Get geofence violations data
        violations_data = await get_geofence_violations(hours, db)
        violations = violations_data['geofence_violations']
        
        charts = {}
        
        if not violations:
            return {'status': 'success', 'charts': {}, 'message': 'No geofence violations found'}
        
        # 1. GPS Map with violations and geofences
        violation_lats = []
        violation_lons = []
        violation_texts = []
        violation_colors = []
        
        for violation in violations:
            if 'current_lat' in violation and 'current_lon' in violation:
                violation_lats.append(violation['current_lat'])
                violation_lons.append(violation['current_lon'])
                violation_texts.append(f"{violation['equipment_id']}<br>{violation['violation_type']}<br>{violation['details']}")
                
                color_map = {'CRITICAL': 'red', 'HIGH': 'orange', 'MEDIUM': 'yellow', 'LOW': 'green'}
                violation_colors.append(color_map.get(violation['severity'], 'blue'))
        
        if violation_lats:
            fig_map = go.Figure()
            
            # Add violation points
            fig_map.add_trace(go.Scattermapbox(
                lat=violation_lats,
                lon=violation_lons,
                mode='markers',
                marker=dict(size=12, color=violation_colors),
                text=violation_texts,
                hovertemplate='%{text}<extra></extra>',
                name='Violations'
            ))
            
            # Add geofence circles (simplified as center points)
            geofence_lats = []
            geofence_lons = []
            geofence_texts = []
            
            for site_id, geofence in SITE_GEOFENCES.items():
                geofence_lats.append(geofence['center_lat'])
                geofence_lons.append(geofence['center_lon'])
                geofence_texts.append(f"{geofence['name']}<br>Radius: {geofence['radius_km']}km")
            
            fig_map.add_trace(go.Scattermapbox(
                lat=geofence_lats,
                lon=geofence_lons,
                mode='markers',
                marker=dict(size=15, color='blue', symbol='circle-open'),
                text=geofence_texts,
                hovertemplate='%{text}<extra></extra>',
                name='Geofences'
            ))
            
            fig_map.update_layout(
                title='Equipment Violations and Geofences',
                mapbox=dict(
                    style='open-street-map',
                    center=dict(lat=20.5937, lon=78.9629),  # Center of India
                    zoom=5
                ),
                height=600
            )
            
            charts['violations_map'] = json.loads(fig_map.to_json())
        
        # 2. Violations by severity
        df = pd.DataFrame(violations)
        severity_counts = df['severity'].value_counts()
        
        severity_colors = {
            'CRITICAL': '#B71C1C',
            'HIGH': '#FF4444',
            'MEDIUM': '#FF8800',
            'LOW': '#FFC107'
        }
        
        fig_severity = go.Figure(data=[go.Pie(
            labels=severity_counts.index,
            values=severity_counts.values,
            marker_colors=[severity_colors.get(sev, '#9E9E9E') for sev in severity_counts.index],
            hole=0.3
        )])
        
        fig_severity.update_layout(
            title='Geofence Violations by Severity',
            annotations=[dict(text=f'{len(violations)}<br>Total<br>Violations', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        charts['severity_distribution'] = json.loads(fig_severity.to_json())
        
        # 3. Violations by site
        site_counts = df['site_name'].value_counts()
        
        fig_sites = go.Figure(data=[go.Bar(
            x=site_counts.index,
            y=site_counts.values,
            marker_color='#FF6B6B'
        )])
        
        fig_sites.update_layout(
            title='Geofence Violations by Site',
            xaxis_title='Site',
            yaxis_title='Number of Violations',
            xaxis_tickangle=-45
        )
        
        charts['violations_by_site'] = json.loads(fig_sites.to_json())
        
        # 4. Timeline of violations
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        
        hourly_counts = df.groupby('hour').size().reindex(range(24), fill_value=0)
        
        fig_timeline = go.Figure(data=[go.Scatter(
            x=list(range(24)),
            y=hourly_counts.values,
            mode='lines+markers',
            line=dict(color='#FF4444', width=3),
            marker=dict(size=8)
        )])
        
        fig_timeline.update_layout(
            title='Geofence Violations by Hour of Day',
            xaxis_title='Hour of Day',
            yaxis_title='Number of Violations',
            xaxis=dict(tickmode='linear', tick0=0, dtick=2)
        )
        
        charts['violations_timeline'] = json.loads(fig_timeline.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating geofence charts: {str(e)}")

@router.get("/insights")
async def generate_geofence_insights(hours: int = 24, db: Session = Depends(get_db)):
    """Generate text insights for geofence violations"""
    try:
        now = datetime.now(IST)
        
        # Get geofence violations data
        violations_data = await get_geofence_violations(hours, db)
        violations = violations_data['geofence_violations']
        summary = violations_data['summary']
        
        insights = []
        
        if not violations:
            insights.append("✅ No geofence violations detected - all equipment within authorized areas.")
            return {'status': 'success', 'insights': insights}
        
        total_violations = len(violations)
        current_violations = summary['current_violations']
        historical_violations = summary['historical_violations']
        
        insights.append(f"📍 {total_violations} geofence violations detected ({current_violations} current, {historical_violations} historical).")
        
        # Severity analysis
        severity_breakdown = summary['severity_breakdown']
        critical_count = severity_breakdown.get('CRITICAL', 0)
        high_count = severity_breakdown.get('HIGH', 0)
        
        if critical_count > 0:
            insights.append(f"🚨 {critical_count} CRITICAL violations - equipment far outside authorized zones.")
        
        if high_count > 0:
            insights.append(f"⚠️ {high_count} HIGH severity violations require immediate investigation.")
        
        # Site analysis
        site_breakdown = summary['site_breakdown']
        if site_breakdown:
            most_problematic_site = max(site_breakdown, key=site_breakdown.get)
            site_count = site_breakdown[most_problematic_site]
            insights.append(f"🏗️ {most_problematic_site} has {site_count} violations - review site security measures.")
        
        # Current position violations
        current_breaches = [v for v in violations if v['violation_type'] == 'GEOFENCE_BREACH' and 'current_lat' in v]
        if current_breaches:
            # Find the equipment furthest from its geofence
            furthest_breach = max(current_breaches, key=lambda x: x.get('distance_from_center', 0))
            eq_id = furthest_breach['equipment_id']
            distance = furthest_breach.get('distance_from_center', 0)
            site_name = furthest_breach['site_name']
            
            insights.append(f"📍 Equipment {eq_id} is {distance}km outside {site_name} perimeter - immediate location verification needed.")
        
        # Movement analysis
        suspicious_movements = [v for v in violations if v['violation_type'] == 'SUSPICIOUS_MOVEMENT']
        if suspicious_movements:
            insights.append(f"🚚 {len(suspicious_movements)} suspicious movement patterns detected - review transport logs.")
        
        # Time-based patterns
        df = pd.DataFrame(violations)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            
            # Night-time violations (22:00 - 06:00)
            night_violations = df[(df['hour'] >= 22) | (df['hour'] <= 6)]
            if not night_violations.empty:
                insights.append(f"🌙 {len(night_violations)} violations during night hours - potential unauthorized access.")
            
            # Peak violation hours
            hourly_counts = df['hour'].value_counts()
            if not hourly_counts.empty:
                peak_hour = hourly_counts.index[0]
                peak_count = hourly_counts.iloc[0]
                insights.append(f"⏰ Peak violation time: {peak_hour:02d}:00 ({peak_count} incidents).")
        
        # Equipment-specific analysis
        equipment_counts = {}
        for violation in violations:
            eq_id = violation['equipment_id']
            equipment_counts[eq_id] = equipment_counts.get(eq_id, 0) + 1
        
        repeat_offenders = {eq: count for eq, count in equipment_counts.items() if count > 1}
        if repeat_offenders:
            worst_offender = max(repeat_offenders, key=repeat_offenders.get)
            offense_count = repeat_offenders[worst_offender]
            insights.append(f"🚜 Equipment {worst_offender} has {offense_count} violations - investigate potential tracking issues.")
        
        # Distance analysis for current breaches
        if current_breaches:
            avg_distance = sum(v.get('distance_from_center', 0) for v in current_breaches) / len(current_breaches)
            max_distance = max(v.get('distance_from_center', 0) for v in current_breaches)
            
            insights.append(f"📏 Average breach distance: {avg_distance:.1f}km, maximum: {max_distance:.1f}km.")
        
        # Geofence effectiveness
        total_equipment = db.query(func.count(MasterEquipment.equipment_id)).scalar()
        violation_rate = (len(set(v['equipment_id'] for v in violations)) / total_equipment) * 100 if total_equipment > 0 else 0
        
        if violation_rate > 10:
            insights.append(f"📊 {violation_rate:.1f}% of equipment has geofence violations - review geofence boundaries.")
        
        # Recent critical events
        recent_critical = [v for v in violations if v['severity'] in ['CRITICAL', 'HIGH']]
        if recent_critical:
            latest_critical = recent_critical[0]  # Violations are sorted by timestamp desc
            eq_id = latest_critical['equipment_id']
            violation_type = latest_critical['violation_type']
            timestamp = latest_critical['timestamp']
            insights.append(f"🔴 Latest critical violation: {violation_type} by {eq_id} at {timestamp}.")
        
        return {
            'status': 'success',
            'insights': insights,
            'summary_stats': {
                'total_violations': total_violations,
                'current_violations': current_violations,
                'critical_violations': critical_count,
                'violation_rate_percent': round(violation_rate, 1)
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating geofence insights: {str(e)}")