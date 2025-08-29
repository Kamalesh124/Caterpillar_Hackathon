from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import MasterEquipment, Rental, Event, EventType, Severity
from pydantic import BaseModel
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import random
import string
import uuid

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

class OTPRequest(BaseModel):
    equipment_id: str
    operator_id: str

class AuthorizationRequest(BaseModel):
    equipment_id: str
    operator_id: str = None
    session_id: str = None

class OTPVerificationRequest(BaseModel):
    equipment_id: str
    otp_code: str
    operator_id: str = None

@router.get("/events")
async def get_security_events(hours: int = 24, db: Session = Depends(get_db)):
    """Get all security events from the last N hours"""
    try:
        cutoff_time = datetime.now(IST) - timedelta(hours=hours)
        
        # Get security events
        security_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name,
            MasterEquipment.customer_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Event.event_type == EventType.SECURITY,
                Event.ts >= cutoff_time
            )
        ).order_by(Event.ts.desc()).all()
        
        events_data = []
        severity_counts = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'CRITICAL': 0}
        subtype_counts = {}
        
        for event, equipment_type, branch_name, site_name, customer_name in security_events:
            event_data = {
                'event_id': event.event_id,
                'equipment_id': event.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'site_name': site_name,
                'customer_name': customer_name,
                'timestamp': event.ts.strftime('%Y-%m-%d %H:%M:%S'),
                'event_type': event.event_type.value,
                'subtype': event.subtype,
                'severity': event.severity.value,
                'value': event.value,
                'details': event.details,
                'session_id': event.session_id
            }
            
            events_data.append(event_data)
            
            # Count by severity
            severity_counts[event.severity.value] += 1
            
            # Count by subtype
            subtype_counts[event.subtype] = subtype_counts.get(event.subtype, 0) + 1
        
        # Save to CSV
        csv_path = 'outputs/security_events.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if events_data:
            df = pd.DataFrame(events_data)
            df.to_csv(csv_path, index=False)
        
        return {
            'status': 'success',
            'security_events': events_data,
            'csv_file': csv_path,
            'summary': {
                'total_events': len(events_data),
                'severity_breakdown': severity_counts,
                'subtype_breakdown': subtype_counts,
                'time_period_hours': hours
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching security events: {str(e)}")

@router.post("/check-authorization")
async def check_equipment_authorization(
    request: AuthorizationRequest,
    db: Session = Depends(get_db)
):
    """Check if equipment usage is authorized"""
    try:
        now = datetime.now(IST)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()

        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")

        # Check for active rental
        active_rental = db.query(Rental).filter(
            and_(
                Rental.equipment_id == request.equipment_id,
                Rental.contract_start_ts <= now,
                or_(
                    Rental.actual_end_ts.is_(None),
                    Rental.actual_end_ts >= now
                )
            )
        ).first()
        
        authorization_result = {
            'equipment_id': request.equipment_id,
            'timestamp': now.isoformat(),
            'is_authorized': False,
            'authorization_type': None,
            'session_id': request.session_id,
            'operator_id': request.operator_id,
            'violations': []
        }
        
        # Check 1: Active rental exists
        if not active_rental:
            authorization_result['violations'].append('UNASSIGNED_USE')
            await _log_security_event(
                request.equipment_id, 'UNASSIGNED_USE', Severity.HIGH,
                'Equipment started without active rental contract',
                request.session_id, db
            )
        else:
            # Check 2: Within rental time window
            if now < active_rental.contract_start_ts:
                authorization_result['violations'].append('EARLY_START')
                await _log_security_event(
                    request.equipment_id, 'EARLY_START', Severity.MEDIUM,
                    f'Equipment started before contract start time: {active_rental.contract_start_ts}',
                    request.session_id, db
                )
            elif now > active_rental.contract_end_ts_planned:
                authorization_result['violations'].append('OFF_HOURS_USAGE')
                await _log_security_event(
                    request.equipment_id, 'OFF_HOURS_USAGE', Severity.MEDIUM,
                    f'Equipment used past contract end time: {active_rental.contract_end_ts_planned}',
                    request.session_id, db
                )
            
            # Check 3: Operator authorization (if provided)
            if request.operator_id:
                if active_rental.operator_id_on_checkout and active_rental.operator_id_on_checkout != request.operator_id:
                    authorization_result['violations'].append('UNAUTHORIZED_OPERATOR')
                    await _log_security_event(
                        request.equipment_id, 'UNAUTHORIZED_OPERATOR', Severity.HIGH,
                        f'Unauthorized operator {request.operator_id}, expected {active_rental.operator_id_on_checkout}',
                        request.session_id, db
                    )
            
            # Check 4: License requirements (simulated)
            if equipment.license_class_required:
                # In real implementation, would check operator license against requirements
                # For demo, randomly simulate license check failure
                if random.random() < 0.05:  # 5% chance of license mismatch
                    authorization_result['violations'].append('BAD_LICENSE')
                    await _log_security_event(
                        equipment_id, 'BAD_LICENSE', Severity.HIGH,
                        f'Operator license does not match required class: {equipment.license_class_required}',
                        session_id, db
                    )
        
        # Check 5: Session validation (if provided)
        if session_id:
            # In real implementation, would validate session against active sessions
            # For demo, simulate session validation
            if len(session_id) < 8:  # Simple validation
                authorization_result['violations'].append('INVALID_SESSION')
                await _log_security_event(
                    equipment_id, 'INVALID_SESSION', Severity.MEDIUM,
                    f'Invalid or expired session: {session_id}',
                    session_id, db
                )
        
        # Determine authorization status
        critical_violations = ['UNASSIGNED_USE', 'BAD_LICENSE', 'UNAUTHORIZED_OPERATOR']
        has_critical_violation = any(v in authorization_result['violations'] for v in critical_violations)
        
        if not authorization_result['violations']:
            authorization_result['is_authorized'] = True
            authorization_result['authorization_type'] = 'FULL_ACCESS'
        elif has_critical_violation:
            authorization_result['is_authorized'] = False
            authorization_result['authorization_type'] = 'BLOCKED'
        else:
            authorization_result['is_authorized'] = True
            authorization_result['authorization_type'] = 'LIMITED_ACCESS'
        
        db.commit()
        
        return {
            'status': 'success',
            'authorization': authorization_result
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error checking authorization: {str(e)}")

@router.post("/verify-otp")
async def verify_equipment_otp(
    request: OTPVerificationRequest,
    db: Session = Depends(get_db)
):
    """Verify OTP for equipment access"""
    try:
        now = datetime.now(IST)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()

        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")

        # Simulate OTP verification (in real implementation, would check against stored OTP)
        # For demo purposes, accept OTPs that are 6 digits and not '000000'
        is_valid_otp = len(request.otp_code) == 6 and request.otp_code.isdigit() and request.otp_code != '000000'
        
        # Generate session ID if OTP is valid
        session_id = None
        if is_valid_otp:
            session_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        
        verification_result = {
            'equipment_id': request.equipment_id,
            'operator_id': request.operator_id,
            'otp_code': request.otp_code,
            'is_valid': is_valid_otp,
            'session_id': session_id,
            'timestamp': now.isoformat(),
            'expires_at': (now + timedelta(hours=8)).isoformat() if is_valid_otp else None
        }
        
        # Log security event
        if is_valid_otp:
            await _log_security_event(
                request.equipment_id, 'OTP_SUCCESS', Severity.LOW,
                f'Successful OTP verification for operator {request.operator_id}',
                session_id, db
            )
        else:
            await _log_security_event(
                request.equipment_id, 'FAILED_OTP', Severity.MEDIUM,
                f'Failed OTP verification attempt: {request.otp_code}',
                None, db
            )
        
        db.commit()
        
        return {
            'status': 'success',
            'verification': verification_result
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error verifying OTP: {str(e)}")

@router.post("/generate-otp")
async def generate_equipment_otp(
    request: OTPRequest,
    db: Session = Depends(get_db)
):
    """Generate OTP for equipment access"""
    try:
        now = datetime.now(IST)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == request.equipment_id
        ).first()

        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")

        # Generate 6-digit OTP
        otp_code = ''.join(random.choices(string.digits, k=6))

        # In real implementation, would store OTP with expiration time
        # For demo, we'll just return the generated OTP

        otp_result = {
            'equipment_id': request.equipment_id,
            'operator_id': request.operator_id,
            'otp_code': otp_code,
            'generated_at': now.isoformat(),
            'expires_at': (now + timedelta(minutes=10)).isoformat(),
            'equipment_type': equipment.equipment_type,
            'site_name': equipment.site_name
        }
        
        # Log OTP generation
        await _log_security_event(
            request.equipment_id, 'OTP_GENERATED', Severity.LOW,
            f'OTP generated for operator {request.operator_id}',
            None, db
        )
        
        db.commit()
        
        return {
            'status': 'success',
            'otp': otp_result
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error generating OTP: {str(e)}")

async def _log_security_event(
    equipment_id: str,
    subtype: str,
    severity: Severity,
    details: str,
    session_id: str = None,
    db: Session = None
):
    """Log a security event to the database"""
    try:
        event = Event(
            event_id=str(uuid.uuid4()),
            equipment_id=equipment_id,
            ts=datetime.now(IST),
            event_type=EventType.SECURITY,
            subtype=subtype,
            severity=severity,
            value=1.0,  # Binary indicator
            details=details,
            session_id=session_id
        )
        
        db.add(event)
        # Note: commit is handled by calling function
        
    except Exception as e:
        print(f"Error logging security event: {str(e)}")

@router.get("/charts")
async def generate_security_charts(hours: int = 24, db: Session = Depends(get_db)):
    """Generate security event charts"""
    try:
        # Get security events data
        security_data = await get_security_events(hours, db)
        events = security_data['security_events']
        
        charts = {}
        
        if not events:
            return {'status': 'success', 'charts': {}, 'message': 'No security events found'}
        
        # 1. Security events timeline
        df = pd.DataFrame(events)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        
        hourly_counts = df.groupby('hour').size().reindex(range(24), fill_value=0)
        
        fig_timeline = go.Figure(data=[go.Bar(
            x=list(range(24)),
            y=hourly_counts.values,
            marker_color='#FF4444'
        )])
        
        fig_timeline.update_layout(
            title='Security Events by Hour of Day',
            xaxis_title='Hour of Day',
            yaxis_title='Number of Events',
            xaxis=dict(tickmode='linear', tick0=0, dtick=2)
        )
        
        charts['events_timeline'] = json.loads(fig_timeline.to_json())
        
        # 2. Severity distribution
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
            title='Security Events by Severity',
            annotations=[dict(text=f'{len(events)}<br>Total<br>Events', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        charts['severity_distribution'] = json.loads(fig_severity.to_json())
        
        # 3. Event types breakdown
        subtype_counts = df['subtype'].value_counts()
        
        fig_subtypes = go.Figure(data=[go.Bar(
            x=subtype_counts.index,
            y=subtype_counts.values,
            marker_color='#FF6B6B'
        )])
        
        fig_subtypes.update_layout(
            title='Security Event Types',
            xaxis_title='Event Subtype',
            yaxis_title='Count',
            xaxis_tickangle=-45
        )
        
        charts['event_types'] = json.loads(fig_subtypes.to_json())
        
        # 4. Equipment security heatmap
        equipment_hour_matrix = df.groupby(['equipment_id', 'hour']).size().unstack(fill_value=0)
        
        if not equipment_hour_matrix.empty:
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=equipment_hour_matrix.values,
                x=list(range(24)),
                y=equipment_hour_matrix.index,
                colorscale='Reds',
                showscale=True
            ))
            
            fig_heatmap.update_layout(
                title='Security Events Heatmap (Equipment vs Hour)',
                xaxis_title='Hour of Day',
                yaxis_title='Equipment ID',
                height=max(400, len(equipment_hour_matrix.index) * 20)
            )
            
            charts['security_heatmap'] = json.loads(fig_heatmap.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating security charts: {str(e)}")

@router.get("/insights")
async def generate_security_insights(hours: int = 24, db: Session = Depends(get_db)):
    """Generate text insights for security events"""
    try:
        now = datetime.now(IST)
        
        # Get security events data
        security_data = await get_security_events(hours, db)
        events = security_data['security_events']
        summary = security_data['summary']
        
        insights = []
        
        if not events:
            insights.append("✅ No security incidents detected in the last 24 hours - all systems secure.")
            return {'status': 'success', 'insights': insights}
        
        total_events = len(events)
        severity_breakdown = summary['severity_breakdown']
        subtype_breakdown = summary['subtype_breakdown']
        
        insights.append(f"🔒 {total_events} security events detected in the last {hours} hours.")
        
        # Critical and high severity events
        critical_count = severity_breakdown.get('CRITICAL', 0)
        high_count = severity_breakdown.get('HIGH', 0)
        
        if critical_count > 0:
            insights.append(f"🚨 {critical_count} CRITICAL security events require immediate attention.")
        
        if high_count > 0:
            insights.append(f"⚠️ {high_count} HIGH severity security events detected.")
        
        # Most common security issues
        if subtype_breakdown:
            most_common_type = max(subtype_breakdown, key=subtype_breakdown.get)
            count = subtype_breakdown[most_common_type]
            insights.append(f"🔍 Most frequent issue: {most_common_type} ({count} occurrences).")
        
        # Specific security event analysis
        unassigned_use = [e for e in events if e['subtype'] == 'UNASSIGNED_USE']
        if unassigned_use:
            equipment_ids = [e['equipment_id'] for e in unassigned_use]
            insights.append(f"🚫 {len(unassigned_use)} unauthorized equipment starts: {', '.join(equipment_ids[:3])}.")
        
        failed_otp = [e for e in events if e['subtype'] == 'FAILED_OTP']
        if failed_otp:
            insights.append(f"🔑 {len(failed_otp)} failed OTP attempts - possible unauthorized access attempts.")
        
        bad_license = [e for e in events if e['subtype'] == 'BAD_LICENSE']
        if bad_license:
            insights.append(f"📋 {len(bad_license)} license violations detected - operators using equipment without proper certification.")
        
        off_hours = [e for e in events if e['subtype'] == 'OFF_HOURS_USAGE']
        if off_hours:
            insights.append(f"🕐 {len(off_hours)} off-hours usage incidents - equipment used outside rental periods.")
        
        # Time-based patterns
        df = pd.DataFrame(events)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            
            # Peak incident hours
            hourly_counts = df['hour'].value_counts()
            if not hourly_counts.empty:
                peak_hour = hourly_counts.index[0]
                peak_count = hourly_counts.iloc[0]
                insights.append(f"⏰ Peak security incident time: {peak_hour:02d}:00 ({peak_count} events).")
            
            # Night-time incidents (22:00 - 06:00)
            night_events = df[(df['hour'] >= 22) | (df['hour'] <= 6)]
            if not night_events.empty:
                insights.append(f"🌙 {len(night_events)} security events during night hours - review after-hours access.")
        
        # Equipment-specific analysis
        equipment_counts = {}
        for event in events:
            eq_id = event['equipment_id']
            equipment_counts[eq_id] = equipment_counts.get(eq_id, 0) + 1
        
        if equipment_counts:
            most_problematic = max(equipment_counts, key=equipment_counts.get)
            problem_count = equipment_counts[most_problematic]
            if problem_count > 1:
                insights.append(f"🚜 Equipment {most_problematic} has {problem_count} security incidents - investigate potential issues.")
        
        # Branch analysis
        branch_counts = {}
        for event in events:
            branch = event['branch_name']
            branch_counts[branch] = branch_counts.get(branch, 0) + 1
        
        if branch_counts:
            most_problematic_branch = max(branch_counts, key=branch_counts.get)
            branch_count = branch_counts[most_problematic_branch]
            insights.append(f"🏢 {most_problematic_branch} branch has {branch_count} security events - review local security protocols.")
        
        # Recent critical events
        recent_critical = [e for e in events if e['severity'] in ['CRITICAL', 'HIGH']]
        if recent_critical:
            latest_critical = recent_critical[0]  # Events are sorted by timestamp desc
            eq_id = latest_critical['equipment_id']
            subtype = latest_critical['subtype']
            timestamp = latest_critical['timestamp']
            insights.append(f"🔴 Latest critical event: {subtype} on {eq_id} at {timestamp}.")
        
        return {
            'status': 'success',
            'insights': insights,
            'summary_stats': {
                'total_events': total_events,
                'critical_events': critical_count,
                'high_events': high_count,
                'most_common_type': most_common_type if subtype_breakdown else None
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating security insights: {str(e)}")

@router.get("/equipment/{equipment_id}")
async def get_equipment_security_history(equipment_id: str, days: int = 7, db: Session = Depends(get_db)):
    """Get security event history for specific equipment"""
    try:
        cutoff_time = datetime.now(IST) - timedelta(days=days)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get security events for this equipment
        events = db.query(Event).filter(
            and_(
                Event.equipment_id == equipment_id,
                Event.event_type == EventType.SECURITY,
                Event.ts >= cutoff_time
            )
        ).order_by(Event.ts.desc()).all()
        
        event_history = []
        severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        subtype_counts = {}
        
        for event in events:
            event_data = {
                'event_id': event.event_id,
                'timestamp': event.ts.strftime('%Y-%m-%d %H:%M:%S'),
                'subtype': event.subtype,
                'severity': event.severity.value,
                'details': event.details,
                'session_id': event.session_id
            }
            
            event_history.append(event_data)
            severity_counts[event.severity.value] += 1
            subtype_counts[event.subtype] = subtype_counts.get(event.subtype, 0) + 1
        
        # Calculate security score (0-100, higher is better)
        total_events = len(events)
        critical_events = severity_counts['CRITICAL']
        high_events = severity_counts['HIGH']
        
        # Base score starts at 100, deduct points for incidents
        security_score = 100
        security_score -= (critical_events * 20)  # -20 points per critical
        security_score -= (high_events * 10)      # -10 points per high
        security_score -= (total_events * 2)      # -2 points per any incident
        security_score = max(0, security_score)   # Don't go below 0
        
        return {
            'status': 'success',
            'equipment_info': {
                'equipment_id': equipment_id,
                'equipment_type': equipment.equipment_type,
                'branch_name': equipment.branch_name,
                'site_name': equipment.site_name,
                'security_score': security_score
            },
            'security_history': event_history,
            'summary': {
                'total_events': total_events,
                'severity_breakdown': severity_counts,
                'subtype_breakdown': subtype_counts,
                'days_analyzed': days,
                'security_rating': _get_security_rating(security_score)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching equipment security history: {str(e)}")

def _get_security_rating(score):
    """Convert security score to rating"""
    if score >= 90:
        return 'EXCELLENT'
    elif score >= 75:
        return 'GOOD'
    elif score >= 60:
        return 'FAIR'
    elif score >= 40:
        return 'POOR'
    else:
        return 'CRITICAL'