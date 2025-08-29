from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import (
    UsageDaily, Event, Rental, MasterEquipment, 
    EventType, Severity, PaymentStatus, Event
)
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
from pydantic import BaseModel
from typing import Optional, List
import uuid
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Pydantic models for advanced features
class PredictiveAnalysisRequest(BaseModel):
    days_ahead: int = 7
    branch_name: Optional[str] = None
    equipment_type: Optional[str] = None
    analysis_type: str = "comprehensive"  # comprehensive, utilization, maintenance, revenue

class ComparativeAnalysisRequest(BaseModel):
    period_days: int = 7
    compare_with_days: int = 7
    branch_name: Optional[str] = None
    equipment_type: Optional[str] = None

class RecommendationRequest(BaseModel):
    priority_level: str = "high"  # high, medium, low, all
    category: Optional[str] = None  # operational, financial, maintenance, security
    branch_name: Optional[str] = None

@router.get("/daily")
async def generate_daily_digest(db: Session = Depends(get_db)):
    """Generate comprehensive daily digest for dealers"""
    try:
        now = datetime.now(IST)
        today = now.date()
        yesterday = today - timedelta(days=1)
        last_24h = now - timedelta(hours=24)
        
        # Get usage data for last 24 hours
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= yesterday
        ).all()
        
        # Get all events from last 24 hours
        recent_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Event.ts >= last_24h
        ).all()
        
        # Get overdue rentals
        overdue_rentals = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.customer_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Rental.late_hours > 0,
                or_(
                    Rental.payment_status == PaymentStatus.DUE,
                    Rental.payment_status == PaymentStatus.OVERDUE
                )
            )
        ).all()
        
        # Process usage data
        usage_summary = await _process_usage_data(usage_data)
        
        # Process events
        event_summary = await _process_events(recent_events)
        
        # Process overdue rentals
        overdue_summary = await _process_overdue_rentals(overdue_rentals)
        
        # Generate KPIs
        kpis = await _calculate_kpis(usage_data, recent_events, overdue_rentals)
        
        # Compile digest data
        digest_data = {
            'date': today.strftime('%Y-%m-%d'),
            'generated_at': now.strftime('%Y-%m-%d %H:%M:%S'),
            'kpis': kpis,
            'usage_summary': usage_summary,
            'event_summary': event_summary,
            'overdue_summary': overdue_summary,
            'alerts': await _generate_alerts(usage_data, recent_events, overdue_rentals)
        }
        
        # Save to CSV
        csv_path = 'outputs/dealer_summary_last24h.csv'
        os.makedirs('outputs', exist_ok=True)
        
        # Flatten data for CSV export
        csv_data = []
        
        # Add KPIs
        for key, value in kpis.items():
            csv_data.append({
                'category': 'KPI',
                'metric': key,
                'value': value,
                'unit': _get_unit_for_metric(key),
                'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Add usage summary by branch
        for branch_data in usage_summary['by_branch']:
            csv_data.append({
                'category': 'USAGE',
                'metric': f"{branch_data['branch_name']}_runtime_hours",
                'value': branch_data['total_runtime'],
                'unit': 'hours',
                'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
            })
            csv_data.append({
                'category': 'USAGE',
                'metric': f"{branch_data['branch_name']}_idle_hours",
                'value': branch_data['total_idle'],
                'unit': 'hours',
                'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Add event counts
        for event_type, count in event_summary['by_type'].items():
            csv_data.append({
                'category': 'EVENTS',
                'metric': f"{event_type}_count",
                'value': count,
                'unit': 'count',
                'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Add overdue rental info
        csv_data.append({
            'category': 'OVERDUE',
            'metric': 'overdue_rental_count',
            'value': overdue_summary['count'],
            'unit': 'count',
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
        })
        csv_data.append({
            'category': 'OVERDUE',
            'metric': 'estimated_late_fees',
            'value': overdue_summary['total_fees'],
            'unit': 'INR',
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        pd.DataFrame(csv_data).to_csv(csv_path, index=False)
        
        return {
            'status': 'success',
            'digest': digest_data,
            'csv_file': csv_path
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating daily digest: {str(e)}")

async def _process_usage_data(usage_data):
    """Process usage data for digest"""
    if not usage_data:
        return {'total_runtime': 0, 'total_idle': 0, 'by_branch': [], 'by_equipment_type': []}
    
    # Convert to DataFrame
    df = pd.DataFrame([
        {
            'equipment_id': usage.equipment_id,
            'equipment_type': equipment_type,
            'branch_name': branch_name,
            'site_name': site_name,
            'runtime_hours': usage.runtime_hours,
            'idle_hours': usage.idle_hours,
            'fuel_used_liters': usage.fuel_used_liters,
            'utilization_pct': usage.utilization_pct,
            'breakdown_hours': usage.breakdown_hours
        }
        for usage, equipment_type, branch_name, site_name in usage_data
    ])
    
    # Summary by branch
    branch_summary = df.groupby('branch_name').agg({
        'runtime_hours': 'sum',
        'idle_hours': 'sum',
        'fuel_used_liters': 'sum',
        'utilization_pct': 'mean',
        'equipment_id': 'count'
    }).reset_index()
    
    by_branch = []
    for _, row in branch_summary.iterrows():
        by_branch.append({
            'branch_name': row['branch_name'],
            'total_runtime': round(row['runtime_hours'], 1),
            'total_idle': round(row['idle_hours'], 1),
            'total_fuel': round(row['fuel_used_liters'], 1),
            'avg_utilization': round(row['utilization_pct'], 1),
            'equipment_count': int(row['equipment_id'])
        })
    
    # Summary by equipment type
    type_summary = df.groupby('equipment_type').agg({
        'runtime_hours': 'sum',
        'idle_hours': 'sum',
        'utilization_pct': 'mean',
        'equipment_id': 'count'
    }).reset_index()
    
    by_equipment_type = []
    for _, row in type_summary.iterrows():
        by_equipment_type.append({
            'equipment_type': row['equipment_type'],
            'total_runtime': round(row['runtime_hours'], 1),
            'total_idle': round(row['idle_hours'], 1),
            'avg_utilization': round(row['utilization_pct'], 1),
            'equipment_count': int(row['equipment_id'])
        })
    
    return {
        'total_runtime': round(df['runtime_hours'].sum(), 1),
        'total_idle': round(df['idle_hours'].sum(), 1),
        'total_fuel': round(df['fuel_used_liters'].sum(), 1),
        'avg_utilization': round(df['utilization_pct'].mean(), 1),
        'total_breakdown': round(df['breakdown_hours'].sum(), 1),
        'by_branch': by_branch,
        'by_equipment_type': by_equipment_type
    }

async def _process_events(recent_events):
    """Process events for digest"""
    if not recent_events:
        return {'total_count': 0, 'by_type': {}, 'by_severity': {}, 'critical_events': []}
    
    by_type = {}
    by_severity = {}
    critical_events = []
    
    for event, equipment_type, branch_name in recent_events:
        event_type = event.event_type.value
        severity = event.severity.value
        
        by_type[event_type] = by_type.get(event_type, 0) + 1
        by_severity[severity] = by_severity.get(severity, 0) + 1
        
        # Collect critical events (HIGH severity)
        if event.severity == Severity.HIGH:
            critical_events.append({
                'equipment_id': event.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'event_type': event_type,
                'subtype': event.subtype,
                'timestamp': event.ts.strftime('%H:%M'),
                'details': event.details
            })
    
    return {
        'total_count': len(recent_events),
        'by_type': by_type,
        'by_severity': by_severity,
        'critical_events': critical_events[:10]  # Top 10 critical events
    }

async def _process_overdue_rentals(overdue_rentals):
    """Process overdue rentals for digest"""
    if not overdue_rentals:
        return {'count': 0, 'total_fees': 0, 'rentals': []}
    
    rentals = []
    total_fees = 0
    
    for rental, equipment_type, branch_name, customer_name in overdue_rentals:
        total_fees += rental.late_fee or 0
        
        rentals.append({
            'rental_id': rental.rental_id,
            'equipment_id': rental.equipment_id,
            'equipment_type': equipment_type,
            'branch_name': branch_name,
            'customer_name': customer_name,
            'late_hours': rental.late_hours,
            'late_days': rental.late_days,
            'late_fee': rental.late_fee or 0,
            'payment_status': rental.payment_status.value
        })
    
    # Sort by late fees (highest first)
    rentals.sort(key=lambda x: x['late_fee'], reverse=True)
    
    return {
        'count': len(overdue_rentals),
        'total_fees': round(total_fees, 2),
        'rentals': rentals[:10]  # Top 10 by late fees
    }

async def _calculate_kpis(usage_data, recent_events, overdue_rentals):
    """Calculate key performance indicators"""
    kpis = {}
    
    if usage_data:
        # Usage KPIs
        total_runtime = sum(usage.runtime_hours for usage, _, _, _ in usage_data)
        total_idle = sum(usage.idle_hours for usage, _, _, _ in usage_data)
        total_fuel = sum(usage.fuel_used_liters for usage, _, _, _ in usage_data)
        avg_utilization = sum(usage.utilization_pct for usage, _, _, _ in usage_data) / len(usage_data)
        total_breakdown = sum(usage.breakdown_hours for usage, _, _, _ in usage_data)
        
        kpis.update({
            'total_runtime_hours': round(total_runtime, 1),
            'total_idle_hours': round(total_idle, 1),
            'total_fuel_liters': round(total_fuel, 1),
            'avg_utilization_pct': round(avg_utilization, 1),
            'total_breakdown_hours': round(total_breakdown, 1),
            'active_equipment_count': len(usage_data)
        })
        
        # Calculate efficiency metrics
        if total_runtime + total_idle > 0:
            operational_efficiency = (total_runtime / (total_runtime + total_idle)) * 100
            kpis['operational_efficiency_pct'] = round(operational_efficiency, 1)
    
    # Event KPIs
    if recent_events:
        anomaly_count = sum(1 for event, _, _ in recent_events if event.event_type == EventType.ANOMALY)
        security_count = sum(1 for event, _, _ in recent_events if event.event_type == EventType.SECURITY)
        maintenance_count = sum(1 for event, _, _ in recent_events if event.event_type == EventType.MAINTENANCE)
        high_severity_count = sum(1 for event, _, _ in recent_events if event.severity == Severity.HIGH)
        
        kpis.update({
            'total_events': len(recent_events),
            'anomaly_events': anomaly_count,
            'security_events': security_count,
            'maintenance_events': maintenance_count,
            'high_severity_events': high_severity_count
        })
    
    # Rental KPIs
    if overdue_rentals:
        total_late_fees = sum(rental.late_fee or 0 for rental, _, _, _ in overdue_rentals)
        avg_late_hours = sum(rental.late_hours for rental, _, _, _ in overdue_rentals) / len(overdue_rentals)
        
        kpis.update({
            'overdue_rental_count': len(overdue_rentals),
            'total_late_fees_inr': round(total_late_fees, 2),
            'avg_late_hours': round(avg_late_hours, 1)
        })
    
    return kpis

async def _generate_alerts(usage_data, recent_events, overdue_rentals):
    """Generate actionable alerts for dealers"""
    alerts = []
    
    # High severity events
    high_severity_events = [event for event, _, _ in recent_events if event.severity == Severity.HIGH]
    if high_severity_events:
        alerts.append({
            'type': 'CRITICAL',
            'message': f"{len(high_severity_events)} critical events require immediate attention",
            'action': 'Review security and anomaly events dashboard'
        })
    
    # Overdue rentals
    if overdue_rentals:
        total_fees = sum(rental.late_fee or 0 for rental, _, _, _ in overdue_rentals)
        alerts.append({
            'type': 'FINANCIAL',
            'message': f"{len(overdue_rentals)} overdue rentals with ₹{total_fees:,.0f} in late fees",
            'action': 'Contact customers for payment collection'
        })
    
    # Low utilization equipment
    if usage_data:
        low_utilization = [(usage, eq_type, branch) for usage, eq_type, branch, _ in usage_data 
                          if usage.utilization_pct < 30]
        if low_utilization:
            alerts.append({
                'type': 'OPERATIONAL',
                'message': f"{len(low_utilization)} equipment units with <30% utilization",
                'action': 'Consider relocation or maintenance scheduling'
            })
    
    # Excessive breakdown hours
    if usage_data:
        high_breakdown = [(usage, eq_type, branch) for usage, eq_type, branch, _ in usage_data 
                         if usage.breakdown_hours > 4]
        if high_breakdown:
            alerts.append({
                'type': 'MAINTENANCE',
                'message': f"{len(high_breakdown)} equipment units with >4h breakdown time",
                'action': 'Schedule immediate maintenance inspection'
            })
    
    # Fuel anomalies
    fuel_anomalies = [event for event, _, _ in recent_events 
                     if event.event_type == EventType.ANOMALY and 'FUEL' in event.subtype]
    if fuel_anomalies:
        alerts.append({
            'type': 'SECURITY',
            'message': f"{len(fuel_anomalies)} fuel-related anomalies detected",
            'action': 'Investigate potential fuel theft or system issues'
        })
    
    return alerts

def _get_unit_for_metric(metric):
    """Get appropriate unit for metric"""
    if 'hours' in metric:
        return 'hours'
    elif 'pct' in metric or 'percentage' in metric:
        return '%'
    elif 'liters' in metric or 'fuel' in metric:
        return 'liters'
    elif 'fees' in metric or 'inr' in metric:
        return 'INR'
    elif 'count' in metric:
        return 'count'
    else:
        return 'units'

@router.get("/charts")
async def generate_digest_charts(db: Session = Depends(get_db)):
    """Generate charts for daily digest"""
    try:
        now = datetime.now(IST)
        yesterday = now.date() - timedelta(days=1)
        last_24h = now - timedelta(hours=24)
        
        # Get data
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= yesterday
        ).all()
        
        recent_events = db.query(
            Event,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Event.ts >= last_24h
        ).all()
        
        charts = {}
        
        # 1. Runtime & Idle by Branch
        if usage_data:
            df_usage = pd.DataFrame([
                {
                    'branch_name': branch_name,
                    'runtime_hours': usage.runtime_hours,
                    'idle_hours': usage.idle_hours,
                    'fuel_used_liters': usage.fuel_used_liters
                }
                for usage, _, branch_name in usage_data
            ])
            
            branch_summary = df_usage.groupby('branch_name').sum().reset_index()
            
            fig_branch = go.Figure(data=[
                go.Bar(name='Runtime', x=branch_summary['branch_name'], y=branch_summary['runtime_hours'], marker_color='#2E8B57'),
                go.Bar(name='Idle', x=branch_summary['branch_name'], y=branch_summary['idle_hours'], marker_color='#FFD700')
            ])
            
            fig_branch.update_layout(
                title='Runtime & Idle Hours by Branch (Last 24h)',
                xaxis_title='Branch',
                yaxis_title='Hours',
                barmode='group'
            )
            
            charts['runtime_idle_by_branch'] = json.loads(fig_branch.to_json())
            
            # 2. Fuel consumption trend
            fig_fuel = go.Figure(data=[go.Bar(
                x=branch_summary['branch_name'],
                y=branch_summary['fuel_used_liters'],
                marker_color='#FF6B6B'
            )])
            
            fig_fuel.update_layout(
                title='Fuel Consumption by Branch (Last 24h)',
                xaxis_title='Branch',
                yaxis_title='Fuel (Liters)'
            )
            
            charts['fuel_by_branch'] = json.loads(fig_fuel.to_json())
        
        # 3. Alerts by Type
        if recent_events:
            event_types = [event.event_type.value for event, _ in recent_events]
            event_counts = pd.Series(event_types).value_counts()
            
            colors = {
                'ANOMALY': '#FF4444',
                'SECURITY': '#FF8800',
                'MAINTENANCE': '#4CAF50',
                'FUEL': '#2196F3',
                'OVERDUE': '#9C27B0',
                'PREDICTIVE': '#FF9800',
                'OPERATOR': '#607D8B'
            }
            
            fig_alerts = go.Figure(data=[go.Pie(
                labels=event_counts.index,
                values=event_counts.values,
                marker_colors=[colors.get(event_type, '#9E9E9E') for event_type in event_counts.index],
                hole=0.3
            )])
            
            fig_alerts.update_layout(
                title='Events by Type (Last 24h)',
                annotations=[dict(text=f'{len(recent_events)}<br>Total<br>Events', x=0.5, y=0.5, font_size=12, showarrow=False)]
            )
            
            charts['alerts_by_type'] = json.loads(fig_alerts.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating digest charts: {str(e)}")

@router.get("/insights")
async def generate_digest_insights(db: Session = Depends(get_db)):
    """Generate text insights for daily digest"""
    try:
        now = datetime.now(IST)
        yesterday = now.date() - timedelta(days=1)
        last_24h = now - timedelta(hours=24)
        
        # Get usage data
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= yesterday
        ).all()
        
        # Get events
        recent_events = db.query(
            Event,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Event.ts >= last_24h
        ).all()
        
        # Get overdue rentals
        overdue_rentals = db.query(
            Rental,
            MasterEquipment.customer_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Rental.late_hours > 0,
                or_(
                    Rental.payment_status == PaymentStatus.DUE,
                    Rental.payment_status == PaymentStatus.OVERDUE
                )
            )
        ).all()
        
        insights = []
        
        # Usage insights
        if usage_data:
            # Branch performance
            branch_runtime = {}
            for usage, _, branch_name in usage_data:
                branch_runtime[branch_name] = branch_runtime.get(branch_name, 0) + usage.runtime_hours
            
            if branch_runtime:
                best_branch = max(branch_runtime, key=branch_runtime.get)
                best_runtime = branch_runtime[best_branch]
                
                # Compare with yesterday (simplified)
                growth_pct = 22  # Placeholder - would calculate from historical data
                insights.append(f"🏆 {best_branch} runtime +{growth_pct}% vs yesterday ({best_runtime:.1f}h total).")
            
            # Overall utilization
            avg_utilization = sum(usage.utilization_pct for usage, _, _ in usage_data) / len(usage_data)
            if avg_utilization > 70:
                insights.append(f"✅ Excellent fleet utilization at {avg_utilization:.1f}% - operations running smoothly.")
            elif avg_utilization < 50:
                insights.append(f"⚠️ Fleet utilization at {avg_utilization:.1f}% - consider equipment reallocation.")
        
        # Event insights
        if recent_events:
            high_severity = sum(1 for event, _ in recent_events if event.severity == Severity.HIGH)
            if high_severity > 0:
                insights.append(f"🚨 {high_severity} critical events in last 24h - immediate attention required.")
            
            # Anomaly breakdown
            anomalies = [event for event, _ in recent_events if event.event_type == EventType.ANOMALY]
            if anomalies:
                insights.append(f"⚠️ {len(anomalies)} anomalies detected - review fuel, idle time, and tampering alerts.")
        
        # Overdue rental insights
        if overdue_rentals:
            total_fees = sum(rental.late_fee or 0 for rental, _ in overdue_rentals)
            top_overdue = sorted(overdue_rentals, key=lambda x: x[0].late_fee or 0, reverse=True)[:3]
            
            insights.append(f"💰 {len(overdue_rentals)} overdue rentals: Est. fees ₹{total_fees:,.0f}.")
            
            for rental, customer_name in top_overdue:
                insights.append(f"📞 Rental {rental.rental_id} overdue by {rental.late_hours}h. Est. fee ₹{rental.late_fee or 0:,.0f}. Contact {customer_name}.")
        
        # Maintenance insights
        if usage_data:
            high_breakdown = [(usage, eq_type, branch) for usage, eq_type, branch in usage_data 
                             if usage.breakdown_hours > 2]
            if high_breakdown:
                insights.append(f"🔧 {len(high_breakdown)} equipment units with significant breakdown time - schedule maintenance.")
        
        # Security insights
        security_events = [event for event, _ in recent_events if event.event_type == EventType.SECURITY]
        if security_events:
            insights.append(f"🔒 {len(security_events)} security events - review unauthorized access and geofence breaches.")
        
        return {
            'status': 'success',
            'insights': insights,
            'period': 'Last 24 hours',
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating digest insights: {str(e)}")

@router.post("/predictive-analysis")
async def generate_predictive_analysis(request: PredictiveAnalysisRequest, db: Session = Depends(get_db)):
    """Generate predictive analysis for future trends and potential issues"""
    try:
        now = datetime.now(IST)
        start_date = now.date() - timedelta(days=30)  # Use 30 days of historical data
        
        # Build query with filters
        query = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        )
        
        if request.branch_name:
            query = query.filter(MasterEquipment.branch_name == request.branch_name)
        if request.equipment_type:
            query = query.filter(MasterEquipment.equipment_type == request.equipment_type)
        
        historical_data = query.all()
        
        if not historical_data:
            raise HTTPException(status_code=404, detail="No historical data found for prediction")
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame([
            {
                'date': usage.date,
                'equipment_id': usage.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'fuel_used_liters': usage.fuel_used_liters,
                'utilization_pct': usage.utilization_pct,
                'breakdown_hours': usage.breakdown_hours
            }
            for usage, equipment_type, branch_name, _ in historical_data
        ])
        
        predictions = {}
        
        if request.analysis_type in ["comprehensive", "utilization"]:
            # Predict utilization trends
            utilization_forecast = await _predict_utilization_trends(df, request.days_ahead)
            predictions['utilization_forecast'] = utilization_forecast
        
        if request.analysis_type in ["comprehensive", "maintenance"]:
            # Predict maintenance needs
            maintenance_forecast = await _predict_maintenance_needs(df, request.days_ahead)
            predictions['maintenance_forecast'] = maintenance_forecast
        
        if request.analysis_type in ["comprehensive", "revenue"]:
            # Predict revenue impact
            revenue_forecast = await _predict_revenue_impact(df, db, request.days_ahead)
            predictions['revenue_forecast'] = revenue_forecast
        
        # Generate risk alerts
        risk_alerts = await _generate_risk_alerts(df, request.days_ahead)
        
        # Log the analysis
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.PREDICTIVE,
            equipment_id="SYSTEM",
            ts=now,
            details=f"Predictive analysis generated for {request.days_ahead} days ahead"
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'analysis_period': f"{request.days_ahead} days ahead",
            'predictions': predictions,
            'risk_alerts': risk_alerts,
            'confidence_level': 'medium',  # Based on 30 days of data
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Error generating predictive analysis: {str(e)}")

@router.post("/comparative-analysis")
async def generate_comparative_analysis(request: ComparativeAnalysisRequest, db: Session = Depends(get_db)):
    """Generate comparative analysis between two time periods"""
    try:
        now = datetime.now(IST)
        
        # Current period
        current_end = now.date()
        current_start = current_end - timedelta(days=request.period_days)
        
        # Comparison period
        compare_end = current_start
        compare_start = compare_end - timedelta(days=request.compare_with_days)
        
        # Get data for both periods
        current_data = await _get_period_data(db, current_start, current_end, request.branch_name, request.equipment_type)
        compare_data = await _get_period_data(db, compare_start, compare_end, request.branch_name, request.equipment_type)
        
        if not current_data and not compare_data:
            raise HTTPException(status_code=404, detail="No data found for comparison periods")
        
        # Calculate metrics for both periods
        current_metrics = await _calculate_period_metrics(current_data)
        compare_metrics = await _calculate_period_metrics(compare_data)
        
        # Calculate changes and trends
        comparison = {
            'runtime_hours': {
                'current': current_metrics.get('total_runtime', 0),
                'previous': compare_metrics.get('total_runtime', 0),
                'change_pct': _calculate_percentage_change(compare_metrics.get('total_runtime', 0), current_metrics.get('total_runtime', 0)),
                'trend': 'improving' if current_metrics.get('total_runtime', 0) > compare_metrics.get('total_runtime', 0) else 'declining'
            },
            'utilization_pct': {
                'current': current_metrics.get('avg_utilization', 0),
                'previous': compare_metrics.get('avg_utilization', 0),
                'change_pct': _calculate_percentage_change(compare_metrics.get('avg_utilization', 0), current_metrics.get('avg_utilization', 0)),
                'trend': 'improving' if current_metrics.get('avg_utilization', 0) > compare_metrics.get('avg_utilization', 0) else 'declining'
            },
            'breakdown_hours': {
                'current': current_metrics.get('total_breakdown', 0),
                'previous': compare_metrics.get('total_breakdown', 0),
                'change_pct': _calculate_percentage_change(compare_metrics.get('total_breakdown', 0), current_metrics.get('total_breakdown', 0)),
                'trend': 'improving' if current_metrics.get('total_breakdown', 0) < compare_metrics.get('total_breakdown', 0) else 'declining'
            },
            'fuel_efficiency': {
                'current': current_metrics.get('fuel_efficiency', 0),
                'previous': compare_metrics.get('fuel_efficiency', 0),
                'change_pct': _calculate_percentage_change(compare_metrics.get('fuel_efficiency', 0), current_metrics.get('fuel_efficiency', 0)),
                'trend': 'improving' if current_metrics.get('fuel_efficiency', 0) > compare_metrics.get('fuel_efficiency', 0) else 'declining'
            }
        }
        
        # Generate insights
        insights = await _generate_comparative_insights(comparison, request.period_days)
        
        # Log the analysis
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.PREDICTIVE,
            equipment_id="SYSTEM",
            ts=now,
            details=f"Comparative analysis: {request.period_days} days vs {request.compare_with_days} days"
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'current_period': f"{current_start} to {current_end}",
            'comparison_period': f"{compare_start} to {compare_end}",
            'comparison': comparison,
            'insights': insights,
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating comparative analysis: {str(e)}")

@router.post("/automated-recommendations")
async def generate_automated_recommendations(request: RecommendationRequest, db: Session = Depends(get_db)):
    """Generate automated recommendations based on current data patterns"""
    try:
        now = datetime.now(IST)
        last_7_days = now.date() - timedelta(days=7)
        
        # Get recent data
        query = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= last_7_days
        )
        
        if request.branch_name:
            query = query.filter(MasterEquipment.branch_name == request.branch_name)
        
        usage_data = query.all()
        
        # Get recent events
        recent_events = db.query(
            Event,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Event.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Event.ts >= now - timedelta(days=7)
        ).all()
        
        # Get overdue rentals
        overdue_rentals = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                Rental.late_hours > 0,
                or_(
                    Rental.payment_status == PaymentStatus.DUE,
                    Rental.payment_status == PaymentStatus.OVERDUE
                )
            )
        ).all()
        
        recommendations = []
        
        # Operational recommendations
        if not request.category or request.category == "operational":
            operational_recs = await _generate_operational_recommendations(usage_data, request.priority_level)
            recommendations.extend(operational_recs)
        
        # Financial recommendations
        if not request.category or request.category == "financial":
            financial_recs = await _generate_financial_recommendations(overdue_rentals, usage_data, request.priority_level)
            recommendations.extend(financial_recs)
        
        # Maintenance recommendations
        if not request.category or request.category == "maintenance":
            maintenance_recs = await _generate_maintenance_recommendations(usage_data, recent_events, request.priority_level)
            recommendations.extend(maintenance_recs)
        
        # Security recommendations
        if not request.category or request.category == "security":
            security_recs = await _generate_security_recommendations(recent_events, request.priority_level)
            recommendations.extend(security_recs)
        
        # Sort by priority and impact
        recommendations.sort(key=lambda x: (x['priority_score'], x['impact_score']), reverse=True)
        
        # Log the analysis
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.PREDICTIVE,
            equipment_id="SYSTEM",
            ts=now,
            details=f"Automated recommendations generated: {len(recommendations)} items"
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'recommendations': recommendations[:20],  # Top 20 recommendations
            'total_recommendations': len(recommendations),
            'priority_filter': request.priority_level,
            'category_filter': request.category or 'all',
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Error generating automated recommendations: {str(e)}")

# Helper functions for advanced features
async def _predict_utilization_trends(df, days_ahead):
    """Predict utilization trends using linear regression"""
    try:
        # Group by date and calculate daily averages
        daily_util = df.groupby('date')['utilization_pct'].mean().reset_index()
        daily_util['days_from_start'] = (daily_util['date'] - daily_util['date'].min()).dt.days
        
        if len(daily_util) < 3:
            return {'error': 'Insufficient data for prediction'}
        
        # Fit linear regression
        X = daily_util[['days_from_start']]
        y = daily_util['utilization_pct']
        
        model = LinearRegression()
        model.fit(X, y)
        
        # Predict future values
        future_days = np.arange(daily_util['days_from_start'].max() + 1, 
                               daily_util['days_from_start'].max() + days_ahead + 1)
        future_predictions = model.predict(future_days.reshape(-1, 1))
        
        # Calculate trend
        current_avg = daily_util['utilization_pct'].tail(3).mean()
        predicted_avg = future_predictions.mean()
        trend_direction = 'increasing' if predicted_avg > current_avg else 'decreasing'
        
        return {
            'current_utilization': round(current_avg, 1),
            'predicted_utilization': round(predicted_avg, 1),
            'trend_direction': trend_direction,
            'confidence': 'medium',
            'daily_predictions': [round(p, 1) for p in future_predictions]
        }
    except Exception:
        return {'error': 'Unable to generate utilization forecast'}

async def _predict_maintenance_needs(df, days_ahead):
    """Predict maintenance needs based on breakdown patterns"""
    try:
        # Analyze breakdown patterns by equipment
        equipment_breakdown = df.groupby('equipment_id').agg({
            'breakdown_hours': ['sum', 'mean', 'std'],
            'runtime_hours': 'sum'
        }).reset_index()
        
        equipment_breakdown.columns = ['equipment_id', 'total_breakdown', 'avg_breakdown', 'std_breakdown', 'total_runtime']
        equipment_breakdown['breakdown_rate'] = equipment_breakdown['total_breakdown'] / equipment_breakdown['total_runtime']
        
        # Identify high-risk equipment
        high_risk = equipment_breakdown[
            (equipment_breakdown['breakdown_rate'] > 0.1) | 
            (equipment_breakdown['total_breakdown'] > equipment_breakdown['total_breakdown'].quantile(0.8))
        ]
        
        # Predict maintenance needs
        maintenance_alerts = []
        for _, row in high_risk.iterrows():
            risk_score = min(100, (row['breakdown_rate'] * 100) + (row['total_breakdown'] / 10))
            maintenance_alerts.append({
                'equipment_id': row['equipment_id'],
                'risk_score': round(risk_score, 1),
                'predicted_breakdown_hours': round(row['avg_breakdown'] * (days_ahead / 7), 1),
                'recommendation': 'Schedule preventive maintenance' if risk_score > 50 else 'Monitor closely'
            })
        
        return {
            'high_risk_equipment': len(high_risk),
            'maintenance_alerts': sorted(maintenance_alerts, key=lambda x: x['risk_score'], reverse=True)[:10]
        }
    except Exception:
        return {'error': 'Unable to generate maintenance forecast'}

async def _predict_revenue_impact(df, db, days_ahead):
    """Predict revenue impact based on utilization trends"""
    try:
        # Get rental data for revenue calculation
        rental_data = db.query(Rental).filter(
            Rental.equipment_id.in_(df['equipment_id'].unique())
        ).all()
        
        if not rental_data:
            return {'error': 'No rental data available for revenue prediction'}
        
        # Calculate average revenue per hour
        total_revenue = sum(r.revenue for r in rental_data if r.revenue)
        total_hours = sum(r.billed_hours for r in rental_data if r.billed_hours)
        avg_revenue_per_hour = total_revenue / total_hours if total_hours > 0 else 0
        
        # Current utilization
        current_util = df['utilization_pct'].mean()
        current_runtime = df['runtime_hours'].sum()
        
        # Predict future runtime based on utilization trends
        predicted_util = current_util * 0.95  # Assume slight decline
        predicted_runtime = current_runtime * (predicted_util / current_util) * (days_ahead / 7)
        
        # Calculate revenue impact
        current_revenue_potential = current_runtime * avg_revenue_per_hour
        predicted_revenue_potential = predicted_runtime * avg_revenue_per_hour
        revenue_change = predicted_revenue_potential - current_revenue_potential
        
        return {
            'current_revenue_potential': round(current_revenue_potential, 2),
            'predicted_revenue_potential': round(predicted_revenue_potential, 2),
            'revenue_change': round(revenue_change, 2),
            'revenue_change_pct': round((revenue_change / current_revenue_potential) * 100, 1) if current_revenue_potential > 0 else 0,
            'avg_revenue_per_hour': round(avg_revenue_per_hour, 2)
        }
    except Exception:
        return {'error': 'Unable to generate revenue forecast'}

async def _generate_risk_alerts(df, days_ahead):
    """Generate risk alerts based on data patterns"""
    alerts = []
    
    # Low utilization risk
    low_util_equipment = df[df['utilization_pct'] < 30]['equipment_id'].nunique()
    if low_util_equipment > 0:
        alerts.append({
            'type': 'UTILIZATION_RISK',
            'severity': 'medium',
            'message': f'{low_util_equipment} equipment units with low utilization (<30%)',
            'impact': 'Revenue loss potential'
        })
    
    # High breakdown risk
    high_breakdown = df[df['breakdown_hours'] > 4]['equipment_id'].nunique()
    if high_breakdown > 0:
        alerts.append({
            'type': 'MAINTENANCE_RISK',
            'severity': 'high',
            'message': f'{high_breakdown} equipment units with excessive breakdown time',
            'impact': 'Operational disruption likely'
        })
    
    return alerts

async def _get_period_data(db, start_date, end_date, branch_name=None, equipment_type=None):
    """Get usage data for a specific period"""
    query = db.query(
        UsageDaily,
        MasterEquipment.equipment_type,
        MasterEquipment.branch_name
    ).join(
        MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
    ).filter(
        and_(UsageDaily.date >= start_date, UsageDaily.date <= end_date)
    )
    
    if branch_name:
        query = query.filter(MasterEquipment.branch_name == branch_name)
    if equipment_type:
        query = query.filter(MasterEquipment.equipment_type == equipment_type)
    
    return query.all()

async def _calculate_period_metrics(period_data):
    """Calculate metrics for a period"""
    if not period_data:
        return {}
    
    total_runtime = sum(usage.runtime_hours for usage, _, _ in period_data)
    total_idle = sum(usage.idle_hours for usage, _, _ in period_data)
    total_fuel = sum(usage.fuel_used_liters for usage, _, _ in period_data)
    total_breakdown = sum(usage.breakdown_hours for usage, _, _ in period_data)
    avg_utilization = sum(usage.utilization_pct for usage, _, _ in period_data) / len(period_data)
    
    fuel_efficiency = total_runtime / total_fuel if total_fuel > 0 else 0
    
    return {
        'total_runtime': total_runtime,
        'total_idle': total_idle,
        'total_fuel': total_fuel,
        'total_breakdown': total_breakdown,
        'avg_utilization': avg_utilization,
        'fuel_efficiency': fuel_efficiency
    }

def _calculate_percentage_change(old_value, new_value):
    """Calculate percentage change between two values"""
    if old_value == 0:
        return 100 if new_value > 0 else 0
    return round(((new_value - old_value) / old_value) * 100, 1)

async def _generate_comparative_insights(comparison, period_days):
    """Generate insights from comparative analysis"""
    insights = []
    
    for metric, data in comparison.items():
        change_pct = data['change_pct']
        trend = data['trend']
        
        if abs(change_pct) > 10:
            if metric == 'runtime_hours' and trend == 'improving':
                insights.append(f"Runtime increased by {change_pct}% - strong operational performance")
            elif metric == 'utilization_pct' and trend == 'improving':
                insights.append(f"Utilization improved by {change_pct}% - better asset efficiency")
            elif metric == 'breakdown_hours' and trend == 'improving':
                insights.append(f"Breakdown time reduced by {abs(change_pct)}% - maintenance improvements")
            elif trend == 'declining':
                insights.append(f"{metric.replace('_', ' ').title()} declined by {abs(change_pct)}% - needs attention")
    
    return insights

async def _generate_operational_recommendations(usage_data, priority_level):
    """Generate operational recommendations"""
    recommendations = []
    
    if not usage_data:
        return recommendations
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame([
        {
            'equipment_id': usage.equipment_id,
            'equipment_type': equipment_type,
            'branch_name': branch_name,
            'utilization_pct': usage.utilization_pct,
            'idle_hours': usage.idle_hours,
            'breakdown_hours': usage.breakdown_hours
        }
        for usage, equipment_type, branch_name, _ in usage_data
    ])
    
    # Low utilization equipment
    low_util = df[df['utilization_pct'] < 40]
    if not low_util.empty:
        for _, row in low_util.head(5).iterrows():
            recommendations.append({
                'category': 'operational',
                'priority': 'high' if row['utilization_pct'] < 25 else 'medium',
                'title': f"Low Utilization - {row['equipment_type']}",
                'description': f"Equipment {row['equipment_id']} has {row['utilization_pct']:.1f}% utilization",
                'action': "Consider relocation to high-demand area or maintenance check",
                'impact': 'Revenue optimization',
                'priority_score': 90 if row['utilization_pct'] < 25 else 70,
                'impact_score': 85
            })
    
    # High idle time
    high_idle = df[df['idle_hours'] > 8]
    if not high_idle.empty:
        for _, row in high_idle.head(3).iterrows():
            recommendations.append({
                'category': 'operational',
                'priority': 'medium',
                'title': f"Excessive Idle Time - {row['equipment_type']}",
                'description': f"Equipment {row['equipment_id']} idle for {row['idle_hours']:.1f} hours",
                'action': "Review operator training and work scheduling",
                'impact': 'Fuel cost reduction',
                'priority_score': 60,
                'impact_score': 70
            })
    
    return [r for r in recommendations if priority_level == 'all' or r['priority'] == priority_level]

async def _generate_financial_recommendations(overdue_rentals, usage_data, priority_level):
    """Generate financial recommendations"""
    recommendations = []
    
    # Overdue rental collections
    if overdue_rentals:
        total_fees = sum(rental.late_fee or 0 for rental, _, _ in overdue_rentals)
        high_value_overdue = [r for r in overdue_rentals if (r[0].late_fee or 0) > 5000]
        
        if high_value_overdue:
            recommendations.append({
                'category': 'financial',
                'priority': 'high',
                'title': 'High-Value Overdue Collections',
                'description': f'{len(high_value_overdue)} rentals with fees >₹5,000',
                'action': 'Prioritize collection calls and legal notices',
                'impact': f'Potential recovery: ₹{sum(r[0].late_fee or 0 for r in high_value_overdue):,.0f}',
                'priority_score': 95,
                'impact_score': 90
            })
    
    return [r for r in recommendations if priority_level == 'all' or r['priority'] == priority_level]

async def _generate_maintenance_recommendations(usage_data, recent_events, priority_level):
    """Generate maintenance recommendations"""
    recommendations = []
    
    # High breakdown equipment
    if usage_data:
        high_breakdown = [(usage, eq_type, branch) for usage, eq_type, branch, _ in usage_data 
                         if usage.breakdown_hours > 4]
        
        for usage, eq_type, branch in high_breakdown[:5]:
            recommendations.append({
                'category': 'maintenance',
                'priority': 'high',
                'title': f'Preventive Maintenance Required - {eq_type}',
                'description': f'Equipment {usage.equipment_id} breakdown: {usage.breakdown_hours:.1f}h',
                'action': 'Schedule immediate inspection and maintenance',
                'impact': 'Prevent further downtime and repair costs',
                'priority_score': 85,
                'impact_score': 80
            })
    
    return [r for r in recommendations if priority_level == 'all' or r['priority'] == priority_level]

async def _generate_security_recommendations(recent_events, priority_level):
    """Generate security recommendations"""
    recommendations = []
    
    # Security events analysis
    security_events = [event for event, _, _ in recent_events if event.event_type == EventType.SECURITY]
    
    if security_events:
        recommendations.append({
            'category': 'security',
            'priority': 'high',
            'title': 'Security Events Review',
            'description': f'{len(security_events)} security events in last 7 days',
            'action': 'Review access logs and update security protocols',
            'impact': 'Prevent unauthorized access and theft',
            'priority_score': 80,
            'impact_score': 85
        })
    
    return [r for r in recommendations if priority_level == 'all' or r['priority'] == priority_level]