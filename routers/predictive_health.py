from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import MasterEquipment, UsageDaily, Event, EventType, Severity, PredictiveHealth, OperatorScore
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import random

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

@router.get("/health-scores")
async def get_predictive_health_scores(days: int = 7, db: Session = Depends(get_db)):
    """Get predictive health scores for all equipment"""
    try:
        cutoff_date = datetime.now(IST).date() - timedelta(days=days)
        
        # Get recent usage data for analysis
        usage_data = db.query(
            UsageDaily.equipment_id,
            UsageDaily.date,
            UsageDaily.runtime_hours,
            UsageDaily.idle_hours,
            UsageDaily.fuel_used_liters,
            UsageDaily.fuel_eff_lph,
            UsageDaily.breakdown_hours,
            UsageDaily.utilization_pct,
            MasterEquipment.equipment_type,
            MasterEquipment.make,
            MasterEquipment.model,
            MasterEquipment.year,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= cutoff_date
        ).all()
        
        # Process data by equipment
        equipment_health = {}
        
        for usage in usage_data:
            eq_id = usage.equipment_id
            
            if eq_id not in equipment_health:
                equipment_health[eq_id] = {
                    'equipment_id': eq_id,
                    'equipment_type': usage.equipment_type,
                    'make': usage.make,
                    'model': usage.model,
                    'year': usage.year,
                    'branch_name': usage.branch_name,
                    'site_name': usage.site_name,
                    'usage_records': []
                }
            
            equipment_health[eq_id]['usage_records'].append({
                'date': usage.date,
                'runtime_hours': usage.runtime_hours or 0,
                'idle_hours': usage.idle_hours or 0,
                'fuel_used': usage.fuel_used_liters or 0,
                'fuel_efficiency': usage.fuel_eff_lph or 0,
                'breakdown_hours': usage.breakdown_hours or 0,
                'utilization': usage.utilization_pct or 0
            })
        
        # Calculate health scores
        health_scores = []
        
        for eq_id, eq_data in equipment_health.items():
            if len(eq_data['usage_records']) < 3:  # Need minimum data
                continue
            
            health_score = await _calculate_health_score(eq_data)
            health_scores.append(health_score)
        
        # Sort by failure probability (highest risk first)
        health_scores.sort(key=lambda x: x['failure_probability'], reverse=True)
        
        # Save to CSV
        csv_path = 'outputs/predictive_health.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if health_scores:
            df = pd.DataFrame(health_scores)
            df.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        total_equipment = len(health_scores)
        critical_equipment = len([h for h in health_scores if h['risk_level'] == 'CRITICAL'])
        high_risk_equipment = len([h for h in health_scores if h['risk_level'] == 'HIGH'])
        
        avg_health_score = sum(h['health_score'] for h in health_scores) / total_equipment if total_equipment > 0 else 0
        
        return {
            'status': 'success',
            'health_scores': health_scores,
            'csv_file': csv_path,
            'summary': {
                'total_equipment_analyzed': total_equipment,
                'critical_risk_count': critical_equipment,
                'high_risk_count': high_risk_equipment,
                'average_health_score': round(avg_health_score, 1),
                'days_analyzed': days
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating health scores: {str(e)}")

async def _calculate_health_score(equipment_data):
    """Calculate predictive health score for equipment"""
    records = equipment_data['usage_records']
    
    # Calculate key metrics
    total_runtime = sum(r['runtime_hours'] for r in records)
    total_breakdown = sum(r['breakdown_hours'] for r in records)
    avg_utilization = sum(r['utilization'] for r in records) / len(records)
    avg_fuel_efficiency = sum(r['fuel_efficiency'] for r in records if r['fuel_efficiency'] > 0) / max(1, len([r for r in records if r['fuel_efficiency'] > 0]))
    
    # Calculate trends
    recent_breakdown = sum(r['breakdown_hours'] for r in records[-3:])  # Last 3 days
    early_breakdown = sum(r['breakdown_hours'] for r in records[:3])   # First 3 days
    breakdown_trend = recent_breakdown - early_breakdown
    
    # Fuel efficiency trend
    recent_fuel_eff = [r['fuel_efficiency'] for r in records[-3:] if r['fuel_efficiency'] > 0]
    early_fuel_eff = [r['fuel_efficiency'] for r in records[:3] if r['fuel_efficiency'] > 0]
    
    fuel_eff_trend = 0
    if recent_fuel_eff and early_fuel_eff:
        fuel_eff_trend = sum(recent_fuel_eff) / len(recent_fuel_eff) - sum(early_fuel_eff) / len(early_fuel_eff)
    
    # Calculate health score (0-100, higher is better)
    health_score = 100
    
    # Deduct points for issues
    breakdown_ratio = (total_breakdown / max(1, total_runtime)) * 100
    health_score -= breakdown_ratio * 10  # Heavy penalty for breakdowns
    
    if avg_utilization < 30:  # Low utilization might indicate issues
        health_score -= (30 - avg_utilization) * 0.5
    
    if breakdown_trend > 0:  # Increasing breakdown hours
        health_score -= breakdown_trend * 5
    
    if fuel_eff_trend < -0.5:  # Decreasing fuel efficiency
        health_score -= abs(fuel_eff_trend) * 10
    
    # Age factor (older equipment more likely to fail)
    current_year = datetime.now().year
    age = current_year - equipment_data['year']
    if age > 10:
        health_score -= (age - 10) * 2
    
    # Ensure score is between 0 and 100
    health_score = max(0, min(100, health_score))
    
    # Calculate failure probability (inverse of health score)
    failure_probability = (100 - health_score) / 100
    
    # Determine risk level
    if health_score >= 80:
        risk_level = 'LOW'
        maintenance_priority = 'ROUTINE'
    elif health_score >= 60:
        risk_level = 'MEDIUM'
        maintenance_priority = 'SCHEDULED'
    elif health_score >= 40:
        risk_level = 'HIGH'
        maintenance_priority = 'URGENT'
    else:
        risk_level = 'CRITICAL'
        maintenance_priority = 'IMMEDIATE'
    
    # Generate specific predictions
    predictions = []
    
    if breakdown_trend > 2:
        predictions.append("Increasing breakdown frequency detected - inspect mechanical systems")
    
    if fuel_eff_trend < -1:
        predictions.append("Declining fuel efficiency - check engine performance and filters")
    
    if breakdown_ratio > 5:
        predictions.append("High breakdown ratio - comprehensive maintenance required")
    
    if age > 15:
        predictions.append("Equipment age exceeds 15 years - consider replacement planning")
    
    # Simulate specific component predictions
    component_risks = _simulate_component_analysis(equipment_data, health_score)
    
    return {
        'equipment_id': equipment_data['equipment_id'],
        'equipment_type': equipment_data['equipment_type'],
        'make': equipment_data['make'],
        'model': equipment_data['model'],
        'year': equipment_data['year'],
        'branch_name': equipment_data['branch_name'],
        'site_name': equipment_data['site_name'],
        'health_score': round(health_score, 1),
        'failure_probability': round(failure_probability, 3),
        'risk_level': risk_level,
        'maintenance_priority': maintenance_priority,
        'breakdown_hours_total': total_breakdown,
        'breakdown_trend': round(breakdown_trend, 1),
        'avg_utilization': round(avg_utilization, 1),
        'fuel_efficiency_trend': round(fuel_eff_trend, 2),
        'equipment_age': age,
        'predictions': predictions,
        'component_risks': component_risks,
        'next_maintenance_days': _calculate_maintenance_window(health_score),
        'analysis_date': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    }

def _simulate_component_analysis(equipment_data, health_score):
    """Simulate component-specific risk analysis"""
    components = {
        'Engine': random.uniform(0.1, 0.9),
        'Hydraulic System': random.uniform(0.1, 0.8),
        'Transmission': random.uniform(0.1, 0.7),
        'Cooling System': random.uniform(0.1, 0.6),
        'Electrical System': random.uniform(0.1, 0.5)
    }
    
    # Adjust based on health score
    health_factor = (100 - health_score) / 100
    
    component_risks = []
    for component, base_risk in components.items():
        adjusted_risk = min(0.95, base_risk * (1 + health_factor))
        
        if adjusted_risk > 0.8:
            risk_level = 'CRITICAL'
        elif adjusted_risk > 0.6:
            risk_level = 'HIGH'
        elif adjusted_risk > 0.4:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        component_risks.append({
            'component': component,
            'failure_risk': round(adjusted_risk, 3),
            'risk_level': risk_level,
            'estimated_failure_days': int(30 * (1 - adjusted_risk)) if adjusted_risk > 0.7 else None
        })
    
    return component_risks

def _calculate_maintenance_window(health_score):
    """Calculate recommended maintenance window based on health score"""
    if health_score >= 80:
        return 30  # Monthly maintenance
    elif health_score >= 60:
        return 14  # Bi-weekly maintenance
    elif health_score >= 40:
        return 7   # Weekly maintenance
    else:
        return 1   # Immediate maintenance

@router.get("/operator-scores")
async def get_operator_scores(days: int = 7, db: Session = Depends(get_db)):
    """Get operator efficiency scores"""
    try:
        cutoff_date = datetime.now(IST).date() - timedelta(days=days)
        
        # Get operator events and usage data
        operator_events = db.query(Event).filter(
            and_(
                Event.event_type == EventType.OPERATOR,
                Event.ts >= datetime.combine(cutoff_date, datetime.min.time())
            )
        ).all()
        
        # Get usage data with operator information (simulated)
        usage_data = db.query(
            UsageDaily.equipment_id,
            UsageDaily.date,
            UsageDaily.runtime_hours,
            UsageDaily.idle_hours,
            UsageDaily.fuel_used_liters,
            UsageDaily.fuel_eff_lph,
            UsageDaily.breakdown_hours,
            UsageDaily.utilization_pct,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= cutoff_date
        ).all()
        
        # Simulate operator assignments (in real implementation, would come from rental/session data)
        operator_data = {}
        
        for usage in usage_data:
            # Simulate operator ID assignment
            operator_id = f"OP{random.randint(100, 999)}"
            
            if operator_id not in operator_data:
                operator_data[operator_id] = {
                    'operator_id': operator_id,
                    'sessions': [],
                    'equipment_types': set(),
                    'branches': set()
                }
            
            operator_data[operator_id]['sessions'].append({
                'equipment_id': usage.equipment_id,
                'equipment_type': usage.equipment_type,
                'branch_name': usage.branch_name,
                'date': usage.date,
                'runtime_hours': usage.runtime_hours or 0,
                'idle_hours': usage.idle_hours or 0,
                'fuel_used': usage.fuel_used_liters or 0,
                'fuel_efficiency': usage.fuel_eff_lph or 0,
                'breakdown_hours': usage.breakdown_hours or 0,
                'utilization': usage.utilization_pct or 0
            })
            
            operator_data[operator_id]['equipment_types'].add(usage.equipment_type)
            operator_data[operator_id]['branches'].add(usage.branch_name)
        
        # Calculate operator scores
        operator_scores = []
        
        for operator_id, op_data in operator_data.items():
            if len(op_data['sessions']) < 2:  # Need minimum sessions
                continue
            
            score = await _calculate_operator_score(operator_id, op_data)
            operator_scores.append(score)
        
        # Sort by efficiency score (highest first)
        operator_scores.sort(key=lambda x: x['efficiency_score'], reverse=True)
        
        # Save to CSV
        csv_path = 'outputs/operator_scores.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if operator_scores:
            df = pd.DataFrame(operator_scores)
            df.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        total_operators = len(operator_scores)
        top_performers = len([o for o in operator_scores if o['performance_tier'] == 'EXCELLENT'])
        poor_performers = len([o for o in operator_scores if o['performance_tier'] == 'POOR'])
        
        avg_efficiency = sum(o['efficiency_score'] for o in operator_scores) / total_operators if total_operators > 0 else 0
        
        return {
            'status': 'success',
            'operator_scores': operator_scores,
            'csv_file': csv_path,
            'summary': {
                'total_operators_analyzed': total_operators,
                'top_performers': top_performers,
                'poor_performers': poor_performers,
                'average_efficiency_score': round(avg_efficiency, 1),
                'days_analyzed': days
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating operator scores: {str(e)}")

async def _calculate_operator_score(operator_id, operator_data):
    """Calculate operator efficiency score"""
    sessions = operator_data['sessions']
    
    # Calculate key metrics
    total_runtime = sum(s['runtime_hours'] for s in sessions)
    total_idle = sum(s['idle_hours'] for s in sessions)
    total_fuel = sum(s['fuel_used'] for s in sessions)
    total_breakdown = sum(s['breakdown_hours'] for s in sessions)
    
    avg_utilization = sum(s['utilization'] for s in sessions) / len(sessions)
    avg_fuel_efficiency = sum(s['fuel_efficiency'] for s in sessions if s['fuel_efficiency'] > 0) / max(1, len([s for s in sessions if s['fuel_efficiency'] > 0]))
    
    # Calculate efficiency metrics
    idle_ratio = (total_idle / max(1, total_runtime + total_idle)) * 100
    breakdown_ratio = (total_breakdown / max(1, total_runtime)) * 100
    
    # Calculate efficiency score (0-100, higher is better)
    efficiency_score = 100
    
    # Deduct points for inefficiencies
    if idle_ratio > 20:  # More than 20% idle time
        efficiency_score -= (idle_ratio - 20) * 2
    
    if breakdown_ratio > 2:  # More than 2% breakdown time
        efficiency_score -= breakdown_ratio * 5
    
    if avg_utilization < 50:  # Low utilization
        efficiency_score -= (50 - avg_utilization) * 0.5
    
    if avg_fuel_efficiency > 0 and avg_fuel_efficiency < 3:  # Poor fuel efficiency
        efficiency_score -= (3 - avg_fuel_efficiency) * 10
    
    # Bonus points for good performance
    if idle_ratio < 10:
        efficiency_score += 5
    
    if breakdown_ratio < 1:
        efficiency_score += 5
    
    if avg_utilization > 80:
        efficiency_score += 10
    
    # Ensure score is between 0 and 100
    efficiency_score = max(0, min(100, efficiency_score))
    
    # Determine performance tier
    if efficiency_score >= 90:
        performance_tier = 'EXCELLENT'
        percentile = 95
    elif efficiency_score >= 80:
        performance_tier = 'GOOD'
        percentile = 80
    elif efficiency_score >= 70:
        performance_tier = 'AVERAGE'
        percentile = 60
    elif efficiency_score >= 60:
        performance_tier = 'BELOW_AVERAGE'
        percentile = 40
    else:
        performance_tier = 'POOR'
        percentile = 20
    
    # Generate improvement suggestions
    suggestions = []
    
    if idle_ratio > 25:
        suggestions.append("Reduce idle time - focus on continuous operation")
    
    if breakdown_ratio > 3:
        suggestions.append("Improve equipment handling to reduce breakdowns")
    
    if avg_utilization < 60:
        suggestions.append("Increase equipment utilization during shifts")
    
    if avg_fuel_efficiency > 0 and avg_fuel_efficiency < 4:
        suggestions.append("Optimize fuel consumption through smoother operation")
    
    # Calculate consistency score (lower variance is better)
    utilization_values = [s['utilization'] for s in sessions]
    utilization_variance = np.var(utilization_values) if len(utilization_values) > 1 else 0
    consistency_score = max(0, 100 - utilization_variance)
    
    return {
        'operator_id': operator_id,
        'efficiency_score': round(efficiency_score, 1),
        'performance_tier': performance_tier,
        'percentile_rank': percentile,
        'total_sessions': len(sessions),
        'total_runtime_hours': round(total_runtime, 1),
        'idle_ratio_percent': round(idle_ratio, 1),
        'breakdown_ratio_percent': round(breakdown_ratio, 2),
        'avg_utilization_percent': round(avg_utilization, 1),
        'avg_fuel_efficiency': round(avg_fuel_efficiency, 2),
        'consistency_score': round(consistency_score, 1),
        'equipment_types_operated': list(operator_data['equipment_types']),
        'branches_worked': list(operator_data['branches']),
        'improvement_suggestions': suggestions,
        'analysis_date': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    }

@router.get("/charts")
async def generate_predictive_charts(db: Session = Depends(get_db)):
    """Generate predictive health and operator performance charts"""
    try:
        # Get health scores and operator scores
        health_data = await get_predictive_health_scores(7, db)
        operator_data = await get_operator_scores(7, db)
        
        health_scores = health_data['health_scores']
        operator_scores = operator_data['operator_scores']
        
        charts = {}
        
        # 1. Equipment Health Score Distribution
        if health_scores:
            health_df = pd.DataFrame(health_scores)
            
            fig_health_dist = go.Figure(data=[go.Histogram(
                x=health_df['health_score'],
                nbinsx=20,
                marker_color='#4CAF50'
            )])
            
            fig_health_dist.update_layout(
                title='Equipment Health Score Distribution',
                xaxis_title='Health Score',
                yaxis_title='Number of Equipment'
            )
            
            charts['health_distribution'] = json.loads(fig_health_dist.to_json())
            
            # 2. Risk Level Breakdown
            risk_counts = health_df['risk_level'].value_counts()
            risk_colors = {
                'LOW': '#4CAF50',
                'MEDIUM': '#FF9800',
                'HIGH': '#FF5722',
                'CRITICAL': '#B71C1C'
            }
            
            fig_risk = go.Figure(data=[go.Pie(
                labels=risk_counts.index,
                values=risk_counts.values,
                marker_colors=[risk_colors.get(risk, '#9E9E9E') for risk in risk_counts.index],
                hole=0.3
            )])
            
            fig_risk.update_layout(
                title='Equipment Risk Level Distribution',
                annotations=[dict(text=f'{len(health_scores)}<br>Total<br>Equipment', x=0.5, y=0.5, font_size=12, showarrow=False)]
            )
            
            charts['risk_distribution'] = json.loads(fig_risk.to_json())
            
            # 3. Failure Probability Timeline
            health_df_sorted = health_df.sort_values('failure_probability', ascending=False)
            top_10_equipment = health_df_sorted.head(10)
            
            fig_failure = go.Figure(data=[go.Bar(
                x=top_10_equipment['equipment_id'],
                y=top_10_equipment['failure_probability'],
                marker_color='#FF4444'
            )])
            
            fig_failure.update_layout(
                title='Top 10 Equipment by Failure Probability',
                xaxis_title='Equipment ID',
                yaxis_title='Failure Probability',
                xaxis_tickangle=-45
            )
            
            charts['failure_probability'] = json.loads(fig_failure.to_json())
        
        # 4. Operator Performance Leaderboard
        if operator_scores:
            operator_df = pd.DataFrame(operator_scores)
            top_10_operators = operator_df.head(10)
            
            fig_operators = go.Figure(data=[go.Bar(
                x=top_10_operators['operator_id'],
                y=top_10_operators['efficiency_score'],
                marker_color='#2196F3'
            )])
            
            fig_operators.update_layout(
                title='Top 10 Operators by Efficiency Score',
                xaxis_title='Operator ID',
                yaxis_title='Efficiency Score',
                xaxis_tickangle=-45
            )
            
            charts['operator_leaderboard'] = json.loads(fig_operators.to_json())
            
            # 5. Performance Tier Distribution
            tier_counts = operator_df['performance_tier'].value_counts()
            tier_colors = {
                'EXCELLENT': '#4CAF50',
                'GOOD': '#8BC34A',
                'AVERAGE': '#FFC107',
                'BELOW_AVERAGE': '#FF9800',
                'POOR': '#F44336'
            }
            
            fig_tiers = go.Figure(data=[go.Pie(
                labels=tier_counts.index,
                values=tier_counts.values,
                marker_colors=[tier_colors.get(tier, '#9E9E9E') for tier in tier_counts.index]
            )])
            
            fig_tiers.update_layout(
                title='Operator Performance Tier Distribution'
            )
            
            charts['performance_tiers'] = json.loads(fig_tiers.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating predictive charts: {str(e)}")

@router.get("/insights")
async def generate_predictive_insights(db: Session = Depends(get_db)):
    """Generate text insights for predictive health and operator performance"""
    try:
        now = datetime.now(IST)
        
        # Get health and operator data
        health_data = await get_predictive_health_scores(7, db)
        operator_data = await get_operator_scores(7, db)
        
        health_scores = health_data['health_scores']
        operator_scores = operator_data['operator_scores']
        
        insights = []
        
        # Equipment Health Insights
        if health_scores:
            total_equipment = len(health_scores)
            critical_equipment = [h for h in health_scores if h['risk_level'] == 'CRITICAL']
            high_risk_equipment = [h for h in health_scores if h['risk_level'] == 'HIGH']
            
            insights.append(f"🔧 Analyzed {total_equipment} equipment units for predictive maintenance.")
            
            if critical_equipment:
                critical_eq = critical_equipment[0]
                eq_id = critical_eq['equipment_id']
                failure_prob = critical_eq['failure_probability']
                insights.append(f"🚨 CRITICAL: Equipment {eq_id} has {failure_prob:.1%} failure probability - immediate maintenance required.")
            
            if high_risk_equipment:
                insights.append(f"⚠️ {len(high_risk_equipment)} equipment units at HIGH risk - schedule urgent maintenance.")
            
            # Component-specific insights
            all_components = {}
            for equipment in health_scores:
                for component in equipment.get('component_risks', []):
                    comp_name = component['component']
                    if comp_name not in all_components:
                        all_components[comp_name] = []
                    all_components[comp_name].append(component['failure_risk'])
            
            if all_components:
                most_problematic_component = max(all_components, key=lambda x: sum(all_components[x]) / len(all_components[x]))
                avg_risk = sum(all_components[most_problematic_component]) / len(all_components[most_problematic_component])
                insights.append(f"⚙️ {most_problematic_component} systems show highest average failure risk ({avg_risk:.1%}).")
            
            # Age-based insights
            old_equipment = [h for h in health_scores if h['equipment_age'] > 10]
            if old_equipment:
                insights.append(f"📅 {len(old_equipment)} equipment units over 10 years old - consider replacement planning.")
            
            # Maintenance scheduling
            immediate_maintenance = [h for h in health_scores if h['next_maintenance_days'] <= 1]
            if immediate_maintenance:
                insights.append(f"🔧 {len(immediate_maintenance)} equipment units need immediate maintenance within 24 hours.")
        
        # Operator Performance Insights
        if operator_scores:
            total_operators = len(operator_scores)
            excellent_operators = [o for o in operator_scores if o['performance_tier'] == 'EXCELLENT']
            poor_operators = [o for o in operator_scores if o['performance_tier'] == 'POOR']
            
            insights.append(f"👥 Analyzed {total_operators} operators for efficiency scoring.")
            
            if excellent_operators:
                top_operator = excellent_operators[0]
                op_id = top_operator['operator_id']
                score = top_operator['efficiency_score']
                insights.append(f"🏆 Operator {op_id} achieved top efficiency score of {score} (Top 5%).")
            
            if poor_operators:
                insights.append(f"📉 {len(poor_operators)} operators need performance improvement training.")
            
            # Efficiency metrics
            avg_idle_ratio = sum(o['idle_ratio_percent'] for o in operator_scores) / len(operator_scores)
            if avg_idle_ratio > 20:
                insights.append(f"⏱️ Average idle time: {avg_idle_ratio:.1f}% - implement idle reduction training.")
            
            # Breakdown analysis
            high_breakdown_operators = [o for o in operator_scores if o['breakdown_ratio_percent'] > 3]
            if high_breakdown_operators:
                insights.append(f"🔧 {len(high_breakdown_operators)} operators have high breakdown rates - review handling techniques.")
            
            # Fuel efficiency
            fuel_efficient_operators = [o for o in operator_scores if o['avg_fuel_efficiency'] > 5]
            if fuel_efficient_operators:
                best_fuel_op = max(fuel_efficient_operators, key=lambda x: x['avg_fuel_efficiency'])
                insights.append(f"⛽ Operator {best_fuel_op['operator_id']} achieves best fuel efficiency: {best_fuel_op['avg_fuel_efficiency']:.1f} L/h.")
        
        # Cross-analysis insights
        if health_scores and operator_scores:
            # Equipment with both health and operator issues
            critical_equipment_ids = [h['equipment_id'] for h in health_scores if h['risk_level'] in ['CRITICAL', 'HIGH']]
            
            # Simulate correlation (in real implementation, would join on actual operator-equipment assignments)
            if critical_equipment_ids and poor_operators:
                insights.append(f"🔄 Consider reassigning top operators to critical equipment for improved outcomes.")
        
        # Predictive maintenance ROI
        if health_scores:
            potential_failures = len([h for h in health_scores if h['failure_probability'] > 0.7])
            if potential_failures > 0:
                estimated_savings = potential_failures * 50000  # Assume ₹50,000 per prevented failure
                insights.append(f"💰 Preventive maintenance could save ₹{estimated_savings:,} by preventing {potential_failures} potential failures.")
        
        return {
            'status': 'success',
            'insights': insights,
            'summary_stats': {
                'equipment_analyzed': len(health_scores),
                'operators_analyzed': len(operator_scores),
                'critical_equipment': len([h for h in health_scores if h['risk_level'] == 'CRITICAL']),
                'top_operators': len([o for o in operator_scores if o['performance_tier'] == 'EXCELLENT'])
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating predictive insights: {str(e)}")

@router.get("/equipment/{equipment_id}/health")
async def get_equipment_health_detail(equipment_id: str, db: Session = Depends(get_db)):
    """Get detailed health analysis for specific equipment"""
    try:
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get recent usage data
        cutoff_date = datetime.now(IST).date() - timedelta(days=30)
        usage_history = db.query(UsageDaily).filter(
            and_(
                UsageDaily.equipment_id == equipment_id,
                UsageDaily.date >= cutoff_date
            )
        ).order_by(UsageDaily.date).all()
        
        if not usage_history:
            raise HTTPException(status_code=404, detail="No usage data found for equipment")
        
        # Calculate detailed health metrics
        usage_records = []
        for usage in usage_history:
            usage_records.append({
                'date': usage.date,
                'runtime_hours': usage.runtime_hours or 0,
                'idle_hours': usage.idle_hours or 0,
                'fuel_used': usage.fuel_used_liters or 0,
                'fuel_efficiency': usage.fuel_eff_lph or 0,
                'breakdown_hours': usage.breakdown_hours or 0,
                'utilization': usage.utilization_pct or 0
            })
        
        equipment_data = {
            'equipment_id': equipment_id,
            'equipment_type': equipment.equipment_type,
            'make': equipment.make,
            'model': equipment.model,
            'year': equipment.year,
            'branch_name': equipment.branch_name,
            'site_name': equipment.site_name,
            'usage_records': usage_records
        }
        
        # Calculate health score
        health_analysis = await _calculate_health_score(equipment_data)
        
        # Add trend analysis
        df = pd.DataFrame(usage_records)
        trends = {
            'utilization_trend': df['utilization'].diff().mean(),
            'fuel_efficiency_trend': df['fuel_efficiency'].diff().mean(),
            'breakdown_trend': df['breakdown_hours'].diff().mean()
        }
        
        health_analysis['trends'] = trends
        health_analysis['usage_history'] = usage_records
        
        return {
            'status': 'success',
            'equipment_health': health_analysis
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching equipment health detail: {str(e)}")