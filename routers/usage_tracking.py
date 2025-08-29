from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from database import get_db
from models import UsageDaily, Rental, MasterEquipment, Event, EventType
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import uuid
from scipy import stats

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Pydantic models for request bodies
class PerformanceBenchmarkRequest(BaseModel):
    equipment_type: Optional[str] = None
    branch_id: Optional[str] = None
    benchmark_period_days: int = 30
    comparison_metric: str = "utilization"  # utilization, fuel_efficiency, runtime

class MaintenanceAlertRequest(BaseModel):
    equipment_id: str
    alert_type: str  # SCHEDULED, PREDICTIVE, BREAKDOWN
    priority: str = "MEDIUM"  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    estimated_hours: Optional[float] = None

@router.get("/per-site")
async def get_usage_per_site(days: int = 3, db: Session = Depends(get_db)):
    """Get usage data per equipment per site for last N days"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get usage data with equipment and rental info
        usage_query = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.branch_name,
            MasterEquipment.equipment_type,
            MasterEquipment.customer_name,
            Rental.rental_id
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).outerjoin(
            Rental, and_(
                Rental.equipment_id == UsageDaily.equipment_id,
                Rental.contract_start_ts <= UsageDaily.date,
                Rental.contract_end_ts_planned >= UsageDaily.date
            )
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        if not usage_query:
            raise HTTPException(status_code=404, detail="No usage data found")
        
        # Process data
        usage_data = []
        for usage, site_name, branch_name, equipment_type, customer_name, rental_id in usage_query:
            total_hours = usage.runtime_hours + usage.idle_hours
            utilization = (usage.runtime_hours / total_hours * 100) if total_hours > 0 else 0
            
            usage_data.append({
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'branch_name': branch_name,
                'equipment_type': equipment_type,
                'date': usage.date.strftime('%Y-%m-%d'),
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'total_hours': total_hours,
                'utilization_pct': round(utilization, 1),
                'distance_km': usage.distance_km,
                'fuel_used_liters': usage.fuel_used_liters,
                'fuel_efficiency_lph': usage.fuel_eff_lph,
                'breakdown_hours': usage.breakdown_hours,
                'gps_lat': usage.last_gps_lat,
                'gps_lon': usage.last_gps_lon,
                'rental_id': rental_id,
                'customer_name': customer_name,
                'rental_status': 'ACTIVE' if rental_id else 'IDLE'
            })
        
        # Save to CSV
        df_usage = pd.DataFrame(usage_data)
        csv_path = f'outputs/usage_per_site_last{days}days.csv'
        os.makedirs('outputs', exist_ok=True)
        df_usage.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        summary_stats = {
            'total_equipment': len(df_usage['equipment_id'].unique()),
            'total_sites': len(df_usage['site_name'].unique()),
            'avg_utilization': round(df_usage['utilization_pct'].mean(), 1),
            'total_runtime': round(df_usage['runtime_hours'].sum(), 1),
            'total_idle': round(df_usage['idle_hours'].sum(), 1),
            'total_fuel': round(df_usage['fuel_used_liters'].sum(), 1),
            'active_rentals': len(df_usage[df_usage['rental_status'] == 'ACTIVE'])
        }
        
        return {
            'status': 'success',
            'usage_data': usage_data,
            'csv_file': csv_path,
            'summary': summary_stats,
            'period': f'Last {days} days'
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching usage data: {str(e)}")

@router.get("/charts")
async def generate_usage_charts(days: int = 3, db: Session = Depends(get_db)):
    """Generate usage tracking charts"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get usage data
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.branch_name,
            MasterEquipment.equipment_type
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        if not usage_data:
            raise HTTPException(status_code=404, detail="No usage data found")
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'branch_name': branch_name,
                'equipment_type': equipment_type,
                'date': usage.date.strftime('%Y-%m-%d'),
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'fuel_used_liters': usage.fuel_used_liters,
                'gps_lat': usage.last_gps_lat,
                'gps_lon': usage.last_gps_lon
            }
            for usage, site_name, branch_name, equipment_type in usage_data
        ])
        
        charts = {}
        
        # 1. Pie chart - Runtime vs Idle (aggregated)
        total_runtime = df['runtime_hours'].sum()
        total_idle = df['idle_hours'].sum()
        
        fig_pie = go.Figure(data=[go.Pie(
            labels=['Runtime', 'Idle'],
            values=[total_runtime, total_idle],
            hole=0.3,
            marker_colors=['#2E8B57', '#FF6B6B']
        )])
        
        fig_pie.update_layout(
            title=f'Overall Runtime vs Idle Hours (Last {days} Days)',
            annotations=[dict(text=f'{total_runtime + total_idle:.1f}h<br>Total', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        charts['runtime_idle_pie'] = json.loads(fig_pie.to_json())
        
        # 2. Bar chart - Fuel consumption per site
        fuel_by_site = df.groupby('site_name')['fuel_used_liters'].sum().sort_values(ascending=False).head(10)
        
        fig_fuel_bar = go.Figure(data=[go.Bar(
            x=fuel_by_site.index,
            y=fuel_by_site.values,
            marker_color='#4CAF50'
        )])
        
        fig_fuel_bar.update_layout(
            title='Fuel Consumption by Site (Top 10)',
            xaxis_title='Site',
            yaxis_title='Fuel Used (Liters)',
            xaxis_tickangle=-45
        )
        
        charts['fuel_by_site'] = json.loads(fig_fuel_bar.to_json())
        
        # 3. GPS Map - Equipment locations
        # Filter out invalid GPS coordinates
        gps_data = df[(df['gps_lat'].notna()) & (df['gps_lon'].notna()) & 
                     (df['gps_lat'] != 0) & (df['gps_lon'] != 0)].copy()
        
        if len(gps_data) > 0:
            # Get latest position for each equipment
            latest_positions = gps_data.sort_values('date').groupby('equipment_id').tail(1)
            
            fig_map = go.Figure(data=go.Scattermapbox(
                lat=latest_positions['gps_lat'],
                lon=latest_positions['gps_lon'],
                mode='markers',
                marker=dict(
                    size=10,
                    color=latest_positions['equipment_type'].astype('category').cat.codes,
                    colorscale='Viridis',
                    showscale=True
                ),
                text=latest_positions.apply(
                    lambda x: f"{x['equipment_id']}<br>{x['site_name']}<br>{x['equipment_type']}", axis=1
                ),
                hovertemplate='<b>%{text}</b><br>Lat: %{lat}<br>Lon: %{lon}<extra></extra>'
            ))
            
            fig_map.update_layout(
                mapbox=dict(
                    style="open-street-map",
                    center=dict(
                        lat=latest_positions['gps_lat'].mean(),
                        lon=latest_positions['gps_lon'].mean()
                    ),
                    zoom=8
                ),
                title='Equipment GPS Locations (Latest Positions)',
                height=500
            )
            
            charts['gps_map'] = json.loads(fig_map.to_json())
        
        # 4. Utilization heatmap by equipment and date
        df['total_hours'] = df['runtime_hours'] + df['idle_hours']
        df['utilization_pct'] = (df['runtime_hours'] / df['total_hours'] * 100).fillna(0)
        
        # Create pivot table for heatmap
        utilization_pivot = df.pivot_table(
            values='utilization_pct',
            index='equipment_id',
            columns='date',
            aggfunc='mean',
            fill_value=0
        )
        
        # Limit to top 20 equipment for readability
        if len(utilization_pivot) > 20:
            avg_utilization = utilization_pivot.mean(axis=1).sort_values(ascending=False)
            utilization_pivot = utilization_pivot.loc[avg_utilization.head(20).index]
        
        fig_heatmap = go.Figure(data=go.Heatmap(
            z=utilization_pivot.values,
            x=utilization_pivot.columns,
            y=utilization_pivot.index,
            colorscale='RdYlGn',
            zmin=0,
            zmax=100,
            text=utilization_pivot.values.round(1),
            texttemplate="%{text}%",
            textfont={"size": 8}
        ))
        
        fig_heatmap.update_layout(
            title='Equipment Utilization Heatmap (%)',
            xaxis_title='Date',
            yaxis_title='Equipment ID',
            height=600
        )
        
        charts['utilization_heatmap'] = json.loads(fig_heatmap.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")

@router.get("/insights")
async def generate_usage_insights(days: int = 3, db: Session = Depends(get_db)):
    """Generate text insights for usage tracking"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get usage data with equipment info
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.equipment_type
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        if not usage_data:
            return {'insights': ['No usage data available for analysis.']}
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame([
            {
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'equipment_type': equipment_type,
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'fuel_used_liters': usage.fuel_used_liters,
                'utilization_pct': usage.utilization_pct,
                'breakdown_hours': usage.breakdown_hours
            }
            for usage, site_name, equipment_type in usage_data
        ])
        
        insights = []
        
        # Equipment with highest idle time
        df['total_hours'] = df['runtime_hours'] + df['idle_hours']
        df['idle_percentage'] = (df['idle_hours'] / df['total_hours'] * 100).fillna(0)
        
        high_idle_equipment = df[df['idle_percentage'] > 60]
        if len(high_idle_equipment) > 0:
            worst_idle = high_idle_equipment.loc[high_idle_equipment['idle_percentage'].idxmax()]
            insights.append(
                f"⚠️ {worst_idle['equipment_id']} idled {worst_idle['idle_percentage']:.0f}% of active time at {worst_idle['site_name']}."
            )
        
        # Site with highest fuel consumption
        fuel_by_site = df.groupby('site_name')['fuel_used_liters'].sum().sort_values(ascending=False)
        if len(fuel_by_site) > 0:
            top_fuel_site = fuel_by_site.index[0]
            top_fuel_amount = fuel_by_site.iloc[0]
            insights.append(
                f"⛽ {top_fuel_site} consumed {top_fuel_amount:.1f}L fuel in the last {days} days."
            )
        
        # Equipment type with best utilization
        utilization_by_type = df.groupby('equipment_type')['utilization_pct'].mean().sort_values(ascending=False)
        if len(utilization_by_type) > 0:
            best_type = utilization_by_type.index[0]
            best_utilization = utilization_by_type.iloc[0]
            insights.append(
                f"🏆 {best_type} equipment shows best utilization at {best_utilization:.1f}% average."
            )
        
        # Breakdown analysis
        total_breakdown = df['breakdown_hours'].sum()
        if total_breakdown > 0:
            equipment_with_breakdown = df[df['breakdown_hours'] > 0]
            breakdown_count = len(equipment_with_breakdown)
            insights.append(
                f"🔧 {breakdown_count} equipment experienced {total_breakdown:.1f} hours of breakdown time."
            )
        
        # Fuel efficiency analysis
        df_with_fuel = df[df['fuel_used_liters'] > 0]
        if len(df_with_fuel) > 0:
            avg_fuel_per_hour = (df_with_fuel['fuel_used_liters'] / df_with_fuel['runtime_hours']).mean()
            insights.append(
                f"📊 Average fuel consumption: {avg_fuel_per_hour:.2f} L/hour across active equipment."
            )
        
        # Low utilization warning
        low_utilization = df[df['utilization_pct'] < 30]
        if len(low_utilization) > 0:
            low_util_count = len(low_utilization['equipment_id'].unique())
            insights.append(
                f"📉 {low_util_count} equipment showing low utilization (<30%) - consider reallocation."
            )
        
        return {
            'status': 'success',
            'insights': insights,
            'period': f'Last {days} days',
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating insights: {str(e)}")

@router.get("/equipment/{equipment_id}")
async def get_equipment_usage_detail(equipment_id: str, days: int = 7, db: Session = Depends(get_db)):
    """Get detailed usage information for specific equipment"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get equipment info
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Get usage data
        usage_data = db.query(UsageDaily).filter(
            and_(
                UsageDaily.equipment_id == equipment_id,
                UsageDaily.date >= start_date
            )
        ).order_by(UsageDaily.date.desc()).all()
        
        # Get active rental if any
        active_rental = db.query(Rental).filter(
            and_(
                Rental.equipment_id == equipment_id,
                Rental.contract_start_ts <= now,
                Rental.contract_end_ts_planned >= now
            )
        ).first()
        
        usage_records = [
            {
                'date': usage.date.strftime('%Y-%m-%d'),
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'utilization_pct': usage.utilization_pct,
                'fuel_used_liters': usage.fuel_used_liters,
                'distance_km': usage.distance_km,
                'breakdown_hours': usage.breakdown_hours,
                'gps_lat': usage.last_gps_lat,
                'gps_lon': usage.last_gps_lon
            }
            for usage in usage_data
        ]
        
        # Calculate summary statistics
        if usage_records:
            total_runtime = sum(r['runtime_hours'] for r in usage_records)
            total_idle = sum(r['idle_hours'] for r in usage_records)
            avg_utilization = sum(r['utilization_pct'] for r in usage_records) / len(usage_records)
            total_fuel = sum(r['fuel_used_liters'] for r in usage_records)
            total_distance = sum(r['distance_km'] for r in usage_records)
        else:
            total_runtime = total_idle = avg_utilization = total_fuel = total_distance = 0
        
        return {
            'status': 'success',
            'equipment_info': {
                'equipment_id': equipment.equipment_id,
                'equipment_type': equipment.equipment_type,
                'make': equipment.make,
                'model': equipment.model,
                'site_name': equipment.site_name,
                'branch_name': equipment.branch_name,
                'status': equipment.status.value
            },
            'active_rental': {
                'rental_id': active_rental.rental_id if active_rental else None,
                'customer_name': active_rental.customer_id if active_rental else None,
                'contract_end': active_rental.contract_end_ts_planned.isoformat() if active_rental else None
            } if active_rental else None,
            'usage_records': usage_records,
            'summary': {
                'total_runtime_hours': round(total_runtime, 1),
                'total_idle_hours': round(total_idle, 1),
                'average_utilization_pct': round(avg_utilization, 1),
                'total_fuel_liters': round(total_fuel, 1),
                'total_distance_km': round(total_distance, 1),
                'days_analyzed': len(usage_records)
            },
            'period': f'Last {days} days'
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching equipment details: {str(e)}")

@router.post("/performance-benchmark")
async def generate_performance_benchmark(request: PerformanceBenchmarkRequest, db: Session = Depends(get_db)):
    """Generate performance benchmarks and identify top/bottom performers"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=request.benchmark_period_days)
        
        # Get usage data with equipment info
        query = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.branch_name,
            MasterEquipment.equipment_type,
            MasterEquipment.make,
            MasterEquipment.model
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        )
        
        if request.equipment_type:
            query = query.filter(MasterEquipment.equipment_type == request.equipment_type)
        if request.branch_id:
            query = query.filter(MasterEquipment.branch_name == request.branch_id)
            
        usage_data = query.all()
        
        if len(usage_data) < 10:
            raise HTTPException(status_code=400, detail="Insufficient data for benchmarking")
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'branch_name': branch_name,
                'equipment_type': equipment_type,
                'make': make,
                'model': model,
                'date': usage.date,
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'fuel_used_liters': usage.fuel_used_liters,
                'fuel_eff_lph': usage.fuel_eff_lph,
                'utilization_pct': usage.utilization_pct,
                'breakdown_hours': usage.breakdown_hours
            }
            for usage, site_name, branch_name, equipment_type, make, model in usage_data
        ])
        
        # Calculate performance metrics by equipment
        equipment_performance = df.groupby('equipment_id').agg({
            'runtime_hours': 'sum',
            'idle_hours': 'sum',
            'fuel_used_liters': 'sum',
            'fuel_eff_lph': 'mean',
            'utilization_pct': 'mean',
            'breakdown_hours': 'sum',
            'site_name': 'first',
            'branch_name': 'first',
            'equipment_type': 'first',
            'make': 'first',
            'model': 'first'
        }).reset_index()
        
        # Calculate total hours and efficiency metrics
        equipment_performance['total_hours'] = equipment_performance['runtime_hours'] + equipment_performance['idle_hours']
        equipment_performance['fuel_per_runtime_hour'] = equipment_performance['fuel_used_liters'] / equipment_performance['runtime_hours']
        equipment_performance['fuel_per_runtime_hour'] = equipment_performance['fuel_per_runtime_hour'].fillna(0)
        
        # Determine benchmark metric
        if request.comparison_metric == "utilization":
            metric_col = 'utilization_pct'
            higher_is_better = True
        elif request.comparison_metric == "fuel_efficiency":
            metric_col = 'fuel_per_runtime_hour'
            higher_is_better = False  # Lower fuel consumption is better
        else:  # runtime
            metric_col = 'runtime_hours'
            higher_is_better = True
        
        # Calculate percentiles and rankings
        equipment_performance['metric_value'] = equipment_performance[metric_col]
        equipment_performance['percentile'] = equipment_performance['metric_value'].rank(pct=True) * 100
        
        if not higher_is_better:
            equipment_performance['percentile'] = 100 - equipment_performance['percentile']
        
        # Classify performance tiers
        def get_performance_tier(percentile):
            if percentile >= 90:
                return 'EXCELLENT'
            elif percentile >= 75:
                return 'GOOD'
            elif percentile >= 50:
                return 'AVERAGE'
            elif percentile >= 25:
                return 'BELOW_AVERAGE'
            else:
                return 'POOR'
        
        equipment_performance['performance_tier'] = equipment_performance['percentile'].apply(get_performance_tier)
        
        # Get top and bottom performers
        top_performers = equipment_performance.nlargest(5, 'percentile')
        bottom_performers = equipment_performance.nsmallest(5, 'percentile')
        
        # Calculate industry benchmarks by equipment type
        type_benchmarks = equipment_performance.groupby('equipment_type').agg({
            'utilization_pct': ['mean', 'median', 'std'],
            'fuel_per_runtime_hour': ['mean', 'median', 'std'],
            'runtime_hours': ['mean', 'median', 'std']
        }).round(2)
        
        # Flatten column names
        type_benchmarks.columns = ['_'.join(col).strip() for col in type_benchmarks.columns]
        type_benchmarks = type_benchmarks.reset_index()
        
        # Log the benchmark generation
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.SYSTEM_ALERT,
            timestamp=now,
            description=f"Performance benchmark generated for {len(equipment_performance)} equipment units",
            metadata={
                'benchmark_period_days': request.benchmark_period_days,
                'comparison_metric': request.comparison_metric,
                'equipment_count': len(equipment_performance),
                'equipment_type_filter': request.equipment_type,
                'branch_filter': request.branch_id
            }
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'benchmark_summary': {
                'metric': request.comparison_metric,
                'period_days': request.benchmark_period_days,
                'total_equipment': len(equipment_performance),
                'excellent_count': len(equipment_performance[equipment_performance['performance_tier'] == 'EXCELLENT']),
                'poor_count': len(equipment_performance[equipment_performance['performance_tier'] == 'POOR'])
            },
            'top_performers': top_performers[['equipment_id', 'site_name', 'equipment_type', 'metric_value', 'percentile', 'performance_tier']].to_dict('records'),
            'bottom_performers': bottom_performers[['equipment_id', 'site_name', 'equipment_type', 'metric_value', 'percentile', 'performance_tier']].to_dict('records'),
            'type_benchmarks': type_benchmarks.to_dict('records'),
            'all_equipment_performance': equipment_performance[['equipment_id', 'site_name', 'equipment_type', 'metric_value', 'percentile', 'performance_tier']].to_dict('records')
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating performance benchmark: {str(e)}")

@router.get("/predictive-maintenance")
async def predict_maintenance_needs(db: Session = Depends(get_db)):
    """Predict equipment maintenance needs based on usage patterns"""
    try:
        now = datetime.now(IST)
        
        # Get recent usage data (last 60 days for trend analysis)
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.equipment_type,
            MasterEquipment.make,
            MasterEquipment.model
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= now - timedelta(days=60)
        ).all()
        
        if len(usage_data) < 30:
            return {'maintenance_alerts': [], 'message': 'Insufficient data for predictive analysis'}
        
        df = pd.DataFrame([
            {
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'equipment_type': equipment_type,
                'make': make,
                'model': model,
                'date': usage.date,
                'runtime_hours': usage.runtime_hours,
                'fuel_eff_lph': usage.fuel_eff_lph,
                'breakdown_hours': usage.breakdown_hours,
                'utilization_pct': usage.utilization_pct
            }
            for usage, site_name, equipment_type, make, model in usage_data
        ])
        
        maintenance_alerts = []
        
        # Analyze each equipment for maintenance needs
        for equipment_id in df['equipment_id'].unique():
            equipment_data = df[df['equipment_id'] == equipment_id].sort_values('date')
            
            if len(equipment_data) < 14:  # Need at least 2 weeks of data
                continue
                
            equipment_info = equipment_data.iloc[0]
            
            # 1. Detect declining fuel efficiency trend
            if len(equipment_data[equipment_data['fuel_eff_lph'] > 0]) >= 10:
                fuel_eff_data = equipment_data[equipment_data['fuel_eff_lph'] > 0]
                
                # Linear regression on fuel efficiency over time
                X = np.arange(len(fuel_eff_data)).reshape(-1, 1)
                y = fuel_eff_data['fuel_eff_lph'].values
                
                if len(np.unique(y)) > 1:
                    model = LinearRegression().fit(X, y)
                    slope = model.coef_[0]
                    
                    # If fuel efficiency is declining (increasing consumption)
                    if slope > 0.1:  # Significant increase in fuel consumption
                        recent_avg = fuel_eff_data.tail(5)['fuel_eff_lph'].mean()
                        older_avg = fuel_eff_data.head(5)['fuel_eff_lph'].mean()
                        
                        if older_avg > 0:
                            efficiency_decline = ((recent_avg - older_avg) / older_avg) * 100
                            
                            if efficiency_decline > 15:  # 15% decline in efficiency
                                maintenance_alerts.append({
                                    'alert_id': str(uuid.uuid4()),
                                    'equipment_id': equipment_id,
                                    'site_name': equipment_info['site_name'],
                                    'equipment_type': equipment_info['equipment_type'],
                                    'alert_type': 'FUEL_EFFICIENCY_DECLINE',
                                    'priority': 'MEDIUM',
                                    'description': f'Fuel efficiency declined by {efficiency_decline:.1f}% - engine maintenance recommended',
                                    'metric_value': efficiency_decline,
                                    'recommendation': 'Schedule engine inspection and tune-up'
                                })
            
            # 2. Detect excessive breakdown hours
            recent_breakdowns = equipment_data.tail(14)['breakdown_hours'].sum()
            if recent_breakdowns > 8:  # More than 8 hours of breakdown in 2 weeks
                maintenance_alerts.append({
                    'alert_id': str(uuid.uuid4()),
                    'equipment_id': equipment_id,
                    'site_name': equipment_info['site_name'],
                    'equipment_type': equipment_info['equipment_type'],
                    'alert_type': 'EXCESSIVE_BREAKDOWNS',
                    'priority': 'HIGH',
                    'description': f'{recent_breakdowns:.1f} hours of breakdown time in last 14 days',
                    'metric_value': recent_breakdowns,
                    'recommendation': 'Immediate comprehensive inspection required'
                })
            
            # 3. Detect declining utilization (potential mechanical issues)
            if len(equipment_data) >= 21:
                recent_utilization = equipment_data.tail(7)['utilization_pct'].mean()
                older_utilization = equipment_data.head(14)['utilization_pct'].mean()
                
                if older_utilization > 30 and recent_utilization < older_utilization * 0.7:  # 30% drop in utilization
                    utilization_drop = older_utilization - recent_utilization
                    
                    maintenance_alerts.append({
                        'alert_id': str(uuid.uuid4()),
                        'equipment_id': equipment_id,
                        'site_name': equipment_info['site_name'],
                        'equipment_type': equipment_info['equipment_type'],
                        'alert_type': 'UTILIZATION_DECLINE',
                        'priority': 'MEDIUM',
                        'description': f'Utilization dropped by {utilization_drop:.1f}% - potential mechanical issues',
                        'metric_value': utilization_drop,
                        'recommendation': 'Check for mechanical issues affecting performance'
                    })
            
            # 4. High runtime hours (scheduled maintenance)
            total_runtime = equipment_data['runtime_hours'].sum()
            if total_runtime > 200:  # More than 200 hours in 60 days
                maintenance_alerts.append({
                    'alert_id': str(uuid.uuid4()),
                    'equipment_id': equipment_id,
                    'site_name': equipment_info['site_name'],
                    'equipment_type': equipment_info['equipment_type'],
                    'alert_type': 'SCHEDULED_MAINTENANCE',
                    'priority': 'LOW',
                    'description': f'{total_runtime:.1f} runtime hours in 60 days - scheduled maintenance due',
                    'metric_value': total_runtime,
                    'recommendation': 'Schedule routine maintenance service'
                })
        
        # Sort alerts by priority
        priority_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        maintenance_alerts.sort(key=lambda x: priority_order.get(x['priority'], 4))
        
        # Log predictive maintenance analysis
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.SYSTEM_ALERT,
            timestamp=now,
            description=f"Predictive maintenance analysis completed - {len(maintenance_alerts)} alerts generated",
            metadata={
                'alerts_count': len(maintenance_alerts),
                'high_priority_count': len([a for a in maintenance_alerts if a['priority'] == 'HIGH']),
                'analysis_period_days': 60
            }
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'maintenance_alerts': maintenance_alerts,
            'summary': {
                'total_alerts': len(maintenance_alerts),
                'critical_count': len([a for a in maintenance_alerts if a['priority'] == 'CRITICAL']),
                'high_count': len([a for a in maintenance_alerts if a['priority'] == 'HIGH']),
                'medium_count': len([a for a in maintenance_alerts if a['priority'] == 'MEDIUM']),
                'low_count': len([a for a in maintenance_alerts if a['priority'] == 'LOW'])
            },
            'analysis_date': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in predictive maintenance analysis: {str(e)}")

@router.post("/maintenance-alert")
async def create_maintenance_alert(alert: MaintenanceAlertRequest, db: Session = Depends(get_db)):
    """Create a manual maintenance alert for equipment"""
    try:
        now = datetime.now(IST)
        
        # Verify equipment exists
        equipment = db.query(MasterEquipment).filter(
            MasterEquipment.equipment_id == alert.equipment_id
        ).first()
        
        if not equipment:
            raise HTTPException(status_code=404, detail="Equipment not found")
        
        # Log the maintenance alert
        log_entry = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.MAINTENANCE_ALERT,
            timestamp=now,
            description=f"Manual maintenance alert created for {alert.equipment_id}: {alert.description}",
            metadata={
                'equipment_id': alert.equipment_id,
                'alert_type': alert.alert_type,
                'priority': alert.priority,
                'estimated_hours': alert.estimated_hours,
                'site_name': equipment.site_name,
                'equipment_type': equipment.equipment_type
            }
        )
        db.add(log_entry)
        db.commit()
        
        return {
            'status': 'success',
            'alert_id': log_entry.event_id,
            'message': f'Maintenance alert created for {alert.equipment_id}',
            'equipment_info': {
                'equipment_id': equipment.equipment_id,
                'site_name': equipment.site_name,
                'equipment_type': equipment.equipment_type
            },
            'alert_details': alert.dict()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating maintenance alert: {str(e)}")

@router.get("/efficiency-analysis")
async def analyze_fuel_efficiency(days: int = 30, db: Session = Depends(get_db)):
    """Analyze fuel efficiency patterns and identify optimization opportunities"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get usage data with equipment info
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.site_name,
            MasterEquipment.equipment_type,
            MasterEquipment.make,
            MasterEquipment.model
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            and_(
                UsageDaily.date >= start_date,
                UsageDaily.fuel_used_liters > 0,
                UsageDaily.runtime_hours > 0
            )
        ).all()
        
        if len(usage_data) < 10:
            return {'message': 'Insufficient fuel usage data for analysis'}
        
        df = pd.DataFrame([
            {
                'equipment_id': usage.equipment_id,
                'site_name': site_name,
                'equipment_type': equipment_type,
                'make': make,
                'model': model,
                'date': usage.date,
                'runtime_hours': usage.runtime_hours,
                'fuel_used_liters': usage.fuel_used_liters,
                'fuel_eff_lph': usage.fuel_eff_lph
            }
            for usage, site_name, equipment_type, make, model in usage_data
        ])
        
        # Calculate fuel consumption per runtime hour
        df['fuel_per_hour'] = df['fuel_used_liters'] / df['runtime_hours']
        
        # Efficiency analysis by equipment type
        efficiency_by_type = df.groupby('equipment_type').agg({
            'fuel_per_hour': ['mean', 'median', 'std', 'min', 'max'],
            'equipment_id': 'nunique'
        }).round(3)
        
        efficiency_by_type.columns = ['avg_fuel_per_hour', 'median_fuel_per_hour', 'std_fuel_per_hour', 'best_fuel_per_hour', 'worst_fuel_per_hour', 'equipment_count']
        efficiency_by_type = efficiency_by_type.reset_index()
        
        # Identify fuel efficiency outliers using statistical methods
        outliers = []
        for equipment_type in df['equipment_type'].unique():
            type_data = df[df['equipment_type'] == equipment_type]
            
            if len(type_data) >= 10:
                Q1 = type_data['fuel_per_hour'].quantile(0.25)
                Q3 = type_data['fuel_per_hour'].quantile(0.75)
                IQR = Q3 - Q1
                
                # Equipment with fuel consumption > Q3 + 1.5*IQR (poor efficiency)
                poor_efficiency = type_data[type_data['fuel_per_hour'] > Q3 + 1.5 * IQR]
                
                for _, row in poor_efficiency.iterrows():
                    outliers.append({
                        'equipment_id': row['equipment_id'],
                        'site_name': row['site_name'],
                        'equipment_type': row['equipment_type'],
                        'fuel_per_hour': round(row['fuel_per_hour'], 3),
                        'type_median': round(type_data['fuel_per_hour'].median(), 3),
                        'efficiency_score': 'POOR',
                        'potential_savings_pct': round(((row['fuel_per_hour'] - type_data['fuel_per_hour'].median()) / row['fuel_per_hour']) * 100, 1)
                    })
        
        # Site-level efficiency analysis
        site_efficiency = df.groupby('site_name').agg({
            'fuel_per_hour': 'mean',
            'fuel_used_liters': 'sum',
            'runtime_hours': 'sum',
            'equipment_id': 'nunique'
        }).round(3)
        
        site_efficiency['total_fuel_cost_estimate'] = site_efficiency['fuel_used_liters'] * 1.5  # Assume $1.5 per liter
        site_efficiency = site_efficiency.reset_index()
        
        # Calculate potential savings
        total_fuel_used = df['fuel_used_liters'].sum()
        if len(outliers) > 0:
            avg_savings_pct = np.mean([o['potential_savings_pct'] for o in outliers])
            estimated_savings_liters = total_fuel_used * (avg_savings_pct / 100)
            estimated_cost_savings = estimated_savings_liters * 1.5
        else:
            estimated_savings_liters = 0
            estimated_cost_savings = 0
        
        return {
            'status': 'success',
            'analysis_summary': {
                'period_days': days,
                'total_equipment_analyzed': len(df['equipment_id'].unique()),
                'total_fuel_consumed_liters': round(total_fuel_used, 1),
                'avg_fuel_per_hour': round(df['fuel_per_hour'].mean(), 3),
                'poor_efficiency_count': len(outliers),
                'estimated_savings_liters': round(estimated_savings_liters, 1),
                'estimated_cost_savings_usd': round(estimated_cost_savings, 2)
            },
            'efficiency_by_type': efficiency_by_type.to_dict('records'),
            'efficiency_outliers': outliers,
            'site_efficiency': site_efficiency.to_dict('records'),
            'recommendations': [
                f"Focus on {len(outliers)} equipment with poor fuel efficiency" if len(outliers) > 0 else "All equipment operating within normal efficiency ranges",
                f"Potential fuel savings: {estimated_savings_liters:.1f}L (${estimated_cost_savings:.2f})" if estimated_savings_liters > 0 else "No significant savings opportunities identified",
                "Regular maintenance and operator training can improve fuel efficiency by 10-15%"
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in efficiency analysis: {str(e)}")