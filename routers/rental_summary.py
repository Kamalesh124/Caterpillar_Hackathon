from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from database import get_db
from models import Rental, UsageDaily, MasterEquipment, PaymentStatus, Event, EventType
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
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Pydantic models for request/response
class RentalAnalysisRequest(BaseModel):
    days: Optional[int] = 30
    branch_name: Optional[str] = None
    equipment_type: Optional[str] = None
    analysis_type: Optional[str] = "comprehensive"  # comprehensive, financial, operational

class CustomerSegmentationRequest(BaseModel):
    days: Optional[int] = 90
    min_rentals: Optional[int] = 3
    segment_count: Optional[int] = 4

class RevenueOptimizationRequest(BaseModel):
    equipment_type: str
    target_utilization: Optional[float] = 75.0
    price_adjustment_limit: Optional[float] = 0.2  # 20% max adjustment

@router.get("/summary")
async def get_rental_summary(days: int = 14, db: Session = Depends(get_db)):
    """Get rental time and utilization summary for the last N days"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get rental data with equipment info
        rental_query = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Rental.contract_start_ts >= start_date
        ).all()
        
        # Get corresponding usage data
        usage_query = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        # Process rental data
        rental_data = []
        for rental, equipment_type, branch_name, site_name in rental_query:
            # Calculate actual rental duration
            end_time = rental.actual_end_ts or rental.contract_end_ts_planned
            duration_hours = (end_time - rental.contract_start_ts).total_seconds() / 3600
            
            # Calculate revenue per hour
            revenue_per_hour = rental.revenue / duration_hours if duration_hours > 0 else 0
            
            rental_data.append({
                'rental_id': rental.rental_id,
                'equipment_id': rental.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'site_name': site_name,
                'customer_id': rental.customer_id,
                'contract_start': rental.contract_start_ts.strftime('%Y-%m-%d %H:%M'),
                'contract_end_planned': rental.contract_end_ts_planned.strftime('%Y-%m-%d %H:%M'),
                'actual_end': rental.actual_end_ts.strftime('%Y-%m-%d %H:%M') if rental.actual_end_ts else None,
                'duration_hours': round(duration_hours, 1),
                'billed_hours': rental.billed_hours,
                'revenue': rental.revenue,
                'revenue_per_hour': round(revenue_per_hour, 2),
                'payment_status': rental.payment_status.value,
                'late_hours': rental.late_hours,
                'late_fee': rental.late_fee,
                'is_overdue': rental.late_hours > 0,
                'utilization_rate': round((rental.billed_hours / duration_hours * 100) if duration_hours > 0 else 0, 1)
            })
        
        # Process usage data for utilization analysis
        usage_data = []
        for usage, equipment_type, branch_name in usage_query:
            total_hours = usage.runtime_hours + usage.idle_hours
            utilization_pct = (usage.runtime_hours / total_hours * 100) if total_hours > 0 else 0
            
            usage_data.append({
                'equipment_id': usage.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'date': usage.date.strftime('%Y-%m-%d'),
                'runtime_hours': usage.runtime_hours,
                'idle_hours': usage.idle_hours,
                'total_hours': total_hours,
                'utilization_pct': round(utilization_pct, 1),
                'breakdown_hours': usage.breakdown_hours,
                'availability_flag': usage.availability_flag
            })
        
        # Save to CSV
        df_rental = pd.DataFrame(rental_data)
        df_usage = pd.DataFrame(usage_data)
        
        csv_path = 'outputs/rental_time_and_utilization_summary.csv'
        os.makedirs('outputs', exist_ok=True)
        
        # Combine data for comprehensive summary
        combined_data = []
        
        # Add rental summary by equipment type
        if rental_data:
            rental_summary = df_rental.groupby('equipment_type').agg({
                'duration_hours': 'sum',
                'revenue': 'sum',
                'late_hours': 'sum',
                'rental_id': 'count'
            }).reset_index()
            rental_summary.columns = ['equipment_type', 'total_rental_hours', 'total_revenue', 'total_late_hours', 'rental_count']
            
            for _, row in rental_summary.iterrows():
                combined_data.append({
                    'metric_type': 'RENTAL_SUMMARY',
                    'equipment_type': row['equipment_type'],
                    'total_rental_hours': row['total_rental_hours'],
                    'total_revenue': row['total_revenue'],
                    'total_late_hours': row['total_late_hours'],
                    'rental_count': row['rental_count'],
                    'avg_revenue_per_hour': row['total_revenue'] / row['total_rental_hours'] if row['total_rental_hours'] > 0 else 0
                })
        
        # Add utilization summary by equipment type
        if usage_data:
            utilization_summary = df_usage.groupby('equipment_type').agg({
                'runtime_hours': 'sum',
                'idle_hours': 'sum',
                'breakdown_hours': 'sum',
                'utilization_pct': 'mean'
            }).reset_index()
            
            for _, row in utilization_summary.iterrows():
                combined_data.append({
                    'metric_type': 'UTILIZATION_SUMMARY',
                    'equipment_type': row['equipment_type'],
                    'total_runtime_hours': row['runtime_hours'],
                    'total_idle_hours': row['idle_hours'],
                    'total_breakdown_hours': row['breakdown_hours'],
                    'avg_utilization_pct': round(row['utilization_pct'], 1)
                })
        
        # Save combined summary
        pd.DataFrame(combined_data).to_csv(csv_path, index=False)
        
        # Calculate overall statistics
        total_rentals = len(rental_data)
        total_revenue = sum(r['revenue'] for r in rental_data)
        total_rental_hours = sum(r['duration_hours'] for r in rental_data)
        overdue_rentals = len([r for r in rental_data if r['is_overdue']])
        total_late_fees = sum(r['late_fee'] for r in rental_data)
        
        avg_utilization = sum(u['utilization_pct'] for u in usage_data) / len(usage_data) if usage_data else 0
        total_breakdown_hours = sum(u['breakdown_hours'] for u in usage_data)
        
        return {
            'status': 'success',
            'rental_data': rental_data,
            'usage_data': usage_data,
            'csv_file': csv_path,
            'summary_stats': {
                'total_rentals': total_rentals,
                'total_revenue': round(total_revenue, 2),
                'total_rental_hours': round(total_rental_hours, 1),
                'avg_revenue_per_hour': round(total_revenue / total_rental_hours, 2) if total_rental_hours > 0 else 0,
                'overdue_rentals': overdue_rentals,
                'overdue_percentage': round((overdue_rentals / total_rentals * 100) if total_rentals > 0 else 0, 1),
                'total_late_fees': round(total_late_fees, 2),
                'avg_utilization_pct': round(avg_utilization, 1),
                'total_breakdown_hours': round(total_breakdown_hours, 1)
            },
            'period': f'Last {days} days'
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating rental summary: {str(e)}")

@router.get("/charts")
async def generate_rental_charts(days: int = 14, db: Session = Depends(get_db)):
    """Generate rental summary charts"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get data for charts
        rental_data = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Rental.contract_start_ts >= start_date
        ).all()
        
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        charts = {}
        
        # 1. Stacked bar chart - Runtime vs Idle vs Breakdown by equipment type
        if usage_data:
            usage_df = pd.DataFrame([
                {
                    'equipment_type': equipment_type,
                    'runtime_hours': usage.runtime_hours,
                    'idle_hours': usage.idle_hours,
                    'breakdown_hours': usage.breakdown_hours
                }
                for usage, equipment_type, _ in usage_data
            ])
            
            usage_summary = usage_df.groupby('equipment_type').sum().reset_index()
            
            fig_stacked = go.Figure(data=[
                go.Bar(name='Runtime', x=usage_summary['equipment_type'], y=usage_summary['runtime_hours'], marker_color='#2E8B57'),
                go.Bar(name='Idle', x=usage_summary['equipment_type'], y=usage_summary['idle_hours'], marker_color='#FFD700'),
                go.Bar(name='Breakdown', x=usage_summary['equipment_type'], y=usage_summary['breakdown_hours'], marker_color='#FF6B6B')
            ])
            
            fig_stacked.update_layout(
                title='Equipment Hours by Type (Runtime vs Idle vs Breakdown)',
                xaxis_title='Equipment Type',
                yaxis_title='Hours',
                barmode='stack'
            )
            
            charts['hours_stacked'] = json.loads(fig_stacked.to_json())
        
        # 2. Line chart - Utilization trend over time
        if usage_data:
            daily_utilization = pd.DataFrame([
                {
                    'date': usage.date.strftime('%Y-%m-%d'),
                    'equipment_type': equipment_type,
                    'utilization_pct': usage.utilization_pct
                }
                for usage, equipment_type, _ in usage_data
            ])
            
            # Calculate daily average utilization by equipment type
            daily_avg = daily_utilization.groupby(['date', 'equipment_type'])['utilization_pct'].mean().reset_index()
            
            fig_line = go.Figure()
            
            for equipment_type in daily_avg['equipment_type'].unique():
                type_data = daily_avg[daily_avg['equipment_type'] == equipment_type]
                fig_line.add_trace(go.Scatter(
                    x=type_data['date'],
                    y=type_data['utilization_pct'],
                    mode='lines+markers',
                    name=equipment_type,
                    line=dict(width=2)
                ))
            
            fig_line.update_layout(
                title='Utilization Trend by Equipment Type',
                xaxis_title='Date',
                yaxis_title='Utilization (%)',
                hovermode='x unified'
            )
            
            charts['utilization_trend'] = json.loads(fig_line.to_json())
        
        # 3. Revenue analysis by branch
        if rental_data:
            revenue_df = pd.DataFrame([
                {
                    'branch_name': branch_name,
                    'equipment_type': equipment_type,
                    'revenue': rental.revenue,
                    'duration_hours': (rental.actual_end_ts or rental.contract_end_ts_planned - rental.contract_start_ts).total_seconds() / 3600
                }
                for rental, equipment_type, branch_name in rental_data
            ])
            
            branch_revenue = revenue_df.groupby('branch_name')['revenue'].sum().sort_values(ascending=False)
            
            fig_revenue = go.Figure(data=[go.Bar(
                x=branch_revenue.index,
                y=branch_revenue.values,
                marker_color='#4CAF50'
            )])
            
            fig_revenue.update_layout(
                title='Total Revenue by Branch',
                xaxis_title='Branch',
                yaxis_title='Revenue (₹)',
                xaxis_tickangle=-45
            )
            
            charts['revenue_by_branch'] = json.loads(fig_revenue.to_json())
        
        # 4. Payment status distribution
        if rental_data:
            payment_status_counts = pd.Series([rental.payment_status.value for rental, _, _ in rental_data]).value_counts()
            
            colors = {'PAID': '#4CAF50', 'DUE': '#FF9800', 'OVERDUE': '#F44336', 'PARTIAL': '#2196F3'}
            
            fig_payment = go.Figure(data=[go.Pie(
                labels=payment_status_counts.index,
                values=payment_status_counts.values,
                marker_colors=[colors.get(status, '#9E9E9E') for status in payment_status_counts.index],
                hole=0.3
            )])
            
            fig_payment.update_layout(
                title='Payment Status Distribution',
                annotations=[dict(text=f'{len(rental_data)}<br>Total<br>Rentals', x=0.5, y=0.5, font_size=12, showarrow=False)]
            )
            
            charts['payment_status'] = json.loads(fig_payment.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")

@router.get("/insights")
async def generate_rental_insights(days: int = 14, db: Session = Depends(get_db)):
    """Generate text insights for rental summary"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get rental and usage data
        rental_data = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Rental.contract_start_ts >= start_date
        ).all()
        
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.equipment_type
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        ).all()
        
        insights = []
        
        if not rental_data and not usage_data:
            return {'insights': ['No rental or usage data available for analysis.']}
        
        # Rental performance insights
        if rental_data:
            total_revenue = sum(rental.revenue for rental, _, _ in rental_data)
            total_rentals = len(rental_data)
            overdue_count = sum(1 for rental, _, _ in rental_data if rental.late_hours > 0)
            
            insights.append(f"💰 Generated ₹{total_revenue:,.0f} revenue from {total_rentals} rentals in the last {days} days.")
            
            if overdue_count > 0:
                overdue_pct = (overdue_count / total_rentals) * 100
                insights.append(f"⚠️ {overdue_count} rentals ({overdue_pct:.1f}%) are overdue - follow up required.")
            
            # Best performing equipment type by revenue
            revenue_by_type = {}
            for rental, equipment_type, _ in rental_data:
                revenue_by_type[equipment_type] = revenue_by_type.get(equipment_type, 0) + rental.revenue
            
            if revenue_by_type:
                best_type = max(revenue_by_type, key=revenue_by_type.get)
                best_revenue = revenue_by_type[best_type]
                insights.append(f"🏆 {best_type} equipment generated highest revenue: ₹{best_revenue:,.0f}.")
        
        # Utilization insights
        if usage_data:
            utilization_by_type = {}
            for usage, equipment_type in usage_data:
                if equipment_type not in utilization_by_type:
                    utilization_by_type[equipment_type] = []
                utilization_by_type[equipment_type].append(usage.utilization_pct)
            
            # Calculate average utilization by type
            avg_utilization_by_type = {
                eq_type: sum(utils) / len(utils)
                for eq_type, utils in utilization_by_type.items()
            }
            
            # Find equipment types with low utilization
            low_utilization_types = [
                (eq_type, avg_util) for eq_type, avg_util in avg_utilization_by_type.items()
                if avg_util < 50
            ]
            
            if low_utilization_types:
                worst_type, worst_util = min(low_utilization_types, key=lambda x: x[1])
                insights.append(f"📉 {worst_type} equipment shows low utilization at {worst_util:.1f}% - consider relocation.")
            
            # Overall utilization
            overall_utilization = sum(
                usage.utilization_pct for usage, _ in usage_data
            ) / len(usage_data)
            
            status = "excellent" if overall_utilization > 70 else "good" if overall_utilization > 50 else "needs improvement"
            insights.append(f"📊 Overall fleet utilization: {overall_utilization:.1f}% - {status}.")
        
        # Breakdown analysis
        if usage_data:
            total_breakdown = sum(usage.breakdown_hours for usage, _ in usage_data)
            if total_breakdown > 0:
                insights.append(f"🔧 Total breakdown time: {total_breakdown:.1f} hours - schedule preventive maintenance.")
        
        # Branch performance (if rental data available)
        if rental_data:
            branch_performance = {}
            for rental, _, branch_name in rental_data:
                if branch_name not in branch_performance:
                    branch_performance[branch_name] = {'revenue': 0, 'count': 0}
                branch_performance[branch_name]['revenue'] += rental.revenue
                branch_performance[branch_name]['count'] += 1
            
            if branch_performance:
                best_branch = max(branch_performance, key=lambda x: branch_performance[x]['revenue'])
                best_branch_revenue = branch_performance[best_branch]['revenue']
                insights.append(f"🌟 {best_branch} branch leads with ₹{best_branch_revenue:,.0f} revenue.")
        
        return {
            'status': 'success',
            'insights': insights,
            'period': f'Last {days} days',
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating insights: {str(e)}")

@router.get("/equipment-performance")
async def get_equipment_performance(days: int = 14, db: Session = Depends(get_db)):
    """Get detailed equipment performance metrics"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=days)
        
        # Get equipment performance data
        performance_query = db.query(
            MasterEquipment.equipment_id,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            func.count(Rental.rental_id).label('rental_count'),
            func.sum(Rental.revenue).label('total_revenue'),
            func.sum(Rental.billed_hours).label('total_billed_hours'),
            func.avg(UsageDaily.utilization_pct).label('avg_utilization'),
            func.sum(UsageDaily.breakdown_hours).label('total_breakdown_hours')
        ).outerjoin(
            Rental, and_(
                Rental.equipment_id == MasterEquipment.equipment_id,
                Rental.contract_start_ts >= start_date
            )
        ).outerjoin(
            UsageDaily, and_(
                UsageDaily.equipment_id == MasterEquipment.equipment_id,
                UsageDaily.date >= start_date
            )
        ).group_by(
            MasterEquipment.equipment_id,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).all()
        
        performance_data = []
        for row in performance_query:
            revenue_per_hour = (row.total_revenue / row.total_billed_hours) if row.total_billed_hours and row.total_billed_hours > 0 else 0
            
            performance_data.append({
                'equipment_id': row.equipment_id,
                'equipment_type': row.equipment_type,
                'branch_name': row.branch_name,
                'rental_count': row.rental_count or 0,
                'total_revenue': round(row.total_revenue or 0, 2),
                'total_billed_hours': round(row.total_billed_hours or 0, 1),
                'revenue_per_hour': round(revenue_per_hour, 2),
                'avg_utilization_pct': round(row.avg_utilization or 0, 1),
                'total_breakdown_hours': round(row.total_breakdown_hours or 0, 1),
                'performance_score': round(
                    (row.avg_utilization or 0) * 0.4 +
                    min(100, (row.rental_count or 0) * 10) * 0.3 +
                    max(0, 100 - (row.total_breakdown_hours or 0) * 5) * 0.3,
                    1
                )
            })
        
        # Sort by performance score
        performance_data.sort(key=lambda x: x['performance_score'], reverse=True)
        
        return {
            'status': 'success',
            'equipment_performance': performance_data,
            'period': f'Last {days} days',
            'top_performers': performance_data[:5],
            'bottom_performers': performance_data[-5:] if len(performance_data) > 5 else []
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching equipment performance: {str(e)}")

@router.post("/advanced-analysis")
async def advanced_rental_analysis(request: RentalAnalysisRequest, db: Session = Depends(get_db)):
    """Generate advanced rental analysis with financial and operational insights"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=request.days)
        
        # Build base query with filters
        query = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.model,
            MasterEquipment.year_manufactured
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Rental.contract_start_ts >= start_date
        )
        
        if request.branch_name:
            query = query.filter(MasterEquipment.branch_name == request.branch_name)
        if request.equipment_type:
            query = query.filter(MasterEquipment.equipment_type == request.equipment_type)
        
        rental_data = query.all()
        
        if not rental_data:
            return {'status': 'error', 'message': 'No rental data found for the specified criteria'}
        
        # Get usage data for the same period
        usage_query = db.query(
            UsageDaily,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            UsageDaily.date >= start_date
        )
        
        if request.branch_name:
            usage_query = usage_query.filter(MasterEquipment.branch_name == request.branch_name)
        if request.equipment_type:
            usage_query = usage_query.filter(MasterEquipment.equipment_type == request.equipment_type)
        
        usage_data = usage_query.all()
        
        analysis_results = {}
        
        # Financial Analysis
        if request.analysis_type in ['comprehensive', 'financial']:
            total_revenue = sum(rental.revenue for rental, _, _, _, _ in rental_data)
            total_rentals = len(rental_data)
            avg_revenue_per_rental = total_revenue / total_rentals if total_rentals > 0 else 0
            
            # Revenue by equipment type
            revenue_by_type = {}
            rental_count_by_type = {}
            for rental, equipment_type, _, _, _ in rental_data:
                revenue_by_type[equipment_type] = revenue_by_type.get(equipment_type, 0) + rental.revenue
                rental_count_by_type[equipment_type] = rental_count_by_type.get(equipment_type, 0) + 1
            
            # Payment analysis
            payment_status_counts = {}
            overdue_revenue = 0
            for rental, _, _, _, _ in rental_data:
                status = rental.payment_status.value
                payment_status_counts[status] = payment_status_counts.get(status, 0) + 1
                if status == 'OVERDUE':
                    overdue_revenue += rental.revenue
            
            analysis_results['financial'] = {
                'total_revenue': round(total_revenue, 2),
                'total_rentals': total_rentals,
                'avg_revenue_per_rental': round(avg_revenue_per_rental, 2),
                'revenue_by_equipment_type': revenue_by_type,
                'rental_count_by_type': rental_count_by_type,
                'payment_status_distribution': payment_status_counts,
                'overdue_revenue': round(overdue_revenue, 2),
                'collection_efficiency': round((total_revenue - overdue_revenue) / total_revenue * 100, 1) if total_revenue > 0 else 0
            }
        
        # Operational Analysis
        if request.analysis_type in ['comprehensive', 'operational']:
            # Calculate operational metrics
            total_billed_hours = sum(rental.billed_hours for rental, _, _, _, _ in rental_data if rental.billed_hours)
            total_late_hours = sum(rental.late_hours for rental, _, _, _, _ in rental_data if rental.late_hours)
            
            # Utilization analysis from usage data
            utilization_metrics = {}
            if usage_data:
                utilization_by_type = {}
                breakdown_by_type = {}
                
                for usage, equipment_type, _ in usage_data:
                    if equipment_type not in utilization_by_type:
                        utilization_by_type[equipment_type] = []
                        breakdown_by_type[equipment_type] = 0
                    
                    utilization_by_type[equipment_type].append(usage.utilization_pct)
                    breakdown_by_type[equipment_type] += usage.breakdown_hours
                
                for eq_type in utilization_by_type:
                    utilization_metrics[eq_type] = {
                        'avg_utilization': round(np.mean(utilization_by_type[eq_type]), 1),
                        'utilization_std': round(np.std(utilization_by_type[eq_type]), 1),
                        'total_breakdown_hours': round(breakdown_by_type[eq_type], 1)
                    }
            
            analysis_results['operational'] = {
                'total_billed_hours': round(total_billed_hours, 1),
                'total_late_hours': round(total_late_hours, 1),
                'on_time_delivery_rate': round((total_rentals - sum(1 for r, _, _, _, _ in rental_data if r.late_hours > 0)) / total_rentals * 100, 1) if total_rentals > 0 else 0,
                'utilization_metrics': utilization_metrics
            }
        
        # Equipment age analysis
        current_year = now.year
        age_analysis = {}
        for rental, equipment_type, _, _, year_manufactured in rental_data:
            age = current_year - (year_manufactured or current_year)
            age_group = 'New (0-2 years)' if age <= 2 else 'Medium (3-5 years)' if age <= 5 else 'Old (6+ years)'
            
            if age_group not in age_analysis:
                age_analysis[age_group] = {'count': 0, 'revenue': 0}
            age_analysis[age_group]['count'] += 1
            age_analysis[age_group]['revenue'] += rental.revenue
        
        # Log the analysis event
        event_log = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.ANALYSIS_GENERATED,
            equipment_id=None,
            description=f"Advanced rental analysis generated for {request.days} days",
            severity="INFO",
            metadata={
                'analysis_type': request.analysis_type,
                'total_rentals': len(rental_data),
                'branch_filter': request.branch_name,
                'equipment_type_filter': request.equipment_type
            }
        )
        db.add(event_log)
        db.commit()
        
        return {
            'status': 'success',
            'analysis_results': analysis_results,
            'equipment_age_analysis': age_analysis,
            'period': f'Last {request.days} days',
            'filters_applied': {
                'branch_name': request.branch_name,
                'equipment_type': request.equipment_type,
                'analysis_type': request.analysis_type
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating advanced analysis: {str(e)}")

@router.post("/customer-segmentation")
async def customer_segmentation_analysis(request: CustomerSegmentationRequest, db: Session = Depends(get_db)):
    """Segment customers based on rental behavior using machine learning"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=request.days)
        
        # Get customer rental data
        customer_data = db.query(
            Rental.customer_name,
            func.count(Rental.rental_id).label('rental_count'),
            func.sum(Rental.revenue).label('total_revenue'),
            func.avg(Rental.revenue).label('avg_revenue_per_rental'),
            func.sum(Rental.billed_hours).label('total_hours'),
            func.sum(Rental.late_hours).label('total_late_hours'),
            func.count(func.distinct(MasterEquipment.equipment_type)).label('equipment_types_used')
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            Rental.contract_start_ts >= start_date,
            Rental.customer_name.isnot(None)
        ).group_by(
            Rental.customer_name
        ).having(
            func.count(Rental.rental_id) >= request.min_rentals
        ).all()
        
        if len(customer_data) < request.segment_count:
            return {
                'status': 'error', 
                'message': f'Insufficient customer data for segmentation. Found {len(customer_data)} customers, need at least {request.segment_count}'
            }
        
        # Prepare data for clustering
        features = []
        customer_names = []
        
        for row in customer_data:
            # Calculate derived metrics
            avg_hours_per_rental = row.total_hours / row.rental_count if row.rental_count > 0 else 0
            late_hour_ratio = row.total_late_hours / row.total_hours if row.total_hours > 0 else 0
            
            features.append([
                row.rental_count,
                row.total_revenue,
                row.avg_revenue_per_rental,
                avg_hours_per_rental,
                late_hour_ratio,
                row.equipment_types_used
            ])
            customer_names.append(row.customer_name)
        
        # Standardize features for clustering
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        # Perform K-means clustering
        kmeans = KMeans(n_clusters=request.segment_count, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(features_scaled)
        
        # Analyze segments
        segments = {}
        for i in range(request.segment_count):
            segment_customers = [customer_names[j] for j, label in enumerate(cluster_labels) if label == i]
            segment_features = [features[j] for j, label in enumerate(cluster_labels) if label == i]
            
            if segment_features:
                avg_features = np.mean(segment_features, axis=0)
                segments[f'Segment_{i+1}'] = {
                    'customer_count': len(segment_customers),
                    'customers': segment_customers[:10],  # Show first 10 customers
                    'characteristics': {
                        'avg_rental_count': round(avg_features[0], 1),
                        'avg_total_revenue': round(avg_features[1], 2),
                        'avg_revenue_per_rental': round(avg_features[2], 2),
                        'avg_hours_per_rental': round(avg_features[3], 1),
                        'avg_late_hour_ratio': round(avg_features[4], 3),
                        'avg_equipment_types_used': round(avg_features[5], 1)
                    }
                }
        
        # Generate segment descriptions based on characteristics
        segment_descriptions = {}
        for segment_name, segment_data in segments.items():
            chars = segment_data['characteristics']
            
            # Classify segment based on characteristics
            if chars['avg_total_revenue'] > np.mean([s['characteristics']['avg_total_revenue'] for s in segments.values()]):
                if chars['avg_rental_count'] > np.mean([s['characteristics']['avg_rental_count'] for s in segments.values()]):
                    description = "High-Value Frequent Customers"
                    strategy = "Maintain loyalty with premium services and exclusive offers"
                else:
                    description = "High-Value Occasional Customers"
                    strategy = "Increase rental frequency with targeted promotions"
            else:
                if chars['avg_rental_count'] > np.mean([s['characteristics']['avg_rental_count'] for s in segments.values()]):
                    description = "Frequent Low-Value Customers"
                    strategy = "Upsell to higher-value equipment and longer rentals"
                else:
                    description = "Occasional Low-Value Customers"
                    strategy = "Focus on retention and basic service quality"
            
            segment_descriptions[segment_name] = {
                'description': description,
                'strategy': strategy,
                'risk_level': 'High' if chars['avg_late_hour_ratio'] > 0.1 else 'Medium' if chars['avg_late_hour_ratio'] > 0.05 else 'Low'
            }
        
        # Log the segmentation event
        event_log = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.ANALYSIS_GENERATED,
            equipment_id=None,
            description=f"Customer segmentation analysis completed for {len(customer_data)} customers",
            severity="INFO",
            metadata={
                'total_customers': len(customer_data),
                'segments_created': request.segment_count,
                'analysis_period_days': request.days
            }
        )
        db.add(event_log)
        db.commit()
        
        return {
            'status': 'success',
            'segments': segments,
            'segment_descriptions': segment_descriptions,
            'analysis_summary': {
                'total_customers_analyzed': len(customer_data),
                'segments_created': request.segment_count,
                'period': f'Last {request.days} days',
                'min_rentals_threshold': request.min_rentals
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing customer segmentation: {str(e)}")

@router.post("/revenue-optimization")
async def revenue_optimization_analysis(request: RevenueOptimizationRequest, db: Session = Depends(get_db)):
    """Analyze revenue optimization opportunities for specific equipment types"""
    try:
        now = datetime.now(IST)
        start_date = now - timedelta(days=90)  # Use 90 days for better analysis
        
        # Get rental and usage data for the specified equipment type
        rental_data = db.query(
            Rental,
            MasterEquipment.branch_name,
            MasterEquipment.model
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            MasterEquipment.equipment_type == request.equipment_type,
            Rental.contract_start_ts >= start_date
        ).all()
        
        usage_data = db.query(
            UsageDaily,
            MasterEquipment.branch_name,
            MasterEquipment.equipment_id
        ).join(
            MasterEquipment, UsageDaily.equipment_id == MasterEquipment.equipment_id
        ).filter(
            MasterEquipment.equipment_type == request.equipment_type,
            UsageDaily.date >= start_date
        ).all()
        
        if not rental_data and not usage_data:
            return {
                'status': 'error',
                'message': f'No data found for equipment type: {request.equipment_type}'
            }
        
        # Calculate current performance metrics
        current_metrics = {}
        
        if rental_data:
            total_revenue = sum(rental.revenue for rental, _, _ in rental_data)
            total_rentals = len(rental_data)
            avg_revenue_per_rental = total_revenue / total_rentals if total_rentals > 0 else 0
            
            # Calculate revenue per hour
            total_billed_hours = sum(rental.billed_hours for rental, _, _ in rental_data if rental.billed_hours)
            revenue_per_hour = total_revenue / total_billed_hours if total_billed_hours > 0 else 0
            
            current_metrics['rental'] = {
                'total_revenue': round(total_revenue, 2),
                'total_rentals': total_rentals,
                'avg_revenue_per_rental': round(avg_revenue_per_rental, 2),
                'revenue_per_hour': round(revenue_per_hour, 2),
                'total_billed_hours': round(total_billed_hours, 1)
            }
        
        # Calculate utilization metrics
        if usage_data:
            utilization_rates = [usage.utilization_pct for usage, _, _ in usage_data]
            avg_utilization = np.mean(utilization_rates)
            utilization_std = np.std(utilization_rates)
            
            # Group by equipment for individual analysis
            equipment_utilization = {}
            for usage, branch_name, equipment_id in usage_data:
                if equipment_id not in equipment_utilization:
                    equipment_utilization[equipment_id] = {
                        'branch': branch_name,
                        'utilization_rates': [],
                        'breakdown_hours': 0
                    }
                equipment_utilization[equipment_id]['utilization_rates'].append(usage.utilization_pct)
                equipment_utilization[equipment_id]['breakdown_hours'] += usage.breakdown_hours
            
            current_metrics['utilization'] = {
                'avg_utilization': round(avg_utilization, 1),
                'utilization_std': round(utilization_std, 1),
                'target_utilization': request.target_utilization,
                'utilization_gap': round(request.target_utilization - avg_utilization, 1),
                'equipment_count': len(equipment_utilization)
            }
        
        # Generate optimization recommendations
        recommendations = []
        potential_revenue_increase = 0
        
        if 'utilization' in current_metrics:
            utilization_gap = current_metrics['utilization']['utilization_gap']
            
            if utilization_gap > 10:  # Low utilization
                # Recommend price reduction to increase demand
                price_reduction = min(0.15, utilization_gap / 100)  # Max 15% reduction
                if price_reduction <= request.price_adjustment_limit:
                    recommendations.append({
                        'type': 'PRICE_REDUCTION',
                        'description': f'Reduce rental rates by {price_reduction*100:.1f}% to increase demand',
                        'expected_utilization_increase': round(price_reduction * 50, 1),  # Estimated elasticity
                        'price_adjustment': -price_reduction,
                        'priority': 'HIGH'
                    })
            
            elif utilization_gap < -5:  # High utilization (above target)
                # Recommend price increase
                price_increase = min(request.price_adjustment_limit, abs(utilization_gap) / 100)
                recommendations.append({
                    'type': 'PRICE_INCREASE',
                    'description': f'Increase rental rates by {price_increase*100:.1f}% due to high demand',
                    'expected_revenue_increase': round(price_increase * current_metrics['rental']['total_revenue'], 2) if 'rental' in current_metrics else 0,
                    'price_adjustment': price_increase,
                    'priority': 'MEDIUM'
                })
                potential_revenue_increase += price_increase * current_metrics['rental']['total_revenue'] if 'rental' in current_metrics else 0
        
        # Branch-specific recommendations
        branch_recommendations = {}
        if usage_data:
            branch_utilization = {}
            for usage, branch_name, _ in usage_data:
                if branch_name not in branch_utilization:
                    branch_utilization[branch_name] = []
                branch_utilization[branch_name].append(usage.utilization_pct)
            
            for branch, utilization_rates in branch_utilization.items():
                avg_branch_utilization = np.mean(utilization_rates)
                
                if avg_branch_utilization < 40:
                    branch_recommendations[branch] = {
                        'action': 'RELOCATE_OR_REDUCE_FLEET',
                        'current_utilization': round(avg_branch_utilization, 1),
                        'recommendation': 'Consider relocating equipment to higher-demand branches'
                    }
                elif avg_branch_utilization > 85:
                    branch_recommendations[branch] = {
                        'action': 'EXPAND_FLEET',
                        'current_utilization': round(avg_branch_utilization, 1),
                        'recommendation': 'Consider adding more equipment to meet demand'
                    }
        
        # Calculate potential impact
        impact_analysis = {
            'potential_revenue_increase': round(potential_revenue_increase, 2),
            'utilization_improvement_potential': max(0, current_metrics['utilization']['utilization_gap']) if 'utilization' in current_metrics else 0,
            'optimization_score': 0
        }
        
        # Calculate optimization score (0-100)
        if 'utilization' in current_metrics and 'rental' in current_metrics:
            utilization_score = min(100, current_metrics['utilization']['avg_utilization'])
            revenue_efficiency = min(100, current_metrics['rental']['revenue_per_hour'] / 100)  # Assuming ₹100/hour as baseline
            impact_analysis['optimization_score'] = round((utilization_score + revenue_efficiency) / 2, 1)
        
        # Log the optimization analysis event
        event_log = Event(
            event_id=str(uuid.uuid4()),
            event_type=EventType.ANALYSIS_GENERATED,
            equipment_id=None,
            description=f"Revenue optimization analysis for {request.equipment_type}",
            severity="INFO",
            metadata={
                'equipment_type': request.equipment_type,
                'recommendations_count': len(recommendations),
                'potential_revenue_increase': potential_revenue_increase
            }
        )
        db.add(event_log)
        db.commit()
        
        return {
            'status': 'success',
            'equipment_type': request.equipment_type,
            'current_metrics': current_metrics,
            'recommendations': recommendations,
            'branch_recommendations': branch_recommendations,
            'impact_analysis': impact_analysis,
            'analysis_parameters': {
                'target_utilization': request.target_utilization,
                'max_price_adjustment': request.price_adjustment_limit,
                'analysis_period': '90 days'
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing revenue optimization analysis: {str(e)}")