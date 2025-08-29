from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from database import get_db
from models import Rental, MasterEquipment, PaymentStatus, Event, EventType
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import json
import pytz
import os
import uuid

router = APIRouter()
IST = pytz.timezone('Asia/Kolkata')

# Pydantic models for request bodies
class PaymentUpdateRequest(BaseModel):
    rental_id: str
    payment_status: str
    amount_paid: Optional[float] = None
    payment_method: Optional[str] = None
    notes: Optional[str] = None

class ReminderRequest(BaseModel):
    rental_ids: List[str]
    reminder_type: str = "email"  # email, sms, call
    message: Optional[str] = None
    schedule_datetime: Optional[datetime] = None

class FeeWaiverRequest(BaseModel):
    rental_id: str
    waiver_amount: float
    reason: str
    approved_by: str

@router.get("/overdue")
async def get_overdue_rentals(db: Session = Depends(get_db)):
    """Get all overdue rentals with calculated fees"""
    try:
        now = datetime.now(IST)
        
        # Get all rentals that are potentially overdue
        rentals_query = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name,
            MasterEquipment.site_name,
            MasterEquipment.customer_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            or_(
                # Rentals that have ended but are overdue
                and_(
                    Rental.actual_end_ts.isnot(None),
                    Rental.actual_end_ts > Rental.contract_end_ts_planned
                ),
                # Active rentals that are past their planned end time
                and_(
                    Rental.actual_end_ts.is_(None),
                    Rental.contract_end_ts_planned < now
                )
            )
        ).all()
        
        overdue_rentals = []
        total_fees = 0
        
        for rental, equipment_type, branch_name, site_name, customer_name in rentals_query:
            # Calculate overdue hours and fees
            overdue_info = await _calculate_overdue_fees(rental, now)
            
            if overdue_info['is_overdue']:
                rental_data = {
                    'rental_id': rental.rental_id,
                    'equipment_id': rental.equipment_id,
                    'equipment_type': equipment_type,
                    'branch_name': branch_name,
                    'site_name': site_name,
                    'customer_id': rental.customer_id,
                    'customer_name': customer_name,
                    'contract_start': rental.contract_start_ts.strftime('%Y-%m-%d %H:%M'),
                    'contract_end_planned': rental.contract_end_ts_planned.strftime('%Y-%m-%d %H:%M'),
                    'actual_end': rental.actual_end_ts.strftime('%Y-%m-%d %H:%M') if rental.actual_end_ts else 'ONGOING',
                    'payment_status': rental.payment_status.value,
                    'rate_per_day': rental.rate_day,
                    'grace_minutes': rental.grace_minutes or 0,
                    **overdue_info
                }
                
                overdue_rentals.append(rental_data)
                total_fees += overdue_info['calculated_late_fee']
        
        # Sort by late fees (highest first)
        overdue_rentals.sort(key=lambda x: x['calculated_late_fee'], reverse=True)
        
        # Save to CSV
        csv_path = 'outputs/overdue_and_fees.csv'
        os.makedirs('outputs', exist_ok=True)
        
        if overdue_rentals:
            df = pd.DataFrame(overdue_rentals)
            df.to_csv(csv_path, index=False)
        
        # Generate summary statistics
        payment_status_counts = {}
        branch_overdue_counts = {}
        
        for rental in overdue_rentals:
            status = rental['payment_status']
            branch = rental['branch_name']
            
            payment_status_counts[status] = payment_status_counts.get(status, 0) + 1
            branch_overdue_counts[branch] = branch_overdue_counts.get(branch, 0) + 1
        
        return {
            'status': 'success',
            'overdue_rentals': overdue_rentals,
            'csv_file': csv_path,
            'summary': {
                'total_overdue_count': len(overdue_rentals),
                'total_estimated_fees': round(total_fees, 2),
                'avg_overdue_hours': round(sum(r['overdue_hours'] for r in overdue_rentals) / len(overdue_rentals), 1) if overdue_rentals else 0,
                'payment_status_breakdown': payment_status_counts,
                'branch_breakdown': branch_overdue_counts,
                'most_problematic_branch': max(branch_overdue_counts.items(), key=lambda x: x[1]) if branch_overdue_counts else None
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching overdue rentals: {str(e)}")

async def _calculate_overdue_fees(rental: Rental, current_time: datetime):
    """Calculate overdue hours and fees for a rental"""
    grace_minutes = rental.grace_minutes or 0
    grace_period = timedelta(minutes=grace_minutes)
    
    # Determine the actual end time
    if rental.actual_end_ts:
        # Rental has ended
        end_time = rental.actual_end_ts
    else:
        # Rental is ongoing, use current time
        end_time = current_time
    
    # Calculate overdue time
    planned_end_with_grace = rental.contract_end_ts_planned + grace_period
    
    if end_time > planned_end_with_grace:
        overdue_duration = end_time - planned_end_with_grace
        overdue_hours = overdue_duration.total_seconds() / 3600
        overdue_days = overdue_hours / 24
        
        # Calculate late fee
        # Standard late fee calculation: 10% of daily rate per day overdue (minimum 1 day)
        late_fee_rate = 0.10  # 10% of daily rate
        days_for_fee = max(1, int(overdue_days) + (1 if overdue_hours % 24 > 0 else 0))  # Round up to next day
        calculated_late_fee = rental.rate_day * late_fee_rate * days_for_fee
        
        # Use existing late fee if available, otherwise use calculated
        final_late_fee = rental.late_fee if rental.late_fee else calculated_late_fee
        
        return {
            'is_overdue': True,
            'overdue_hours': round(overdue_hours, 1),
            'overdue_days': round(overdue_days, 1),
            'calculated_late_fee': round(calculated_late_fee, 2),
            'recorded_late_fee': rental.late_fee or 0,
            'final_late_fee': round(final_late_fee, 2),
            'overdue_since': planned_end_with_grace.strftime('%Y-%m-%d %H:%M'),
            'severity': _get_overdue_severity(overdue_hours)
        }
    else:
        return {
            'is_overdue': False,
            'overdue_hours': 0,
            'overdue_days': 0,
            'calculated_late_fee': 0,
            'recorded_late_fee': rental.late_fee or 0,
            'final_late_fee': rental.late_fee or 0,
            'overdue_since': None,
            'severity': 'NONE'
        }

def _get_overdue_severity(overdue_hours):
    """Determine severity based on overdue hours"""
    if overdue_hours >= 72:  # 3+ days
        return 'CRITICAL'
    elif overdue_hours >= 24:  # 1+ days
        return 'HIGH'
    elif overdue_hours >= 4:  # 4+ hours
        return 'MEDIUM'
    else:
        return 'LOW'

@router.post("/update-fees")
async def update_overdue_fees(db: Session = Depends(get_db)):
    """Update late fees for all overdue rentals"""
    try:
        now = datetime.now(IST)
        updated_count = 0
        total_fees_added = 0
        
        # Get all potentially overdue rentals
        rentals = db.query(Rental).filter(
            or_(
                # Rentals that have ended but are overdue
                and_(
                    Rental.actual_end_ts.isnot(None),
                    Rental.actual_end_ts > Rental.contract_end_ts_planned
                ),
                # Active rentals that are past their planned end time
                and_(
                    Rental.actual_end_ts.is_(None),
                    Rental.contract_end_ts_planned < now
                )
            )
        ).all()
        
        for rental in rentals:
            overdue_info = await _calculate_overdue_fees(rental, now)
            
            if overdue_info['is_overdue']:
                # Update rental with calculated fees
                old_fee = rental.late_fee or 0
                new_fee = overdue_info['calculated_late_fee']
                
                rental.late_hours = overdue_info['overdue_hours']
                rental.late_days = overdue_info['overdue_days']
                rental.late_fee = new_fee
                
                # Update payment status if necessary
                if rental.payment_status == PaymentStatus.PAID and new_fee > old_fee:
                    rental.payment_status = PaymentStatus.DUE
                elif overdue_info['overdue_hours'] >= 72:  # 3+ days overdue
                    rental.payment_status = PaymentStatus.OVERDUE
                
                updated_count += 1
                total_fees_added += (new_fee - old_fee)
        
        db.commit()
        
        return {
            'status': 'success',
            'updated_rentals': updated_count,
            'total_fees_added': round(total_fees_added, 2),
            'timestamp': now.isoformat()
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating overdue fees: {str(e)}")

@router.get("/charts")
async def generate_overdue_charts(db: Session = Depends(get_db)):
    """Generate overdue rental charts"""
    try:
        now = datetime.now(IST)
        
        # Get overdue rentals data
        overdue_data = await get_overdue_rentals(db)
        overdue_rentals = overdue_data['overdue_rentals']
        
        charts = {}
        
        if not overdue_rentals:
            return {'status': 'success', 'charts': {}, 'message': 'No overdue rentals found'}
        
        # 1. Fees by branch
        branch_fees = {}
        for rental in overdue_rentals:
            branch = rental['branch_name']
            branch_fees[branch] = branch_fees.get(branch, 0) + rental['final_late_fee']
        
        fig_fees = go.Figure(data=[go.Bar(
            x=list(branch_fees.keys()),
            y=list(branch_fees.values()),
            marker_color='#FF6B6B'
        )])
        
        fig_fees.update_layout(
            title='Late Fees by Branch',
            xaxis_title='Branch',
            yaxis_title='Late Fees (₹)',
            xaxis_tickangle=-45
        )
        
        charts['fees_by_branch'] = json.loads(fig_fees.to_json())
        
        # 2. Overdue trend (last 30 days)
        # Simulate trend data - in real implementation, would query historical data
        dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(29, -1, -1)]
        overdue_counts = [len(overdue_rentals) + (i % 7 - 3) for i in range(30)]  # Simulated trend
        
        fig_trend = go.Figure(data=[go.Scatter(
            x=dates,
            y=overdue_counts,
            mode='lines+markers',
            line=dict(color='#FF8800', width=3),
            marker=dict(size=6)
        )])
        
        fig_trend.update_layout(
            title='Overdue Rentals Trend (Last 30 Days)',
            xaxis_title='Date',
            yaxis_title='Number of Overdue Rentals',
            xaxis_tickangle=-45
        )
        
        charts['overdue_trend'] = json.loads(fig_trend.to_json())
        
        # 3. On-time vs Overdue distribution
        # Get total rentals for comparison
        total_rentals = db.query(func.count(Rental.rental_id)).scalar()
        on_time_rentals = total_rentals - len(overdue_rentals)
        
        fig_distribution = go.Figure(data=[go.Pie(
            labels=['On Time', 'Overdue'],
            values=[on_time_rentals, len(overdue_rentals)],
            marker_colors=['#4CAF50', '#FF4444'],
            hole=0.3
        )])
        
        fig_distribution.update_layout(
            title='Rental Performance Distribution',
            annotations=[dict(text=f'{total_rentals}<br>Total<br>Rentals', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        charts['performance_distribution'] = json.loads(fig_distribution.to_json())
        
        # 4. Severity distribution
        severity_counts = {}
        for rental in overdue_rentals:
            severity = rental['severity']
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        severity_colors = {
            'CRITICAL': '#B71C1C',
            'HIGH': '#FF4444',
            'MEDIUM': '#FF8800',
            'LOW': '#FFC107'
        }
        
        fig_severity = go.Figure(data=[go.Bar(
            x=list(severity_counts.keys()),
            y=list(severity_counts.values()),
            marker_color=[severity_colors.get(sev, '#9E9E9E') for sev in severity_counts.keys()]
        )])
        
        fig_severity.update_layout(
            title='Overdue Rentals by Severity',
            xaxis_title='Severity Level',
            yaxis_title='Count'
        )
        
        charts['severity_distribution'] = json.loads(fig_severity.to_json())
        
        return {
            'status': 'success',
            'charts': charts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating overdue charts: {str(e)}")

@router.get("/insights")
async def generate_overdue_insights(db: Session = Depends(get_db)):
    """Generate text insights for overdue rentals"""
    try:
        now = datetime.now(IST)
        
        # Get overdue data
        overdue_data = await get_overdue_rentals(db)
        overdue_rentals = overdue_data['overdue_rentals']
        summary = overdue_data['summary']
        
        insights = []
        
        if not overdue_rentals:
            insights.append("✅ No overdue rentals currently - excellent payment compliance!")
            return {'status': 'success', 'insights': insights}
        
        total_count = len(overdue_rentals)
        total_fees = summary['total_estimated_fees']
        
        insights.append(f"💰 {total_count} overdue rentals with ₹{total_fees:,.0f} in estimated late fees.")
        
        # Critical overdue rentals
        critical_rentals = [r for r in overdue_rentals if r['severity'] == 'CRITICAL']
        if critical_rentals:
            insights.append(f"🚨 {len(critical_rentals)} CRITICAL overdue rentals (3+ days) require immediate escalation.")
        
        # Top overdue rentals by fees
        top_3_overdue = overdue_rentals[:3]
        for rental in top_3_overdue:
            customer = rental['customer_name']
            rental_id = rental['rental_id']
            overdue_hours = rental['overdue_hours']
            late_fee = rental['final_late_fee']
            
            insights.append(f"📞 Rental {rental_id} overdue by {overdue_hours:.0f}h. Est. fee ₹{late_fee:,.0f}. Contact {customer}.")
        
        # Branch analysis
        if summary['most_problematic_branch']:
            branch_name, branch_count = summary['most_problematic_branch']
            insights.append(f"🏢 {branch_name} branch has {branch_count} overdue rentals - review local operations.")
        
        # Payment status analysis
        payment_breakdown = summary['payment_status_breakdown']
        if 'OVERDUE' in payment_breakdown:
            overdue_count = payment_breakdown['OVERDUE']
            insights.append(f"⚠️ {overdue_count} rentals marked as OVERDUE status - prioritize collection efforts.")
        
        # Average overdue time
        avg_overdue = summary['avg_overdue_hours']
        if avg_overdue > 48:
            insights.append(f"⏰ Average overdue time: {avg_overdue:.1f} hours - implement earlier intervention.")
        
        # Equipment type analysis
        equipment_overdue = {}
        for rental in overdue_rentals:
            eq_type = rental['equipment_type']
            equipment_overdue[eq_type] = equipment_overdue.get(eq_type, 0) + 1
        
        if equipment_overdue:
            most_overdue_type = max(equipment_overdue, key=equipment_overdue.get)
            count = equipment_overdue[most_overdue_type]
            insights.append(f"🚜 {most_overdue_type} equipment has {count} overdue rentals - review rental terms.")
        
        # Financial impact
        if total_fees > 50000:  # More than ₹50,000
            insights.append(f"💸 High financial impact: ₹{total_fees:,.0f} in potential late fees - urgent collection required.")
        
        # Grace period analysis
        grace_exceeded = [r for r in overdue_rentals if r['overdue_hours'] > (r['grace_minutes'] / 60)]
        if grace_exceeded:
            insights.append(f"⏱️ {len(grace_exceeded)} rentals exceeded grace period - standard late fees apply.")
        
        return {
            'status': 'success',
            'insights': insights,
            'summary_stats': {
                'total_overdue': total_count,
                'total_fees': total_fees,
                'avg_overdue_hours': avg_overdue,
                'critical_count': len(critical_rentals)
            },
            'generated_at': now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating overdue insights: {str(e)}")

@router.get("/customer/{customer_id}")
async def get_customer_overdue_history(customer_id: str, db: Session = Depends(get_db)):
    """Get overdue rental history for a specific customer"""
    try:
        now = datetime.now(IST)
        
        # Get customer info
        customer_equipment = db.query(MasterEquipment).filter(
            MasterEquipment.customer_id == customer_id
        ).first()
        
        if not customer_equipment:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Get all rentals for this customer (including historical overdue)
        customer_rentals = db.query(
            Rental,
            MasterEquipment.equipment_type,
            MasterEquipment.branch_name
        ).join(
            MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
        ).filter(
            MasterEquipment.customer_id == customer_id
        ).order_by(Rental.contract_start_ts.desc()).all()
        
        rental_history = []
        total_late_fees = 0
        overdue_count = 0
        
        for rental, equipment_type, branch_name in customer_rentals:
            overdue_info = await _calculate_overdue_fees(rental, now)
            
            if overdue_info['is_overdue'] or rental.late_hours > 0:
                overdue_count += 1
            
            total_late_fees += rental.late_fee or 0
            
            rental_history.append({
                'rental_id': rental.rental_id,
                'equipment_id': rental.equipment_id,
                'equipment_type': equipment_type,
                'branch_name': branch_name,
                'contract_start': rental.contract_start_ts.strftime('%Y-%m-%d'),
                'contract_end_planned': rental.contract_end_ts_planned.strftime('%Y-%m-%d'),
                'actual_end': rental.actual_end_ts.strftime('%Y-%m-%d') if rental.actual_end_ts else 'ONGOING',
                'payment_status': rental.payment_status.value,
                'late_hours': rental.late_hours or 0,
                'late_fee': rental.late_fee or 0,
                'is_currently_overdue': overdue_info['is_overdue'],
                'revenue': rental.revenue
            })
        
        # Calculate customer reliability score
        total_rentals = len(customer_rentals)
        on_time_rentals = total_rentals - overdue_count
        reliability_score = (on_time_rentals / total_rentals * 100) if total_rentals > 0 else 100
        
        return {
            'status': 'success',
            'customer_info': {
                'customer_id': customer_id,
                'customer_name': customer_equipment.customer_name,
                'reliability_score': round(reliability_score, 1)
            },
            'rental_history': rental_history,
            'summary': {
                'total_rentals': total_rentals,
                'overdue_rentals': overdue_count,
                'total_late_fees': round(total_late_fees, 2),
                'total_revenue': round(sum(r['revenue'] for r in rental_history), 2),
                'on_time_percentage': round(reliability_score, 1)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching customer overdue history: {str(e)}")

@router.post("/payment-update")
async def update_payment_status(request: PaymentUpdateRequest, db: Session = Depends(get_db)):
    """Update payment status for a rental"""
    try:
        rental = db.query(Rental).filter(Rental.rental_id == request.rental_id).first()
        
        if not rental:
            raise HTTPException(status_code=404, detail="Rental not found")
        
        # Update payment status
        old_status = rental.payment_status.value
        rental.payment_status = PaymentStatus(request.payment_status)
        
        # If payment is made, potentially reduce late fees
        if request.payment_status == "PAID" and request.amount_paid:
            if request.amount_paid >= (rental.late_fee or 0):
                rental.late_fee = 0
            else:
                rental.late_fee = (rental.late_fee or 0) - request.amount_paid
        
        rental.updated_at = datetime.now(IST)
        
        # Log the payment update event
        event_log = Event(
            event_id=str(uuid.uuid4()),
            equipment_id=rental.equipment_id,
            event_type=EventType.OVERDUE,
            severity="INFO",
            description=f"Payment status updated from {old_status} to {request.payment_status}",
            details={
                "rental_id": request.rental_id,
                "old_status": old_status,
                "new_status": request.payment_status,
                "amount_paid": request.amount_paid,
                "payment_method": request.payment_method,
                "notes": request.notes
            },
            timestamp=datetime.now(IST)
        )
        
        db.add(event_log)
        db.commit()
        
        return {
            'status': 'success',
            'rental_id': request.rental_id,
            'old_status': old_status,
            'new_status': request.payment_status,
            'remaining_late_fee': rental.late_fee or 0,
            'message': f"Payment status updated successfully"
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating payment status: {str(e)}")

@router.post("/send-reminders")
async def send_payment_reminders(request: ReminderRequest, db: Session = Depends(get_db)):
    """Send payment reminders for overdue rentals"""
    try:
        now = datetime.now(IST)
        reminders_sent = []
        
        for rental_id in request.rental_ids:
            rental = db.query(Rental, MasterEquipment.customer_name, MasterEquipment.customer_id).join(
                MasterEquipment, Rental.equipment_id == MasterEquipment.equipment_id
            ).filter(Rental.rental_id == rental_id).first()
            
            if not rental:
                continue
                
            rental_obj, customer_name, customer_id = rental
            
            # Calculate current overdue info
            overdue_info = await _calculate_overdue_fees(rental_obj, now)
            
            if overdue_info['is_overdue']:
                # Create reminder message
                default_message = f"""Dear {customer_name},
                
This is a reminder that your rental {rental_id} is overdue by {overdue_info['overdue_hours']:.1f} hours.
Late fee: ₹{overdue_info['final_late_fee']:.2f}
                
Please contact us to arrange payment or return the equipment.
                
Thank you."""
                
                message = request.message or default_message
                
                # Log the reminder event
                event_log = Event(
                    event_id=str(uuid.uuid4()),
                    equipment_id=rental_obj.equipment_id,
                    event_type=EventType.OVERDUE,
                    severity="MEDIUM",
                    description=f"Payment reminder sent via {request.reminder_type}",
                    details={
                        "rental_id": rental_id,
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "reminder_type": request.reminder_type,
                        "overdue_hours": overdue_info['overdue_hours'],
                        "late_fee": overdue_info['final_late_fee'],
                        "message": message,
                        "scheduled_for": request.schedule_datetime.isoformat() if request.schedule_datetime else now.isoformat()
                    },
                    timestamp=now
                )
                
                db.add(event_log)
                
                reminders_sent.append({
                    'rental_id': rental_id,
                    'customer_name': customer_name,
                    'reminder_type': request.reminder_type,
                    'overdue_hours': overdue_info['overdue_hours'],
                    'late_fee': overdue_info['final_late_fee']
                })
        
        db.commit()
        
        return {
            'status': 'success',
            'reminders_sent': len(reminders_sent),
            'reminder_details': reminders_sent,
            'message': f"Sent {len(reminders_sent)} payment reminders via {request.reminder_type}"
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error sending reminders: {str(e)}")

@router.post("/waive-fees")
async def waive_late_fees(request: FeeWaiverRequest, db: Session = Depends(get_db)):
    """Waive late fees for a rental (requires approval)"""
    try:
        rental = db.query(Rental).filter(Rental.rental_id == request.rental_id).first()
        
        if not rental:
            raise HTTPException(status_code=404, detail="Rental not found")
        
        if request.waiver_amount > (rental.late_fee or 0):
            raise HTTPException(status_code=400, detail="Waiver amount cannot exceed current late fee")
        
        old_fee = rental.late_fee or 0
        rental.late_fee = max(0, old_fee - request.waiver_amount)
        rental.updated_at = datetime.now(IST)
        
        # Log the fee waiver event
        event_log = Event(
            event_id=str(uuid.uuid4()),
            equipment_id=rental.equipment_id,
            event_type=EventType.OVERDUE,
            severity="INFO",
            description=f"Late fee waiver of ₹{request.waiver_amount} approved",
            details={
                "rental_id": request.rental_id,
                "waiver_amount": request.waiver_amount,
                "old_late_fee": old_fee,
                "new_late_fee": rental.late_fee,
                "reason": request.reason,
                "approved_by": request.approved_by
            },
            timestamp=datetime.now(IST)
        )
        
        db.add(event_log)
        db.commit()
        
        return {
            'status': 'success',
            'rental_id': request.rental_id,
            'waiver_amount': request.waiver_amount,
            'old_late_fee': old_fee,
            'new_late_fee': rental.late_fee,
            'approved_by': request.approved_by,
            'message': f"Late fee waiver of ₹{request.waiver_amount} approved successfully"
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error processing fee waiver: {str(e)}")

@router.get("/escalation-report")
async def generate_escalation_report(db: Session = Depends(get_db)):
    """Generate escalation report for critical overdue rentals"""
    try:
        now = datetime.now(IST)
        
        # Get overdue data
        overdue_data = await get_overdue_rentals(db)
        overdue_rentals = overdue_data['overdue_rentals']
        
        # Filter critical and high severity rentals
        critical_rentals = [r for r in overdue_rentals if r['severity'] in ['CRITICAL', 'HIGH']]
        
        escalation_actions = []
        
        for rental in critical_rentals:
            # Determine escalation level based on overdue duration and fees
            overdue_hours = rental['overdue_hours']
            late_fee = rental['final_late_fee']
            
            if overdue_hours >= 168:  # 7+ days
                escalation_level = "LEGAL_ACTION"
                action = "Initiate legal proceedings for equipment recovery"
            elif overdue_hours >= 120:  # 5+ days
                escalation_level = "MANAGEMENT_ESCALATION"
                action = "Escalate to senior management and consider equipment recovery"
            elif overdue_hours >= 72:  # 3+ days
                escalation_level = "SUPERVISOR_ESCALATION"
                action = "Escalate to supervisor for immediate customer contact"
            else:
                escalation_level = "IMMEDIATE_FOLLOWUP"
                action = "Immediate phone call and site visit required"
            
            escalation_actions.append({
                'rental_id': rental['rental_id'],
                'customer_name': rental['customer_name'],
                'equipment_id': rental['equipment_id'],
                'equipment_type': rental['equipment_type'],
                'branch_name': rental['branch_name'],
                'overdue_hours': overdue_hours,
                'overdue_days': rental['overdue_days'],
                'late_fee': late_fee,
                'severity': rental['severity'],
                'escalation_level': escalation_level,
                'recommended_action': action,
                'priority_score': overdue_hours + (late_fee / 1000)  # Combined score for prioritization
            })
        
        # Sort by priority score (highest first)
        escalation_actions.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Log escalation report generation
        if escalation_actions:
            event_log = Event(
                event_id=str(uuid.uuid4()),
                equipment_id="SYSTEM",
                event_type=EventType.OVERDUE,
                severity="HIGH",
                description=f"Escalation report generated for {len(escalation_actions)} critical overdue rentals",
                details={
                    "total_critical_rentals": len(escalation_actions),
                    "total_fees_at_risk": sum(r['late_fee'] for r in escalation_actions),
                    "escalation_levels": {level: len([r for r in escalation_actions if r['escalation_level'] == level]) 
                                         for level in set(r['escalation_level'] for r in escalation_actions)}
                },
                timestamp=now
            )
            
            db.add(event_log)
            db.commit()
        
        return {
            'status': 'success',
            'escalation_report': {
                'generated_at': now.isoformat(),
                'total_critical_rentals': len(escalation_actions),
                'total_fees_at_risk': round(sum(r['late_fee'] for r in escalation_actions), 2),
                'escalation_actions': escalation_actions,
                'summary_by_level': {
                    level: {
                        'count': len([r for r in escalation_actions if r['escalation_level'] == level]),
                        'total_fees': round(sum(r['late_fee'] for r in escalation_actions if r['escalation_level'] == level), 2)
                    }
                    for level in set(r['escalation_level'] for r in escalation_actions)
                }
            },
            'message': f"Generated escalation report for {len(escalation_actions)} critical overdue rentals"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating escalation report: {str(e)}")