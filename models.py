from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

Base = declarative_base()

class EquipmentStatus(enum.Enum):
    AVAILABLE = "AVAILABLE"
    RENTED = "RENTED"
    MAINTENANCE = "MAINTENANCE"
    OUT_OF_SERVICE = "OUT_OF_SERVICE"

class PaymentStatus(enum.Enum):
    PAID = "PAID"
    DUE = "DUE"
    OVERDUE = "OVERDUE"
    PARTIAL = "PARTIAL"

class EventType(enum.Enum):
    ANOMALY = "ANOMALY"
    MAINTENANCE = "MAINTENANCE"
    FUEL = "FUEL"
    SECURITY = "SECURITY"
    OVERDUE = "OVERDUE"
    PREDICTIVE = "PREDICTIVE"
    OPERATOR = "OPERATOR"

class Severity(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class MasterEquipment(Base):
    __tablename__ = "master_equipment"
    
    equipment_id = Column(String(20), primary_key=True, index=True)
    equipment_type = Column(String(50), nullable=False, index=True)
    make = Column(String(50), nullable=False)
    model = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    capacity = Column(Float, nullable=False)
    fuel_type = Column(String(20), nullable=False)
    branch_id = Column(String(10), nullable=False, index=True)
    branch_name = Column(String(100), nullable=False)
    site_id = Column(String(20), nullable=False, index=True)
    site_name = Column(String(100), nullable=False)
    customer_id = Column(String(20), nullable=True)
    customer_name = Column(String(100), nullable=True)
    status = Column(Enum(EquipmentStatus), nullable=False, default=EquipmentStatus.AVAILABLE)
    qr_code = Column(String(100), nullable=True)
    license_class_required = Column(String(10), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    rentals = relationship("Rental", back_populates="equipment")
    usage_records = relationship("UsageDaily", back_populates="equipment")
    events = relationship("Event", back_populates="equipment")

class Rental(Base):
    __tablename__ = "rentals"
    
    rental_id = Column(String(20), primary_key=True, index=True)
    equipment_id = Column(String(20), ForeignKey("master_equipment.equipment_id"), nullable=False, index=True)
    site_id = Column(String(20), nullable=False, index=True)
    customer_id = Column(String(20), nullable=False, index=True)
    contract_start_ts = Column(DateTime, nullable=False)
    contract_end_ts_planned = Column(DateTime, nullable=False)
    actual_end_ts = Column(DateTime, nullable=True)
    billed_days = Column(Integer, nullable=False)
    billed_hours = Column(Float, nullable=False)
    rate_day = Column(Float, nullable=False)
    discount_pct = Column(Float, default=0.0)
    revenue = Column(Float, nullable=False)
    payment_status = Column(Enum(PaymentStatus), nullable=False, default=PaymentStatus.DUE)
    late_hours = Column(Float, default=0.0)
    late_days = Column(Integer, default=0)
    late_fee = Column(Float, default=0.0)
    operator_id_on_checkout = Column(String(20), nullable=True)
    deposit_amount = Column(Float, nullable=True)
    grace_minutes = Column(Integer, default=60)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    equipment = relationship("MasterEquipment", back_populates="rentals")

class UsageDaily(Base):
    __tablename__ = "usage_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment_id = Column(String(20), ForeignKey("master_equipment.equipment_id"), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    runtime_hours = Column(Float, default=0.0)
    idle_hours = Column(Float, default=0.0)
    distance_km = Column(Float, default=0.0)
    fuel_used_liters = Column(Float, default=0.0)
    fuel_eff_lph = Column(Float, default=0.0)
    utilization_pct = Column(Float, default=0.0)
    availability_flag = Column(Boolean, default=True)
    breakdown_hours = Column(Float, default=0.0)
    last_gps_lat = Column(Float, nullable=True)
    last_gps_lon = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    equipment = relationship("MasterEquipment", back_populates="usage_records")

class DemandDaily(Base):
    __tablename__ = "demand_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    branch_id = Column(String(10), nullable=False, index=True)
    equipment_type = Column(String(50), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    rental_requests = Column(Integer, default=0)
    confirmed_rentals = Column(Integer, default=0)
    cancellations = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class Event(Base):
    __tablename__ = "events"
    
    event_id = Column(String(50), primary_key=True, index=True)
    equipment_id = Column(String(20), ForeignKey("master_equipment.equipment_id"), nullable=False, index=True)
    ts = Column(DateTime, nullable=False, index=True)
    event_type = Column(Enum(EventType), nullable=False, index=True)
    subtype = Column(String(50), nullable=True)
    severity = Column(Enum(Severity), nullable=True)
    value = Column(Float, nullable=True)
    details = Column(Text, nullable=True)
    site_id = Column(String(20), nullable=True)
    customer_id = Column(String(20), nullable=True)
    session_id = Column(String(50), nullable=True)
    resolved = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    equipment = relationship("MasterEquipment", back_populates="events")

class PredictiveHealth(Base):
    __tablename__ = "predictive_health"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment_id = Column(String(20), ForeignKey("master_equipment.equipment_id"), nullable=False, index=True)
    prediction_date = Column(DateTime, nullable=False, index=True)
    failure_probability = Column(Float, nullable=False)
    predicted_failure_type = Column(String(100), nullable=True)
    recommended_action = Column(Text, nullable=True)
    confidence_score = Column(Float, nullable=True)
    days_until_failure = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class OperatorScore(Base):
    __tablename__ = "operator_scores"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(String(20), nullable=False, index=True)
    equipment_id = Column(String(20), ForeignKey("master_equipment.equipment_id"), nullable=False)
    date = Column(DateTime, nullable=False, index=True)
    efficiency_score = Column(Float, nullable=False)
    fuel_efficiency_score = Column(Float, nullable=False)
    idle_time_score = Column(Float, nullable=False)
    safety_score = Column(Float, nullable=False)
    overall_score = Column(Float, nullable=False)
    rank_percentile = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)