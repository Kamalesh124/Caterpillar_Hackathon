# Smart Rental Management System - Streamlit Dashboard

A fast and interactive dashboard built with Streamlit for the Smart Rental Management System.

## Features

- **Real-time Overview**: System health, metrics, and revenue trends
- **Usage Tracking**: Equipment usage analysis by site
- **Anomaly Detection**: Real-time monitoring and alerts
- **Demand Forecast**: 14-day equipment demand predictions
- **Rental Summary**: Comprehensive rental analytics
- **Dealer Digest**: Daily KPIs and insights
- **Overdue & Fees**: Payment tracking and late fee management
- **Security System**: Geofence monitoring and security events

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Backend API running on `http://localhost:8081`

### Installation

1. Navigate to the streamlit dashboard directory:
```bash
cd streamlit_dashboard
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the dashboard:
```bash
streamlit run app.py
```

4. Open your browser and go to `http://localhost:8501`

## Dashboard Sections

### 📊 Overview
- System health status
- Key performance metrics
- Revenue trends

### ⏱️ Usage Tracking
- Equipment runtime and idle hours
- Site-wise usage analysis
- Custom date range filtering

### 🚨 Anomaly Detection
- Real-time anomaly detection
- Fuel theft, tampering, and idle abuse monitoring
- Historical anomaly records

### 📈 Demand Forecast
- 14-day equipment demand predictions
- Peak demand identification
- Demand variance analysis

### 📋 Rental Summary
- Equipment type performance
- Revenue distribution
- Utilization metrics

### 📊 Dealer Digest
- Daily KPIs and insights
- Performance trends
- Actionable recommendations

### 💰 Overdue & Fees
- Overdue rental tracking
- Late fee calculations
- Payment status monitoring

### 🔒 Security System
- Geofence violation alerts
- Security event monitoring
- Real-time threat detection

## API Integration

The dashboard connects to the backend API at `http://localhost:8081/api/v1`. Make sure the backend server is running before starting the Streamlit app.

## Customization

You can customize the dashboard by:
- Modifying the API endpoints in `app.py`
- Adding new visualizations using Plotly
- Extending the sidebar navigation
- Adding new dashboard sections

## Troubleshooting

- **Connection Error**: Ensure the backend API is running on port 8081
- **Module Not Found**: Install missing dependencies with `pip install -r requirements.txt`
- **Port Already in Use**: Streamlit will automatically suggest an alternative port

## Performance

This Streamlit dashboard is designed for quick deployment and immediate visualization. It provides:
- Fast startup time (< 10 seconds)
- Interactive charts and real-time data
- Responsive design for different screen sizes
- Efficient API integration with error handling