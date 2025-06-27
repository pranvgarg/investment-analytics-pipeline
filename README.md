# 🚀 Production-Grade Investment Analytics Pipeline

A comprehensive, scalable, and production-ready investment portfolio tracking system built with modern data engineering best practices.

## 🎯 Project Overview

This project demonstrates a **complete end-to-end data engineering pipeline** for investment portfolio analysis, featuring:

- ⚡ **Real-time data ingestion** from professional APIs (Polygon.io, Alpaca)
- 🔄 **Robust orchestration** with Apache Airflow using Astro Runtime
- 🏗️ **Data transformation** with dbt for accurate financial calculations
- 📊 **Interactive dashboards** with Streamlit
- ✅ **Data quality validation** with Great Expectations
- 📈 **Advanced financial metrics** (Sharpe ratio, Alpha, Time-weighted returns)
- 🔧 **Production-ready infrastructure** with Docker Compose
- 📱 **Monitoring & alerting** with Slack integration

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Orchestration │    │   Storage       │
│                 │    │                 │    │                 │
│ • Polygon.io    ├────┤ Apache Airflow  ├────┤ PostgreSQL      │
│ • Alpaca API    │    │ • TaskFlow API  │    │ • Partitioned   │
│ • Economic Data │    │ • Great Expect. │    │ • Time-series   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Transformation  │    │  Presentation   │
                       │                 │    │                 │
                       │ dbt Core        ├────┤ Streamlit       │
                       │ • Staging       │    │ • Real-time     │
                       │ • Marts         │    │ • Interactive   │
                       │ • Tests         │    │ • Responsive    │
                       └─────────────────┘    └─────────────────┘
```

## 🔧 Prerequisites

### Required Accounts & API Keys

1. **Polygon.io Account** (Free tier available)
   - Sign up at https://polygon.io/
   - Get your API key from the dashboard

2. **Alpaca Trading Account** (Paper trading - free)
   - Sign up at https://alpaca.markets/
   - Enable paper trading and get API keys

3. **Slack Workspace** (Optional, for alerts)
   - Create a Slack app and get webhook URL

### System Requirements

- Docker & Docker Compose
- 4GB+ RAM (recommended 8GB)
- 10GB+ disk space
- macOS, Linux, or Windows with WSL2

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <your-repository>
cd investment-analytics-pipeline

# Copy environment template
cp .env.example .env
```

### 2. Configure Environment Variables

Edit `.env` file with your actual API keys:

```bash
# Required: Add your real API keys
POLYGON_API_KEY=your_polygon_io_api_key_here
ALPACA_API_KEY=your_alpaca_api_key_here
ALPACA_SECRET_KEY=your_alpaca_secret_key_here

# Security: Change default passwords
POSTGRES_PASSWORD=your_secure_password_here
AIRFLOW_FERNET_KEY=your_generated_fernet_key_here
AIRFLOW_SECRET_KEY=your_generated_secret_key_here

# Optional: Slack alerts
SLACK_WEBHOOK_URL=your_slack_webhook_url_here
```

### 3. Generate Security Keys

```bash
# Generate Airflow Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Generate random secret key
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 4. Start the Infrastructure

```bash
# Start all services
docker-compose up -d

# Verify services are healthy
docker-compose ps
```

### 5. Initialize Great Expectations

```bash
# Initialize GE project
docker exec -it investment_airflow bash
cd /usr/local/airflow/include
great_expectations init
```

### 6. Access the Applications

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Streamlit Dashboard**: http://localhost:8501
- **PostgreSQL**: localhost:5432

## 📊 Dashboard Features

### Portfolio Overview
- Real-time portfolio value and P&L
- Asset allocation visualization
- Performance metrics and trends
- Risk analysis and concentration metrics

### Data Quality Monitoring
- Pipeline health status
- Data freshness indicators
- Quality check results
- Error tracking and alerts

### Financial Analytics
- Time-weighted returns calculation
- Sharpe ratio and risk metrics
- Benchmark comparison (vs SPY)
- Sector allocation analysis

## 🔄 Data Pipeline

### Daily Workflow (Automated)

1. **Extract** (9 AM EST, weekdays)
   - Fetch current market prices from Polygon.io
   - Update portfolio holdings from Alpaca
   - Retrieve recent transactions

2. **Validate** (Great Expectations)
   - Data freshness checks
   - Price validity ranges
   - Schema validation
   - Business rule validation

3. **Transform** (dbt)
   - Clean and standardize data
   - Calculate daily returns
   - Compute portfolio metrics
   - Generate analytics tables

4. **Load & Present**
   - Update dashboard data
   - Refresh visualizations
   - Send alerts if needed

### Data Quality Checks

The pipeline includes comprehensive data validation:

- **Freshness**: Data must be < 24 hours old
- **Completeness**: No missing critical fields
- **Accuracy**: Prices within reasonable ranges
- **Consistency**: Portfolio math must balance
- **Uniqueness**: No duplicate transactions

## 🧮 Financial Calculations

### Key Metrics Implemented

1. **Time-Weighted Return (TWR)**
   ```sql
   -- Removes impact of cash flows for accurate performance measurement
   SELECT 
     symbol,
     EXP(SUM(LN(1 + daily_return))) - 1 as time_weighted_return
   FROM daily_returns
   ```

2. **Sharpe Ratio**
   ```sql
   -- Risk-adjusted return metric
   SELECT 
     (portfolio_return - risk_free_rate) / portfolio_volatility as sharpe_ratio
   ```

3. **Alpha vs Benchmark**
   ```sql
   -- Performance vs market (SPY)
   SELECT 
     portfolio_return - benchmark_return as alpha
   ```

## 🏗️ Database Design

### Partitioned Tables for Performance

```sql
-- Market data partitioned by time for fast queries
CREATE TABLE raw_market_data (
    id UUID DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(19,4) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    -- ... other columns
) PARTITION BY RANGE (timestamp);
```

### Optimized Indexes

```sql
-- Compound indexes for common query patterns
CREATE INDEX idx_market_data_symbol_timestamp 
ON raw_market_data(symbol, timestamp DESC);
```

## 🔍 Monitoring & Observability

### Health Checks

- Database connectivity
- API availability
- Data freshness
- Pipeline execution status

### Alerting

- Slack notifications for failures
- Email alerts for critical issues
- Dashboard status indicators

### Logging

- Structured logging with correlation IDs
- Performance metrics tracking
- Error tracking and debugging

## 🧪 Testing

```bash
# Run data quality tests
docker exec -it investment_airflow great_expectations checkpoint run raw_data_quality_check

# Run dbt tests
docker exec -it investment_airflow dbt test --profiles-dir /usr/local/airflow/include/dbt/profiles

# Run Python unit tests
docker exec -it investment_airflow pytest /usr/local/airflow/include/tests/
```

## 📈 Scaling Considerations

### For Production Use

1. **Infrastructure**
   - Move to cloud (AWS RDS, ECS/EKS)
   - Implement proper secret management
   - Add load balancing and auto-scaling

2. **Data**
   - Implement data lake (S3) for historical storage
   - Add real-time streaming (Kafka/Kinesis)
   - Implement proper backup strategies

3. **Security**
   - Enable SSL/TLS encryption
   - Implement OAuth authentication
   - Add network security (VPC, security groups)

4. **Monitoring**
   - Add APM (DataDog, New Relic)
   - Implement cost monitoring
   - Set up proper alerting escalation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all quality checks pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with modern data engineering best practices
- Inspired by real-world production systems
- Designed for educational and professional use

---

**Note**: This is a demonstration project. For actual investment decisions, please consult with financial professionals and use properly licensed trading systems.
