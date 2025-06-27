-- Investment Analytics Pipeline Database Schema
-- PostgreSQL DDL with partitioning for scalable time-series data

-- Drop existing tables if they exist
DROP TABLE IF EXISTS data_quality_audits CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;
DROP TABLE IF EXISTS raw_transactions CASCADE;
DROP TABLE IF EXISTS raw_portfolio_holdings CASCADE;
DROP TABLE IF EXISTS raw_market_data CASCADE;
DROP TABLE IF EXISTS raw_benchmark_data CASCADE;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Market data table with time-based partitioning for performance
CREATE TABLE raw_market_data (
    id UUID DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(19,4) NOT NULL CHECK (price > 0),
    volume BIGINT CHECK (volume >= 0),
    open_price DECIMAL(19,4),
    high_price DECIMAL(19,4), 
    low_price DECIMAL(19,4),
    close_price DECIMAL(19,4),
    adjusted_close DECIMAL(19,4),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'polygon',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_price_range CHECK (price BETWEEN 0.01 AND 100000),
    CONSTRAINT valid_timestamp CHECK (timestamp <= CURRENT_TIMESTAMP)
) PARTITION BY RANGE (timestamp);

-- Benchmark data for performance comparison (e.g., SPY, QQQ)
CREATE TABLE raw_benchmark_data (
    id UUID DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(19,4) NOT NULL CHECK (price > 0),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'polygon',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (timestamp);

-- Portfolio holdings with proper constraints
CREATE TABLE raw_portfolio_holdings (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    account_id VARCHAR(100) NOT NULL DEFAULT 'default',
    symbol VARCHAR(20) NOT NULL,
    shares DECIMAL(18,8) NOT NULL CHECK (shares >= 0),
    avg_cost DECIMAL(19,4) NOT NULL CHECK (avg_cost > 0),
    cost_basis DECIMAL(19,2) GENERATED ALWAYS AS (shares * avg_cost) STORED,
    sector VARCHAR(100),
    industry VARCHAR(100),
    asset_class VARCHAR(50) DEFAULT 'equity',
    currency VARCHAR(3) DEFAULT 'USD',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_account_symbol UNIQUE (account_id, symbol)
);

-- Transactions with broker integration support
CREATE TABLE raw_transactions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL, -- From broker API
    account_id VARCHAR(100) NOT NULL DEFAULT 'default',
    transaction_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    action VARCHAR(20) NOT NULL CHECK (action IN ('BUY', 'SELL', 'DIVIDEND', 'SPLIT', 'TRANSFER_IN', 'TRANSFER_OUT')),
    shares DECIMAL(18,8) NOT NULL CHECK (shares > 0),
    price DECIMAL(19,4) NOT NULL CHECK (price > 0),
    fees DECIMAL(10,2) DEFAULT 0 CHECK (fees >= 0),
    total_amount DECIMAL(19,2) GENERATED ALWAYS AS (
        CASE 
            WHEN action IN ('BUY', 'TRANSFER_IN') THEN shares * price + fees
            WHEN action IN ('SELL', 'TRANSFER_OUT') THEN shares * price - fees
            ELSE shares * price
        END
    ) STORED,
    currency VARCHAR(3) DEFAULT 'USD',
    exchange VARCHAR(20),
    settlement_date DATE,
    notes TEXT,
    source VARCHAR(50) DEFAULT 'alpaca',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_transaction_date CHECK (transaction_date <= CURRENT_DATE),
    CONSTRAINT valid_settlement_date CHECK (settlement_date IS NULL OR settlement_date >= transaction_date)
);

-- Great Expectations data quality audits
CREATE TABLE data_quality_audits (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    expectation_suite_name VARCHAR(255) NOT NULL,
    data_asset_name VARCHAR(255) NOT NULL,
    success BOOLEAN NOT NULL,
    evaluated_expectations INTEGER NOT NULL,
    successful_expectations INTEGER NOT NULL,
    success_percent DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE WHEN evaluated_expectations > 0 
             THEN (successful_expectations::DECIMAL / evaluated_expectations * 100)
             ELSE 0 END
    ) STORED,
    expectation_results JSONB,
    audit_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dag_run_id VARCHAR(255),
    task_id VARCHAR(255)
);

-- Economic indicators for enhanced analysis
CREATE TABLE raw_economic_indicators (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_value DECIMAL(15,6) NOT NULL,
    indicator_date DATE NOT NULL,
    frequency VARCHAR(20) DEFAULT 'daily', -- daily, weekly, monthly, quarterly
    units VARCHAR(50),
    source VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_indicator_date UNIQUE (indicator_name, indicator_date)
);

-- Pipeline execution tracking
CREATE TABLE pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(100),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance optimization
-- Note: These will be created on the actual partitions in create_partitions.sql
CREATE INDEX idx_portfolio_holdings_symbol ON raw_portfolio_holdings(symbol);
CREATE INDEX idx_transactions_symbol_date ON raw_transactions(symbol, transaction_date DESC);
CREATE INDEX idx_quality_audits_timestamp ON data_quality_audits(audit_timestamp DESC);

-- Create a view for latest market prices
CREATE OR REPLACE VIEW latest_market_prices AS
SELECT DISTINCT ON (symbol) 
    symbol,
    price,
    volume,
    timestamp,
    source
FROM raw_market_data 
ORDER BY symbol, timestamp DESC;

-- Create a view for current portfolio summary
CREATE OR REPLACE VIEW current_portfolio_summary AS
SELECT 
    h.symbol,
    h.shares,
    h.avg_cost,
    h.cost_basis,
    COALESCE(m.price, 0) as current_price,
    COALESCE(h.shares * m.price, 0) as market_value,
    COALESCE(h.shares * m.price - h.cost_basis, 0) as unrealized_pnl,
    CASE 
        WHEN h.cost_basis > 0 THEN 
            COALESCE((h.shares * m.price - h.cost_basis) / h.cost_basis * 100, 0)
        ELSE 0 
    END as return_percentage,
    h.sector,
    h.industry,
    h.updated_at
FROM raw_portfolio_holdings h
LEFT JOIN latest_market_prices m ON h.symbol = m.symbol
WHERE h.shares > 0;

-- Insert sample data for testing
INSERT INTO raw_portfolio_holdings (symbol, shares, avg_cost, sector, industry) VALUES
('AAPL', 100.0, 150.00, 'Technology', 'Consumer Electronics'),
('GOOGL', 50.0, 2600.00, 'Technology', 'Internet Search'),
('MSFT', 75.0, 380.00, 'Technology', 'Software'),
('TSLA', 25.0, 800.00, 'Automotive', 'Electric Vehicles'),
('AMZN', 30.0, 3200.00, 'Technology', 'E-commerce'),
('NVDA', 40.0, 450.00, 'Technology', 'Semiconductors'),
('META', 60.0, 300.00, 'Technology', 'Social Media'),
('NFLX', 20.0, 400.00, 'Technology', 'Streaming Services');

INSERT INTO raw_transactions (transaction_id, transaction_date, symbol, action, shares, price, fees) VALUES
('DEMO_TXN_001', '2024-01-15', 'AAPL', 'BUY', 50.0, 145.00, 9.99),
('DEMO_TXN_002', '2024-02-01', 'AAPL', 'BUY', 50.0, 155.00, 9.99),
('DEMO_TXN_003', '2024-01-20', 'GOOGL', 'BUY', 50.0, 2600.00, 9.99),
('DEMO_TXN_004', '2024-02-15', 'MSFT', 'BUY', 75.0, 380.00, 9.99),
('DEMO_TXN_005', '2024-03-01', 'TSLA', 'BUY', 25.0, 800.00, 9.99),
('DEMO_TXN_006', '2024-03-15', 'AMZN', 'BUY', 30.0, 3200.00, 9.99),
('DEMO_TXN_007', '2024-04-01', 'NVDA', 'BUY', 40.0, 450.00, 9.99),
('DEMO_TXN_008', '2024-04-15', 'META', 'BUY', 60.0, 300.00, 9.99),
('DEMO_TXN_009', '2024-05-01', 'NFLX', 'BUY', 20.0, 400.00, 9.99);

-- Grant permissions (adjust as needed for production)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO investment_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO investment_user;
