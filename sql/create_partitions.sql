-- Create partitions for time-series tables
-- This dramatically improves query performance by scanning only relevant time ranges

-- Market data partitions (monthly partitions for better granularity)
CREATE TABLE raw_market_data_y2023 PARTITION OF raw_market_data 
FOR VALUES FROM ('2023-01-01 00:00:00+00') TO ('2024-01-01 00:00:00+00');

CREATE TABLE raw_market_data_y2024 PARTITION OF raw_market_data 
FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');

CREATE TABLE raw_market_data_y2025 PARTITION OF raw_market_data 
FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

-- Benchmark data partitions  
CREATE TABLE raw_benchmark_data_y2023 PARTITION OF raw_benchmark_data 
FOR VALUES FROM ('2023-01-01 00:00:00+00') TO ('2024-01-01 00:00:00+00');

CREATE TABLE raw_benchmark_data_y2024 PARTITION OF raw_benchmark_data 
FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');

CREATE TABLE raw_benchmark_data_y2025 PARTITION OF raw_benchmark_data 
FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

-- Indexes on partitioned tables for optimal performance
CREATE INDEX idx_market_data_y2024_symbol_ts ON raw_market_data_y2024(symbol, timestamp DESC);
CREATE INDEX idx_market_data_y2025_symbol_ts ON raw_market_data_y2025(symbol, timestamp DESC);

CREATE INDEX idx_benchmark_data_y2024_symbol_ts ON raw_benchmark_data_y2024(symbol, timestamp DESC);
CREATE INDEX idx_benchmark_data_y2025_symbol_ts ON raw_benchmark_data_y2025(symbol, timestamp DESC);

-- Function to automatically create new partitions (can be called by Airflow)
CREATE OR REPLACE FUNCTION create_monthly_partitions(table_name TEXT, start_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMP WITH TIME ZONE;
    end_ts TIMESTAMP WITH TIME ZONE;
BEGIN
    start_ts := start_date::TIMESTAMP WITH TIME ZONE;
    end_ts := (start_date + INTERVAL '1 month')::TIMESTAMP WITH TIME ZONE;
    
    partition_name := table_name || '_' || to_char(start_date, 'YYYY_MM');
    
    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                   partition_name, table_name, start_ts, end_ts);
                   
    EXECUTE format('CREATE INDEX idx_%s_symbol_ts ON %I(symbol, timestamp DESC)',
                   partition_name, partition_name);
END;
$$ LANGUAGE plpgsql;
