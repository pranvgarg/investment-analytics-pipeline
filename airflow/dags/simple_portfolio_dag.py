"""
Simple Portfolio Refresh DAG
A working demo DAG that creates the missing portfolio_performance table
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'simple_portfolio_refresh',
    default_args=default_args,
    description='Simple portfolio data refresh',
    schedule_interval='@daily',
    catchup=False,
    tags=['portfolio', 'demo']
)

# Create the portfolio_performance table that the dashboard needs
create_portfolio_performance = PostgresOperator(
    task_id='create_portfolio_performance',
    postgres_conn_id='postgres_default',
    sql="""
    -- Create portfolio_performance table for dashboard
    DROP TABLE IF EXISTS portfolio_performance CASCADE;
    
    CREATE TABLE portfolio_performance AS
    WITH current_positions AS (
        SELECT 
            h.account_id,
            h.symbol,
            h.shares,
            h.avg_cost,
            h.cost_basis,
            h.sector,
            h.industry,
            h.updated_at
        FROM raw_portfolio_holdings h
        WHERE h.shares > 0
    ),
    
    latest_market_prices AS (
        SELECT DISTINCT ON (symbol)
            symbol,
            price as current_price,
            volume,
            timestamp as price_timestamp
        FROM raw_market_data 
        ORDER BY symbol, timestamp DESC
    ),
    
    position_performance AS (
        SELECT 
            p.account_id,
            p.symbol,
            p.shares,
            p.avg_cost,
            p.cost_basis,
            p.sector,
            p.industry,
            
            -- Market data
            COALESCE(m.current_price, 0) as current_price,
            m.volume,
            m.price_timestamp,
            
            -- Performance calculations
            COALESCE(p.shares * m.current_price, 0) as market_value,
            COALESCE(p.shares * m.current_price - p.cost_basis, 0) as unrealized_pnl,
            
            CASE 
                WHEN p.cost_basis > 0 THEN 
                    COALESCE((p.shares * m.current_price - p.cost_basis) / p.cost_basis * 100, 0)
                ELSE 0 
            END as return_percentage,
            
            p.updated_at
        FROM current_positions p
        LEFT JOIN latest_market_prices m ON p.symbol = m.symbol
    ),
    
    portfolio_totals AS (
        SELECT 
            SUM(market_value) as total_portfolio_value,
            SUM(cost_basis) as total_cost_basis,
            SUM(unrealized_pnl) as total_unrealized_pnl
        FROM position_performance
    )
    
    SELECT 
        pp.*,
        pt.total_portfolio_value,
        pt.total_cost_basis,
        pt.total_unrealized_pnl,
        
        -- Portfolio allocation
        CASE 
            WHEN pt.total_portfolio_value > 0 THEN 
                pp.market_value / pt.total_portfolio_value * 100 
            ELSE 0 
        END as allocation_percentage,
        
        -- Risk metrics (simplified)
        CASE 
            WHEN pp.market_value / pt.total_portfolio_value > 0.20 THEN 'High Concentration'
            WHEN pp.market_value / pt.total_portfolio_value > 0.10 THEN 'Medium Concentration'
            ELSE 'Diversified'
        END as concentration_risk,
        
        -- Performance categorization
        CASE 
            WHEN pp.return_percentage > 20 THEN 'Strong Performer'
            WHEN pp.return_percentage > 5 THEN 'Good Performer'
            WHEN pp.return_percentage > -5 THEN 'Neutral'
            WHEN pp.return_percentage > -20 THEN 'Underperformer'
            ELSE 'Poor Performer'
        END as performance_category,
        
        CURRENT_TIMESTAMP as analysis_timestamp
        
    FROM position_performance pp
    CROSS JOIN portfolio_totals pt
    ORDER BY market_value DESC;
    """,
    dag=dag
)

# Create the data_quality_checks table that the dashboard needs  
create_quality_table = PostgresOperator(
    task_id='create_quality_table',
    postgres_conn_id='postgres_default',
    sql="""
    -- Create data_quality_checks table for dashboard compatibility
    CREATE TABLE IF NOT EXISTS data_quality_checks (
        id SERIAL PRIMARY KEY,
        check_name VARCHAR(100) NOT NULL,
        accuracy_percentage DECIMAL(5,2) DEFAULT 95.0,
        check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        check_result BOOLEAN DEFAULT true,
        total_records INTEGER DEFAULT 100
    );
    
    -- Insert some sample quality data
    INSERT INTO data_quality_checks (check_name, accuracy_percentage, check_result, total_records) 
    VALUES 
        ('market_data_freshness', 98.5, true, 8),
        ('portfolio_consistency', 100.0, true, 8),
        ('price_validity', 99.2, true, 8)
    ON CONFLICT DO NOTHING;
    """,
    dag=dag
)

# Simple status check
status_check = BashOperator(
    task_id='status_check',
    bash_command='echo "Portfolio refresh completed successfully!"',
    dag=dag
)

# Set dependencies
create_quality_table >> create_portfolio_performance >> status_check
