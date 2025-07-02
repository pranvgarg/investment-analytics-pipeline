"""
Investment Analytics Pipeline - Streamlit Dashboard
Real-time portfolio tracking and performance analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.database.connection import get_db_connection, DatabaseConnection
import streamlit as st

# Load database connection settings from Streamlit secrets
db_config = {
    "host": st.secrets["postgres"]["host"],
    "port": st.secrets["postgres"]["port"],
    "dbname": st.secrets["postgres"]["dbname"],
    "user": st.secrets["postgres"]["user"],
    "password": st.secrets["postgres"]["password"]
}

# Create a connection string
connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# Set environment variables for the database connection
import os
os.environ["POSTGRES_URL"] = connection_string

# Page configuration
st.set_page_config(
    page_title="Investment Analytics Pipeline",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stMetric > div > div > div > div {
        font-size: 1.2rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_portfolio_data():
    """Load portfolio performance data"""
    try:
        db = get_db_connection()
        query = """
        SELECT * FROM portfolio_performance 
        ORDER BY market_value DESC
        """
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading portfolio data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_quality_metrics():
    """Load data quality metrics"""
    try:
        # Explicitly create a database connection with the connection string
        db = DatabaseConnection(connection_string=connection_string)
        query = """
        SELECT 
            check_name, 
            accuracy_percentage, 
            check_timestamp,
            check_result,
            total_records
        FROM data_quality_checks
        WHERE check_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY check_timestamp DESC
        """
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading quality metrics: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_transaction_summary():
    """Load transaction summary data"""
    try:
        db = get_db_connection()
        query = """
        SELECT 
            symbol,
            action,
            SUM(shares) as total_shares,
            AVG(price) as avg_price,
            COUNT(*) as transaction_count,
            MAX(transaction_date) as last_transaction
        FROM raw_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY symbol, action
        ORDER BY last_transaction DESC
        """
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading transaction data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_sector_allocation():
    """Load sector allocation data"""
    try:
        db = get_db_connection()
        query = """
        SELECT 
            sector,
            SUM(market_value) as total_value,
            COUNT(*) as holdings_count,
            AVG(return_percentage) as avg_return
        FROM portfolio_performance
        WHERE sector IS NOT NULL
        GROUP BY sector
        ORDER BY total_value DESC
        """
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading sector data: {e}")
        return pd.DataFrame()

def main():
    """Main dashboard application"""
    
    # Title and header
    st.title("🚀 Investment Analytics Pipeline")
    st.markdown("### Real-time Portfolio Tracking & Performance Analysis")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Refresh button
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        
        # Filters
        st.subheader("Filters")
        show_all_positions = st.checkbox("Show All Positions", value=True)
        min_value_filter = st.number_input("Min Position Value ($)", min_value=0, value=0)
        
        st.markdown("---")
        
        # Data freshness indicator
        st.subheader("📡 Data Status")
        portfolio_df = load_portfolio_data()
        if not portfolio_df.empty and 'analysis_timestamp' in portfolio_df.columns:
            last_update = portfolio_df['analysis_timestamp'].iloc[0]
            st.info(f"Last Update: {last_update}")
        else:
            st.warning("No data available")
    
    # Load data
    portfolio_df = load_portfolio_data()
    quality_df = load_quality_metrics()
    sector_df = load_sector_allocation()
    transaction_df = load_transaction_summary()
    
    if portfolio_df.empty:
        st.error("⚠️ No portfolio data available. Please check your data pipeline.")
        return
    
    # Apply filters
    if not show_all_positions:
        portfolio_df = portfolio_df[portfolio_df['market_value'] >= min_value_filter]
    
    # Key metrics row
    st.markdown("## 📈 Portfolio Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_value = portfolio_df['market_value'].sum()
        st.metric("Total Portfolio Value", f"${total_value:,.0f}")
    
    with col2:
        total_pnl = portfolio_df['unrealized_pnl'].sum()
        total_cost = portfolio_df['cost_basis'].sum()
        overall_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        st.metric("Total P&L", f"${total_pnl:,.0f}", f"{overall_return:+.1f}%")
    
    with col3:
        avg_return = portfolio_df['return_percentage'].mean()
        st.metric("Avg Return", f"{avg_return:.1f}%")
    
    with col4:
        position_count = len(portfolio_df)
        st.metric("Total Positions", position_count)
    
    with col5:
        if not quality_df.empty:
            avg_accuracy = quality_df['accuracy_percentage'].mean()
            st.metric("Data Quality", f"{avg_accuracy:.1f}%")
        else:
            st.metric("Data Quality", "N/A")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Portfolio", "🏭 Sectors", "💱 Transactions", "🔍 Data Quality"])
    
    with tab1:
        st.markdown("## Portfolio Holdings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Portfolio allocation pie chart
            fig_pie = px.pie(
                portfolio_df, 
                values='market_value', 
                names='symbol',
                title="Portfolio Allocation by Market Value",
                hover_data=['return_percentage'],
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Performance bar chart
            portfolio_sorted = portfolio_df.sort_values('return_percentage', ascending=True)
            fig_bar = px.bar(
                portfolio_sorted, 
                x='return_percentage', 
                y='symbol',
                title="Performance by Symbol",
                color='return_percentage',
                color_continuous_scale='RdYlGn',
                orientation='h'
            )
            fig_bar.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Detailed holdings table
        st.markdown("### 📋 Detailed Holdings")
        
        # Format the dataframe for display
        display_df = portfolio_df[[
            'symbol', 'shares', 'avg_cost', 'current_price', 'market_value', 
            'unrealized_pnl', 'return_percentage', 'allocation_percentage',
            'performance_category', 'sector'
        ]].copy()
        
        # Format numeric columns
        display_df['shares'] = display_df['shares'].round(2)
        display_df['avg_cost'] = display_df['avg_cost'].round(2)
        display_df['current_price'] = display_df['current_price'].round(2)
        display_df['market_value'] = display_df['market_value'].round(0)
        display_df['unrealized_pnl'] = display_df['unrealized_pnl'].round(0)
        display_df['return_percentage'] = display_df['return_percentage'].round(1)
        display_df['allocation_percentage'] = display_df['allocation_percentage'].round(1)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "symbol": "Symbol",
                "shares": "Shares",
                "avg_cost": st.column_config.NumberColumn("Avg Cost", format="$%.2f"),
                "current_price": st.column_config.NumberColumn("Current Price", format="$%.2f"),
                "market_value": st.column_config.NumberColumn("Market Value", format="$%.0f"),
                "unrealized_pnl": st.column_config.NumberColumn("Unrealized P&L", format="$%.0f"),
                "return_percentage": st.column_config.NumberColumn("Return %", format="%.1f%%"),
                "allocation_percentage": st.column_config.NumberColumn("Allocation %", format="%.1f%%"),
                "performance_category": "Performance",
                "sector": "Sector"
            }
        )
    
    with tab2:
        st.markdown("## Sector Analysis")
        
        if not sector_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Sector allocation
                fig_sector = px.pie(
                    sector_df, 
                    values='total_value', 
                    names='sector',
                    title="Allocation by Sector",
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig_sector, use_container_width=True)
            
            with col2:
                # Sector performance
                fig_sector_perf = px.bar(
                    sector_df, 
                    x='sector', 
                    y='avg_return',
                    title="Average Return by Sector",
                    color='avg_return',
                    color_continuous_scale='RdYlGn'
                )
                fig_sector_perf.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig_sector_perf, use_container_width=True)
            
            # Sector summary table
            st.markdown("### Sector Summary")
            sector_display = sector_df.copy()
            sector_display['total_value'] = sector_display['total_value'].round(0)
            sector_display['avg_return'] = sector_display['avg_return'].round(1)
            
            st.dataframe(
                sector_display,
                use_container_width=True,
                column_config={
                    "sector": "Sector",
                    "total_value": st.column_config.NumberColumn("Total Value", format="$%.0f"),
                    "holdings_count": "# Holdings",
                    "avg_return": st.column_config.NumberColumn("Avg Return", format="%.1f%%")
                }
            )
        else:
            st.info("No sector data available")
    
    with tab3:
        st.markdown("## Recent Transactions")
        
        if not transaction_df.empty:
            # Transaction summary by action
            buy_transactions = transaction_df[transaction_df['action'] == 'BUY']
            sell_transactions = transaction_df[transaction_df['action'] == 'SELL']
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not buy_transactions.empty:
                    fig_buy = px.bar(
                        buy_transactions, 
                        x='symbol', 
                        y='total_shares',
                        title="Recent Purchases (Last 30 Days)",
                        color='avg_price'
                    )
                    st.plotly_chart(fig_buy, use_container_width=True)
                else:
                    st.info("No recent purchases")
            
            with col2:
                if not sell_transactions.empty:
                    fig_sell = px.bar(
                        sell_transactions, 
                        x='symbol', 
                        y='total_shares',
                        title="Recent Sales (Last 30 Days)",
                        color='avg_price'
                    )
                    st.plotly_chart(fig_sell, use_container_width=True)
                else:
                    st.info("No recent sales")
            
            # Transaction details table
            st.markdown("### Transaction Summary")
            transaction_display = transaction_df.copy()
            transaction_display['avg_price'] = transaction_display['avg_price'].round(2)
            transaction_display['total_shares'] = transaction_display['total_shares'].round(2)
            
            st.dataframe(
                transaction_display,
                use_container_width=True,
                column_config={
                    "symbol": "Symbol",
                    "action": "Action",
                    "total_shares": "Total Shares",
                    "avg_price": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
                    "transaction_count": "# Transactions",
                    "last_transaction": "Last Transaction"
                }
            )
        else:
            st.info("No recent transaction data available")
    
    with tab4:
        st.markdown("## Data Quality Monitoring")
        
        if not quality_df.empty:
            # Quality metrics over time
            fig_quality = px.line(
                quality_df, 
                x='check_timestamp', 
                y='accuracy_percentage',
                color='check_name',
                title="Data Quality Trends (Last 7 Days)",
                markers=True
            )
            fig_quality.update_layout(yaxis_title="Accuracy %")
            st.plotly_chart(fig_quality, use_container_width=True)
            
            # Quality summary
            st.markdown("### Quality Check Results")
            
            # Latest results
            latest_results = quality_df.drop_duplicates(subset=['check_name'], keep='first')
            
            col1, col2, col3 = st.columns(3)
            
            for i, (_, row) in enumerate(latest_results.iterrows()):
                col = [col1, col2, col3][i % 3]
                
                with col:
                    status_icon = "✅" if row['check_result'] else "❌"
                    accuracy = row['accuracy_percentage']
                    st.metric(
                        f"{status_icon} {row['check_name'].replace('_', ' ').title()}",
                        f"{accuracy:.1f}%",
                        f"{row['total_records']} records"
                    )
            
            # Detailed quality table
            st.markdown("### Detailed Quality History")
            quality_display = quality_df.copy()
            quality_display['check_result'] = quality_display['check_result'].map({True: '✅ Pass', False: '❌ Fail'})
            quality_display['accuracy_percentage'] = quality_display['accuracy_percentage'].round(1)
            
            st.dataframe(
                quality_display,
                use_container_width=True,
                column_config={
                    "check_name": "Check Name",
                    "check_result": "Result",
                    "accuracy_percentage": st.column_config.NumberColumn("Accuracy", format="%.1f%%"),
                    "total_records": "Records",
                    "check_timestamp": "Timestamp"
                }
            )
        else:
            st.info("No quality metrics available")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "📊 **Investment Analytics Pipeline** | "
        "Built with Streamlit, dbt, Airflow & PostgreSQL | "
        f"Last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

if __name__ == "__main__":
    main()
