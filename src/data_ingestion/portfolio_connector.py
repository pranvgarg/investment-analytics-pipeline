"""
Portfolio data connector for investment analytics pipeline
In production, this would connect to broker APIs (TD Ameritrade, Interactive Brokers, etc.)
For now, using sample data and CSV/JSON inputs
"""
import pandas as pd
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

class PortfolioConnector:
    """Connects to portfolio data sources (broker APIs, CSV files, etc.)"""
    
    def __init__(self, broker_api_key: Optional[str] = None, data_source: str = "sample"):
        self.broker_api_key = broker_api_key
        self.data_source = data_source
        
    def get_holdings(self) -> pd.DataFrame:
        """Fetch current portfolio holdings"""
        if self.data_source == "sample":
            return self._get_sample_holdings()
        elif self.data_source == "csv":
            return self._get_holdings_from_csv()
        elif self.data_source == "api":
            return self._get_holdings_from_api()
        else:
            logger.warning(f"Unknown data source: {self.data_source}")
            return pd.DataFrame()
    
    def get_transactions(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Fetch transaction history"""
        if self.data_source == "sample":
            return self._get_sample_transactions(start_date, end_date)
        elif self.data_source == "csv":
            return self._get_transactions_from_csv(start_date, end_date)
        elif self.data_source == "api":
            return self._get_transactions_from_api(start_date, end_date)
        else:
            logger.warning(f"Unknown data source: {self.data_source}")
            return pd.DataFrame()
    
    def _get_sample_holdings(self) -> pd.DataFrame:
        """Generate sample portfolio holdings data"""
        holdings_data = [
            {'symbol': 'AAPL', 'shares': 100.0, 'avg_cost': 150.00, 'sector': 'Technology', 'industry': 'Consumer Electronics'},
            {'symbol': 'GOOGL', 'shares': 50.0, 'avg_cost': 2600.00, 'sector': 'Technology', 'industry': 'Internet Search'},
            {'symbol': 'MSFT', 'shares': 75.0, 'avg_cost': 380.00, 'sector': 'Technology', 'industry': 'Software'},
            {'symbol': 'TSLA', 'shares': 25.0, 'avg_cost': 800.00, 'sector': 'Automotive', 'industry': 'Electric Vehicles'},
            {'symbol': 'AMZN', 'shares': 30.0, 'avg_cost': 3200.00, 'sector': 'Technology', 'industry': 'E-commerce'},
            {'symbol': 'NVDA', 'shares': 40.0, 'avg_cost': 450.00, 'sector': 'Technology', 'industry': 'Semiconductors'},
            {'symbol': 'META', 'shares': 60.0, 'avg_cost': 300.00, 'sector': 'Technology', 'industry': 'Social Media'},
            {'symbol': 'NFLX', 'shares': 20.0, 'avg_cost': 400.00, 'sector': 'Technology', 'industry': 'Streaming Services'},
        ]
        
        df = pd.DataFrame(holdings_data)
        df['account_id'] = 'sample_account'
        df['updated_at'] = pd.Timestamp.now()
        
        logger.info(f"Generated {len(df)} sample holdings records")
        return df
    
    def _get_sample_transactions(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Generate sample transaction history"""
        transactions_data = [
            {'date': '2024-01-15', 'symbol': 'AAPL', 'action': 'BUY', 'shares': 50.0, 'price': 145.00, 'fees': 9.99},
            {'date': '2024-02-01', 'symbol': 'AAPL', 'action': 'BUY', 'shares': 50.0, 'price': 155.00, 'fees': 9.99},
            {'date': '2024-01-20', 'symbol': 'GOOGL', 'action': 'BUY', 'shares': 50.0, 'price': 2600.00, 'fees': 9.99},
            {'date': '2024-02-15', 'symbol': 'MSFT', 'action': 'BUY', 'shares': 75.0, 'price': 380.00, 'fees': 9.99},
            {'date': '2024-03-01', 'symbol': 'TSLA', 'action': 'BUY', 'shares': 25.0, 'price': 800.00, 'fees': 9.99},
            {'date': '2024-03-15', 'symbol': 'AMZN', 'action': 'BUY', 'shares': 30.0, 'price': 3200.00, 'fees': 9.99},
            {'date': '2024-04-01', 'symbol': 'NVDA', 'action': 'BUY', 'shares': 40.0, 'price': 450.00, 'fees': 9.99},
            {'date': '2024-04-15', 'symbol': 'META', 'action': 'BUY', 'shares': 60.0, 'price': 300.00, 'fees': 9.99},
            {'date': '2024-05-01', 'symbol': 'NFLX', 'action': 'BUY', 'shares': 20.0, 'price': 400.00, 'fees': 9.99},
            {'date': '2024-06-01', 'symbol': 'AAPL', 'action': 'SELL', 'shares': 10.0, 'price': 180.00, 'fees': 9.99},
        ]
        
        df = pd.DataFrame(transactions_data)
        df['transaction_date'] = pd.to_datetime(df['date'])
        df['account_id'] = 'sample_account'
        df.drop('date', axis=1, inplace=True)
        
        # Filter by date range if provided
        if start_date:
            start_date = pd.to_datetime(start_date)
            df = df[df['transaction_date'] >= start_date]
        
        if end_date:
            end_date = pd.to_datetime(end_date)
            df = df[df['transaction_date'] <= end_date]
        
        logger.info(f"Generated {len(df)} sample transaction records")
        return df
    
    def _get_holdings_from_csv(self, file_path: str = "data/holdings.csv") -> pd.DataFrame:
        """Load holdings from CSV file"""
        try:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                logger.info(f"Loaded {len(df)} holdings from {file_path}")
                return df
            else:
                logger.warning(f"Holdings file not found: {file_path}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading holdings from CSV: {e}")
            return pd.DataFrame()
    
    def _get_transactions_from_csv(self, start_date: str = None, end_date: str = None,
                                   file_path: str = "data/transactions.csv") -> pd.DataFrame:
        """Load transactions from CSV file"""
        try:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['transaction_date'] = pd.to_datetime(df['transaction_date'])
                
                # Filter by date range if provided
                if start_date:
                    start_date = pd.to_datetime(start_date)
                    df = df[df['transaction_date'] >= start_date]
                
                if end_date:
                    end_date = pd.to_datetime(end_date)
                    df = df[df['transaction_date'] <= end_date]
                
                logger.info(f"Loaded {len(df)} transactions from {file_path}")
                return df
            else:
                logger.warning(f"Transactions file not found: {file_path}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading transactions from CSV: {e}")
            return pd.DataFrame()
    
    def _get_holdings_from_api(self) -> pd.DataFrame:
        """Fetch holdings from broker API (placeholder for production implementation)"""
        # TODO: Implement actual broker API integration
        # Examples: TD Ameritrade API, Interactive Brokers API, Alpaca API
        logger.info("API-based holdings fetch not implemented yet")
        return self._get_sample_holdings()
    
    def _get_transactions_from_api(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Fetch transactions from broker API (placeholder for production implementation)"""
        # TODO: Implement actual broker API integration
        logger.info("API-based transactions fetch not implemented yet")
        return self._get_sample_transactions(start_date, end_date)
    
    def validate_data(self, df: pd.DataFrame, data_type: str) -> bool:
        """Validate portfolio data quality"""
        if df.empty:
            logger.warning(f"Empty {data_type} data")
            return False
        
        if data_type == "holdings":
            required_columns = ['symbol', 'shares', 'avg_cost']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in holdings data: {required_columns}")
                return False
            
            # Check for negative shares or prices
            if (df['shares'] < 0).any() or (df['avg_cost'] <= 0).any():
                logger.error("Invalid negative shares or zero/negative prices in holdings")
                return False
        
        elif data_type == "transactions":
            required_columns = ['symbol', 'action', 'shares', 'price', 'transaction_date']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in transaction data: {required_columns}")
                return False
            
            # Check for valid actions
            valid_actions = ['BUY', 'SELL', 'DIVIDEND', 'SPLIT']
            if not df['action'].isin(valid_actions).all():
                logger.error(f"Invalid transaction actions. Must be one of: {valid_actions}")
                return False
        
        logger.info(f"Data validation passed for {data_type}")
        return True
