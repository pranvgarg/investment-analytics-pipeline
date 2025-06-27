"""
Robust market data connector using professional-grade APIs
Supports Polygon.io for reliable market data with retry logic and proper error handling
"""
import os
import logging
import requests
import pandas as pd
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException, HTTPError, Timeout
from dataclasses import dataclass

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MarketDataPoint:
    """Structure for market data points"""
    symbol: str
    price: float
    volume: int
    timestamp: datetime
    source: str
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None

class MarketDataConnector:
    """Professional market data connector using Polygon.io"""
    
    def __init__(self):
        self.api_key = os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable not set")
        
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'Investment-Analytics-Pipeline/1.0'
        })
        
        # Rate limiting parameters
        self.requests_per_minute = 5  # Free tier limit
        self.last_request_time = 0
        
        logger.info("Initialized MarketDataConnector with Polygon.io")

    def _rate_limit(self):
        """Implement rate limiting to respect API limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60 / self.requests_per_minute
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((RequestException, HTTPError, Timeout))
    )
    def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Make HTTP request with retry logic and error handling"""
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-specific errors
            if data.get('status') == 'ERROR':
                error_msg = data.get('error', 'Unknown API error')
                logger.error(f"API Error: {error_msg}")
                raise HTTPError(f"Polygon API Error: {error_msg}")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for URL: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for URL: {url}, Status: {e.response.status_code}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for URL: {url}, Error: {e}")
            raise

    def get_last_trade(self, symbol: str) -> Optional[MarketDataPoint]:
        """Fetch the last trade for a single symbol"""
        try:
            url = f"{self.base_url}/v2/last/trade/{symbol}"
            logger.info(f"Fetching last trade for {symbol}")
            
            data = self._make_request(url)
            
            if 'results' not in data:
                logger.warning(f"No results found for symbol {symbol}")
                return None
            
            result = data['results']
            
            return MarketDataPoint(
                symbol=result['T'],
                price=float(result['p']),
                volume=int(result['s']),
                timestamp=pd.to_datetime(result['t'], unit='ns', utc=True),
                source='polygon'
            )
            
        except Exception as e:
            logger.error(f"Failed to fetch last trade for {symbol}: {e}")
            return None

    def get_daily_open_close(self, symbol: str, date: str) -> Optional[MarketDataPoint]:
        """Get daily OHLC data for a specific date"""
        try:
            url = f"{self.base_url}/v1/open-close/{symbol}/{date}"
            logger.info(f"Fetching daily OHLC for {symbol} on {date}")
            
            data = self._make_request(url)
            
            return MarketDataPoint(
                symbol=data['symbol'],
                price=float(data['close']),
                volume=int(data['volume']),
                timestamp=pd.to_datetime(date).tz_localize('UTC'),
                source='polygon',
                open_price=float(data['open']),
                high_price=float(data['high']),
                low_price=float(data['low']),
                close_price=float(data['close'])
            )
            
        except Exception as e:
            logger.error(f"Failed to fetch daily OHLC for {symbol}: {e}")
            return None

    def get_stock_prices(self, symbols: List[str]) -> pd.DataFrame:
        """Fetch current stock prices for multiple symbols"""
        logger.info(f"Fetching prices for {len(symbols)} symbols: {symbols}")
        
        market_data_points = []
        failed_symbols = []
        
        for symbol in symbols:
            data_point = self.get_last_trade(symbol)
            if data_point:
                market_data_points.append(data_point)
            else:
                failed_symbols.append(symbol)
        
        if failed_symbols:
            logger.warning(f"Failed to fetch data for symbols: {failed_symbols}")
        
        if not market_data_points:
            logger.error("No market data retrieved for any symbols")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'symbol': dp.symbol,
                'price': dp.price,
                'volume': dp.volume,
                'timestamp': dp.timestamp,
                'source': dp.source,
                'open_price': dp.open_price,
                'high_price': dp.high_price,
                'low_price': dp.low_price,
                'close_price': dp.close_price or dp.price,
                'adjusted_close': dp.price  # Assume no adjustment needed for current prices
            }
            for dp in market_data_points
        ])
        
        logger.info(f"Successfully retrieved data for {len(df)} symbols")
        return df

    def get_historical_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get historical aggregated data for a symbol"""
        try:
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            logger.info(f"Fetching historical data for {symbol} from {start_date} to {end_date}")
            
            data = self._make_request(url)
            
            if 'results' not in data or not data['results']:
                logger.warning(f"No historical data found for {symbol}")
                return pd.DataFrame()
            
            records = []
            for result in data['results']:
                records.append({
                    'symbol': symbol,
                    'timestamp': pd.to_datetime(result['t'], unit='ms', utc=True),
                    'open_price': float(result['o']),
                    'high_price': float(result['h']),
                    'low_price': float(result['l']),
                    'close_price': float(result['c']),
                    'price': float(result['c']),  # Use close as price
                    'volume': int(result['v']),
                    'source': 'polygon'
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} historical records for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return pd.DataFrame()

    def get_company_info(self, symbol: str) -> Dict:
        """Get company details/fundamentals"""
        try:
            url = f"{self.base_url}/v3/reference/tickers/{symbol}"
            logger.info(f"Fetching company info for {symbol}")
            
            data = self._make_request(url)
            
            if 'results' not in data:
                logger.warning(f"No company info found for {symbol}")
                return {}
            
            result = data['results']
            return {
                'symbol': result.get('ticker'),
                'name': result.get('name'),
                'market': result.get('market'),
                'locale': result.get('locale'),
                'primary_exchange': result.get('primary_exchange'),
                'type': result.get('type'),
                'currency_name': result.get('currency_name'),
                'description': result.get('description'),
                'homepage_url': result.get('homepage_url'),
                'total_employees': result.get('total_employees'),
                'market_cap': result.get('market_cap'),
                'share_class_shares_outstanding': result.get('share_class_shares_outstanding'),
                'weighted_shares_outstanding': result.get('weighted_shares_outstanding')
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch company info for {symbol}: {e}")
            return {}

    def validate_connection(self) -> bool:
        """Test API connectivity and authentication"""
        try:
            url = f"{self.base_url}/v2/last/trade/AAPL"
            self._make_request(url)
            logger.info("API connection validated successfully")
            return True
        except Exception as e:
            logger.error(f"API connection validation failed: {e}")
            return False

    def close(self):
        """Clean up resources"""
        if hasattr(self, 'session'):
            self.session.close()
            logger.info("Market data connector session closed")
