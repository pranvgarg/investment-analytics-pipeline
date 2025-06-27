"""
Data quality checking framework for investment analytics pipeline
"""
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from src.database.connection import get_db_connection

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Comprehensive data quality validation for investment data"""
    
    def __init__(self):
        self.db = get_db_connection()
        self.quality_thresholds = {
            'freshness_hours': 24,
            'price_validity_min': 0.01,
            'price_validity_max': 50000,
            'accuracy_threshold': 95.0
        }
    
    def check_market_data_freshness(self) -> Dict:
        """Check if market data is recent enough"""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN timestamp >= NOW() - INTERVAL '%s hours' THEN 1 END) as fresh_records
        FROM raw_market_data
        WHERE timestamp >= CURRENT_DATE - INTERVAL '2 days'
        """
        
        try:
            result = self.db.execute_query(
                query % self.quality_thresholds['freshness_hours']
            ).iloc[0]
            
            total = result['total_records']
            fresh = result['fresh_records']
            accuracy = (fresh / total * 100) if total > 0 else 0
            
            check_result = {
                'check_name': 'market_data_freshness',
                'passed': accuracy >= self.quality_thresholds['accuracy_threshold'],
                'accuracy': accuracy,
                'total_records': total,
                'fresh_records': fresh,
                'threshold_hours': self.quality_thresholds['freshness_hours']
            }
            
            logger.info(f"Market data freshness check: {accuracy:.2f}% fresh records")
            return check_result
            
        except Exception as e:
            logger.error(f"Error in freshness check: {e}")
            return self._error_check_result('market_data_freshness', str(e))
    
    def check_price_validity(self) -> Dict:
        """Check for valid price ranges and outliers"""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN price BETWEEN %s AND %s THEN 1 END) as valid_prices,
            COUNT(CASE WHEN price <= 0 THEN 1 END) as zero_negative_prices,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM raw_market_data
        WHERE timestamp >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        try:
            result = self.db.execute_query(query, {
                'min_price': self.quality_thresholds['price_validity_min'],
                'max_price': self.quality_thresholds['price_validity_max']
            }).iloc[0]
            
            total = result['total_records']
            valid = result['valid_prices']
            accuracy = (valid / total * 100) if total > 0 else 0
            
            check_result = {
                'check_name': 'price_validity',
                'passed': accuracy >= 99.0,  # Higher threshold for price validity
                'accuracy': accuracy,
                'total_records': total,
                'valid_records': valid,
                'zero_negative_prices': result['zero_negative_prices'],
                'price_stats': {
                    'avg': float(result['avg_price']) if result['avg_price'] else 0,
                    'min': float(result['min_price']) if result['min_price'] else 0,
                    'max': float(result['max_price']) if result['max_price'] else 0
                }
            }
            
            logger.info(f"Price validity check: {accuracy:.2f}% valid prices")
            return check_result
            
        except Exception as e:
            logger.error(f"Error in price validity check: {e}")
            return self._error_check_result('price_validity', str(e))
    
    def check_portfolio_consistency(self) -> Dict:
        """Check portfolio holdings for consistency"""
        query = """
        SELECT 
            COUNT(*) as total_holdings,
            COUNT(CASE WHEN shares > 0 THEN 1 END) as positive_shares,
            COUNT(CASE WHEN avg_cost > 0 THEN 1 END) as positive_costs,
            COUNT(DISTINCT symbol) as unique_symbols,
            SUM(CASE WHEN shares * avg_cost > 1000000 THEN 1 ELSE 0 END) as large_positions
        FROM raw_portfolio_holdings
        WHERE shares > 0
        """
        
        try:
            result = self.db.execute_query(query).iloc[0]
            
            total = result['total_holdings']
            valid_shares = result['positive_shares']
            valid_costs = result['positive_costs']
            
            # Check if all holdings have positive shares and costs
            consistency_score = (
                (valid_shares + valid_costs) / (2 * total) * 100
            ) if total > 0 else 0
            
            check_result = {
                'check_name': 'portfolio_consistency',
                'passed': consistency_score >= 99.0,
                'accuracy': consistency_score,
                'total_holdings': total,
                'unique_symbols': result['unique_symbols'],
                'large_positions': result['large_positions'],
                'issues': {
                    'negative_shares': total - valid_shares,
                    'negative_costs': total - valid_costs
                }
            }
            
            logger.info(f"Portfolio consistency check: {consistency_score:.2f}% consistent")
            return check_result
            
        except Exception as e:
            logger.error(f"Error in portfolio consistency check: {e}")
            return self._error_check_result('portfolio_consistency', str(e))
    
    def check_transaction_integrity(self) -> Dict:
        """Check transaction data integrity"""
        query = """
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(CASE WHEN action IN ('BUY', 'SELL', 'DIVIDEND', 'SPLIT') THEN 1 END) as valid_actions,
            COUNT(CASE WHEN shares > 0 THEN 1 END) as positive_shares,
            COUNT(CASE WHEN price > 0 THEN 1 END) as positive_prices,
            COUNT(CASE WHEN transaction_date <= CURRENT_DATE THEN 1 END) as valid_dates
        FROM raw_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 year'
        """
        
        try:
            result = self.db.execute_query(query).iloc[0]
            
            total = result['total_transactions']
            valid_components = (
                result['valid_actions'] + 
                result['positive_shares'] + 
                result['positive_prices'] + 
                result['valid_dates']
            )
            
            integrity_score = (valid_components / (4 * total) * 100) if total > 0 else 0
            
            check_result = {
                'check_name': 'transaction_integrity',
                'passed': integrity_score >= 98.0,
                'accuracy': integrity_score,
                'total_transactions': total,
                'validation_details': {
                    'valid_actions': result['valid_actions'],
                    'positive_shares': result['positive_shares'],
                    'positive_prices': result['positive_prices'],
                    'valid_dates': result['valid_dates']
                }
            }
            
            logger.info(f"Transaction integrity check: {integrity_score:.2f}% valid")
            return check_result
            
        except Exception as e:
            logger.error(f"Error in transaction integrity check: {e}")
            return self._error_check_result('transaction_integrity', str(e))
    
    def check_data_completeness(self) -> Dict:
        """Check for missing data across all tables"""
        queries = {
            'market_data': "SELECT COUNT(*) FROM raw_market_data WHERE timestamp >= CURRENT_DATE",
            'portfolio_holdings': "SELECT COUNT(*) FROM raw_portfolio_holdings WHERE shares > 0",
            'transactions': "SELECT COUNT(*) FROM raw_transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'"
        }
        
        try:
            completeness_results = {}
            for table, query in queries.items():
                count = self.db.execute_query(query).iloc[0, 0]
                completeness_results[table] = count
            
            # Determine if we have minimum required data
            min_requirements = {
                'market_data': 1,  # At least 1 price record today
                'portfolio_holdings': 1,  # At least 1 holding
                'transactions': 0  # Transactions are optional
            }
            
            passed_checks = sum(
                1 for table, count in completeness_results.items()
                if count >= min_requirements.get(table, 0)
            )
            
            completeness_score = (passed_checks / len(min_requirements)) * 100
            
            check_result = {
                'check_name': 'data_completeness',
                'passed': completeness_score >= 66.0,  # At least 2/3 tables have data
                'accuracy': completeness_score,
                'table_counts': completeness_results,
                'min_requirements': min_requirements
            }
            
            logger.info(f"Data completeness check: {completeness_score:.2f}% complete")
            return check_result
            
        except Exception as e:
            logger.error(f"Error in completeness check: {e}")
            return self._error_check_result('data_completeness', str(e))
    
    def run_all_checks(self, dag_run_id: str = None, task_id: str = None) -> Dict:
        """Run all quality checks and save results"""
        checks = [
            self.check_market_data_freshness(),
            self.check_price_validity(),
            self.check_portfolio_consistency(),
            self.check_transaction_integrity(),
            self.check_data_completeness()
        ]
        
        # Save results to database
        for check in checks:
            self.save_quality_check_result(check, dag_run_id, task_id)
        
        # Calculate overall quality score
        passed_checks = sum(1 for check in checks if check['passed'])
        overall_score = (passed_checks / len(checks)) * 100
        
        summary = {
            'overall_score': overall_score,
            'total_checks': len(checks),
            'passed_checks': passed_checks,
            'failed_checks': len(checks) - passed_checks,
            'individual_results': {check['check_name']: check for check in checks},
            'timestamp': datetime.now()
        }
        
        logger.info(f"Data quality summary: {overall_score:.1f}% ({passed_checks}/{len(checks)} checks passed)")
        return summary
    
    def save_quality_check_result(self, check_result: Dict, dag_run_id: str = None, task_id: str = None):
        """Save quality check results to database"""
        try:
            insert_query = """
            INSERT INTO data_quality_checks 
            (table_name, check_name, check_result, total_records, accuracy_percentage, 
             error_details, dag_run_id, task_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Determine table name from check name
            table_mapping = {
                'market_data_freshness': 'raw_market_data',
                'price_validity': 'raw_market_data',
                'portfolio_consistency': 'raw_portfolio_holdings',
                'transaction_integrity': 'raw_transactions',
                'data_completeness': 'all_tables'
            }
            
            table_name = table_mapping.get(check_result['check_name'], 'unknown')
            total_records = check_result.get('total_records', 0)
            error_details = check_result.get('error_message', '')
            
            self.db.execute_statement(insert_query, {
                'table_name': table_name,
                'check_name': check_result['check_name'],
                'check_result': check_result['passed'],
                'total_records': total_records,
                'accuracy_percentage': check_result['accuracy'],
                'error_details': error_details,
                'dag_run_id': dag_run_id,
                'task_id': task_id
            })
            
        except Exception as e:
            logger.error(f"Error saving quality check result: {e}")
    
    def _error_check_result(self, check_name: str, error_message: str) -> Dict:
        """Create error result for failed checks"""
        return {
            'check_name': check_name,
            'passed': False,
            'accuracy': 0.0,
            'total_records': 0,
            'error_message': error_message
        }
    
    def get_quality_history(self, days: int = 7) -> pd.DataFrame:
        """Get quality check history for the last N days"""
        query = """
        SELECT 
            check_name,
            check_result,
            accuracy_percentage,
            total_records,
            check_timestamp,
            table_name
        FROM data_quality_checks
        WHERE check_timestamp >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY check_timestamp DESC
        """
        
        try:
            return self.db.execute_query(query % days)
        except Exception as e:
            logger.error(f"Error fetching quality history: {e}")
            return pd.DataFrame()
