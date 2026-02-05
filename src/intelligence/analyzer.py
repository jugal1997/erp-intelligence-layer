"""
Intelligence Analyzer
Reads dbt marts and generates structured business alerts
"""

import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Alert:
    """Structured alert object for WhatsApp"""
    alert_type: str
    severity: str
    entity: str
    metric_value: float
    threshold: float
    message: str
    action: str
    timestamp: datetime


class IntelligenceAnalyzer:
    """Fetches mart data and generates actionable alerts"""
    
    def __init__(self, project_id: str, dataset: str = 'staging'):
        # Load .env from project root (2 levels up from this file)
        project_root = Path(__file__).parent.parent.parent
        dotenv_path = project_root / '.env'
        load_dotenv(dotenv_path)
        
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        print(f"🤖 Intelligence Analyzer initialized")
        print(f"   Project: {project_id}")
        print(f"   Dataset: {dataset}")
        
        if not credentials_path or not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials not found. Check .env file.")
        
        self.client = bigquery.Client.from_service_account_json(
            credentials_path, 
            project=project_id
        )
        self.project_id = project_id
        self.dataset = dataset


    
    def fetch_bleeding_wounds(self) -> Dict[str, pd.DataFrame]:
        """Fetch all critical mart data"""
        queries = {
            'dead_stock': f"""
                SELECT 
                    product_name as entity,
                    days_since_last_sale as metric_value,
                    alert_severity as severity,
                    recommended_action as action,
                    estimated_value_locked
                FROM `{self.project_id}.{self.dataset}.mart_dead_stock`
                WHERE alert_severity IN ('CRITICAL', 'HIGH')
                ORDER BY days_since_last_sale DESC
                LIMIT 10
            """,
            
            'low_margin': f"""
                SELECT 
                    CONCAT(customer_name, ' - ', product_name) as entity,
                    margin_percentage * 100 as metric_value,
                    alert_severity as severity,
                    probable_cause as action
                FROM `{self.project_id}.{self.dataset}.mart_low_margin_sales`
                WHERE alert_severity IN ('CRITICAL', 'HIGH')
                ORDER BY margin_percentage ASC
                LIMIT 10
            """,
            
            'credit_risk': f"""
                SELECT 
                    customer_name as entity,
                    total_overdue_amount as metric_value,
                    alert_severity as severity,
                    recommended_action as action,
                    payment_score
                FROM `{self.project_id}.{self.dataset}.mart_credit_risk`
                WHERE alert_severity IN ('CRITICAL', 'HIGH')
                ORDER BY total_overdue_amount DESC
                LIMIT 10
            """
        }
        
        results = {}
        for name, query in queries.items():
            try:
                df = self.client.query(query).to_dataframe()
                results[name] = df
                print(f"   📊 {name}: {len(df)} alerts")
            except Exception as e:
                print(f"   ❌ {name}: {str(e)}")
                results[name] = pd.DataFrame()
        
        return results
    
    def generate_alerts(self, data: Dict[str, pd.DataFrame]) -> List[Alert]:
        """Convert dataframes to structured alerts"""
        alerts = []
        
        # Dead stock alerts
        for _, row in data['dead_stock'].iterrows():
            alerts.append(Alert(
                alert_type='DEAD_STOCK',
                severity=row['severity'],
                entity=row['entity'],
                metric_value=float(row['metric_value']),
                threshold=90.0,
                message=f"{row['entity']} not sold for {row['metric_value']:.0f} days",
                action=row['action'],
                timestamp=datetime.now()
            ))
        
        # Low margin alerts
        for _, row in data['low_margin'].iterrows():
            alerts.append(Alert(
                alert_type='LOW_MARGIN',
                severity=row['severity'],
                entity=row['entity'],
                metric_value=float(row['metric_value']),
                threshold=10.0,
                message=f"{row['entity']} has {row['metric_value']:.1f}% margin",
                action=row['action'],
                timestamp=datetime.now()
            ))
        
        # Credit risk alerts
        for _, row in data['credit_risk'].iterrows():
            alerts.append(Alert(
                alert_type='CREDIT_RISK',
                severity=row['severity'],
                entity=row['entity'],
                metric_value=float(row['metric_value']),
                threshold=0.0,
                message=f"{row['entity']} owes ₹{row['metric_value']:,.0f}",
                action=row['action'],
                timestamp=datetime.now()
            ))
        
        # Sort by severity
        severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        alerts.sort(key=lambda x: (severity_order.get(x.severity, 99), -x.metric_value))
        
        return alerts[:10]  # Top 10 alerts


# Test function
def test_analyzer():
    """Test the analyzer with your data"""
    analyzer = IntelligenceAnalyzer('erp-intelligence-prod', 'staging')
    
    print("\n🔍 Fetching bleeding wounds...")
    data = analyzer.fetch_bleeding_wounds()
    
    print("\n🔔 Generating alerts...")
    alerts = analyzer.generate_alerts(data)
    
    print(f"\n📊 Summary: {len(alerts)} alerts found")
    for i, alert in enumerate(alerts, 1):
        emoji = {'CRITICAL': '🔴', 'HIGH': '🟠', 'MEDIUM': '🟡'}.get(alert.severity, '🟢')
        print(f"{i}. {emoji} {alert.alert_type}: {alert.entity}")
        print(f"   {alert.message}")
        print(f"   Action: {alert.action}\n")


if __name__ == "__main__":
    test_analyzer()
