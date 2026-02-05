"""
Main Orchestrator
Runs the complete ERP Intelligence pipeline from end to end
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.intelligence.analyzer import IntelligenceAnalyzer
from src.delivery.message_formatter import WhatsAppMessageFormatter


class ERPIntelligenceOrchestrator:
    """
    Main orchestrator that runs the complete intelligence pipeline
    """
    
    def __init__(self):
        # Load environment
        dotenv_path = project_root / '.env'
        load_dotenv(dotenv_path)
        
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        self.dataset = os.getenv('BIGQUERY_DATASET_STAGING', 'staging')
        self.business_name = os.getenv('BUSINESS_NAME', 'Your Business')
        
        print("🚀 ERP Intelligence Orchestrator")
        print("=" * 60)
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏢 Business: {self.business_name}")
        print(f"📊 Project: {self.project_id}")
        print(f"🗄️  Dataset: {self.dataset}")
        print("=" * 60)
    
    def run_pipeline(self) -> str:
        """
        Execute complete intelligence pipeline
        Returns: Formatted WhatsApp message
        """
        try:
            # Step 1: Fetch data from BigQuery marts
            print("\n📊 STEP 1: Fetching data from BigQuery marts...")
            analyzer = IntelligenceAnalyzer(self.project_id, self.dataset)
            data = analyzer.fetch_bleeding_wounds()
            
            total_issues = sum(len(df) for df in data.values())
            print(f"   ✅ Found {total_issues} issues across all marts")
            
            # Step 2: Generate structured alerts
            print("\n🔔 STEP 2: Generating alerts...")
            alerts = analyzer.generate_alerts(data)
            print(f"   ✅ Generated {len(alerts)} prioritized alerts")
            
            if len(alerts) == 0:
                print("   ℹ️  No critical alerts to report today")
            
            # Step 3: Batch alerts by type
            print("\n📦 STEP 3: Organizing alerts...")
            batched_alerts = defaultdict(list)
            for alert in alerts:
                batched_alerts[alert.alert_type].append(alert)
            
            alert_summary = {k: len(v) for k, v in batched_alerts.items()}
            print(f"   ✅ Alert breakdown: {alert_summary}")
            
            # Step 4: Format WhatsApp message
            print("\n📱 STEP 4: Formatting WhatsApp message...")
            formatter = WhatsAppMessageFormatter(self.business_name)
            message = formatter.format_daily_digest(dict(batched_alerts))
            print(f"   ✅ Message ready ({len(message)} characters)")
            
            # Step 5: Display final message
            print("\n" + "=" * 60)
            print("📱 WHATSAPP MESSAGE")
            print("=" * 60)
            print(message)
            print("=" * 60)
            
            # Summary
            print(f"\n✅ Pipeline completed successfully!")
            print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return message
            
        except Exception as e:
            print(f"\n❌ Pipeline failed!")
            print(f"   Error: {str(e)}")
            raise


def main():
    """Entry point for orchestrator"""
    orchestrator = ERPIntelligenceOrchestrator()
    
    try:
        message = orchestrator.run_pipeline()
        
        # Future: Send via WhatsApp
        print("\n💡 Next steps:")
        print("   - Copy message above and send via WhatsApp")
        print("   - Or set up Twilio to send automatically")
        
    except Exception as e:
        print(f"\n❌ Orchestrator failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
