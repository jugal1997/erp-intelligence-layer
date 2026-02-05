"""
WhatsApp Message Formatter
Converts alerts into mobile-optimized WhatsApp messages
"""

import os
import sys
from pathlib import Path
from typing import List, Dict
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.intelligence.analyzer import Alert


class WhatsAppMessageFormatter:
    """Formats alerts for WhatsApp (scannable, mobile-first)"""
    
    def __init__(self, business_name: str = "Your Business"):
        self.business_name = business_name
    
    def format_daily_digest(self, alerts_by_type: Dict[str, List[Alert]]) -> str:
        """Create daily digest message"""
        
        if not any(alerts_by_type.values()):
            return self._no_alerts_message()
        
        parts = [
            f"🚨 *{self.business_name} - Daily Alert*",
            f"📅 {datetime.now().strftime('%d %b %Y, %I:%M %p')}",
            "",
            "⚠️ *ACTION REQUIRED*",
            ""
        ]
        
        # Credit Risk Section (highest priority)
        if 'CREDIT_RISK' in alerts_by_type:
            parts.append("💳 *OVERDUE PAYMENTS*")
            for i, alert in enumerate(alerts_by_type['CREDIT_RISK'][:3], 1):
                emoji = self._severity_emoji(alert.severity)
                parts.append(
                    f"{emoji} {i}. {alert.entity}\n"
                    f"   └ Owes: ₹{alert.metric_value:,.0f}\n"
                    f"   └ {alert.action}"
                )
            parts.append("")
        
        # Dead Stock Section
        if 'DEAD_STOCK' in alerts_by_type:
            parts.append("📦 *DEAD STOCK*")
            for i, alert in enumerate(alerts_by_type['DEAD_STOCK'][:3], 1):
                emoji = self._severity_emoji(alert.severity)
                parts.append(
                    f"{emoji} {i}. {alert.entity}\n"
                    f"   └ Not sold: {int(alert.metric_value)} days\n"
                    f"   └ {alert.action}"
                )
            parts.append("")
        
        # Low Margin Section
        if 'LOW_MARGIN' in alerts_by_type:
            parts.append("💰 *LOW MARGIN SALES*")
            for i, alert in enumerate(alerts_by_type['LOW_MARGIN'][:3], 1):
                emoji = self._severity_emoji(alert.severity)
                parts.append(
                    f"{emoji} {i}. {alert.entity}\n"
                    f"   └ Margin: {alert.metric_value:.1f}%\n"
                    f"   └ {alert.action}"
                )
            parts.append("")
        
        parts.extend([
            "---",
            "💬 Reply HELP for support"
        ])
        
        return "\n".join(parts)
    
    def _severity_emoji(self, severity: str) -> str:
        """Get emoji for severity level"""
        return {
            'CRITICAL': '🔴',
            'HIGH': '🟠',
            'MEDIUM': '🟡',
            'LOW': '🟢'
        }.get(severity, '⚪')
    
    def _no_alerts_message(self) -> str:
        """Message when no alerts"""
        return (
            f"✅ *{self.business_name} - Daily Report*\n\n"
            f"No critical issues today!\n"
            f"All systems running smoothly 🎉"
        )


# Test
if __name__ == "__main__":
    from src.intelligence.analyzer import IntelligenceAnalyzer
    from collections import defaultdict
    
    print("📱 Testing WhatsApp Message Formatter...\n")
    
    # Get alerts
    analyzer = IntelligenceAnalyzer('erp-intelligence-prod', 'staging')
    data = analyzer.fetch_bleeding_wounds()
    alerts = analyzer.generate_alerts(data)
    
    # Batch by type
    batched = defaultdict(list)
    for alert in alerts:
        batched[alert.alert_type].append(alert)
    
    # Format message
    formatter = WhatsAppMessageFormatter("ABC Pumps Distributors")
    message = formatter.format_daily_digest(dict(batched))
    
    print("=" * 60)
    print(message)
    print("=" * 60)
    print(f"\n✅ Message ready! ({len(message)} characters)")
    print(f"📊 Total alerts: {len(alerts)}")
