"""
Automated CSV Cleaner
Intelligently cleans and standardizes ERP data from any source
"""

import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from typing import Dict, List, Tuple
from fuzzywuzzy import fuzz, process
from datetime import datetime


class AutomatedCSVCleaner:
    """
    Smart CSV cleaner that auto-detects ERP format and cleans data
    """
    
    def __init__(self):
        # Load universal schema
        config_path = Path(__file__).parent.parent.parent / 'config/universal_schema.yaml'
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Load all ERP mappings
        self.erp_mappings = self.config['erp_mappings']
        
        # Expected fields from universal schema
        self.universal_fields = self.config['entities']['sales_transactions']['fields']
        self.required_fields = [
            'transaction_id', 'transaction_date', 'customer_name', 
            'product_name', 'quantity', 'unit_price', 'total_amount'
        ]
        
        print("🧹 Automated CSV Cleaner initialized")
        print(f"   Supported ERPs: {list(self.erp_mappings.keys())}")
    
    def detect_erp_system(self, df: pd.DataFrame) -> str:
        """
        Auto-detect which ERP system the CSV is from
        Based on column name matching
        """
        columns = df.columns.tolist()
        scores = {}
        
        for erp_name, mapping in self.erp_mappings.items():
            # Count how many expected columns are present
            expected_cols = list(mapping['sales_transactions'].values())
            matches = sum(1 for col in expected_cols if col in columns)
            scores[erp_name] = matches
        
        detected = max(scores, key=scores.get)
        confidence = scores[detected] / len(self.erp_mappings[detected]['sales_transactions'])
        
        print(f"\n🔍 ERP Detection:")
        print(f"   Detected: {detected.upper()} (confidence: {confidence*100:.0f}%)")
        
        if confidence < 0.5:
            print(f"   ⚠️  Low confidence - may need manual column mapping")
        
        return detected
    
    def fuzzy_match_columns(self, df_columns: List[str], expected_mapping: Dict) -> Dict:
        """
        Fuzzy match actual columns to expected columns
        Handles typos and variations
        """
        matched = {}
        unmatched = []
        
        for universal_name, expected_name in expected_mapping.items():
            # Try exact match first
            if expected_name in df_columns:
                matched[expected_name] = universal_name
                continue
            
            # Fuzzy match
            best_match = process.extractOne(
                expected_name, 
                df_columns, 
                scorer=fuzz.token_sort_ratio,
                score_cutoff=70
            )
            
            if best_match:
                matched[best_match[0]] = universal_name
                if best_match[1] < 100:
                    print(f"   📝 Fuzzy matched: '{best_match[0]}' → '{universal_name}' ({best_match[1]}% match)")
            else:
                unmatched.append(universal_name)
        
        if unmatched:
            print(f"   ⚠️  Could not match: {unmatched}")
        
        return matched
    
    def clean_dates(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """Clean and standardize date formats"""
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Remove future dates
            future_dates = df[date_col] > pd.Timestamp.now()
            if future_dates.any():
                print(f"   ⚠️  Removed {future_dates.sum()} rows with future dates")
                df = df[~future_dates]
            
            # Convert to YYYY-MM-DD format
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            
        except Exception as e:
            print(f"   ❌ Date cleaning error: {e}")
        
        return df
    
    def clean_numeric(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Clean numeric columns"""
        # Remove currency symbols, commas
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace('₹', '').str.replace(',', '').str.strip()
        
        # Convert to numeric
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def validate_data_quality(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Validate data quality and return cleaned df + quality report
        """
        report = {
        'original_rows': len(df),
        'issues_found': [],
        'rows_removed': 0
        }
    
        original_len = len(df)
    
        # Remove duplicates (only if transaction_id exists)
        if 'transaction_id' in df.columns:
            duplicates = df.duplicated(subset=['transaction_id'])
            if duplicates.any():
                df = df[~duplicates]
                report['issues_found'].append(f"Removed {duplicates.sum()} duplicate transactions")
        
        # Remove rows with null required fields (only check fields that exist)
        for field in self.required_fields:
            if field in df.columns:
                nulls = df[field].isna()
                if nulls.any():
                    df = df[~nulls]
                    report['issues_found'].append(f"Removed {nulls.sum()} rows with null {field}")
        
        # Validate quantities > 0 (only if column exists)
        if 'quantity' in df.columns:
            invalid_qty = (df['quantity'] <= 0) | (df['quantity'].isna())
            if invalid_qty.any():
                df = df[~invalid_qty]
                report['issues_found'].append(f"Removed {invalid_qty.sum()} rows with invalid quantity")
        
        # Validate prices > 0 (only if column exists)
        if 'unit_price' in df.columns:
            invalid_price = (df['unit_price'] <= 0) | (df['unit_price'].isna())
            if invalid_price.any():
                df = df[~invalid_price]
                report['issues_found'].append(f"Removed {invalid_price.sum()} rows with invalid price")
        
        # Check margin (if both columns exist)
        if 'cost_price' in df.columns and 'unit_price' in df.columns:
            negative_margin = (df['cost_price'] > df['unit_price']) & df['cost_price'].notna()
            if negative_margin.any():
                report['issues_found'].append(f"⚠️  {negative_margin.sum()} rows have selling price < cost price")
        
        # Check for missing critical fields
        missing_fields = [f for f in self.required_fields if f not in df.columns]
        if missing_fields:
            report['issues_found'].append(f"⚠️  Missing fields: {missing_fields}")
        
        report['rows_removed'] = original_len - len(df)
        report['final_rows'] = len(df)
        
        if report['final_rows'] == 0:
            raise ValueError("❌ All rows were removed during cleaning. Check your data quality!")
        
        return df, report

    
    def clean_csv(
        self, 
        input_path: str, 
        output_path: str,
        erp_system: str = None
    ) -> pd.DataFrame:
        """
        Main cleaning function - one command to clean any CSV
        """
        print(f"\n{'='*60}")
        print(f"🧹 CLEANING CSV: {Path(input_path).name}")
        print(f"{'='*60}")
        
        # Read CSV
        df = pd.read_csv(input_path)
        print(f"\n📊 Input Data:")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Column names: {list(df.columns)[:5]}{'...' if len(df.columns) > 5 else ''}")
        
        # Detect ERP system
        if not erp_system:
            erp_system = self.detect_erp_system(df)
        
        # Get mapping for detected ERP
        erp_mapping = self.erp_mappings[erp_system]['sales_transactions']
        
        # Fuzzy match columns
        print(f"\n🔗 Mapping Columns:")
        column_mapping = self.fuzzy_match_columns(df.columns.tolist(), erp_mapping)
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Clean dates
        if 'transaction_date' in df.columns:
            print(f"\n📅 Cleaning Dates:")
            df = self.clean_dates(df, 'transaction_date')
        
        # Clean numeric columns
        print(f"\n🔢 Cleaning Numbers:")
        numeric_cols = ['quantity', 'unit_price', 'cost_price', 'total_amount', 'tax_amount']
        for col in numeric_cols:
            if col in df.columns:
                df = self.clean_numeric(df, col)
                print(f"   ✓ {col}")
        
        # Validate and clean
        print(f"\n✅ Validating Data Quality:")
        df, report = self.validate_data_quality(df)
        
        for issue in report['issues_found']:
            print(f"   • {issue}")
        
        # Add metadata columns
        df['loaded_at'] = datetime.now().isoformat()
        df['source_file'] = Path(input_path).name
        
        # Save cleaned CSV
        df.to_csv(output_path, index=False)
        
        # Final report
        print(f"\n{'='*60}")
        print(f"✅ CLEANING COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Summary:")
        print(f"   Input rows:     {report['original_rows']:,}")
        print(f"   Removed rows:   {report['rows_removed']:,}")
        print(f"   Final rows:     {report['final_rows']:,}")
        print(f"   Data quality:   {(report['final_rows']/report['original_rows']*100):.1f}%")
        print(f"\n💾 Output saved: {output_path}")
        print(f"{'='*60}\n")
        
        return df


# CLI interface
def main():
    """Command-line interface for cleaning CSVs"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python csv_cleaner.py <input_csv> [output_csv] [erp_system]")
        print("\nExample:")
        print("  python csv_cleaner.py data/sample_inputs/sales.csv")
        print("  python csv_cleaner.py sales.csv cleaned_sales.csv gofrugal")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.csv', '_cleaned.csv')
    erp_system = sys.argv[3] if len(sys.argv) > 3 else None
    
    cleaner = AutomatedCSVCleaner()
    cleaner.clean_csv(input_file, output_file, erp_system)


if __name__ == "__main__":
    main()
