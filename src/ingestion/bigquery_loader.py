"""
BigQuery Loader - Upload CSV files to BigQuery
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from typing import Optional
import yaml


class BigQueryLoader:
    """
    Handles uploading data to BigQuery from CSV files
    """
    
    def __init__(self):
        """Initialize BigQuery client"""
        load_dotenv()
        
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        self.dataset_raw = os.getenv('BIGQUERY_DATASET_RAW', 'raw_erp')
        self.client = bigquery.Client(project=self.project_id)
        
        print(f"📊 BigQuery Loader initialized")
        print(f"   Project: {self.project_id}")
        print(f"   Dataset: {self.dataset_raw}")
    
    def load_csv_to_bigquery(
        self, 
        csv_path: str, 
        table_name: str,
        if_exists: str = 'replace'
    ) -> bool:
        """
        Load CSV file to BigQuery
        
        Args:
            csv_path: Path to CSV file
            table_name: Name of BigQuery table to create
            if_exists: 'replace' or 'append'
            
        Returns:
            bool: Success status
        """
        try:
            print(f"\n🔄 Loading CSV to BigQuery...")
            print(f"   File: {csv_path}")
            print(f"   Table: {self.dataset_raw}.{table_name}")
            
            # Read CSV
            df = pd.read_csv(csv_path)
            print(f"   Rows: {len(df)}")
            print(f"   Columns: {list(df.columns)}")
            
            # Define table reference
            table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE 
                    if if_exists == 'replace' 
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
                autodetect=True,  # Auto-detect schema
            )
            
            # Load data
            job = self.client.load_table_from_dataframe(
                df, 
                table_id, 
                job_config=job_config
            )
            
            # Wait for completion
            job.result()
            
            print(f"✅ Data loaded successfully!")
            print(f"   Table: {table_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to load data!")
            print(f"   Error: {str(e)}")
            return False
    
    def query_table(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """
        Query a table and return results
        
        Args:
            table_name: Table to query
            limit: Number of rows to return
            
        Returns:
            DataFrame with results
        """
        query = f"""
            SELECT * 
            FROM `{self.project_id}.{self.dataset_raw}.{table_name}`
            LIMIT {limit}
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            return df
        except Exception as e:
            print(f"❌ Query failed: {str(e)}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False


# Example usage
if __name__ == "__main__":
    loader = BigQueryLoader()
    
    # Test with sample template
    sample_file = "data/sample_inputs/sample_sales_template.csv"
    
    if os.path.exists(sample_file):
        print(f"\n📝 Testing with sample file...")
        success = loader.load_csv_to_bigquery(
            csv_path=sample_file,
            table_name="sales_raw"
        )
        
        if success:
            print(f"\n📊 Querying loaded data...")
            df = loader.query_table("sales_raw")
            print(df)
    else:
        print(f"❌ Sample file not found: {sample_file}")
        print(f"   Create it first or provide a different CSV")
