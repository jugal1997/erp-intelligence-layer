"""
Test BigQuery connection
Run this to verify your credentials work
"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

def test_connection():
    """Test BigQuery connection"""
    
    # Load environment variables
    load_dotenv()
    
    # Get project ID
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    
    print(f"🔄 Testing connection to BigQuery...")
    print(f"   Project: {project_id}")
    
    try:
        # Create BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Run a simple query
        query = """
            SELECT 
                'Connection successful!' as message,
                CURRENT_TIMESTAMP() as timestamp
        """
        
        results = client.query(query).result()
        
        for row in results:
            print(f"\n✅ {row.message}")
            print(f"   Timestamp: {row.timestamp}")
        
        # List datasets
        print(f"\n📊 Available datasets:")
        datasets = list(client.list_datasets())
        
        if datasets:
            for dataset in datasets:
                print(f"   - {dataset.dataset_id}")
        else:
            print("   No datasets found (this is OK if you just created the project)")
        
        print(f"\n✅ BigQuery connection test PASSED!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test FAILED!")
        print(f"   Error: {str(e)}")
        print(f"\n🔧 Troubleshooting:")
        print(f"   1. Check that GOOGLE_CLOUD_PROJECT in .env matches your GCP project ID")
        print(f"   2. Verify credentials/gcp-service-account.json exists")
        print(f"   3. Make sure BigQuery API is enabled in GCP")
        return False

if __name__ == "__main__":
    test_connection()
