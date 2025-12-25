#!/usr/bin/env python3
"""
Test script for NiFi Dataflow Manager
This script tests the functionality without actually performing destructive operations
"""

import sys
import os
from nifi_dataflow_manager import NiFiDataflowManager

def test_connection():
    """Test connection to NiFi and get dataflows list"""
    print("Testing NiFi connection and dataflow retrieval...")
    
    # Configuration - Update these values as needed
    nifi_url = 'https://nifiui.eucrm.cctools.capillarytech.com/nifi-api'
    username = 'tools-auser'
    password = 'e5zrttpWs0'
    process_group_id = '564cffc4-0173-1000-14d4-23a93d64c5f5'
    
    try:
        # Initialize the manager
        manager = NiFiDataflowManager(nifi_url, username, password, process_group_id)
        
        # Test getting dataflows
        dataflows = manager.get_all_dataflows()
        
        if dataflows:
            print(f"\n✅ Successfully connected to NiFi!")
            print(f"📊 Found {len(dataflows)} dataflows:")
            for i, dataflow in enumerate(dataflows, 1):
                name = dataflow.get('component', {}).get('name', 'Unknown')
                dataflow_id = dataflow.get('id', 'Unknown')
                state = dataflow.get('component', {}).get('state', 'Unknown')
                print(f"  {i}. {name} (ID: {dataflow_id}, State: {state})")
        else:
            print("⚠️  No dataflows found in the specified process group")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 NiFi Dataflow Manager Test")
    print("=" * 40)
    
    # Test connection and dataflow retrieval
    success = test_connection()
    
    if success:
        print("\n✅ Test completed successfully!")
        print("\nTo run the actual dataflow management:")
        print("python nifi_dataflow_manager.py")
    else:
        print("\n❌ Test failed. Please check your configuration.")
        sys.exit(1)

if __name__ == "__main__":
    main()

