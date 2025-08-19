#!/usr/bin/env python3
"""
Test script for Airbyte API token refresh functionality.

This script demonstrates how to use the updated AirbyteAPIClient with OAuth2 token refresh.
"""

import asyncio
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the parent directory to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.airbyte_api import AirbyteAPIClient, get_airbyte_job_status
from agents.dependencies import AirbyteDependencies

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_oauth_client():
    """Test OAuth2 token refresh functionality."""
    print("=== Testing OAuth2 Token Refresh ===")
    
    client_id = os.getenv("AIRBYTE_CLIENT_ID")
    client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")
    workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID")
    
    if not client_id or not client_secret:
        print("❌ OAuth2 credentials not found. Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET.")
        return False
    
    try:
        # Create client with OAuth2 credentials
        client = AirbyteAPIClient(
            client_id=client_id,
            client_secret=client_secret
        )
        
        print(f"✅ OAuth2 client created successfully")
        print(f"   Client ID: {client_id[:8]}...")
        print(f"   Use OAuth: {client.use_oauth}")
        
        # Test token refresh
        print("\n📋 Testing token refresh...")
        token = await client._refresh_token()
        print(f"✅ Token refreshed successfully: {token[:20]}...")
        
        # Test API call
        print("\n🔗 Testing connections retrieval...")
        connections = await client.get_connections(workspace_id)
        print(f"✅ Retrieved {len(connections)} connections")
        
        return True
        
    except Exception as e:
        print(f"❌ OAuth2 test failed: {e}")
        return False


async def test_legacy_client():
    """Test legacy static API key functionality."""
    print("\n=== Testing Legacy API Key ===")
    
    api_key = os.getenv("AIRBYTE_API_KEY")
    workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID")
    
    if not api_key:
        print("❌ Static API key not found. Set AIRBYTE_API_KEY.")
        return False
    
    try:
        # Create client with static API key
        client = AirbyteAPIClient(api_key=api_key)
        
        print(f"✅ Legacy client created successfully")
        print(f"   API Key: {api_key[:20]}...")
        print(f"   Use OAuth: {client.use_oauth}")
        
        # Test API call
        print("\n🔗 Testing connections retrieval...")
        connections = await client.get_connections(workspace_id)
        print(f"✅ Retrieved {len(connections)} connections")
        
        return True
        
    except Exception as e:
        print(f"❌ Legacy test failed: {e}")
        return False


async def test_convenience_functions():
    """Test the convenience functions with both auth methods."""
    print("\n=== Testing Convenience Functions ===")
    
    workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID")
    
    # Test with OAuth2 if available
    client_id = os.getenv("AIRBYTE_CLIENT_ID")
    client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")
    
    if client_id and client_secret:
        try:
            print("\n📊 Testing job status with OAuth2...")
            job_records = await get_airbyte_job_status(
                client_id=client_id,
                client_secret=client_secret,
                workspace_id=workspace_id,
                limit=5
            )
            print(f"✅ Retrieved {len(job_records)} job records with OAuth2")
            
        except Exception as e:
            print(f"❌ OAuth2 convenience function test failed: {e}")
    
    # Test with API key if available
    api_key = os.getenv("AIRBYTE_API_KEY")
    if api_key:
        try:
            print("\n📊 Testing job status with API key...")
            job_records = await get_airbyte_job_status(
                api_key=api_key,
                workspace_id=workspace_id,
                limit=5
            )
            print(f"✅ Retrieved {len(job_records)} job records with API key")
            
        except Exception as e:
            print(f"❌ API key convenience function test failed: {e}")


async def test_dependencies():
    """Test the updated dependencies classes."""
    print("\n=== Testing Dependencies ===")
    
    try:
        # Test OAuth2 dependencies
        client_id = os.getenv("AIRBYTE_CLIENT_ID")
        client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")
        
        if client_id and client_secret:
            oauth_deps = AirbyteDependencies.from_oauth(
                client_id=client_id,
                client_secret=client_secret,
                workspace_id=os.getenv("AIRBYTE_WORKSPACE_ID")
            )
            print(f"✅ OAuth2 dependencies created: client_id={oauth_deps.client_id[:8]}...")
        
        # Test legacy dependencies
        api_key = os.getenv("AIRBYTE_API_KEY")
        if api_key:
            legacy_deps = AirbyteDependencies(
                api_key=api_key,
                workspace_id=os.getenv("AIRBYTE_WORKSPACE_ID")
            )
            print(f"✅ Legacy dependencies created: api_key={legacy_deps.api_key[:8]}...")
        
        print("✅ Dependencies testing completed")
        
    except Exception as e:
        print(f"❌ Dependencies test failed: {e}")


async def main():
    """Run all tests."""
    print("🚀 Starting Airbyte API Token Refresh Tests")
    print(f"⏰ Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    results = []
    
    # Test OAuth2 functionality
    oauth_success = await test_oauth_client()
    results.append(("OAuth2 Client", oauth_success))
    
    # Test legacy functionality  
    legacy_success = await test_legacy_client()
    results.append(("Legacy Client", legacy_success))
    
    # Test convenience functions
    await test_convenience_functions()
    
    # Test dependencies
    await test_dependencies()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Test Summary:")
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"   {test_name}: {status}")
    
    print(f"\n⏰ Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if all(result[1] for result in results):
        print("🎉 All tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed. Check the logs above.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
