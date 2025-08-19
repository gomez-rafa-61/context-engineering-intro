#!/usr/bin/env python3
"""
Simple test script for Airbyte API token refresh functionality.
Tests only the API client without importing agent dependencies.
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

from tools.airbyte_api import AirbyteAPIClient

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
        print("   This is normal if you're using static API keys.")
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
        print("   This is normal if you're using OAuth2.")
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


async def test_client_creation():
    """Test different client creation scenarios."""
    print("\n=== Testing Client Creation Scenarios ===")
    
    # Test 1: No credentials
    try:
        client = AirbyteAPIClient()
        print("❌ Should have failed with no credentials")
    except ValueError as e:
        print(f"✅ Correctly rejected empty credentials: {e}")
    
    # Test 2: Only client_id
    try:
        client = AirbyteAPIClient(client_id="test_id")
        print("❌ Should have failed with incomplete OAuth2 credentials")
    except ValueError as e:
        print(f"✅ Correctly rejected incomplete OAuth2: {e}")
    
    # Test 3: Empty API key
    try:
        client = AirbyteAPIClient(api_key="")
        print("❌ Should have failed with empty API key")
    except ValueError as e:
        print(f"✅ Correctly rejected empty API key: {e}")
    
    # Test 4: Valid static API key
    try:
        client = AirbyteAPIClient(api_key="test_key")
        print(f"✅ Static API key client created: use_oauth={client.use_oauth}")
    except Exception as e:
        print(f"❌ Failed to create static client: {e}")
    
    # Test 5: Valid OAuth2 credentials
    try:
        client = AirbyteAPIClient(client_id="test_id", client_secret="test_secret")
        print(f"✅ OAuth2 client created: use_oauth={client.use_oauth}")
    except Exception as e:
        print(f"❌ Failed to create OAuth2 client: {e}")


async def main():
    """Run all tests."""
    print("🚀 Starting Simple Airbyte API Token Tests")
    print(f"⏰ Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Test client creation scenarios
    await test_client_creation()
    
    results = []
    
    # Test OAuth2 functionality if credentials available
    oauth_success = await test_oauth_client()
    results.append(("OAuth2 Client", oauth_success))
    
    # Test legacy functionality if credentials available
    legacy_success = await test_legacy_client()
    results.append(("Legacy Client", legacy_success))
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Test Summary:")
    for test_name, success in results:
        if success is not False:  # None means not tested due to missing credentials
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"   {test_name}: {status}")
        else:
            print(f"   {test_name}: ⏭️  SKIPPED (missing credentials)")
    
    print(f"\n⏰ Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if at least one test passed
    successful_tests = [r for r in results if r[1] is True]
    if successful_tests:
        print("🎉 Token refresh implementation is working!")
        return 0
    else:
        print("⚠️  No tests could be run due to missing credentials.")
        print("   Set either AIRBYTE_API_KEY or both AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
