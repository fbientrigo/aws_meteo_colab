import sys
import os

# Add frontend directory to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_imports():
    print("Testing imports...")
    try:
        import streamlit
        import pandas
        import requests
        import pydeck
        from src.api_client import MeteoAPI
        from src.config import Config
        print("✅ Imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        sys.exit(1)

def test_class_instantiation():
    print("Testing class instantiation...")
    try:
        from src.api_client import MeteoAPI
        # We don't need a running API to instantiate the client
        client = MeteoAPI()
        assert client.base_url is not None
        print("✅ MeteoAPI instantiated successfully")
    except Exception as e:
        print(f"❌ Class instantiation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("Starting Smoke Test...")
    test_imports()
    test_class_instantiation()
    print("✅ Frontend Environment & Logic Check Passed")
