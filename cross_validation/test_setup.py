"""
Test script to verify cross-validation system setup.
Run this to check if all dependencies and configurations are correct.
"""

import os
import sys


def check_environment_variables():
    """Check if required environment variables are set."""
    print("\n" + "="*60)
    print("CHECKING ENVIRONMENT VARIABLES")
    print("="*60)
    
    required_vars = {
        "OPENAI_API_KEY": "OpenAI API key for GPT-4o",
        "DB_HOST": "Database host",
        "DB_USER": "Database user",
        "DB_PASSWORD": "Database password",
        "DB_NAME": "Database name",
        "AWS_ACCESS_KEY_ID": "AWS access key",
        "AWS_SECRET_ACCESS_KEY": "AWS secret key",
    }
    
    all_set = True
    for var, description in required_vars.items():
        value = os.getenv(var)
        if value:
            masked = value[:4] + "..." if len(value) > 4 else "***"
            print(f"✅ {var}: {masked} ({description})")
        else:
            print(f"❌ {var}: NOT SET ({description})")
            all_set = False
    
    return all_set


def check_imports():
    """Check if all required packages are installed."""
    print("\n" + "="*60)
    print("CHECKING PYTHON PACKAGES")
    print("="*60)
    
    packages = [
        ("openai", "OpenAI Python SDK"),
        ("mysql.connector", "MySQL connector"),
        ("boto3", "AWS SDK"),
    ]
    
    all_installed = True
    for package, description in packages:
        try:
            __import__(package)
            print(f"✅ {package}: Installed ({description})")
        except ImportError:
            print(f"❌ {package}: NOT INSTALLED ({description})")
            all_installed = False
    
    return all_installed


def test_openai_connection():
    """Test OpenAI API connection."""
    print("\n" + "="*60)
    print("TESTING OPENAI CONNECTION")
    print("="*60)
    
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        
        if not api_key:
            print("❌ OPENAI_API_KEY not set")
            return False
        
        client = OpenAI(api_key=api_key)
        
        # Test with a simple completion
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Say 'test successful'"}],
            max_tokens=10,
            temperature=0
        )
        
        result = response.choices[0].message.content
        print(f"✅ OpenAI API: Connected (Response: '{result}')")
        print(f"   Model: gpt-4o")
        print(f"   Usage: {response.usage.total_tokens} tokens")
        return True
        
    except Exception as e:
        print(f"❌ OpenAI API: Connection failed")
        print(f"   Error: {e}")
        return False


def test_database_connection():
    """Test database connection."""
    print("\n" + "="*60)
    print("TESTING DATABASE CONNECTION")
    print("="*60)
    
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "3.129.145.187"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", "aiagentdb"),
            password=os.getenv("DB_PASSWORD", "Agents@1252"),
            database=os.getenv("DB_NAME", "stage_newskinny")
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        print(f"✅ Database: Connected")
        print(f"   Host: {os.getenv('DB_HOST', '3.129.145.187')}")
        print(f"   Database: {os.getenv('DB_NAME', 'stage_newskinny')}")
        
        # Check if cross_validation_report_path column exists
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = 'tblaiagents' 
            AND COLUMN_NAME = 'cross_validation_report_path'
        """, (os.getenv("DB_NAME", "stage_newskinny"),))
        
        result = cursor.fetchone()
        if result:
            print(f"✅ Column 'cross_validation_report_path' exists in tblaiagents")
        else:
            print(f"⚠️  Column 'cross_validation_report_path' NOT FOUND in tblaiagents")
            print(f"   Run: ALTER TABLE tblaiagents ADD COLUMN cross_validation_report_path VARCHAR(500) NULL;")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database: Connection failed")
        print(f"   Error: {e}")
        return False


def test_s3_connection():
    """Test S3 connection."""
    print("\n" + "="*60)
    print("TESTING S3 CONNECTION")
    print("="*60)
    
    try:
        import boto3
        
        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
        
        # Try to list buckets
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        
        print(f"✅ S3: Connected")
        print(f"   Region: {os.getenv('AWS_REGION', 'us-east-2')}")
        print(f"   Buckets accessible: {len(buckets)}")
        
        if "lendingwise-aiagent" in buckets:
            print(f"   ✅ Target bucket 'lendingwise-aiagent' found")
        else:
            print(f"   ⚠️  Target bucket 'lendingwise-aiagent' NOT FOUND")
        
        return True
        
    except Exception as e:
        print(f"❌ S3: Connection failed")
        print(f"   Error: {e}")
        return False


def test_validation_module():
    """Test if validation modules can be imported."""
    print("\n" + "="*60)
    print("TESTING VALIDATION MODULES")
    print("="*60)
    
    try:
        from cross_validation.validation import EnhancedValidator, GPT4oValidator
        print(f"✅ EnhancedValidator: Imported successfully")
        print(f"✅ GPT4oValidator: Imported successfully")
        
        from cross_validation.models import (
            VerificationReport, 
            ValidationSummary, 
            FieldResult,
            RecommendationResult
        )
        print(f"✅ Data models: Imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Module import failed")
        print(f"   Error: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("CROSS-VALIDATION SYSTEM SETUP TEST")
    print("="*60)
    
    results = {
        "Environment Variables": check_environment_variables(),
        "Python Packages": check_imports(),
        "OpenAI Connection": test_openai_connection(),
        "Database Connection": test_database_connection(),
        "S3 Connection": test_s3_connection(),
        "Validation Modules": test_validation_module()
    }
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(results.values())
    
    print("\n" + "="*60)
    if all_passed:
        print("🎉 ALL TESTS PASSED - System ready for deployment!")
    else:
        print("⚠️  SOME TESTS FAILED - Fix issues before deployment")
    print("="*60 + "\n")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

