#!/usr/bin/env python3
"""
Test script to send an SQS message with EFS path format and trigger the ingestion pipeline.

This script:
1. Sends an SQS message with an EFS file path
2. The ingestion node will automatically:
   - Read the file from EFS
   - Upload to S3
   - Create metadata
   - Continue with the full pipeline (Tamper Check -> OCR -> Classification -> Extraction)

Usage:
    python test_efs_upload.py

Requirements:
    - File must exist at the EFS path
    - AWS credentials configured
    - SQS queue accessible
"""

import os
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SQS_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/685551735768/lendingwise"
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

# Test message configuration
TEST_MESSAGE = {
    "FPCID": "3580",
    "LMRId": "1",
    "file": "/mnt/efs/STAGING/LMRDocNewServer/3580/2025/10/13/1/upload/usa-id-card-and-driver-license-500x500_Mohokar_7da033cf5a2bb13a.png",
    "document-name": "Non-Owner Disclosure",
    "entity_type": "LLC",
    "year": 2025,
    "month": 10,
    "day": 20
}


def send_test_message():
    """Send test SQS message with EFS path."""
    try:
        # Create SQS client
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        
        print("=" * 80)
        print("📤 SENDING TEST SQS MESSAGE")
        print("=" * 80)
        print("\nMessage Content:")
        print(json.dumps(TEST_MESSAGE, indent=2))
        print("\n" + "=" * 80)
        
        # Send message
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(TEST_MESSAGE, indent=2)
        )
        
        message_id = response.get("MessageId")
        print(f"\n✅ Message sent successfully!")
        print(f"   Message ID: {message_id}")
        print(f"   Queue URL: {SQS_QUEUE_URL}")
        
        print("\n" + "=" * 80)
        print("📋 WHAT HAPPENS NEXT:")
        print("=" * 80)
        print("1. The ingestion node will detect the EFS path")
        print(f"2. Read file from: {TEST_MESSAGE['file']}")
        print(f"3. Upload to S3 at: LMRFileDocNew/{TEST_MESSAGE['FPCID']}/{TEST_MESSAGE['year']}/{TEST_MESSAGE['month']:02d}/{TEST_MESSAGE['day']:02d}/{TEST_MESSAGE['LMRId']}/upload/document/")
        print("4. Create metadata JSON in S3")
        print("5. Run Tamper Check")
        print("6. Run OCR")
        print("7. Run Classification")
        print("8. Run Extraction")
        
        print("\n" + "=" * 80)
        print("🚀 TO START PROCESSING:")
        print("=" * 80)
        print("Run the worker in another terminal:")
        print("   python sqs_worker.py")
        print("\nThe worker will:")
        print("   - Poll SQS for messages")
        print("   - Process this message through the full pipeline")
        print("   - Display detailed logs at each step")
        
        print("\n" + "=" * 80)
        print("📝 IMPORTANT NOTES:")
        print("=" * 80)
        print(f"⚠️  Make sure the file exists at: {TEST_MESSAGE['file']}")
        print("⚠️  Make sure the worker has access to the EFS mount")
        print("⚠️  AWS credentials must be configured")
        print("⚠️  OPENAI_API_KEY must be set for LLM steps")
        
        return message_id
        
    except Exception as e:
        print(f"\n❌ Error sending message: {e}")
        return None


def check_queue_status():
    """Check current queue status."""
    try:
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        
        response = sqs.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        
        attrs = response.get('Attributes', {})
        visible = attrs.get('ApproximateNumberOfMessages', '0')
        in_flight = attrs.get('ApproximateNumberOfMessagesNotVisible', '0')
        
        print("\n" + "=" * 80)
        print("📊 QUEUE STATUS:")
        print("=" * 80)
        print(f"Messages waiting: {visible}")
        print(f"Messages in flight: {in_flight}")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n⚠️  Could not check queue status: {e}\n")


def main():
    """Main test function."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "EFS TO S3 UPLOAD TEST SCRIPT" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Check queue status before sending
    check_queue_status()
    
    # Send test message
    message_id = send_test_message()
    
    if message_id:
        # Check queue status after sending
        check_queue_status()
        
        print("\n✅ Test message sent successfully!")
        print("\nNext step: Run 'python sqs_worker.py' to process the message\n")
        return 0
    else:
        print("\n❌ Failed to send test message\n")
        return 1


if __name__ == "__main__":
    exit(main())

