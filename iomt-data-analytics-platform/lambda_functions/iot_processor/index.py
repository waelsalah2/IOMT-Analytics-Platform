"""
IoT Data Processor Lambda Function
Processes incoming data from edge devices (wearables, home health devices)
"""
import json
import boto3
import os
from datetime import datetime
from typing import Any

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def validate_readings(readings: dict) -> list[str]:
    """Validate device readings and return alerts for abnormal values"""
    alerts = []
    
    # Heart rate validation (normal: 60-100 bpm at rest)
    heart_rate = readings.get('heartRate')
    if heart_rate is not None:
        if heart_rate > 120:
            alerts.append('tachycardia')
        elif heart_rate < 50:
            alerts.append('bradycardia')
    
    # Blood oxygen validation (normal: 95-100%)
    blood_oxygen = readings.get('bloodOxygen')
    if blood_oxygen is not None:
        if blood_oxygen < 92:
            alerts.append('hypoxemia')
        elif blood_oxygen < 95:
            alerts.append('low_oxygen_warning')
    
    # Blood pressure validation
    systolic = readings.get('systolicBP')
    diastolic = readings.get('diastolicBP')
    if systolic is not None and diastolic is not None:
        if systolic > 180 or diastolic > 120:
            alerts.append('hypertensive_crisis')
        elif systolic > 140 or diastolic > 90:
            alerts.append('high_blood_pressure')
        elif systolic < 90 or diastolic < 60:
            alerts.append('low_blood_pressure')
    
    # Temperature validation (normal: 36.1-37.2°C)
    temperature = readings.get('temperature')
    if temperature is not None:
        if temperature > 38.5:
            alerts.append('fever')
        elif temperature < 35:
            alerts.append('hypothermia')
    
    # Blood glucose validation (normal fasting: 70-100 mg/dL)
    glucose = readings.get('bloodGlucose')
    if glucose is not None:
        if glucose > 180:
            alerts.append('hyperglycemia')
        elif glucose < 70:
            alerts.append('hypoglycemia')
    
    return alerts


def handler(event: dict, context: Any) -> dict:
    """Process incoming IoT device data"""
    try:
        # Parse device data from IoT Core
        device_id = event.get('deviceId', 'unknown')
        device_type = event.get('deviceType', 'unknown')
        patient_id = event.get('patientId')
        timestamp = event.get('timestamp', datetime.utcnow().isoformat())
        readings = event.get('readings', {})
        metadata = event.get('metadata', {})
        
        # Validate readings and check for anomalies
        alerts = validate_readings(readings)
        
        # Prepare enriched message for downstream processing
        message = {
            'deviceId': device_id,
            'deviceType': device_type,
            'patientId': patient_id,
            'timestamp': timestamp,
            'readings': readings,
            'alerts': alerts,
            'alertSeverity': 'critical' if any(a in ['hypertensive_crisis', 'hypoxemia', 'hypoglycemia'] for a in alerts) else 'warning' if alerts else 'normal',
            'metadata': metadata,
            'processedAt': datetime.utcnow().isoformat(),
            'processingLambdaRequestId': context.aws_request_id if context else None
        }
        
        # Send to SQS for FHIR conversion
        sqs_response = sqs.send_message(
            QueueUrl=os.environ['QUEUE_URL'],
            MessageBody=json.dumps(message),
            MessageAttributes={
                'deviceType': {
                    'DataType': 'String',
                    'StringValue': device_type
                },
                'alertSeverity': {
                    'DataType': 'String',
                    'StringValue': message['alertSeverity']
                }
            }
        )
        
        print(f"Processed device data: {device_id}, alerts: {alerts}, messageId: {sqs_response['MessageId']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data processed successfully',
                'deviceId': device_id,
                'alerts': alerts,
                'alertSeverity': message['alertSeverity'],
                'messageId': sqs_response['MessageId']
            })
        }
        
    except Exception as e:
        print(f"Error processing IoT data: {str(e)}")
        raise
