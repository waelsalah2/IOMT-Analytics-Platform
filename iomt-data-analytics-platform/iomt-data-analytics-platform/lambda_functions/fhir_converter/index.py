"""
FHIR Converter Lambda Function
Converts device data to FHIR R4 format and stores in S3
"""
import json
import boto3
import os
from datetime import datetime
import uuid
from typing import Any

s3 = boto3.client('s3')


def create_fhir_observation(data: dict) -> dict:
    """Create a FHIR R4 Observation resource from device data"""
    observation_id = str(uuid.uuid4())
    
    observation = {
        'resourceType': 'Observation',
        'id': observation_id,
        'meta': {
            'versionId': '1',
            'lastUpdated': datetime.utcnow().isoformat() + 'Z',
            'profile': ['http://hl7.org/fhir/StructureDefinition/vitalsigns']
        },
        'identifier': [{
            'system': 'urn:healthcare:observations',
            'value': observation_id
        }],
        'status': 'final',
        'category': [{
            'coding': [{
                'system': 'http://terminology.hl7.org/CodeSystem/observation-category',
                'code': 'vital-signs',
                'display': 'Vital Signs'
            }]
        }],
        'effectiveDateTime': data.get('timestamp'),
        'issued': datetime.utcnow().isoformat() + 'Z',
        'device': {
            'identifier': {
                'system': 'urn:healthcare:devices',
                'value': data.get('deviceId')
            },
            'display': data.get('deviceType', 'Unknown Device')
        },
        'component': []
    }
    
    # Add patient reference if available
    if data.get('patientId'):
        observation['subject'] = {
            'reference': f"Patient/{data['patientId']}",
            'identifier': {
                'system': 'urn:healthcare:patients',
                'value': data['patientId']
            }
        }
    
    readings = data.get('readings', {})
    
    # Heart Rate (LOINC: 8867-4)
    if 'heartRate' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '8867-4',
                    'display': 'Heart rate'
                }],
                'text': 'Heart rate'
            },
            'valueQuantity': {
                'value': readings['heartRate'],
                'unit': 'beats/minute',
                'system': 'http://unitsofmeasure.org',
                'code': '/min'
            }
        })
    
    # Blood Oxygen Saturation (LOINC: 59408-5)
    if 'bloodOxygen' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '59408-5',
                    'display': 'Oxygen saturation in Arterial blood by Pulse oximetry'
                }],
                'text': 'Oxygen saturation'
            },
            'valueQuantity': {
                'value': readings['bloodOxygen'],
                'unit': '%',
                'system': 'http://unitsofmeasure.org',
                'code': '%'
            }
        })
    
    # Systolic Blood Pressure (LOINC: 8480-6)
    if 'systolicBP' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '8480-6',
                    'display': 'Systolic blood pressure'
                }],
                'text': 'Systolic blood pressure'
            },
            'valueQuantity': {
                'value': readings['systolicBP'],
                'unit': 'mmHg',
                'system': 'http://unitsofmeasure.org',
                'code': 'mm[Hg]'
            }
        })
    
    # Diastolic Blood Pressure (LOINC: 8462-4)
    if 'diastolicBP' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '8462-4',
                    'display': 'Diastolic blood pressure'
                }],
                'text': 'Diastolic blood pressure'
            },
            'valueQuantity': {
                'value': readings['diastolicBP'],
                'unit': 'mmHg',
                'system': 'http://unitsofmeasure.org',
                'code': 'mm[Hg]'
            }
        })
    
    # Body Temperature (LOINC: 8310-5)
    if 'temperature' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '8310-5',
                    'display': 'Body temperature'
                }],
                'text': 'Body temperature'
            },
            'valueQuantity': {
                'value': readings['temperature'],
                'unit': '°C',
                'system': 'http://unitsofmeasure.org',
                'code': 'Cel'
            }
        })
    
    # Blood Glucose (LOINC: 2339-0)
    if 'bloodGlucose' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '2339-0',
                    'display': 'Glucose [Mass/volume] in Blood'
                }],
                'text': 'Blood glucose'
            },
            'valueQuantity': {
                'value': readings['bloodGlucose'],
                'unit': 'mg/dL',
                'system': 'http://unitsofmeasure.org',
                'code': 'mg/dL'
            }
        })
    
    # Respiratory Rate (LOINC: 9279-1)
    if 'respiratoryRate' in readings:
        observation['component'].append({
            'code': {
                'coding': [{
                    'system': 'http://loinc.org',
                    'code': '9279-1',
                    'display': 'Respiratory rate'
                }],
                'text': 'Respiratory rate'
            },
            'valueQuantity': {
                'value': readings['respiratoryRate'],
                'unit': 'breaths/minute',
                'system': 'http://unitsofmeasure.org',
                'code': '/min'
            }
        })
    
    # Add interpretation if alerts present
    alerts = data.get('alerts', [])
    if alerts:
        observation['interpretation'] = [{
            'coding': [{
                'system': 'http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation',
                'code': 'A',
                'display': 'Abnormal'
            }],
            'text': ', '.join(alerts)
        }]
    
    return observation


def handler(event: dict, context: Any) -> dict:
    """Convert device data to FHIR format and store in S3"""
    processed_count = 0
    error_count = 0
    
    try:
        for record in event.get('Records', []):
            try:
                body = json.loads(record['body'])
                
                # Create FHIR Observation
                fhir_observation = create_fhir_observation(body)
                
                # Determine S3 key with date partitioning
                timestamp = body.get('timestamp', datetime.utcnow().isoformat())
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    dt = datetime.utcnow()
                
                date_prefix = dt.strftime('%Y/%m/%d')
                device_type = body.get('deviceType', 'unknown').replace(' ', '_').lower()
                
                key = f"observations/{device_type}/{date_prefix}/{fhir_observation['id']}.json"
                
                # Store in S3
                s3.put_object(
                    Bucket=os.environ['FHIR_BUCKET'],
                    Key=key,
                    Body=json.dumps(fhir_observation, indent=2),
                    ContentType='application/fhir+json',
                    Metadata={
                        'device-id': body.get('deviceId', 'unknown'),
                        'patient-id': body.get('patientId', 'unknown'),
                        'observation-id': fhir_observation['id']
                    }
                )
                
                processed_count += 1
                print(f"Stored FHIR observation: {key}")
                
            except Exception as record_error:
                error_count += 1
                print(f"Error processing record: {str(record_error)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'FHIR conversion complete',
                'processed': processed_count,
                'errors': error_count
            })
        }
        
    except Exception as e:
        print(f"Error in FHIR converter: {str(e)}")
        raise
