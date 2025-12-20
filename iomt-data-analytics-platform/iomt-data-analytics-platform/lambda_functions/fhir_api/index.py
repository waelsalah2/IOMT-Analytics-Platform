"""
FHIR API Lambda Function
Handles FHIR REST API requests through API Gateway
"""
import json
import boto3
import os
from datetime import datetime
from typing import Any, Optional
import uuid

healthlake = boto3.client('healthlake')
s3 = boto3.client('s3')


def search_s3_observations(
    bucket: str,
    patient_id: Optional[str] = None,
    device_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100
) -> list:
    """Search observations stored in S3"""
    observations = []
    
    # Build prefix based on filters
    prefix = 'observations/'
    
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            if len(observations) >= limit:
                break
                
            try:
                response = s3.get_object(Bucket=bucket, Key=obj['Key'])
                observation = json.loads(response['Body'].read().decode('utf-8'))
                
                # Apply filters
                if patient_id:
                    subject = observation.get('subject', {})
                    if patient_id not in subject.get('reference', ''):
                        continue
                
                if device_id:
                    device = observation.get('device', {})
                    device_identifier = device.get('identifier', {})
                    if device_identifier.get('value') != device_id:
                        continue
                
                if date_from:
                    effective_dt = observation.get('effectiveDateTime', '')
                    if effective_dt < date_from:
                        continue
                
                if date_to:
                    effective_dt = observation.get('effectiveDateTime', '')
                    if effective_dt > date_to:
                        continue
                
                observations.append(observation)
                
            except Exception as e:
                print(f"Error reading object {obj['Key']}: {e}")
                continue
    
    return observations


def create_fhir_bundle(resources: list, bundle_type: str = 'searchset') -> dict:
    """Create a FHIR Bundle from a list of resources"""
    return {
        'resourceType': 'Bundle',
        'id': str(uuid.uuid4()),
        'meta': {
            'lastUpdated': datetime.utcnow().isoformat() + 'Z'
        },
        'type': bundle_type,
        'total': len(resources),
        'entry': [
            {
                'fullUrl': f"urn:uuid:{resource.get('id', str(uuid.uuid4()))}",
                'resource': resource
            }
            for resource in resources
        ]
    }


def handle_patient_request(
    method: str,
    path_params: dict,
    query_params: dict,
    body: Optional[str],
    datastore_id: str
) -> dict:
    """Handle Patient resource requests"""
    
    if method == 'GET':
        if path_params and 'id' in path_params:
            # Read specific patient
            try:
                response = healthlake.read_resource(
                    DatastoreId=datastore_id,
                    ResourceType='Patient',
                    ResourceId=path_params['id']
                )
                return {
                    'statusCode': 200,
                    'body': json.dumps(json.loads(response['Resource']))
                }
            except healthlake.exceptions.ResourceNotFoundException:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'resourceType': 'OperationOutcome',
                        'issue': [{
                            'severity': 'error',
                            'code': 'not-found',
                            'diagnostics': f"Patient/{path_params['id']} not found"
                        }]
                    })
                }
        else:
            # Search patients
            search_params = '&'.join([f'{k}={v}' for k, v in (query_params or {}).items()])
            response = healthlake.search_with_get(
                DatastoreId=datastore_id,
                ResourceType='Patient',
                SearchParameters=search_params if search_params else None
            )
            return {
                'statusCode': 200,
                'body': response.get('Resource', '{}')
            }
    
    elif method == 'POST':
        if body:
            resource_data = json.loads(body)
            resource_data['resourceType'] = 'Patient'
            response = healthlake.create_resource(
                DatastoreId=datastore_id,
                ResourceType='Patient',
                ResourceBody=json.dumps(resource_data)
            )
            return {
                'statusCode': 201,
                'body': json.dumps({
                    'resourceType': 'OperationOutcome',
                    'issue': [{
                        'severity': 'information',
                        'code': 'informational',
                        'diagnostics': f"Patient created with ID: {response.get('ResourceId')}"
                    }]
                })
            }
    
    return {
        'statusCode': 400,
        'body': json.dumps({
            'resourceType': 'OperationOutcome',
            'issue': [{
                'severity': 'error',
                'code': 'invalid',
                'diagnostics': 'Invalid request'
            }]
        })
    }


def handle_observation_request(
    method: str,
    path_params: dict,
    query_params: dict,
    body: Optional[str],
    datastore_id: str,
    fhir_bucket: str
) -> dict:
    """Handle Observation resource requests"""
    
    if method == 'GET':
        if path_params and 'id' in path_params:
            # Try HealthLake first, then S3
            try:
                response = healthlake.read_resource(
                    DatastoreId=datastore_id,
                    ResourceType='Observation',
                    ResourceId=path_params['id']
                )
                return {
                    'statusCode': 200,
                    'body': json.dumps(json.loads(response['Resource']))
                }
            except:
                # Search in S3
                observations = search_s3_observations(
                    bucket=fhir_bucket,
                    limit=1000
                )
                for obs in observations:
                    if obs.get('id') == path_params['id']:
                        return {
                            'statusCode': 200,
                            'body': json.dumps(obs)
                        }
                
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'resourceType': 'OperationOutcome',
                        'issue': [{
                            'severity': 'error',
                            'code': 'not-found',
                            'diagnostics': f"Observation/{path_params['id']} not found"
                        }]
                    })
                }
        else:
            # Search observations
            query_params = query_params or {}
            observations = search_s3_observations(
                bucket=fhir_bucket,
                patient_id=query_params.get('patient'),
                device_id=query_params.get('device'),
                date_from=query_params.get('date-from'),
                date_to=query_params.get('date-to'),
                limit=int(query_params.get('_count', 100))
            )
            
            bundle = create_fhir_bundle(observations)
            return {
                'statusCode': 200,
                'body': json.dumps(bundle)
            }
    
    elif method == 'POST':
        if body:
            resource_data = json.loads(body)
            resource_data['resourceType'] = 'Observation'
            
            if 'id' not in resource_data:
                resource_data['id'] = str(uuid.uuid4())
            
            # Store in S3
            date_prefix = datetime.utcnow().strftime('%Y/%m/%d')
            key = f"observations/api/{date_prefix}/{resource_data['id']}.json"
            
            s3.put_object(
                Bucket=fhir_bucket,
                Key=key,
                Body=json.dumps(resource_data, indent=2),
                ContentType='application/fhir+json'
            )
            
            return {
                'statusCode': 201,
                'headers': {
                    'Location': f"/Observation/{resource_data['id']}"
                },
                'body': json.dumps(resource_data)
            }
    
    return {
        'statusCode': 400,
        'body': json.dumps({
            'resourceType': 'OperationOutcome',
            'issue': [{
                'severity': 'error',
                'code': 'invalid',
                'diagnostics': 'Invalid request'
            }]
        })
    }


def handler(event: dict, context: Any) -> dict:
    """Handle FHIR API requests from API Gateway"""
    try:
        http_method = event.get('httpMethod', 'GET')
        resource_path = event.get('resource', '').strip('/')
        path_params = event.get('pathParameters', {}) or {}
        query_params = event.get('queryStringParameters', {}) or {}
        body = event.get('body')
        
        datastore_id = os.environ.get('HEALTHLAKE_DATASTORE_ID', '')
        fhir_bucket = os.environ.get('FHIR_BUCKET', '')
        
        # Extract resource type from path
        resource_type = resource_path.split('/')[0] if resource_path else ''
        
        # Route to appropriate handler
        if resource_type == 'Patient':
            result = handle_patient_request(
                http_method, path_params, query_params, body, datastore_id
            )
        elif resource_type == 'Observation':
            result = handle_observation_request(
                http_method, path_params, query_params, body, datastore_id, fhir_bucket
            )
        elif resource_type == 'metadata':
            # Return capability statement
            result = {
                'statusCode': 200,
                'body': json.dumps({
                    'resourceType': 'CapabilityStatement',
                    'status': 'active',
                    'date': datetime.utcnow().isoformat() + 'Z',
                    'kind': 'instance',
                    'fhirVersion': '4.0.1',
                    'format': ['json'],
                    'rest': [{
                        'mode': 'server',
                        'resource': [
                            {
                                'type': 'Patient',
                                'interaction': [
                                    {'code': 'read'},
                                    {'code': 'search-type'},
                                    {'code': 'create'}
                                ]
                            },
                            {
                                'type': 'Observation',
                                'interaction': [
                                    {'code': 'read'},
                                    {'code': 'search-type'},
                                    {'code': 'create'}
                                ]
                            }
                        ]
                    }]
                })
            }
        else:
            result = {
                'statusCode': 404,
                'body': json.dumps({
                    'resourceType': 'OperationOutcome',
                    'issue': [{
                        'severity': 'error',
                        'code': 'not-supported',
                        'diagnostics': f'Resource type {resource_type} not supported'
                    }]
                })
            }
        
        # Add FHIR headers
        result['headers'] = {
            'Content-Type': 'application/fhir+json',
            'X-Request-Id': context.aws_request_id if context else str(uuid.uuid4()),
            **result.get('headers', {})
        }
        
        return result
        
    except Exception as e:
        print(f"FHIR API Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/fhir+json'},
            'body': json.dumps({
                'resourceType': 'OperationOutcome',
                'issue': [{
                    'severity': 'error',
                    'code': 'exception',
                    'diagnostics': str(e)
                }]
            })
        }
