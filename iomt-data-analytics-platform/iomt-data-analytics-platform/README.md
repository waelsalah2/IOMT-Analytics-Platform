# IoMT Data Analytics Platform - AWS CDK

A comprehensive AWS CDK Python project implementing a HIPAA-compliant Internet of Medical Things (IoMT) data analytics platform for medical device ingestion, EHR integration, and ML-powered analytics.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SECURITY & COMPLIANCE                                   │
│  ┌─────────┐  ┌──────────────┐  ┌────────────┐                                  │
│  │   IAM   │  │ Audit Logging│  │ Encryption │                                  │
│  │         │  │ (CloudTrail) │  │   (KMS)    │                                  │
│  └─────────┘  └──────────────┘  └────────────┘                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
┌──────────────────┐     ┌──────────────────────────┐     ┌───────────────────────┐
│   EDGE DEVICES   │     │  INGESTION & PROCESSING  │     │  DATA REPOSITORY      │
│                  │     │                          │     │                       │
│  ┌───────────┐   │     │  ┌──────────────────┐   │     │  ┌─────────────────┐  │
│  │ Wearables │───┼────▶│  │  AWS IoT Core    │   │     │  │   Amazon S3     │  │
│  └───────────┘   │     │  │  (Rules Engine)  │   │     │  │  (Raw/FHIR)     │  │
│  ┌───────────┐   │     │  └────────┬─────────┘   │     │  └─────────────────┘  │
│  │  Patient  │───┼────▶│           │             │     │          │            │
│  │  Gateway  │   │     │  ┌────────▼─────────┐   │     │  ┌───────▼────────┐   │
│  └───────────┘   │     │  │   Amazon SQS     │   │     │  │ Amazon         │   │
│  ┌───────────┐   │     │  └────────┬─────────┘   │     │  │ HealthLake     │   │
│  │   Home    │───┼────▶│           │             │────▶│  │ (FHIR Store)   │   │
│  │  Health   │   │     │  ┌────────▼─────────┐   │     │  └────────────────┘   │
│  │  Devices  │   │     │  │  Lambda          │   │     │                       │
│  └───────────┘   │     │  │  (IoT Processor) │   │     │                       │
│                  │     │  │  (FHIR Converter)│   │     │                       │
└──────────────────┘     └──────────────────────────┘     └───────────────────────┘
                                                                    │
┌───────────────────────────────────────────────────────────────────┼─────────────┐
│                         ANALYTICS & ML                            │             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │             │
│  │  AWS Glue    │  │   Amazon     │  │   Amazon     │◀───────────┘             │
│  │  (ETL Jobs)  │  │   Athena     │  │  SageMaker   │                          │
│  └──────────────┘  └──────────────┘  └──────────────┘                          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT & INTEGRATION                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   Amazon     │  │   Amazon     │  │   Amazon     │  │   FHIR API   │        │
│  │   Pinpoint   │  │     SNS      │  │  QuickSight  │  │(API Gateway) │        │
│  │(Engagement)  │  │(Notifications│  │ (Dashboards) │  │              │        │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │
│         │                  │                  │                  │              │
│         ▼                  ▼                  ▼                  ▼              │
│    [Patients]        [Clinicians]      [Data Scientists]  [Research Apps]      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Components

### Security & Compliance Layer
- **KMS Encryption**: Customer-managed keys for data encryption at rest
- **CloudTrail**: Comprehensive audit logging with 7-year retention
- **IAM**: Fine-grained access control policies

### Edge Device Layer
- Supports wearables, patient gateways, and home health devices
- Secure MQTT connectivity via AWS IoT Core

### Ingestion & Processing Layer
- **AWS IoT Core**: Device connectivity with rules engine
- **Amazon SQS**: Message queuing for reliable processing
- **AWS Lambda**: 
  - IoT data processor with vital signs validation
  - HL7/FHIR converter for standard healthcare data format

### IoMT Data Repository
- **Amazon S3**: Raw and processed data storage with lifecycle policies
- **Amazon HealthLake**: FHIR R4 compliant data store

### Analytics & ML Layer
- **AWS Glue**: ETL jobs for data transformation
- **Amazon Athena**: SQL analytics workgroup
- **Amazon SageMaker**: ML model development notebooks

### Output & Integration Layer
- **Amazon Pinpoint**: Patient engagement messaging
- **Amazon SNS**: Real-time notifications and alerts
- **API Gateway**: FHIR REST API for data access

## Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js 18.x or later
- Python 3.11 or later
- AWS CDK CLI (`npm install -g aws-cdk`)

## Installation

1. **Clone and navigate to the project:**
   ```bash
   cd iomt-data-analytics-platform
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   # or
   .venv\Scripts\activate     # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Bootstrap CDK (first time only):**
   ```bash
   cdk bootstrap aws://ACCOUNT-ID/REGION
   ```

## Deployment

1. **Synthesize CloudFormation template:**
   ```bash
   cdk synth
   ```

2. **Deploy the stack:**
   ```bash
   cdk deploy
   ```

3. **Deploy with specific context:**
   ```bash
   cdk deploy --context account=123456789012 --context region=us-east-1
   ```

## Project Structure

```
iomt-data-analytics-platform/
├── app.py                          # CDK app entry point
├── cdk.json                        # CDK configuration
├── requirements.txt                # Python dependencies
├── stacks/
│   ├── __init__.py
│   └── iomt_platform_stack.py      # Main CDK stack
├── lambda_functions/
│   ├── iot_processor/
│   │   └── index.py               # IoT data processing
│   ├── fhir_converter/
│   │   └── index.py               # FHIR conversion logic
│   ├── anomaly_detector/
│   │   └── index.py               # Anomaly detection
│   └── fhir_api/
│       └── index.py               # FHIR API handler
└── glue_scripts/
    └── etl_transform.py           # Glue ETL job
```

## IoMT Device Integration

### Publishing Device Data

Devices publish to MQTT topics:
- `iomt/wearables/{deviceId}` - Wearable devices
- `iomt/home-health/{deviceId}` - Home health devices

### Message Format

```json
{
  "deviceId": "device-001",
  "deviceType": "smartwatch",
  "patientId": "patient-123",
  "timestamp": "2024-01-15T10:30:00Z",
  "readings": {
    "heartRate": 72,
    "bloodOxygen": 98,
    "systolicBP": 120,
    "diastolicBP": 80,
    "temperature": 36.6,
    "bloodGlucose": 95,
    "respiratoryRate": 16
  }
}
```

## FHIR API Usage

### Get Patient
```bash
curl -X GET "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Patient/{id}" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Search Observations
```bash
curl -X GET "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Observation?patient=patient-123" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Create Observation
```bash
curl -X POST "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Observation" \
  -H "Content-Type: application/fhir+json" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -d '{"resourceType": "Observation", ...}'
```

## Monitoring

The platform includes a CloudWatch dashboard (`IoMTDataAnalyticsPlatform`) with:
- Lambda invocation and error metrics
- SQS message throughput
- API Gateway request counts
- Dead letter queue depth

## Alert Types

The platform monitors for these vital sign anomalies:
| Alert | Condition | Severity |
|-------|-----------|----------|
| tachycardia | Heart rate > 120 bpm | Warning |
| bradycardia | Heart rate < 50 bpm | Warning |
| hypoxemia | Blood oxygen < 92% | Critical |
| hypertensive_crisis | BP > 180/120 | Critical |
| hypoglycemia | Blood glucose < 70 mg/dL | Critical |
| hyperglycemia | Blood glucose > 180 mg/dL | Warning |
| fever | Temperature > 38.5°C | Warning |

## Security Considerations

- All data encrypted at rest with KMS
- All data encrypted in transit with TLS
- HIPAA-compliant audit logging (7-year retention)
- IAM policies follow least privilege principle
- VPC endpoints recommended for production
- HealthLake provides FHIR-native security

## Cost Optimization

- S3 Intelligent-Tiering for data lifecycle
- Reserved capacity for HealthLake (production)
- Glue job bookmarks to prevent reprocessing
- SageMaker spot instances for training
- Lambda provisioned concurrency for consistent latency

## Useful CDK Commands

```bash
cdk ls          # List all stacks
cdk synth       # Emit CloudFormation template
cdk diff        # Compare deployed stack with current state
cdk deploy      # Deploy stack
cdk destroy     # Destroy stack
```

## Outputs

After deployment, the following outputs are available:
- `RawDataBucketOutput` - S3 bucket for raw IoMT data
- `FHIRDataBucketOutput` - S3 bucket for FHIR resources
- `HealthLakeDatastoreIdOutput` - HealthLake datastore ID
- `FHIRApiEndpoint` - FHIR API Gateway URL
- `IoTEndpoint` - IoT Core topic ARN
- `NotificationTopicArn` - SNS topic for alerts

## License

This project is licensed under the MIT License.
