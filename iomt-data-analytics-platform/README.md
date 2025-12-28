# IoMT Data Analytics Platform - AWS CDK

A comprehensive AWS CDK Python project implementing a HIPAA-compliant Internet of Medical Things (IoMT) data analytics platform for medical device ingestion, EHR integration, and Lambda-based ML analytics (SageMaker-free).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SECURITY & COMPLIANCE                                  │
│  ┌─────────┐  ┌──────────────┐  ┌────────────┐  ┌────────────────────────────┐  │
│  │   IAM   │  │ Audit Logging│  │ Encryption │  │   HIPAA Controls           │  │
│  │  Roles  │  │ (CloudTrail) │  │   (KMS)    │  │   7-Year Retention         │  │
│  └─────────┘  └──────────────┘  └────────────┘  └────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
┌──────────────────┐     ┌──────────────────────────┐     ┌───────────────────────┐
│   EDGE DEVICES   │     │  INGESTION & PROCESSING  │     │  DATA REPOSITORY      │
│                  │     │                          │     │                       │
│  ┌───────────┐   │     │  ┌──────────────────┐   │     │  ┌─────────────────┐  │
│  │ Wearables │───┼────▶│  │  AWS IoT Core    │   │     │  │   Amazon S3     │  │
│  └───────────┘   │     │  │  (Rules Engine)  │   │     │  │  (Raw/FHIR/ML)  │  │
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
│                    ANALYTICS & ML (Lambda-based)                  │             │
│                                                                   │             │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐  │             │
│  │  AWS Glue    │  │   Amazon     │  │   Lambda ML Functions  │◀─┘             │
│  │  (ETL Jobs)  │  │   Athena     │  │  - Anomaly Detection   │               │
│  │  (Crawlers)  │  │  (SQL Query) │  │  - Predictive Analytics│               │
│  └──────────────┘  └──────────────┘  │  - Model Training      │               │
│                                      └────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT & INTEGRATION                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   Amazon     │  │   Amazon     │  │  CloudWatch  │  │   FHIR API   │        │
│  │   Pinpoint   │  │     SNS      │  │  Dashboards  │  │(API Gateway) │        │
│  │(Engagement)  │  │(Notifications│  │  & Alarms    │  │              │        │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │
│         │                  │                  │                  │              │
│         ▼                  ▼                  ▼                  ▼              │
│    [Patients]        [Clinicians]        [DevOps]        [Research Apps]       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Fully Serverless**: Lambda-based ML (no SageMaker) for cost efficiency
- **HIPAA Compliant**: KMS encryption, CloudTrail audit logging, 7-year retention
- **Real-Time Processing**: IoT Core → SQS → Lambda pipeline
- **FHIR R4 Native**: Amazon HealthLake for healthcare interoperability
- **ML-Powered Analytics**: Anomaly detection, predictive analytics, automated retraining

## Components

### Security & Compliance Layer
- **KMS Encryption**: Customer-managed keys for data encryption at rest
- **CloudTrail**: Comprehensive audit logging with 7-year retention (HIPAA)
- **IAM**: Fine-grained access control with least privilege

### Edge Device Layer
- Supports wearables, patient gateways, and home health devices
- Secure MQTT connectivity via AWS IoT Core
- Device authentication and authorization

### Ingestion & Processing Layer
- **AWS IoT Core**: Device connectivity with rules engine
- **Amazon SQS**: Message queuing with dead letter queue
- **AWS Lambda**: 
  - IoT data processor with vital signs validation
  - HL7/FHIR converter for standard healthcare data format
  - Anomaly detection with real-time alerting

### IoMT Data Repository
- **Amazon S3**: Raw, FHIR, and ML model storage with lifecycle policies
- **Amazon HealthLake**: FHIR R4 compliant data store with Synthea preload

### Analytics & ML Layer (Lambda-based, No SageMaker)

| Lambda Function | Memory | Timeout | Purpose |
|-----------------|--------|---------|---------|
| ML Anomaly Detection | 1024 MB | 5 min | Real-time anomaly detection in vital signs |
| Predictive Analytics | 1024 MB | 5 min | Readmission & deterioration risk prediction |
| Model Training | 3008 MB | 15 min | Periodic model retraining (daily at 2 AM) |

- **AWS Glue**: ETL jobs and data crawlers
- **Amazon Athena**: SQL analytics workgroup

### Output & Integration Layer
- **Amazon Pinpoint**: Patient engagement messaging
- **Amazon SNS**: Real-time notifications and anomaly alerts
- **API Gateway**: FHIR REST API for data access
- **CloudWatch**: Dashboards, metrics, and alarms

## Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js 18.x or later
- Python 3.11 or later
- AWS CDK CLI (`npm install -g aws-cdk`)

## Installation

```bash
cd iomt-data-analytics-platform

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or: .venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Bootstrap CDK (first time only)
cdk bootstrap aws://ACCOUNT-ID/REGION
```

## Deployment

```bash
# Synthesize CloudFormation template
cdk synth

# Deploy the stack
cdk deploy

# Deploy with approval for IAM changes
cdk deploy --require-approval broadening
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
│   ├── iot_processor/              # IoT data processing
│   ├── fhir_converter/             # FHIR conversion
│   ├── anomaly_detector/           # Anomaly detection
│   └── fhir_api/                   # FHIR API handler
└── glue_scripts/
    └── etl_transform.py            # Glue ETL job
```

## IoMT Device Integration

### MQTT Topics
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
    "respiratoryRate": 16
  }
}
```

## ML Analytics

### Anomaly Detection Thresholds

| Metric | Warning Range | Critical Range |
|--------|---------------|----------------|
| Heart Rate | < 50 or > 120 bpm | < 40 or > 150 bpm |
| Blood Oxygen | < 92% | < 88% |
| Systolic BP | < 90 or > 140 mmHg | < 80 or > 180 mmHg |
| Diastolic BP | < 60 or > 90 mmHg | < 50 or > 120 mmHg |
| Temperature | < 36.1 or > 37.8°C | < 35 or > 39.5°C |
| Respiratory Rate | < 12 or > 20/min | < 8 or > 30/min |

### Predictive Analytics

**Readmission Risk Prediction:**
- Risk factors: age, chronic conditions, recent hospitalizations, medication adherence
- Output: Risk score (0-1), risk level (LOW/MEDIUM/HIGH), clinical recommendation

**Health Deterioration Prediction:**
- Factors: vital sign variance, chronic conditions
- Output: Risk score, alert threshold (7 or 14 days)

### Model Training Schedule
- **Frequency**: Daily at 2:00 AM UTC
- **Trigger**: Amazon EventBridge
- **Storage**: S3 with model versioning
- **Registry**: `latest.json` pointer for current production model

## FHIR API Usage

### Get Patient
```bash
curl -X GET "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Patient/{id}" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Search Observations
```bash
curl -X GET "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Observation?patient=patient-123"
```

### Create Observation
```bash
curl -X POST "https://{api-id}.execute-api.{region}.amazonaws.com/v1/Observation" \
  -H "Content-Type: application/fhir+json" \
  -d '{"resourceType": "Observation", ...}'
```

## Monitoring

### CloudWatch Dashboard
The platform includes `IoMTDataAnalyticsPlatform` dashboard with:
- Lambda invocations and errors (all functions including ML)
- SQS message throughput
- API Gateway request counts
- Dead letter queue depth

### Custom Metrics (IoMT/Anomalies namespace)
- `AnomalyCount` - Number of anomalies detected
- `RiskScore` - Patient risk scores

### Alarms
- Lambda error thresholds
- DLQ message accumulation
- API Gateway 5XX errors

## Cost Estimates (Monthly)

| Component | Cost |
|-----------|------|
| S3 Storage (100GB) | $2-5 |
| Lambda (1M invocations) | $20 |
| IoT Core (1M messages) | $1 |
| SQS (1M messages) | $0.40 |
| Glue ETL (10 DPU-hours) | $4.40 |
| Athena (100GB scanned) | $0.50 |
| HealthLake | $50-100 |
| API Gateway (1M requests) | $3.50 |
| **Total** | **~$85-140** |

*Note: No SageMaker costs - 95% savings on ML infrastructure*

## Security Considerations

- All data encrypted at rest with customer-managed KMS keys
- All data encrypted in transit with TLS 1.2+
- HIPAA-compliant audit logging (7-year retention)
- IAM policies follow least privilege principle
- HealthLake provides FHIR-native security
- Dead letter queues for failed message handling

## Outputs

After deployment:
- `RawDataBucketOutput` - S3 bucket for raw IoMT data
- `FHIRDataBucketOutput` - S3 bucket for FHIR resources
- `MLModelsBucketOutput` - S3 bucket for ML models
- `HealthLakeDatastoreIdOutput` - HealthLake datastore ID
- `FHIRApiEndpoint` - FHIR API Gateway URL
- `IoTEndpoint` - IoT Core topic ARN
- `NotificationTopicArn` - SNS topic for alerts
- `AnomalyDetectionLambdaArn` - Anomaly detection Lambda
- `PredictiveAnalyticsLambdaArn` - Predictive analytics Lambda

## Useful CDK Commands

```bash
cdk ls          # List all stacks
cdk synth       # Emit CloudFormation template
cdk diff        # Compare deployed stack with current state
cdk deploy      # Deploy stack
cdk destroy     # Destroy stack
```

## Cleanup

```bash
cdk destroy
```

Note: S3 buckets with `RemovalPolicy.RETAIN` must be manually emptied and deleted.

## License

MIT License
