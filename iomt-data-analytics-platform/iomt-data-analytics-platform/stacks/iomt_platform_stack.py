"""
IoMT Data Analytics Platform Stack
Main CDK stack that orchestrates all components of the IoMT data architecture
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    Tags,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_sqs as sqs,
    aws_iot as iot,
    aws_glue as glue,
    aws_athena as athena,
    aws_sagemaker as sagemaker,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_pinpoint as pinpoint,
    aws_logs as logs,
    aws_cloudtrail as cloudtrail,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigateway as apigw,
    aws_healthlake as healthlake,
)
from constructs import Construct


class IoMTPlatformStack(Stack):
    """Main stack for IoMT Data Analytics Platform"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Apply tags to all resources
        Tags.of(self).add("Project", "IoMTDataAnalyticsPlatform")
        Tags.of(self).add("Environment", "Production")
        Tags.of(self).add("Compliance", "HIPAA")

        # =================================================================
        # SECURITY & COMPLIANCE LAYER
        # =================================================================
        
        # KMS Key for encryption at rest
        self.encryption_key = kms.Key(
            self, "IoMTEncryptionKey",
            alias="alias/iomt-platform-key",
            description="KMS key for IoMT data encryption",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Audit logging bucket
        self.audit_logs_bucket = s3.Bucket(
            self, "AuditLogsBucket",
            bucket_name=f"iomt-audit-logs-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ],
                    expiration=Duration.days(2555)  # 7 years for HIPAA
                )
            ]
        )

        # CloudTrail for audit logging
        self.trail = cloudtrail.Trail(
            self, "IoMTAuditTrail",
            bucket=self.audit_logs_bucket,
            encryption_key=self.encryption_key,
            include_global_service_events=True,
            is_multi_region_trail=True,
            enable_file_validation=True,
        )

        # =================================================================
        # HEALTHCARE DATA REPOSITORY (Amazon S3)
        # =================================================================
        
        # Raw data bucket for device data
        self.raw_data_bucket = s3.Bucket(
            self, "RawDataBucket",
            bucket_name=f"iomt-raw-data-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30)
                        )
                    ]
                )
            ]
        )

        # Processed FHIR data bucket
        self.fhir_data_bucket = s3.Bucket(
            self, "FHIRDataBucket",
            bucket_name=f"iomt-fhir-data-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # =================================================================
        # INGESTION & PROCESSING LAYER
        # =================================================================
        
        # SQS Queue for message processing
        self.processing_dlq = sqs.Queue(
            self, "ProcessingDLQ",
            queue_name="iomt-processing-dlq",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.encryption_key,
            retention_period=Duration.days(14),
        )

        self.processing_queue = sqs.Queue(
            self, "ProcessingQueue",
            queue_name="iomt-processing-queue",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.encryption_key,
            visibility_timeout=Duration.minutes(5),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.processing_dlq
            )
        )

        # Lambda execution role
        self.lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Grant permissions to Lambda role
        self.encryption_key.grant_encrypt_decrypt(self.lambda_role)
        self.raw_data_bucket.grant_read_write(self.lambda_role)
        self.fhir_data_bucket.grant_read_write(self.lambda_role)
        self.processing_queue.grant_consume_messages(self.lambda_role)

        # IoT Data Processing Lambda
        self.iot_processor_lambda = lambda_.Function(
            self, "IoTProcessorLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_iot_processor_code()),
            role=self.lambda_role,
            timeout=Duration.minutes(1),
            memory_size=256,
            environment={
                "RAW_BUCKET": self.raw_data_bucket.bucket_name,
                "QUEUE_URL": self.processing_queue.queue_url,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # HL7/FHIR Converter Lambda
        self.fhir_converter_lambda = lambda_.Function(
            self, "FHIRConverterLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_fhir_converter_code()),
            role=self.lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "FHIR_BUCKET": self.fhir_data_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # Add SQS trigger to FHIR converter
        self.fhir_converter_lambda.add_event_source(
            lambda_.event_sources.SqsEventSource(
                self.processing_queue,
                batch_size=10,
            )
        )

        # =================================================================
        # AWS IoT CORE - Edge Device Ingestion
        # =================================================================
        
        # IoT Policy for devices
        self.iot_policy = iot.CfnPolicy(
            self, "IoTDevicePolicy",
            policy_name="IoMTDevicePolicy",
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "iot:Connect",
                            "iot:Publish",
                            "iot:Subscribe",
                            "iot:Receive"
                        ],
                        "Resource": [
                            f"arn:aws:iot:{self.region}:{self.account}:topic/iomt/*",
                            f"arn:aws:iot:{self.region}:{self.account}:client/*"
                        ]
                    }
                ]
            }
        )

        # IoT Topic Rule for processing device data
        self.iot_rule_role = iam.Role(
            self, "IoTRuleRole",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
        )
        self.iot_processor_lambda.grant_invoke(self.iot_rule_role)
        self.raw_data_bucket.grant_write(self.iot_rule_role)

        # IoT Rule for wearables data
        self.wearables_rule = iot.CfnTopicRule(
            self, "WearablesDataRule",
            rule_name="iomt_wearables_rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT * FROM 'iomt/wearables/+'",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.iot_processor_lambda.function_arn
                        )
                    ),
                    iot.CfnTopicRule.ActionProperty(
                        s3=iot.CfnTopicRule.S3ActionProperty(
                            bucket_name=self.raw_data_bucket.bucket_name,
                            key="wearables/${topic(3)}/${timestamp()}.json",
                            role_arn=self.iot_rule_role.role_arn
                        )
                    )
                ],
                aws_iot_sql_version="2016-03-23"
            )
        )

        # IoT Rule for home health devices
        self.home_health_rule = iot.CfnTopicRule(
            self, "HomeHealthDataRule",
            rule_name="iomt_home_health_rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT * FROM 'iomt/home-health/+'",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=self.iot_processor_lambda.function_arn
                        )
                    ),
                    iot.CfnTopicRule.ActionProperty(
                        s3=iot.CfnTopicRule.S3ActionProperty(
                            bucket_name=self.raw_data_bucket.bucket_name,
                            key="home-health/${topic(3)}/${timestamp()}.json",
                            role_arn=self.iot_rule_role.role_arn
                        )
                    )
                ],
                aws_iot_sql_version="2016-03-23"
            )
        )

        # Allow IoT to invoke Lambda
        self.iot_processor_lambda.add_permission(
            "IoTInvokePermission",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
        )

        # =================================================================
        # AMAZON HEALTHLAKE - FHIR Data Store
        # =================================================================
        
        self.healthlake_datastore = healthlake.CfnFHIRDatastore(
            self, "HealthLakeDatastore",
            datastore_name="iomt-fhir-datastore",
            datastore_type_version="R4",
            sse_configuration=healthlake.CfnFHIRDatastore.SseConfigurationProperty(
                kms_encryption_config=healthlake.CfnFHIRDatastore.KmsEncryptionConfigProperty(
                    cmk_type="CUSTOMER_MANAGED_KMS_KEY",
                    kms_key_id=self.encryption_key.key_id
                )
            ),
            preload_data_config=healthlake.CfnFHIRDatastore.PreloadDataConfigProperty(
                preload_data_type="SYNTHEA"
            )
        )

        # =================================================================
        # ANALYTICS & ML LAYER
        # =================================================================
        
        # Glue Database for healthcare data
        self.glue_database = glue.CfnDatabase(
            self, "IoMTGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="iomt_analytics_db",
                description="IoMT analytics database for FHIR and device data"
            )
        )

        # Glue Crawler Role
        self.glue_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ]
        )
        self.raw_data_bucket.grant_read(self.glue_role)
        self.fhir_data_bucket.grant_read(self.glue_role)
        self.encryption_key.grant_decrypt(self.glue_role)

        # Glue Crawler for raw device data
        self.raw_data_crawler = glue.CfnCrawler(
            self, "RawDataCrawler",
            name="iomt-raw-data-crawler",
            role=self.glue_role.role_arn,
            database_name="iomt_analytics_db",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.raw_data_bucket.bucket_name}/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 */6 * * ? *)"  # Every 6 hours
            )
        )

        # Glue Crawler for FHIR data
        self.fhir_data_crawler = glue.CfnCrawler(
            self, "FHIRDataCrawler",
            name="iomt-fhir-data-crawler",
            role=self.glue_role.role_arn,
            database_name="iomt_analytics_db",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.fhir_data_bucket.bucket_name}/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 */6 * * ? *)"
            )
        )

        # Glue ETL Job for data transformation
        self.glue_etl_job = glue.CfnJob(
            self, "IoMTETLJob",
            name="iomt-etl-transform",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{self.raw_data_bucket.bucket_name}/scripts/etl_transform.py"
            ),
            glue_version="4.0",
            max_retries=1,
            timeout=60,
            number_of_workers=2,
            worker_type="G.1X",
            default_arguments={
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-enable",
                "--TempDir": f"s3://{self.raw_data_bucket.bucket_name}/temp/",
            }
        )

        # Athena Workgroup
        self.athena_results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name=f"iomt-athena-results-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.athena_workgroup = athena.CfnWorkGroup(
            self, "IoMTAthenaWorkgroup",
            name="iomt-analytics",
            description="Workgroup for IoMT data analytics",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.athena_results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_KMS",
                        kms_key=self.encryption_key.key_arn
                    )
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
            ),
            state="ENABLED"
        )

        # =================================================================
        # SAGEMAKER - ML Training & Inference
        # =================================================================
        
        # SageMaker execution role
        self.sagemaker_role = iam.Role(
            self, "SageMakerExecutionRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                )
            ]
        )
        self.raw_data_bucket.grant_read(self.sagemaker_role)
        self.fhir_data_bucket.grant_read_write(self.sagemaker_role)
        self.encryption_key.grant_encrypt_decrypt(self.sagemaker_role)

        # SageMaker Notebook Instance for model development
        self.sagemaker_notebook = sagemaker.CfnNotebookInstance(
            self, "MLNotebookInstance",
            instance_type="ml.t3.medium",
            role_arn=self.sagemaker_role.role_arn,
            notebook_instance_name="iomt-ml-notebook",
            volume_size_in_gb=50,
            kms_key_id=self.encryption_key.key_id,
            direct_internet_access="Disabled",
        )

        # =================================================================
        # OUTPUT & INTEGRATION LAYER
        # =================================================================
        
        # SNS Topic for notifications
        self.notification_topic = sns.Topic(
            self, "IoMTNotificationTopic",
            topic_name="iomt-notifications",
            master_key=self.encryption_key,
        )

        # SNS Topic for anomaly alerts
        self.anomaly_alert_topic = sns.Topic(
            self, "AnomalyAlertTopic",
            topic_name="iomt-anomaly-alerts",
            master_key=self.encryption_key,
        )

        # Lambda for anomaly detection
        self.anomaly_detector_lambda = lambda_.Function(
            self, "AnomalyDetectorLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_anomaly_detector_code()),
            role=self.lambda_role,
            timeout=Duration.minutes(1),
            memory_size=256,
            environment={
                "SNS_TOPIC_ARN": self.anomaly_alert_topic.topic_arn,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )
        self.anomaly_alert_topic.grant_publish(self.anomaly_detector_lambda)

        # Pinpoint Application for patient engagement
        self.pinpoint_app = pinpoint.CfnApp(
            self, "PatientEngagementApp",
            name="IoMTPatientEngagement"
        )

        # Pinpoint Email Channel (requires verification in production)
        self.pinpoint_email = pinpoint.CfnEmailChannel(
            self, "PinpointEmailChannel",
            application_id=self.pinpoint_app.ref,
            from_address="noreply@iomt-platform.example.com",
            identity="iomt-platform.example.com",
            enabled=True
        )

        # =================================================================
        # FHIR API (API Gateway)
        # =================================================================
        
        # API Gateway for FHIR endpoints
        self.fhir_api = apigw.RestApi(
            self, "FHIRApi",
            rest_api_name="IoMT FHIR API",
            description="FHIR-compliant REST API for IoMT data",
            deploy_options=apigw.StageOptions(
                stage_name="v1",
                logging_level=apigw.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                metrics_enabled=True,
            ),
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=["GET", "POST", "PUT", "DELETE"],
                allow_headers=["Content-Type", "Authorization"]
            )
        )

        # FHIR API Handler Lambda
        self.fhir_api_lambda = lambda_.Function(
            self, "FHIRApiLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_fhir_api_code()),
            role=self.lambda_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "HEALTHLAKE_DATASTORE_ID": self.healthlake_datastore.attr_datastore_id,
                "FHIR_BUCKET": self.fhir_data_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # Add HealthLake permissions
        self.fhir_api_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "healthlake:ReadResource",
                    "healthlake:CreateResource",
                    "healthlake:UpdateResource",
                    "healthlake:DeleteResource",
                    "healthlake:SearchWithGet",
                    "healthlake:SearchWithPost"
                ],
                resources=["*"]
            )
        )

        # API Gateway resources and methods
        patient_resource = self.fhir_api.root.add_resource("Patient")
        patient_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.fhir_api_lambda),
            authorization_type=apigw.AuthorizationType.IAM
        )
        patient_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.fhir_api_lambda),
            authorization_type=apigw.AuthorizationType.IAM
        )

        observation_resource = self.fhir_api.root.add_resource("Observation")
        observation_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.fhir_api_lambda),
            authorization_type=apigw.AuthorizationType.IAM
        )
        observation_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.fhir_api_lambda),
            authorization_type=apigw.AuthorizationType.IAM
        )

        # =================================================================
        # EVENTBRIDGE RULES for Scheduled Processing
        # =================================================================
        
        # Daily ML training trigger
        self.ml_training_rule = events.Rule(
            self, "MLTrainingRule",
            schedule=events.Schedule.cron(hour="2", minute="0"),
            description="Trigger daily ML model retraining"
        )

        # Daily analytics processing
        self.analytics_rule = events.Rule(
            self, "AnalyticsProcessingRule",
            schedule=events.Schedule.cron(hour="3", minute="0"),
            description="Trigger daily analytics processing"
        )

        # =================================================================
        # CLOUDWATCH DASHBOARDS
        # =================================================================
        
        # Create a CloudWatch Dashboard for monitoring
        self.monitoring_dashboard = self._create_monitoring_dashboard()

        # =================================================================
        # OUTPUTS
        # =================================================================
        
        CfnOutput(self, "RawDataBucketOutput",
            value=self.raw_data_bucket.bucket_name,
            description="S3 bucket for raw IoMT data"
        )

        CfnOutput(self, "FHIRDataBucketOutput",
            value=self.fhir_data_bucket.bucket_name,
            description="S3 bucket for FHIR data"
        )

        CfnOutput(self, "HealthLakeDatastoreIdOutput",
            value=self.healthlake_datastore.attr_datastore_id,
            description="Amazon HealthLake Datastore ID"
        )

        CfnOutput(self, "FHIRApiEndpoint",
            value=self.fhir_api.url,
            description="FHIR API Gateway endpoint URL"
        )

        CfnOutput(self, "IoTEndpoint",
            value=f"arn:aws:iot:{self.region}:{self.account}:topic/iomt",
            description="IoT Core topic ARN for device data"
        )

        CfnOutput(self, "NotificationTopicArn",
            value=self.notification_topic.topic_arn,
            description="SNS topic for IoMT notifications"
        )

    def _get_iot_processor_code(self) -> str:
        """Inline Lambda code for IoT data processing"""
        return '''
import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def handler(event, context):
    """Process incoming IoT device data"""
    try:
        # Parse device data
        device_id = event.get('deviceId', 'unknown')
        device_type = event.get('deviceType', 'unknown')
        timestamp = event.get('timestamp', datetime.utcnow().isoformat())
        readings = event.get('readings', {})
        
        # Validate critical readings
        alerts = []
        if readings.get('heartRate', 0) > 120 or readings.get('heartRate', 100) < 50:
            alerts.append('abnormal_heart_rate')
        if readings.get('bloodOxygen', 100) < 92:
            alerts.append('low_blood_oxygen')
        
        # Prepare message for further processing
        message = {
            'deviceId': device_id,
            'deviceType': device_type,
            'timestamp': timestamp,
            'readings': readings,
            'alerts': alerts,
            'processedAt': datetime.utcnow().isoformat()
        }
        
        # Send to SQS for FHIR conversion
        sqs.send_message(
            QueueUrl=os.environ['QUEUE_URL'],
            MessageBody=json.dumps(message)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data processed successfully', 'alerts': alerts})
        }
    except Exception as e:
        print(f"Error processing IoT data: {str(e)}")
        raise
'''

    def _get_fhir_converter_code(self) -> str:
        """Inline Lambda code for HL7/FHIR conversion"""
        return '''
import json
import boto3
import os
from datetime import datetime
import uuid

s3 = boto3.client('s3')

def handler(event, context):
    """Convert device data to FHIR format"""
    try:
        for record in event.get('Records', []):
            body = json.loads(record['body'])
            
            # Create FHIR Observation resource
            fhir_observation = {
                'resourceType': 'Observation',
                'id': str(uuid.uuid4()),
                'status': 'final',
                'category': [{
                    'coding': [{
                        'system': 'http://terminology.hl7.org/CodeSystem/observation-category',
                        'code': 'vital-signs',
                        'display': 'Vital Signs'
                    }]
                }],
                'effectiveDateTime': body.get('timestamp'),
                'device': {
                    'identifier': {
                        'system': 'urn:healthcare:devices',
                        'value': body.get('deviceId')
                    }
                },
                'component': []
            }
            
            # Add readings as components
            readings = body.get('readings', {})
            if 'heartRate' in readings:
                fhir_observation['component'].append({
                    'code': {
                        'coding': [{
                            'system': 'http://loinc.org',
                            'code': '8867-4',
                            'display': 'Heart rate'
                        }]
                    },
                    'valueQuantity': {
                        'value': readings['heartRate'],
                        'unit': 'beats/minute',
                        'system': 'http://unitsofmeasure.org',
                        'code': '/min'
                    }
                })
            
            if 'bloodOxygen' in readings:
                fhir_observation['component'].append({
                    'code': {
                        'coding': [{
                            'system': 'http://loinc.org',
                            'code': '59408-5',
                            'display': 'Oxygen saturation'
                        }]
                    },
                    'valueQuantity': {
                        'value': readings['bloodOxygen'],
                        'unit': '%',
                        'system': 'http://unitsofmeasure.org',
                        'code': '%'
                    }
                })
            
            # Store FHIR resource in S3
            date_prefix = datetime.utcnow().strftime('%Y/%m/%d')
            key = f"observations/{date_prefix}/{fhir_observation['id']}.json"
            
            s3.put_object(
                Bucket=os.environ['FHIR_BUCKET'],
                Key=key,
                Body=json.dumps(fhir_observation),
                ContentType='application/fhir+json'
            )
        
        return {'statusCode': 200, 'body': 'FHIR conversion complete'}
    except Exception as e:
        print(f"Error converting to FHIR: {str(e)}")
        raise
'''

    def _get_anomaly_detector_code(self) -> str:
        """Inline Lambda code for anomaly detection"""
        return '''
import json
import boto3
import os

sns = boto3.client('sns')

def handler(event, context):
    """Detect anomalies in healthcare data and send alerts"""
    try:
        alerts = event.get('alerts', [])
        device_id = event.get('deviceId', 'unknown')
        readings = event.get('readings', {})
        
        if alerts:
            message = {
                'alertType': 'ANOMALY_DETECTED',
                'deviceId': device_id,
                'alerts': alerts,
                'readings': readings,
                'severity': 'HIGH' if len(alerts) > 1 else 'MEDIUM'
            }
            
            sns.publish(
                TopicArn=os.environ['SNS_TOPIC_ARN'],
                Message=json.dumps(message),
                Subject=f'Healthcare Alert: {", ".join(alerts)}'
            )
        
        return {'statusCode': 200, 'body': json.dumps({'alertsSent': len(alerts)})}
    except Exception as e:
        print(f"Error in anomaly detection: {str(e)}")
        raise
'''

    def _get_fhir_api_code(self) -> str:
        """Inline Lambda code for FHIR API"""
        return '''
import json
import boto3
import os

healthlake = boto3.client('healthlake')
s3 = boto3.client('s3')

def handler(event, context):
    """Handle FHIR API requests"""
    try:
        http_method = event.get('httpMethod', 'GET')
        resource = event.get('resource', '').strip('/')
        path_params = event.get('pathParameters', {}) or {}
        query_params = event.get('queryStringParameters', {}) or {}
        body = event.get('body')
        
        datastore_id = os.environ['HEALTHLAKE_DATASTORE_ID']
        
        if http_method == 'GET':
            # Search or read operation
            if 'id' in path_params:
                response = healthlake.read_resource(
                    DatastoreId=datastore_id,
                    ResourceType=resource,
                    ResourceId=path_params['id']
                )
            else:
                # Search with query parameters
                search_params = '&'.join([f'{k}={v}' for k, v in query_params.items()])
                response = healthlake.search_with_get(
                    DatastoreId=datastore_id,
                    ResourceType=resource,
                    SearchParameters=search_params if search_params else None
                )
            
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/fhir+json'},
                'body': json.dumps(response.get('Resource', {}))
            }
        
        elif http_method == 'POST':
            # Create resource
            if body:
                resource_data = json.loads(body)
                response = healthlake.create_resource(
                    DatastoreId=datastore_id,
                    ResourceType=resource,
                    ResourceBody=json.dumps(resource_data)
                )
                return {
                    'statusCode': 201,
                    'headers': {'Content-Type': 'application/fhir+json'},
                    'body': json.dumps({'message': 'Resource created', 'id': response.get('ResourceId')})
                }
        
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid request'})
        }
    except Exception as e:
        print(f"FHIR API Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
'''

    def _create_monitoring_dashboard(self):
        """Create CloudWatch dashboard for platform monitoring"""
        from aws_cdk import aws_cloudwatch as cloudwatch
        
        dashboard = cloudwatch.Dashboard(
            self, "IoMTDashboard",
            dashboard_name="IoMTDataAnalyticsPlatform"
        )

        # Lambda metrics
        dashboard.add_widgets(
            cloudwatch.TextWidget(
                markdown="# IoMT Data Analytics Platform Monitoring",
                width=24,
                height=1
            )
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Lambda Invocations",
                left=[
                    self.iot_processor_lambda.metric_invocations(),
                    self.fhir_converter_lambda.metric_invocations(),
                    self.fhir_api_lambda.metric_invocations(),
                ],
                width=12
            ),
            cloudwatch.GraphWidget(
                title="Lambda Errors",
                left=[
                    self.iot_processor_lambda.metric_errors(),
                    self.fhir_converter_lambda.metric_errors(),
                    self.fhir_api_lambda.metric_errors(),
                ],
                width=12
            )
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="SQS Messages",
                left=[
                    self.processing_queue.metric_number_of_messages_sent(),
                    self.processing_queue.metric_number_of_messages_received(),
                    self.processing_dlq.metric_approximate_number_of_messages_visible(),
                ],
                width=12
            ),
            cloudwatch.GraphWidget(
                title="API Gateway Requests",
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/ApiGateway",
                        metric_name="Count",
                        dimensions_map={"ApiName": self.fhir_api.rest_api_name},
                        statistic="Sum"
                    )
                ],
                width=12
            )
        )

        return dashboard
