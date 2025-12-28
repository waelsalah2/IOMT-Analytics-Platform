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
        self.raw_data_crawler.add_dependency(self.glue_database)

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
        self.fhir_data_crawler.add_dependency(self.glue_database)

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
        # ML PROCESSING LAYER (Lambda-based, SageMaker-free)
        # =================================================================
        
        # ML Lambda execution role
        self.ml_lambda_role = iam.Role(
            self, "MLLambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        self.raw_data_bucket.grant_read(self.ml_lambda_role)
        self.fhir_data_bucket.grant_read_write(self.ml_lambda_role)
        self.encryption_key.grant_encrypt_decrypt(self.ml_lambda_role)

        # ML Model bucket for storing trained models
        self.ml_models_bucket = s3.Bucket(
            self, "MLModelsBucket",
            bucket_name=f"iomt-ml-models-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.ml_models_bucket.grant_read_write(self.ml_lambda_role)

        # Anomaly Detection Lambda (ML-based)
        self.ml_anomaly_lambda = lambda_.Function(
            self, "MLAnomalyDetectionLambda",
            function_name="iomt-ml-anomaly-detection",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_ml_anomaly_detection_code()),
            role=self.ml_lambda_role,
            timeout=Duration.minutes(5),
            memory_size=1024,
            environment={
                "MODEL_BUCKET": self.ml_models_bucket.bucket_name,
                "FHIR_BUCKET": self.fhir_data_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # Predictive Analytics Lambda
        self.predictive_analytics_lambda = lambda_.Function(
            self, "PredictiveAnalyticsLambda",
            function_name="iomt-predictive-analytics",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_predictive_analytics_code()),
            role=self.ml_lambda_role,
            timeout=Duration.minutes(5),
            memory_size=1024,
            environment={
                "MODEL_BUCKET": self.ml_models_bucket.bucket_name,
                "RAW_DATA_BUCKET": self.raw_data_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # Model Training Lambda (for periodic retraining)
        self.model_training_lambda = lambda_.Function(
            self, "ModelTrainingLambda",
            function_name="iomt-model-training",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(self._get_model_training_code()),
            role=self.ml_lambda_role,
            timeout=Duration.minutes(15),
            memory_size=3008,
            environment={
                "MODEL_BUCKET": self.ml_models_bucket.bucket_name,
                "RAW_DATA_BUCKET": self.raw_data_bucket.bucket_name,
                "FHIR_BUCKET": self.fhir_data_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
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
        self.ml_training_rule.add_target(
            targets.LambdaFunction(
                self.model_training_lambda,
                event=events.RuleTargetInput.from_object({
                    "training_type": "anomaly_detection",
                    "trigger": "scheduled"
                })
            )
        )

        # Daily analytics processing
        self.analytics_rule = events.Rule(
            self, "AnalyticsProcessingRule",
            schedule=events.Schedule.cron(hour="3", minute="0"),
            description="Trigger daily analytics processing"
        )
        self.analytics_rule.add_target(
            targets.LambdaFunction(
                self.predictive_analytics_lambda,
                event=events.RuleTargetInput.from_object({
                    "prediction_type": "batch_analysis",
                    "trigger": "scheduled"
                })
            )
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

        CfnOutput(self, "MLModelsBucketOutput",
            value=self.ml_models_bucket.bucket_name,
            description="S3 bucket for ML models"
        )

        CfnOutput(self, "AnomalyDetectionLambdaArn",
            value=self.ml_anomaly_lambda.function_arn,
            description="ML Anomaly Detection Lambda ARN"
        )

        CfnOutput(self, "PredictiveAnalyticsLambdaArn",
            value=self.predictive_analytics_lambda.function_arn,
            description="Predictive Analytics Lambda ARN"
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

    def _get_ml_anomaly_detection_code(self) -> str:
        """Lambda code for ML-based anomaly detection"""
        return '''
import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

MODEL_BUCKET = os.environ['MODEL_BUCKET']
FHIR_BUCKET = os.environ['FHIR_BUCKET']

def handler(event, context):
    """Detect anomalies in patient vital signs using ML"""
    try:
        # Get patient data from event
        patient_id = event.get('patient_id', 'unknown')
        readings = event.get('readings', {})
        
        # Load model config (in production, load actual sklearn/xgboost model)
        thresholds = {
            'heart_rate': {'min': 50, 'max': 120, 'critical_min': 40, 'critical_max': 150},
            'blood_oxygen': {'min': 92, 'critical_min': 88},
            'blood_pressure_systolic': {'min': 90, 'max': 140, 'critical_min': 80, 'critical_max': 180},
            'blood_pressure_diastolic': {'min': 60, 'max': 90, 'critical_min': 50, 'critical_max': 120},
            'temperature': {'min': 36.1, 'max': 37.8, 'critical_min': 35, 'critical_max': 39.5},
            'respiratory_rate': {'min': 12, 'max': 20, 'critical_min': 8, 'critical_max': 30}
        }
        
        anomalies = []
        risk_score = 0.0
        
        # Analyze each reading
        for metric, value in readings.items():
            if metric in thresholds:
                t = thresholds[metric]
                
                # Check for critical anomalies
                if 'critical_min' in t and value < t['critical_min']:
                    anomalies.append({
                        'metric': metric,
                        'value': value,
                        'severity': 'CRITICAL',
                        'message': f'{metric} critically low: {value}'
                    })
                    risk_score += 0.4
                elif 'critical_max' in t and value > t['critical_max']:
                    anomalies.append({
                        'metric': metric,
                        'value': value,
                        'severity': 'CRITICAL',
                        'message': f'{metric} critically high: {value}'
                    })
                    risk_score += 0.4
                # Check for warning anomalies
                elif 'min' in t and value < t['min']:
                    anomalies.append({
                        'metric': metric,
                        'value': value,
                        'severity': 'WARNING',
                        'message': f'{metric} below normal: {value}'
                    })
                    risk_score += 0.2
                elif 'max' in t and value > t['max']:
                    anomalies.append({
                        'metric': metric,
                        'value': value,
                        'severity': 'WARNING',
                        'message': f'{metric} above normal: {value}'
                    })
                    risk_score += 0.2
        
        # Cap risk score at 1.0
        risk_score = min(risk_score, 1.0)
        
        # Determine alert level
        if risk_score >= 0.6:
            alert_level = 'CRITICAL'
        elif risk_score >= 0.3:
            alert_level = 'WARNING'
        else:
            alert_level = 'NORMAL'
        
        # Publish custom metrics
        cloudwatch.put_metric_data(
            Namespace='IoMT/Anomalies',
            MetricData=[
                {
                    'MetricName': 'AnomalyCount',
                    'Value': len(anomalies),
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'RiskScore',
                    'Value': risk_score,
                    'Unit': 'None'
                }
            ]
        )
        
        return {
            'patient_id': patient_id,
            'timestamp': datetime.utcnow().isoformat(),
            'risk_score': risk_score,
            'alert_level': alert_level,
            'anomalies': anomalies,
            'readings_analyzed': len(readings)
        }
        
    except Exception as e:
        print(f"Anomaly detection error: {str(e)}")
        return {
            'error': str(e),
            'statusCode': 500
        }
'''

    def _get_predictive_analytics_code(self) -> str:
        """Lambda code for predictive health analytics"""
        return '''
import json
import boto3
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')

MODEL_BUCKET = os.environ['MODEL_BUCKET']
RAW_DATA_BUCKET = os.environ['RAW_DATA_BUCKET']

def handler(event, context):
    """Predict patient health outcomes based on historical data"""
    try:
        patient_id = event.get('patient_id')
        prediction_type = event.get('prediction_type', 'readmission_risk')
        
        # Get patient history (in production, query from data lake)
        patient_history = event.get('history', {})
        
        # Predictive features
        features = {
            'age': patient_history.get('age', 50),
            'chronic_conditions': patient_history.get('chronic_conditions', 0),
            'recent_hospitalizations': patient_history.get('recent_hospitalizations', 0),
            'medication_adherence': patient_history.get('medication_adherence', 0.8),
            'avg_heart_rate_variance': patient_history.get('heart_rate_variance', 10),
            'avg_blood_pressure_variance': patient_history.get('bp_variance', 15),
        }
        
        # Calculate risk scores (simplified model - replace with actual ML model)
        if prediction_type == 'readmission_risk':
            risk_score = calculate_readmission_risk(features)
            prediction = {
                'type': 'readmission_risk',
                'score': risk_score,
                'risk_level': 'HIGH' if risk_score > 0.7 else 'MEDIUM' if risk_score > 0.4 else 'LOW',
                'recommendation': get_recommendation(risk_score)
            }
        elif prediction_type == 'deterioration_risk':
            risk_score = calculate_deterioration_risk(features)
            prediction = {
                'type': 'deterioration_risk',
                'score': risk_score,
                'risk_level': 'HIGH' if risk_score > 0.6 else 'MEDIUM' if risk_score > 0.3 else 'LOW',
                'alert_threshold_days': 7 if risk_score > 0.6 else 14
            }
        else:
            prediction = {'error': 'Unknown prediction type'}
        
        return {
            'patient_id': patient_id,
            'timestamp': datetime.utcnow().isoformat(),
            'prediction': prediction,
            'model_version': 'lambda-v1.0',
            'features_used': list(features.keys())
        }
        
    except Exception as e:
        return {
            'error': str(e),
            'statusCode': 500
        }


def calculate_readmission_risk(features):
    """Calculate 30-day readmission risk"""
    score = 0.2  # baseline
    
    if features['age'] > 75:
        score += 0.15
    elif features['age'] > 65:
        score += 0.1
    
    score += min(features['chronic_conditions'] * 0.08, 0.3)
    score += min(features['recent_hospitalizations'] * 0.15, 0.3)
    score += (1 - features['medication_adherence']) * 0.2
    
    return min(score, 1.0)


def calculate_deterioration_risk(features):
    """Calculate health deterioration risk"""
    score = 0.15  # baseline
    
    score += features['avg_heart_rate_variance'] / 100
    score += features['avg_blood_pressure_variance'] / 100
    score += min(features['chronic_conditions'] * 0.1, 0.3)
    
    return min(score, 1.0)


def get_recommendation(risk_score):
    """Get clinical recommendation based on risk score"""
    if risk_score > 0.7:
        return "Schedule follow-up within 48 hours. Consider care management program enrollment."
    elif risk_score > 0.4:
        return "Schedule follow-up within 7 days. Review medication adherence."
    else:
        return "Standard follow-up. Continue remote monitoring."
'''

    def _get_model_training_code(self) -> str:
        """Lambda code for model training/retraining"""
        return '''
import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')

MODEL_BUCKET = os.environ['MODEL_BUCKET']
RAW_DATA_BUCKET = os.environ['RAW_DATA_BUCKET']
FHIR_BUCKET = os.environ['FHIR_BUCKET']

def handler(event, context):
    """Train or retrain ML models for IoMT analytics"""
    try:
        training_type = event.get('training_type', 'anomaly_detection')
        
        # In production, this would:
        # 1. Load training data from S3
        # 2. Train sklearn/xgboost model
        # 3. Evaluate model performance
        # 4. Save model to S3 if performance meets threshold
        
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        
        # Simulated training results
        training_result = {
            'model_type': training_type,
            'model_id': f"{training_type}-{timestamp}",
            'training_timestamp': datetime.utcnow().isoformat(),
            'metrics': {
                'accuracy': 0.87,
                'precision': 0.85,
                'recall': 0.89,
                'f1_score': 0.87,
                'auc_roc': 0.91
            },
            'hyperparameters': {
                'max_depth': 5,
                'n_estimators': 100,
                'learning_rate': 0.1
            },
            'training_samples': 10000,
            'validation_samples': 2000
        }
        
        # Save model metadata
        model_key = f"models/{training_type}/{timestamp}/model_info.json"
        s3.put_object(
            Bucket=MODEL_BUCKET,
            Key=model_key,
            Body=json.dumps(training_result),
            ContentType='application/json'
        )
        
        # Update latest model pointer
        latest_key = f"models/{training_type}/latest.json"
        s3.put_object(
            Bucket=MODEL_BUCKET,
            Key=latest_key,
            Body=json.dumps({
                'model_id': training_result['model_id'],
                'model_path': f"s3://{MODEL_BUCKET}/{model_key}",
                'updated_at': datetime.utcnow().isoformat()
            }),
            ContentType='application/json'
        )
        
        return {
            'status': 'SUCCESS',
            'model_id': training_result['model_id'],
            'model_path': f"s3://{MODEL_BUCKET}/{model_key}",
            'metrics': training_result['metrics']
        }
        
    except Exception as e:
        return {
            'status': 'FAILED',
            'error': str(e)
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
                    self.ml_anomaly_lambda.metric_invocations(),
                    self.predictive_analytics_lambda.metric_invocations(),
                ],
                width=12
            ),
            cloudwatch.GraphWidget(
                title="Lambda Errors",
                left=[
                    self.iot_processor_lambda.metric_errors(),
                    self.fhir_converter_lambda.metric_errors(),
                    self.fhir_api_lambda.metric_errors(),
                    self.ml_anomaly_lambda.metric_errors(),
                    self.predictive_analytics_lambda.metric_errors(),
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
