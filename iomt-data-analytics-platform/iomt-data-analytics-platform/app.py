#!/usr/bin/env python3
"""
IoMT Data Analytics Platform CDK Application
AWS Architecture for Internet of Medical Things data ingestion, processing, and analytics
"""

import aws_cdk as cdk
from stacks.iomt_platform_stack import IoMTPlatformStack

app = cdk.App()

IoMTPlatformStack(
    app, 
    "IoMTDataAnalyticsPlatform",
    description="IoMT Data Analytics Platform with medical device ingestion, EHR integration, and ML analytics",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    )
)

app.synth()
