# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_structured(level, message, request_id, **kwargs):
    """Helper function for structured logging"""
    log_entry = {
        "level": level,
        "message": message,
        "request_id": request_id,
        "table_name": os.environ.get("TABLE_NAME"),
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def handler(event, context):
    request_id = context.request_id
    table = os.environ.get("TABLE_NAME")
    
    log_structured("INFO", "Processing request", request_id, table_name=table)
    
    if event["body"]:
        item = json.loads(event["body"])
        log_structured("INFO", "Received payload", request_id, item=item)
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        log_structured("INFO", "Data inserted successfully", request_id, item_id=id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        log_structured("INFO", "Received request without payload", request_id)
        default_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": default_id},
            },
        )
        message = "Successfully inserted data!"
        log_structured("INFO", "Default data inserted", request_id, item_id=default_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
