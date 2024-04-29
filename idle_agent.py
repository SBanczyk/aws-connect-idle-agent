import subprocess
import os
import boto3
import datetime
import instance_idle_pb2
import time
import base64
import asyncio

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
REGION_NAME = os.environ["REGION_NAME"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
CHECK_INTERVAL = int(os.environ["CHECK_INTERVAL"])
PORT = os.environ["PORT"]
IDLE_TIME = int(os.environ["IDLE_TIME"])
INSTANCE_ID = os.environ["INSTANCE_ID"]


def count_connections():
    result = subprocess.run(
        ["ss", "-t", "-H", "-n", f"dport = {PORT}"], capture_output=True
    )
    return len(result.stdout.decode("utf-8").strip().split("\n")) - 1


async def send_message():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    last_connected_timestamp = None
    if count_connections() > 0:
        last_connected_timestamp = datetime.datetime.now()
    if (
        last_connected_timestamp is None
        or (
            datetime.datetime.now() - last_connected_timestamp.ToDatetime()
        ).total_seconds()
        >= IDLE_TIME
    ):
        sqs = session.resource("sqs")
        instance_idle = instance_idle_pb2.InstanceIdle()
        instance_idle.timestamp.GetCurrentTime()
        instance_idle.instance_id = INSTANCE_ID
        if last_connected_timestamp is not None:
            instance_idle.last_connected_timestamp.FromDatetime(
                last_connected_timestamp
            )
        encoded_message = base64.b64encode(instance_idle.SerializeToString()).decode(
            "ascii"
        )
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
        queue.send_message(
            MessageBody=encoded_message,
            MessageGroupId="idle-agent",
            MessageDeduplicationId=str(time.time_ns()),
            MessageAttributes={
                "origin": {
                    "StringValue": "aws-connect-idle-agent",
                    "DataType": "String",
                }
            },
        )


async def main():
    while True:
        await send_message()
        await asyncio.sleep(CHECK_INTERVAL)


asyncio.run(main())
