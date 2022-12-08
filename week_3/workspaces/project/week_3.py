from typing import List
from dagster import String
from datetime import datetime

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": str},
required_resource_keys={"s3"})
def get_s3_data(context)->List[Stock]:
    key_name = context.op_config["s3_key"]
    #print(key_name)
    result = context.resources.s3.get_data(key_name)
    listnew = []
    #print(result)
    for i in result:
        listnew.append(Stock.from_list(i))
    return listnew


@op
def process_data(stock:List)-> Aggregation:
    result = Aggregation(date=datetime(2020, 1, 1, 0, 0), high=0.0)
    for i in stock:
        if i.high>result.high:
            result.date = i.date
            result.high= i.high
    return result


@op(required_resource_keys={"redis"})
def put_redis_data(context, maximum:Aggregation):
    context.resources.redis.put_data(str(maximum.date), str(maximum.high))
    


@op(required_resource_keys={"s3"})
def put_s3_data(context, data:Aggregation):
    key_name = data.date
    context.resources.s3.put_data(key_name, data)


@graph
def week_3_pipeline():
    stock=get_s3_data()
    s1=  process_data(stock)
    put_redis_data(s1)
    put_s3_data(s1)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}
##stocks = ["prefix/stock_1.csv", "prefix/stock_2.csv",
##"prefix/stock_3.csv", "prefix/stock_4.csv", "prefix/stock_5.csv", "prefix/stock_6.csv",
##"prefix/stock_7.csv", "prefix/stock_8.csv", "prefix/stock_9.csv","prefix/stock_10.csv"]

stocks=[str(i) for i in range(1,11)]
@static_partitioned_config(partition_keys=stocks)
def docker_config(partition_key:str):
    partition_key = "prefix/stock_" + partition_key + ".csv"
    return {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }
            },
               "redis": {
                   "config": {
                      "host": "redis",
                       "port": 6379,
            }
        }
        },
        "ops": {"get_s3_data": {"config": {"s3_key": partition_key}}},
}

week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis":ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis":redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


week_3_schedule_local = ScheduleDefinition(job = week_3_pipeline_local, cron_schedule = "*/15 * * * *")


@schedule(cron_schedule="0 * * * *",job=week_3_pipeline_docker)
def week_3_schedule_docker():
    for file in stocks:
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=file, run_key=file)
        yield request
    


@sensor(job = week_3_pipeline_docker)
def week_3_sensor_docker(context):
    new_files = get_s3_keys("dagster", "prefix", "http://localstack:4566")
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key = new_file,
            run_config = {
                "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }},
            "redis": {
                   "config": {
                      "host": "redis",
                       "port": 6379,
            }
        }},
                "ops": {"get_s3_data": {"config": {"s3_key": new_file}}}
            } )
