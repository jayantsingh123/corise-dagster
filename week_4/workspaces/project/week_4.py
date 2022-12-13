from typing import List
from datetime import datetime

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(config_schema={"s3_key": str},
required_resource_keys={"s3"},
op_tags={"kind": "s3"},
group_name="corise"
)
def get_s3_data(context)->List[Stock]:
    key_name = context.op_config["s3_key"]
    result = context.resources.s3.get_data(key_name)
    listnew = []
    for i in result:
        listnew.append(Stock.from_list(i))
    return listnew
    


@asset(group_name="corise")
def process_data(get_s3_data:List[Stock]) -> Aggregation:
    result = Aggregation(date=datetime(2020, 1, 1, 0, 0), high=0.0)
   # data = context.op_config[get_s3_data]
    for i in get_s3_data:
        if i.high>result.high:
            result.date = i.date
            result.high= i.high
    return result


@asset(required_resource_keys={"redis"},
group_name="corise")
def put_redis_data(context, process_data:Aggregation):
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


@asset(required_resource_keys={"s3"},
group_name="corise")
def put_s3_data(context, process_data:Aggregation):
    key_name = process_data.date
    context.resources.s3.put_data(key_name, process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
     definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
     resource_defs={"s3": s3_resource, "redis": redis_resource},
     resource_config_by_key={
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
        "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}

     })
