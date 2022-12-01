from typing import Iterator,List
from datetime import datetime

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String},
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
def process_data(context,stock:List)-> Aggregation:
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
def week_2_pipeline():
    stock=get_s3_data()
    put_redis_data(process_data(stock))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis":ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis":redis_resource}
)
