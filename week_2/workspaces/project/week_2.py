from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    out={"stocks": Out(dagster_type=List[Stock])},
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get a list of stocl data from S3"
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    stocks = context.resources.s3.get_data(s3_key)
    # put s3 data into list of Stock
    stock_list = []
    for stock in stocks:
        stock_list.append(Stock.from_list(stock))
    return stock_list

@op(
    ins={"stocks": In(dagster_type=List[Stock], description="A list of Stock data")},
    out={"highest_stock": Out(dagster_type=Aggregation, description="The highest high of stock data")},
    description="Get the stock with the greatest high value"
)
def process_data_op(stocks):
    highest_stock = max(stocks, key = lambda s: s.high)
    #print(highest_stock)
    return Aggregation(date = highest_stock.date, high = highest_stock.high)

@op(
    ins={"data": In(dagster_type=Aggregation, description="Aggregated stock data")},
    config_schema={"redis_key": String},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Upload an Aggregation to Redis"
    )
def put_redis_data(context, aggregation):
    # convert result to string
    result_date = str(aggregation.date)
    result_high = str(aggregation.high)
    #put data to Redis
    context.resources.redis.put_data(result_date, result_high)


@op(
    ins={"data": In(dagster_type=Aggregation, description="Aggregated stock data")},
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Upload an Aggregation to S3"
    )
def put_s3_data(context, aggregation):
    # convert result to string
    result_date = str(aggregation.date)
    result_high = str(aggregation.high)
    #put data to Redis
    context.resources.s3.put_data(result_date, result_high)


@graph
def machine_learning_graph():
    highest_stock = process_data_op(get_s3_data())
    put_redis_data(highest_stock)
    put_redis_data(highest_stock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": redis_resource}
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)
