import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    description="Get a list of stocks from a file"
    )
def get_s3_data_op(context):
    file_path = context.op_config["s3_key"]
    return list(csv_helper(file_path))


@op(
    ins={"stocks": In(dagster_type=List[Stock], description="A list of Stock data")},
    out={"highest_stock": Out(dagster_type=Aggregation, description="The highest high of stock data")},
    description="Get the stock with the greatest high value"
)
def process_data_op(context, stocks):
    highest_stock = max(stocks, key = lambda s: s.high)
    #print(highest_stock)
    return Aggregation(date = highest_stock.date, high = highest_stock.high)


@op(
    ins={"data": In(dagster_type=Aggregation, description="Aggregated stock data")},
    description="Upload an Aggregation to Redis"
    )
def put_redis_data_op(context, data):
    pass


@op(
    ins={"data": In(dagster_type=Aggregation, description="Aggregated stock data")},
    description="Upload an Aggregation to S3"
    )
def put_s3_data_op(context, data):
    pass


@job
def machine_learning_job():
    highest_stock = process_data_op(get_s3_data_op())
    put_redis_data_op(highest_stock)
    put_s3_data_op(highest_stock)
