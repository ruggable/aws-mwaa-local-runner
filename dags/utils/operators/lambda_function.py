from __future__ import annotations

from typing import Any
from botocore.config import Config

from airflow.providers.amazon.aws.utils import trim_none_values
from functools import cached_property
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as BaseLambdaInvokeFunctionOperator

class LambdaInvokeFunctionOperator(BaseLambdaInvokeFunctionOperator):
    @cached_property
    def hook(self) -> LambdaHook:
        return LambdaHook(aws_conn_id=self.aws_conn_id)

class LambdaHook(AwsBaseHook):
    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "lambda"
        super().__init__(*args, **kwargs)

    @cached_property
    def conn(self):
        # Increases the read_timeout to 900 seconds
        config_dict = {"connect_timeout": 900, "read_timeout": 900, "tcp_keepalive": True}
        config = Config(**config_dict)
        return self.get_client_type(self.region_name, config)

    def invoke_lambda(
        self,
        *,
        function_name: str,
        invocation_type: str | None = None,
        log_type: str | None = None,
        client_context: str | None = None,
        payload: str | None = None,
        qualifier: str | None = None,
    ):
        invoke_args = {
            "FunctionName": function_name,
            "InvocationType": invocation_type,
            "LogType": log_type,
            "ClientContext": client_context,
            "Payload": payload,
            "Qualifier": qualifier,
        }
        return self.conn.invoke(**trim_none_values(invoke_args))