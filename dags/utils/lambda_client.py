import json
import logging
from typing import TypedDict, Literal, Any

import boto3
from airflow.exceptions import AirflowException
from airflow.models import Variable
from botocore.config import Config

logger = logging.getLogger()



DEFAULT_LAMBDA_CONFIG = Config(
    connect_timeout=900,
    read_timeout=900,
    tcp_keepalive=True,)

class InvokeLambdaResponse(TypedDict, total=False):
    StatusCode: int
    FunctionError: str | None
    LogResult: str | None
    Payload: bytes | str | None
    ExecutedVersion: str
    

class LambdaClient:
    
    client = boto3.client(
        'lambda', 
        config=DEFAULT_LAMBDA_CONFIG
        )
    
    successful_response_map: dict[str, int] = {
        'RequestResponse': 200,
        'Event': 202,
        'DryRun': 204,
    }
    
    @classmethod
    def invoke(
        cls,
        function_name: str,
        payload: bytes | str | None = None,
        invocation_type: Literal['Event','RequestResponse','DryRun'] = 'RequestResponse',
        config: Config | None = None
        ) -> str | dict[Any, Any]:
        
        # Add new config if present
        client = cls.client
        if config and isinstance(config, Config):
            new_config = DEFAULT_LAMBDA_CONFIG.merge(config)
            client = boto3.client('lambda', config=new_config)
            
        response: InvokeLambdaResponse = client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=payload,
        )
        
        # Raise for error
        if response.get("StatusCode") != cls.successful_response_map[invocation_type]:
            msg = f'Lambda ({function_name}) returned a non 200 status code ({response.get("StatusCode")})'
            logger.exception(msg=msg, extra=response)
            raise AirflowException(msg)
        
        
        # Parse response and return
        if response.get('Payload'):
            payload_response: str | dict[Any, Any] = response['Payload'].read().decode('utf-8')
            logger.debug(f'lambda payload as string: {payload_response}')
            try:
                # Try to parse the payload as JSON
                # Manually serializing the lambda response before returning
                # the response in the lambda can lead to serializing an object twice
                # hence the while loop.
                while isinstance(payload_response, str):
                    payload_response = json.loads(payload_response)
            except json.JSONDecodeError:
                msg = 'Unable to parse payload as dict, treating is as str.'
                logger.info(msg)
                
            logger.debug(f'parsed lambda payload: {payload_response}')
                
            return payload_response
        else:
            msg = f'Payload missing from lambda ({function_name}) response.'
            logger.exception(msg)
            raise AirflowException(msg)
        
        