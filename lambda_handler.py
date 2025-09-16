"""
AWS Lambda handler entry point.
This file should be at the root of your deployment package.
"""
from lambda_function.handler import lambda_handler

# AWS Lambda will call this function
__all__ = ['lambda_handler']