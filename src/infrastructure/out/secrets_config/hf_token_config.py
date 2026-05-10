import boto3
import os
import sys
import argparse
from typing import Optional

class HFTokenClass:
    @staticmethod
    def get_hf_token(
        secret_name: str | None,
        region_name: str | None,
        fallback_token: str | None = None
    ) -> str:
        if fallback_token:
            return fallback_token

        if not secret_name:
            raise ValueError("hf_secret_name is required when no fallback token is provided")

        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" not in response:
            raise ValueError("SecretString not found in secret response")

        return response["SecretString"]

    def get_token_from_glue(self) -> str:
        try:
            from awsglue.utils import getResolvedOptions
        except ImportError as e:
            raise RuntimeError("awsglue not available") from e

        args = getResolvedOptions(sys.argv, ["hf_token"])
        return args["hf_token"]

    def _get_token_from_local(self) -> Optional[str]:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--hf_token", required=False)
        known_args, _ = parser.parse_known_args()

        if known_args.hf_token:
            return known_args.hf_token

        return os.getenv("HF_TOKEN")

    def get_hf_token_from_call(self) -> str:
        token = None

        try:
            token = self.get_token_from_glue()
        except Exception:
            token = self._get_token_from_local()

        if not token:
            raise ValueError(
                "Token not found"
            )

        return token