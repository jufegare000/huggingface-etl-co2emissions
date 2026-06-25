import json

import boto3
from botocore.exceptions import ClientError

from shared.domain.services.security.secret_obtainer import SecretObtainer


class SecretsManagerObtainer(SecretObtainer):
    secrets_client = boto3.client("secretsmanager")

    def get_secret_token(self, secret_name: str) -> str:
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
        except ClientError as exc:
            raise RuntimeError(
                f"Could not read Hugging Face token from Secrets Manager secret: {secret_name}"
            ) from exc

        secret_string = response.get("SecretString")

        if not secret_string:
            raise ValueError(
                f"Secret {secret_name} does not contain SecretString. Binary secrets are not supported."
            )

        if not secret_string.strip().startswith("{"):
            return secret_string.strip()

        secret_json = json.loads(secret_string)

        for key in ("HF_TOKEN", "hf_token", "token"):
            token = secret_json.get(key)
            if token:
                return str(token).strip()

        raise ValueError(
            f"Secret {secret_name} is JSON but does not contain one of: HF_TOKEN, hf_token, token"
        )
