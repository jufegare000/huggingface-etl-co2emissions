from shared.domain.services.s3.s3_json_reader_service import S3JsonReaderService


class S3JsonReaderServiceImplemented(S3JsonReaderService):


    def read_json_from_s3(self, bucket: str, key: str) -> Dict[str, Any]:
        if not self.s3_service.s3_object_exists(bucket, key):
            return None

        response = self.s3_service.get_object(bucket, key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)
