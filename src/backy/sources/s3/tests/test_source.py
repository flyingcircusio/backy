from typing import Dict, Optional, Set

from botocore.exceptions import ClientError
from mypy_boto3_s3.type_defs import (
    DeleteObjectOutputTypeDef,
    GetObjectTaggingOutputTypeDef,
    HeadBucketOutputTypeDef,
    ListObjectsV2OutputTypeDef,
    PutObjectTaggingOutputTypeDef,
)


class BotoObjectMock:
    key:str

    def __init__(self):
        pass


SUCCESSFUL_RESPONSE_META = {
    "RequestId": "aaa",
    "HTTPStatusCode": 200,
    "HTTPHeaders": dict(),
    "RetryAttempts": 0,
    "HostId": "",
}

def to_client_error(f):
    def wrapped(self, *args, **kw):
        try:
            f(*args, **kw)
        except Exception as e:
            raise ClientError({}, "") from e

    return wrapped


class BotoClientMock:
    buckets: Dict[str, Dict[str,BotoObjectMock]]

    def __init__(self, **buckets: Set[BotoObjectMock]):
        self.buckets = buckets

    @to_client_error
    def delete_object(self, Bucket: str, Key: str) -> DeleteObjectOutputTypeDef:
        self.buckets[Bucket].

        return {
        "DeleteMarker": False,
        "VersionId": "ahirtn",
        "RequestCharged": "requester",
        "ResponseMetadata": SUCCESSFUL_RESPONSE_META,
        }

    @to_client_error
    def put_object_tagging(self, Bucket: str, Key: str, Tagging: dict) -> PutObjectTaggingOutputTypeDef:
        {"TagSet": tag_set}

    @to_client_error
    def upload_file(self, Filename: str, Bucket: str, Key: str, ExtraArgs: dict) -> None:
        {"Metadata": meta["Metadata"],
         "Tagging": parse.urlencode(meta.get("TagSet", {})),
         }

    @to_client_error
    def get_object_tagging(self, Bucket: str, Key: str) -> GetObjectTaggingOutputTypeDef:
        pass

    @to_client_error
    def list_objects_v2(self, Bucket: str, ContinuationToken: Optional[str] = None) -> ListObjectsV2OutputTypeDef:
        pass

    @to_client_error
    def head_bucket(self, Bucket: str) -> HeadBucketOutputTypeDef:
        assert Bucket in self.buckets
        return {
            "BucketLocationType": "AvailabilityZone",
            "BucketLocationName": "test-location",
            "BucketRegion": "test-region",
            "AccessPointAlias": False,
            "ResponseMetadata": SUCCESSFUL_RESPONSE_META,
        }
