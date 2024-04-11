import asyncio
from asyncio import AbstractEventLoop, Future, Semaphore, Task
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import AsyncIterable, Iterable, Optional, Set, Tuple

import boto3
import boto3.s3
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import DeleteObjectOutputTypeDef
from rich import print as rprint
from structlog.stdlib import BoundLogger

from backy.backends import BackyBackend
from backy.backends.s3 import BucketSnapshot, S3Backend
from backy.backends.s3.obj_types import ObjectRestoreTarget, RemoteS3Obj
from backy.backends.s3.store import S3Obj, TemporaryS3Obj
from backy.revision import Revision
from backy.sources import BackySource, BackySourceContext, BackySourceFactory
from backy.utils import FuturePool

# TODO: service-2.sdk-extras.json should be included in backy, see: https://github.com/ceph/ceph/tree/main/examples/rgw/boto3


class S3(BackySource, BackySourceFactory, BackySourceContext):
    revision: Revision
    log: BoundLogger
    client: "AsyncS3Client"

    def __init__(self, config: dict, log: BoundLogger) -> None:
        self.client = AsyncS3Client(
            config["bucket"],
            config["endpoint_url"],
            config["access_key"],
            config["secret_key"],
        )
        self.log = log.bind(subsystem="s3")

    def ready(self) -> bool:
        return self.client.head_bucket()

    def __call__(self, revision: Revision) -> BackySourceContext:
        self.revision = revision
        return self

    def __enter__(self) -> BackySource:
        return self

    def backup(self, target: BackyBackend) -> None:
        assert isinstance(target, S3Backend)
        asyncio.run(self._backup(target))

    async def _backup(self, target: S3Backend):
        bucket = target.open_multi("wb", self.revision.get_parent())
        async with self.client(THREAD_POOL_SIZE) as client:
            count = 0
            async for obj in self.client.list_obj():
                count += 1
                print(obj.key, count)
                if bucket.create_shallow(obj):
                    # TODO differentiate between modified and etag missmatch
                    continue
                new = bucket.create_incoming_obj()
                fut = await client.submit_download_obj(obj, new)
                fut.add_done_callback(
                    lambda fut: bucket.create_obj(*fut.result())
                )
        # complete all callbacks
        await asyncio.sleep(0)

    def verify(self, target: BackyBackend) -> bool:
        return True


class S3LocalRestoreTarget(ObjectRestoreTarget):
    path: Path
    separator: Optional[str]

    def __init__(self, path: Path, separator: Optional[str] = "/"):
        self.path = path
        self.separator = separator

    def resolve_path(self, key: str):
        if self.separator is None:
            p = self.path / key
        else:
            p = Path(self.path, *key.split(self.separator))
        assert p.resolve().is_relative_to(self.path)
        return p

    async def submit_delete_obj(self, key: str) -> Future[None]:
        p = self.resolve_path(key)
        p.unlink()
        future = Future()
        future.set_result(None)
        return future

    async def submit_upload_obj(self, obj: S3Obj) -> Future[None]:
        p = self.resolve_path(obj.key)
        p.parent.mkdir(parents=True, exist_ok=True)
        with obj.open("rb") as source, p.open("wb") as target:
            target.write(source)
        future = Future()
        future.set_result(None)
        return future

    def list_obj(self, glob: str = "") -> AsyncIterable[RemoteS3Obj]:
        for root, dirs, files in self.path.walk():
            for f in files:
                yield root / f

    def __exit__(self, exc_type, exc_val, exc_tb):
        for root, dirs, files in self.path.walk():
            if not files:
                root.rmdir()


THREAD_POOL_SIZE = 50


class AsyncS3Client(ObjectRestoreTarget):
    bucket: str
    client: S3Client
    future_pool: FuturePool
    log: BoundLogger

    def __init__(
        self,
        bucket: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        log: BoundLogger,
    ):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.log = log.bind(subsystem="s3-client")

    def __call__(self, pool_size: int) -> "AsyncS3Client":
        self.future_pool = FuturePool(
            asyncio.get_running_loop(), pool_size, thread_support=True
        )
        return self

    async def __aenter__(self) -> "AsyncS3Client":
        await self.future_pool.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.future_pool.__aexit__(exc_type, exc_val, exc_tb)

    async def submit_delete_obj(
        self, key: str
    ) -> Future[DeleteObjectOutputTypeDef]:
        return await self.future_pool.submit(
            partial(self.client.delete_object, Bucket=self.bucket, Key=key),
        )

    async def submit_upload_obj(self, obj: S3Obj) -> Future[None]:
        return await self.future_pool.submit(
            partial(
                self.client.upload_file,
                Filename=str(obj.path),
                Bucket=self.bucket,
                Key=obj.key,
                ExtraArgs={"Metadata": obj.get_metadata()},
            ),
        )

    async def submit_download_obj(
        self, spec: RemoteS3Obj, target: TemporaryS3Obj
    ) -> Future[Tuple[RemoteS3Obj, TemporaryS3Obj]]:
        return await self.future_pool.submit(
            partial(self._download_obj, spec, target)
        )

    def _download_obj(
        self, spec: RemoteS3Obj, target: TemporaryS3Obj
    ) -> Tuple[RemoteS3Obj, TemporaryS3Obj]:
        obj = self.client.get_object(Bucket=self.bucket, Key=spec.key)
        with target.open() as f:
            for chunk in obj["Body"].iter_chunks():
                f.write(chunk)
        target.set_metadata(obj["Metadata"])
        # XXX LastModified from listobjects has milliseconds accuracy, getobject only has seconds accuracy
        spec.etag = obj["ETag"]
        return spec, target

    async def list_obj(self, glob: str = "") -> AsyncIterable[RemoteS3Obj]:
        # TODO: ceph extension: unordered, might improve performance: not really
        # MaxKeys is capped at 1k
        inflight: Optional[Task] = asyncio.create_task(
            asyncio.to_thread(self.client.list_objects_v2, Bucket=self.bucket)
        )
        while inflight:
            res = await inflight
            if res["IsTruncated"]:
                inflight = asyncio.create_task(
                    asyncio.to_thread(
                        self.client.list_objects_v2,
                        Bucket=self.bucket,
                        ContinuationToken=res["NextContinuationToken"],
                    )
                )
            else:
                inflight = None
            for o in res["Contents"]:
                yield RemoteS3Obj.from_api(o)
            # rprint(res)

    def head_bucket(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except ClientError:
            self.log.info("head_bucket", exc_style="short")
            return False


class S3RemoteRestoreTarget(S3Client):
    pass
