import asyncio
from typing import Iterable, Optional, Set

import backy.backends.s3
from backy.backends.s3.store import S3Obj, TemporaryS3Obj
from backy.revision import Revision
from backy.sources.obj_types import ObjectRestoreTarget, RemoteS3Obj


class BucketSnapshot:
    parent_id: Optional[int] = None
    id: int
    store: "backy.backends.s3.Store"
    mode: str  # TODO used?

    def __init__(
        self,
        store: "backy.backends.s3.Store",
        mode: str,
        revision: Revision,
        parent: Optional[Revision],
    ):
        self.store = store
        self.mode = mode
        with self.store.db:
            self.id = store.get_or_create_rev(revision)
        if parent:
            self.parent_id = store.get_rev_id(parent)
            if not self.parent_id:
                print("no parent found")

    def create_shallow(self, obj: RemoteS3Obj) -> bool:
        if self.parent_id:
            with self.store.db:
                print("create shallow", obj.key, obj.lastmodified, obj.etag)
                o = self.store.get_object(obj.key, self.parent_id)
                if (
                    o
                    and o.lastmodified == obj.lastmodified
                    and o.etag == obj.etag
                ):
                    self.store.add_obj_rev_relation(o.id, self.id)
                    return True
        return False

    def create_obj(
        self, tempobj: TemporaryS3Obj, remote_obj: RemoteS3Obj
    ) -> S3Obj:
        # TODO mode
        with self.store.db:
            print(
                "adding obj",
                remote_obj.key,
                remote_obj.lastmodified,
                remote_obj.etag,
            )
            obj = self.store.add_object(remote_obj)
            self.store.add_obj_rev_relation(obj.id, self.id)
            obj.path.parent.mkdir(parents=True, exist_ok=True)
            tempobj.path.rename(obj.path)
            tempobj.meta_path.rename(obj.meta_path)
            return obj

    def list_obj(self) -> Iterable[S3Obj]:
        yield from self.store.list_obj(self.id)

    def create_incoming_obj(self) -> TemporaryS3Obj:
        return self.store.create_incoming_obj()

    def get_obj(self, key: str) -> Optional[S3Obj]:
        return self.store.get_object(key, self.id)

    def restore(self, target: ObjectRestoreTarget) -> None:
        # assumption: bucket exists and is empty
        # self.client.create_bucket(Bucket=bucket)
        # self.client.head_bucket(Bucket=bucket)
        asyncio.run(self._restore(target))

    async def _restore(self, target: "ObjectRestoreTarget") -> None:
        remote_obj: Set[str] = set()
        async with target:
            async for obj in target.list_obj():
                remote_obj.add(obj.key)
                print("restore found remote", obj.key)

                # todo: handle locking, versioning, errors
                local_obj = self.get_obj(obj.key)
                if not local_obj:
                    print("restore unknown remote", obj.key)
                    await target.submit_delete_obj(obj.key)
                elif (
                    local_obj.lastmodified != obj.lastmodified
                    or local_obj.etag != obj.etag
                ):
                    # fixme: lastmodified will change when uploading
                    print("restore remote changed", obj.key)
                    print(local_obj.lastmodified, obj.lastmodified)
                    print(local_obj.etag, obj.etag)
                    await target.submit_upload_obj(local_obj)
            for local_obj in self.list_obj():
                if local_obj.key not in remote_obj:
                    print("restore remote missing", local_obj.key)
                    await target.submit_upload_obj(local_obj)
