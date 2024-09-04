import itertools
import json
import logging
import pathlib
import subprocess
from multiprocessing import Pool
from typing import Any

import boto3
import psycopg
import xxhash
from psycopg.rows import dict_row

# FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(process)d - %(message)s"
)
logger = logging.getLogger("backy")


worker_connection = None


def initialize_worker():
    global worker_connection
    worker_connection = psycopg.connect(
        "dbname=ctheune user=ctheune", row_factory=dict_row
    )
    logging.info(worker_connection)


def _object_path(path, id: int, for_writing=False):
    id_str = f"{id:016X}"
    # Create directories for the first 3 (16 bit) words so we end up
    # with 65k entries per directory level.
    directory_str = id_str[:-4]
    while directory_str:
        segment, directory_str = directory_str[:4], directory_str[4:]
        path = path.joinpath(segment)
    path.mkdir(parents=True, exist_ok=True)
    return path.joinpath(id_str).with_suffix(".object")


def backup_batch(arg):
    conn = worker_connection
    assert conn is not None
    batch, bucket, path, revision = arg
    try:
        # XXX theoretically this could be passed into an initializer?
        cur = conn.cursor()

        ACCESS_KEY = "UO16ZELUM61Z1E5VHKQ8"
        SECRET_KEY = "Y2EaYIzoLgS2XpxaYJOlfpi43jdWw2hjdJm3tycg"

        # Detect all buckets we need to back up
        s3_client = boto3.client(
            "s3",
            # XXX I'd prefer if this was doing proper roundtrips on different
            # servers
            # ...
            endpoint_url="https://s3.whq.fcio.net",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )

        known_objects = {}
        for row in cur.execute(
            """\
            SELECT
                id, key, lastmodified, etag
            FROM
                object
            WHERE
                hash is not null AND
                key IN
            """
            + "("
            + ", ".join(("%s",) * len(batch))
            + ")",
            [o["Key"] for o in batch],
        ).fetchall():
            known_objects[(row["key"], row["lastmodified"], row["etag"])] = row[
                "id"
            ]

        queued_revision_object_markers = []

        for obj in batch:
            # XXX Theoretically users can upload the same content, which should
            # result in the same etag to modify the metadata. We're not sure
            # whether this happens sufficiently often, to justify making this
            # more  complex and splitting the metadata + etag update.

            object_id = known_objects.get(
                (obj["Key"], obj["LastModified"], obj["ETag"])
            )

            if not object_id:
                logger.info(f"Backing up object: {obj['Key']}")
                object_data = s3_client.get_object(
                    Bucket=bucket, Key=obj["Key"]
                )

                # XXX consider upserting in the previous select instead?
                object_id = cur.execute(
                    """\
                    INSERT INTO
                        object (key, lastmodified, etag)
                    VALUES
                        (%s, %s, %s)
                    RETURNING
                        id
                    """,
                    (obj["Key"], obj["LastModified"], obj["ETag"]),
                ).fetchone()["id"]

                f = _object_path(path, object_id).open("wb")
                body = object_data.pop("Body")
                hash = xxhash.xxh3_128()

                for chunk in body.iter_chunks():
                    hash.update(chunk)
                    f.write(chunk)

                cur.execute(
                    """\
                    UPDATE object
                    SET
                        hash = %s,
                        metadata = %s,
                        etag = %s
                    WHERE
                        id = %s
                    """,
                    # Note: between listing and retrieving, the ETag might have
                    # changed. Due to inaccuracies in the LastModified
                    # timestamp in get_object we don't now the millisecond
                    # accuracy of the new object. We record the new etag but
                    # we leave the higher accuracy but older timestamp in
                    # place. This might/will cause another backup with the
                    # correct timestamp and etag in the next backup, though.
                    (
                        hash.hexdigest(),
                        json.dumps(
                            object_data["ResponseMetadata"]["HTTPHeaders"]
                        ),
                        object_data["ETag"],
                        object_id,
                    ),
                )

            queued_revision_object_markers.append((revision, object_id))

        # Sync
        # We experimented again with very fine grained fsyncing. However,
        # fine-grained fsyncs cause at least 4% bandwidth overhead and
        # - more importantly - about 5x IOPS overhead. Apparently syncing a
        # full filesystem does have the benefit that journalling and ordering
        # can be smarter. This makes the code much much simpler, however,
        # this may be a bit latency sensitive in some situations where
        # multiple jobs are running in parallel and buffers might be
        # extensively used.
        logger.info("Syncing")
        subprocess.run(["sync", "-f", path], check=True)
        # This also isn't thread-safe: when running _sync nobody should be
        # modifying data in the database or on the disk in parallel.
        insert_items = len(queued_revision_object_markers)
        query = """\
            INSERT INTO
                revision_object (revision, object)
            VALUES
            """ + ", ".join(
            ["(%s, %s)"] * insert_items
        )
        cur.execute(
            query, list(itertools.chain(*queued_revision_object_markers))
        )
        conn.commit()
    except Exception:
        logger.exception("unknown error")
    conn.rollback()
    logger.info("Finished sync")


class BucketBackup:
    revision: int
    bucket: str

    def __init__(self, s3_client, path, revision, bucket):
        self.path = path
        self.s3_client = s3_client
        self.revision = revision
        self.bucket = bucket

        self.sync_attempts = 0

        self.queued_revision_object_markers = []

    def _iter_batches(self):
        # 300ms for 1k objects within WHQ on the storage network which isn't
        # the fastest so that's 5 minutes
        ContinuationToken = ""
        while True:
            logger.info("Getting batch from S3")
            res = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                ContinuationToken=ContinuationToken,
            )
            logger.info("Got batch from S3")
            yield res["Contents"], self.bucket, self.path, self.revision

            # This might be a bottleneck and we likely want to keep track of
            # how much work was actually done and then commit every now and
            # then.
            if not res["IsTruncated"]:
                break
            ContinuationToken = res["NextContinuationToken"]

    def backup_(self):
        batch = next(self._iter_batches())
        backup_batch(batch)

    def backup(self):
        logger.info(f"Backing up bucket: {self.bucket}")

        p = Pool(5, initializer=initialize_worker)
        # XXX create bucket as an object in the database for
        # (id, owner, bucket)
        # XXX also need to record the bucket in the object table

        with p:
            for _ in p.imap_unordered(
                backup_batch, self._iter_batches(), chunksize=1
            ):
                pass


def prepare_db():
    conn = psycopg.connect("dbname=ctheune user=ctheune", row_factory=dict_row)
    with conn.cursor() as cur:
        # XXX table for buckets (per repository) (id, repo, bucket_name)
        cur.execute(
            # XXX bucket id
            """
            CREATE TABLE IF NOT EXISTS object (
                id serial PRIMARY KEY,
                hash VARCHAR,
                key VARCHAR NOT NULL,
                lastmodified varchar NOT NULL,
                etag VARCHAR NOT NULL,
                metadata VARCHAR
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS "
            "obj_key_idx ON object(key,lastmodified,etag)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS "
            "obj_key_lastmodified_etag_idx ON object(key,lastmodified,etag)"
        )
        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS revision (
                id serial PRIMARY KEY
            )
            """
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS revision_object (
                revision INTEGER NOT NULL REFERENCES revision(id)
                    ON DELETE CASCADE,
                object INTEGER NOT NULL REFERENCES object(id)
                    ON DELETE CASCADE,
                PRIMARY KEY (revision, object))
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS "
            "revision_object_obj_idx ON revision_object(object)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS "
            "revision_object_rev_idx ON revision_object(revision)"
        )

        revision = cur.execute(
            "insert into revision default values returning id"
        ).fetchone()["id"]

        conn.commit()

    return revision


def main():
    ACCESS_KEY = "UO16ZELUM61Z1E5VHKQ8"
    SECRET_KEY = "Y2EaYIzoLgS2XpxaYJOlfpi43jdWw2hjdJm3tycg"

    # Detect all buckets we need to back up
    s3_client = boto3.client(
        "s3",
        # XXX I'd prefer if this was doing proper roundtrips on different
        # servers
        # ...
        endpoint_url="https://s3.whq.fcio.net",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    revision = prepare_db()

    for bucket in s3_client.list_buckets()["Buckets"]:
        logging.info(f"Backup up bucket {bucket['Name']}")
        backup = BucketBackup(
            s3_client, pathlib.Path("."), revision, bucket["Name"]
        )
        backup.backup()

    # bucket = target.open_multi("wb", self.revision.get_parent())
    # async with self.client as client:
    #     count = 0
    #     async for obj in self.client.list_obj():
    #         count += 1
    #         if count > 100_000:
    #             break
    #         if count % 1000 == 0:
    #             print(obj.key, count)
    #         if bucket.create_shallow(obj):
    #             # TODO differentiate between modified and etag missmatch?
    #             continue
    #         print("not shallow")
    #         new = bucket.create_incoming_obj()
    #         fut = await client.submit_download_obj(obj, new)
    #         fut.add_done_callback(
    #           lambda fut: bucket.create_obj(*fut.result()))
    # # complete all callbacks
    # await asyncio.sleep(0)
    # start = time.time()
    # bucket.store.db.commit()
    # print("commit time", time.time() - start)


if __name__ == "__main__":
    import cProfile
    import io
    import pstats
    from pstats import SortKey

    pr = cProfile.Profile()
    pr.enable()
    try:
        main()
    except Exception:
        logger.exception("unknown error")
    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats(50)
    print(s.getvalue())
