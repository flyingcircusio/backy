from .file import File
from .store import Store
from .chunk import Chunk
from backy.utils import report_status, END
from backy.revision import TRUST_VERIFIED, TRUST_DISTRUSTED
import logging
import os.path


logger = logging.getLogger(__name__)



class ChunkedFileBackend(object):

    # Normally a new revision will be made by copying the last revision's file.
    # We need to be able to not do this in case of converting from a different
    # format.
    clone_parent = True

    STORES = dict()

    def __init__(self, revision):
        self.backup = revision.backup
        self.revision = revision
        path = self.revision.backup.path + '/chunks'
        if path not in self.STORES:
            self.STORES[path] = Store(path=self.revision.backup.path + '/chunks')
        self.store = self.STORES[path]

    def open(self, mode='rb'):

        if 'w' in mode or '+' in mode and self.clone_parent:
            parent = self.revision.get_parent()
            if parent and not os.path.exists(self.revision.filename):
                with open(self.revision.filename, 'wb') as new, \
                        open(parent.filename, 'rb') as old:
                    # This is ok, this is just metadata, not the actual data.
                    new.write(old.read())
        overlay = False
        if mode == 'o':
            mode = 'rw'
            overlay = True
        file = File(self.revision.filename, self.store, mode, overlay)

        if file.writable() and not self.store.force_writes:
            # "Force write"-mode if any revision is distrusted.
            for revision in self.backup.clean_history:
                if revision.trust == TRUST_DISTRUSTED:
                    logger.warn(
                        'Noticed untrusted revisions - forcing full writes.')
                    self.store.force_writes = True
                    break

        return file

    def purge(self):
        self.store.users = []
        for revision in self.backup.history:
            try:
                self.store.users.append(
                    self.backup.backend_factory(revision).open())
            except ValueError:
                # Invalid format, like purging non-chunked with chunked backend
                pass
        self.store.purge()

    @report_status
    def verify(self):
        logger.info('Verifying revision {} ...'.format(self.revision.uuid))
        verified_chunks = set()

        # Load verified chunks to avoid duplicate work
        for revision in self.backup.clean_history:
            if revision.trust != TRUST_VERIFIED:
                continue
            f = self.backup.backend_factory(revision).open()
            verified_chunks.update(f._mapping.values())

        logger.info('Using {} verified hashes'.format(len(verified_chunks)))

        errors = False
        # Go through all chunks and check them. Delete problematic ones.
        f = self.open()
        hashes = set(f._mapping.values()) - verified_chunks
        yield len(hashes) + 2
        for candidate in hashes:
            yield
            if candidate in verified_chunks:
                continue
            try:
                c = Chunk(f, 0, self.store, candidate)
                c._read_existing()
            except Exception:
                logger.exception('Error in chunk {} '.format(candidate),
                                 exc_info=True)
                errors = True
                if os.path.exists(self.store.chunk_path(candidate)):
                    try:
                        os.unlink(self.store.chunk_path(candidate))
                    except Exception:
                        logger.exception(
                            'Error removing chunk {} '.format(candidate),
                            exc_info=True)
                # This is an optimisation: we can skip this revision, purge it
                # and then keep verifying other chunks. This avoids checking
                # things unnecessarily in duplicate.
                # And we only mark it as verified if we never saw any problems.
                break

        yield

        if errors:
            # Found any issues? Delete this revision as we can't trust it.
            logger.error('Found problem - removing revision.'.format(errors))
            self.revision.remove()
        else:
            # No problems found - mark as verified.
            logger.info('No problems found - marking as verified.')
            self.revision.verify()
            self.revision.write_info()

        yield

        # Purge to ensure that we don't leave unused, potentially untrusted
        # stuff around, especially if this was the last revision.
        logger.info('Purging ...')
        self.purge()

        yield END
        yield None

    def scrub(self, backup, type):
        if type == 'light':
            return self.scrub_light(backup)
        elif type == 'deep':
            return self.scrub_deep(backup)
        else:
            raise RuntimeError('Invalid scrubbing type {}'.format(type))

    def scrub_light(self, backup):
        errors = 0
        print("Validating revisions")
        for revision in backup.history:
            print(revision.uuid)
            backend = backup.backend_factory(revision).open()
            for hash in backend._mapping.values():
                if os.path.exists(backend.store.chunk_path(hash)):
                    continue
                print("Missing chunk {} in revision {}".format(
                      hash, revision.uuid))
                errors += 1
        return errors

    def scrub_deep(self, backup):
        errors = self.scrub_light(backup)
        print("Validating chunks")
        errors += self.store.validate_chunks()
        return errors
