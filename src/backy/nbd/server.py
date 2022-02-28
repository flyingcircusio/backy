"""

Adapted for backy. Taken from
https://github.com/reidrac/swift-nbd-server/blob/master/swiftnbd/server.py
Revision e4ce7356d0b2fbda364336202643f8c108128994

swiftnbd. server module
Copyright (C) 2013-2016 by Juan J. Martinez <jjm@usebox.net>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import asyncio
import logging
import signal
import struct


class AbortedNegotiationError(IOError):
    pass


class Server(object):
    """
    Class implementing the server.
    """

    # NBD's magic
    NBD_HANDSHAKE = 0x49484156454F5054
    NBD_REPLY = 0x3e889045565a9

    NBD_REQUEST = 0x25609513
    NBD_RESPONSE = 0x67446698

    NBD_OPT_EXPORTNAME = 1
    NBD_OPT_ABORT = 2
    NBD_OPT_LIST = 3

    NBD_REP_ACK = 1
    NBD_REP_SERVER = 2
    NBD_REP_ERR_UNSUP = 2**31 + 1

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    # fixed newstyle handshake
    NBD_HANDSHAKE_FLAGS = (1 << 0)

    # has flags, supports flush
    NBD_EXPORT_FLAGS = (1 << 0) ^ (1 << 2)
    NBD_RO_FLAG = (1 << 1)

    def __init__(self, addr, backup):
        self.log = logging.getLogger(__package__)

        self.address = addr
        self.backup = backup

    async def nbd_response(self, writer, handle, error=0, data=None):
        writer.write(struct.pack('>LLQ', self.NBD_RESPONSE, error, handle))
        if data:
            writer.write(data)
        await writer.drain()

    async def handler(self, reader, writer):
        """Handle the connection"""
        revision = None
        try:
            host, port = writer.get_extra_info("peername")
            self.log.info("Incoming connection from %s:%s" % (host, port))

            # initial handshake
            writer.write(b"NBDMAGIC" + struct.pack(">QH", self.NBD_HANDSHAKE,
                                                   self.NBD_HANDSHAKE_FLAGS))
            await writer.drain()

            data = await reader.readexactly(4)
            try:
                client_flag = struct.unpack(">L", data)[0]
            except struct.error:
                raise IOError("Handshake failed, disconnecting")

            # we support both fixed and unfixed new-style handshake
            if client_flag == 0:
                fixed = False
                self.log.warning("Client using new-style non-fixed handshake")
            elif client_flag & 1:
                fixed = True
            else:
                raise IOError("Handshake failed, disconnecting")

            # negotiation phase
            while True:
                header = await reader.readexactly(16)
                try:
                    (magic, opt, length) = struct.unpack(">QLL", header)
                except struct.error:
                    raise IOError("Negotiation failed: Invalid request, "
                                  "disconnecting")

                if magic != self.NBD_HANDSHAKE:
                    raise IOError("Negotiation failed: bad magic number: %s" %
                                  magic)

                if length:
                    data = await reader.readexactly(length)
                    if (len(data) != length):
                        raise IOError("Negotiation failed: %s bytes expected" %
                                      length)
                else:
                    data = None

                self.log.debug("[%s:%s]: opt=%s, len=%s, data=%s" %
                               (host, port, opt, length, data))

                if opt == self.NBD_OPT_EXPORTNAME:
                    if not data:
                        raise IOError("Negotiation failed: no export name was "
                                      "provided")

                    try:
                        uuid = data.decode("utf-8")
                        # 'o' is the special "overlay" mode that allows
                        # read/write but does not modify the underlying image
                        revision = self.backup.find(uuid).open(mode='o')
                        revision.overlay = True
                        revision.flush_target = 50
                    except Exception as e:
                        if not fixed:
                            raise IOError("Negotiation failed: unknown export "
                                          "name {}".format(e))

                        writer.write(
                            struct.pack(">QLLL", self.NBD_REPLY, opt,
                                        self.NBD_REP_ERR_UNSUP, 0))
                        await writer.drain()
                        continue

                    self.log.info("[%s:%s] Negotiated export: %s" %
                                  (host, port, uuid))

                    export_flags = self.NBD_EXPORT_FLAGS
                    # export_flags ^= self.NBD_RO_FLAG
                    # self.log.info("[%s:%s] %s is read only"
                    #              % (host, port, uuid))
                    revision.seek(0, 2)
                    size = revision.tell()
                    revision.seek(0)
                    writer.write(struct.pack('>QH', size, export_flags))
                    writer.write(b"\x00" * 124)
                    await writer.drain()

                    break

                elif opt == self.NBD_OPT_LIST:
                    for r in self.backup.history:
                        uuid = ' '.join([
                            self.backup.path, r.uuid,
                            r.timestamp.isoformat(), ','.join(r.tags)])
                        writer.write(
                            struct.pack(">QLLL", self.NBD_REPLY, opt,
                                        self.NBD_REP_SERVER,
                                        len(uuid) + 4))
                        revision_encoded = uuid.encode("utf-8")
                        writer.write(struct.pack(">L", len(revision_encoded)))
                        writer.write(revision_encoded)
                        await writer.drain()

                    writer.write(
                        struct.pack(">QLLL", self.NBD_REPLY, opt,
                                    self.NBD_REP_ACK, 0))
                    await writer.drain()

                elif opt == self.NBD_OPT_ABORT:
                    writer.write(
                        struct.pack(">QLLL", self.NBD_REPLY, opt,
                                    self.NBD_REP_ACK, 0))
                    await writer.drain()

                    raise AbortedNegotiationError()
                else:
                    # we don't support any other option
                    if not fixed:
                        raise IOError("Unsupported option")

                    writer.write(
                        struct.pack(">QLLL", self.NBD_REPLY, opt,
                                    self.NBD_REP_ERR_UNSUP, 0))
                    await writer.drain()

            # operation phase
            while True:
                header = await reader.readexactly(28)
                try:
                    (magic, cmd, handle, offset,
                     length) = struct.unpack(">LLQQL", header)
                except struct.error:
                    raise IOError("Invalid request, disconnecting")

                if magic != self.NBD_REQUEST:
                    raise IOError("Bad magic number, disconnecting")

                self.log.debug(
                    "[%s:%s]: cmd=%s, handle=%s, offset=%s, len=%s" %
                    (host, port, cmd, handle, offset, length))

                if cmd == self.NBD_CMD_DISC:
                    self.log.info("[%s:%s] disconnecting" % (host, port))
                    break

                elif cmd == self.NBD_CMD_WRITE:
                    # This creates temporary chunks while writing which get
                    # removed when disconnecting and are never associated
                    # with the original revision.

                    data = await reader.readexactly(length)
                    if (len(data) != length):
                        raise IOError("%s bytes expected, disconnecting" %
                                      length)

                    try:
                        revision.seek(offset)
                        revision.write(data)
                        revision._flush_chunks()
                    except IOError as ex:
                        self.log.error("[%s:%s] %s" % (host, port, ex))
                        await self.nbd_response(writer, handle, error=ex.errno)
                        continue

                    await self.nbd_response(writer, handle)

                elif cmd == self.NBD_CMD_READ:
                    try:
                        revision.seek(offset)
                        data = revision.read(length)
                        revision._flush_chunks()
                    except IOError as ex:
                        self.log.error("[%s:%s] %s" % (host, port, ex))
                        await self.nbd_response(writer, handle, error=ex.errno)
                        continue

                    await self.nbd_response(writer, handle, data=data)

                elif cmd == self.NBD_CMD_FLUSH:
                    # Not relevant in overlay mode as we do not persist anyway.
                    await self.nbd_response(writer, handle)

                else:
                    self.log.warning("[%s:%s] Unknown cmd %s, disconnecting" %
                                     (host, port, cmd))
                    break

        except AbortedNegotiationError:
            self.log.info("[%s:%s] Client aborted negotiation" % (host, port))

        except (asyncio.IncompleteReadError, IOError) as ex:
            self.log.error("[%s:%s] %s" % (host, port, ex))

        finally:
            if revision:
                revision.close()

    def serve_forever(self):
        """Create and run the asyncio loop"""
        addr, port = self.address

        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.handler, addr, port, loop=loop)
        server = loop.run_until_complete(coro)

        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.add_signal_handler(signal.SIGINT, loop.stop)

        loop.run_forever()

        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
