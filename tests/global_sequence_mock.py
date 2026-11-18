#!/usr/bin/env python3
# Copyright (c) 2026. Distributed under the librdkafka BSD license.
"""API 98 wire/decoder regression tests, no Kafka or Python packages required.

Run: python3 tests/global_sequence_mock.py build-gst/src/librdkafka.dylib
Fixtures implement Kafka's schema independently of the C codec. All requests
travel through the public C API, Metadata, ApiVersions and a local TCP broker.
Optional system liblz4/libzstd enable those two compression fixtures.
"""
import ctypes as C
import ctypes.util
import dataclasses
import gzip
import os
import subprocess
import socket
import struct
import sys
import threading
import time
import unittest


def pack(fmt, *values):
    return struct.pack('>' + fmt, *values)


def uv(n):
    b = bytearray()
    while n > 127:
        b.append((n & 127) | 128)
        n >>= 7
    b.append(n)
    return bytes(b)


def var(n):
    return uv((n << 1) ^ (n >> 63))


def compact(b):
    return uv(0) if b is None else uv(len(b) + 1) + b


def crc32c(data):
    crc = 0xffffffff
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


TOPIC = pack('qq', 123, 456)
INDEX = pack('qq', 789, 123)
OTHER = pack('qq', 444, 555)


DEFAULT_VALUE = object()

def record(i, key=b'key', value=DEFAULT_VALUE, headers=((b'h', b'v'),), delta=None):
    if value is DEFAULT_VALUE:
        value = ('value%d' % i).encode()
    def nullable(b):
        return var(-1) if b is None else var(len(b)) + b
    body = b'\0' + var(i) + var(i if delta is None else delta)
    body += nullable(key) + nullable(value) + var(len(headers))
    for name, val in headers:
        body += var(len(name)) + name + nullable(val)
    return var(len(body)) + body


def compress(data, codec):
    if codec == 0:
        return data
    if codec == 1:
        return gzip.compress(data)
    if codec in (2, 5):  # Snappy literal block, optionally Xerial framed.
        n = len(data) - 1
        if n < 60:
            literal = bytes([n << 2])
        else:
            sz = (n.bit_length() + 7) // 8
            literal = bytes([(59 + sz) << 2]) + n.to_bytes(sz, 'little')
        raw = uv(len(data)) + literal + data
        return b'\x82SNAPPY\0' + pack('iii', 1, 1, len(raw)) + raw if codec == 5 else raw
    name = ctypes.util.find_library('lz4' if codec == 3 else 'zstd')
    if not name:
        raise unittest.SkipTest('system compression library unavailable')
    lib = C.CDLL(name)
    out = C.create_string_buffer(len(data) * 2 + 1024)
    if codec == 3:
        fn = lib.LZ4F_compressFrame
        fn.argtypes = [C.c_void_p, C.c_size_t, C.c_void_p, C.c_size_t, C.c_void_p]
        fn.restype = C.c_size_t
        n = fn(out, len(out), data, len(data), None)
    else:
        fn = lib.ZSTD_compress
        fn.argtypes = [C.c_void_p, C.c_size_t, C.c_void_p, C.c_size_t, C.c_int]
        fn.restype = C.c_size_t
        n = fn(out, len(out), data, len(data), 3)
    assert n <= len(out)
    return out.raw[:n]


def batch(base=10, count=3, codec=0, attributes=0, records=None, epoch=2):
    data = b''.join(record(i) for i in range(count)) if records is None else records
    data = compress(data, codec)
    attributes |= 2 if codec == 5 else codec
    body = pack('hiqqqhii', attributes, count - 1, 1000, 2000, -1, -1, -1, count) + data
    return pack('qiibI', base, len(body) + 9, epoch, 2, crc32c(body)) + body


@dataclasses.dataclass
class Mapping:
    global_base: int = 0
    partition: int = 0
    physical_base: int = 10
    count: int = 3
    selected_start: int = 0
    selected_end: int = 3
    data: bytes = b''
    last: int = None
    tags: bytes = b'\0'

    def encode(self):
        data = self.data or batch(self.physical_base, self.count)
        last = self.physical_base + self.count - 1 if self.last is None else self.last
        return pack('qiqqiqq', self.global_base, self.partition, self.physical_base,
                    last, self.count, self.selected_start, self.selected_end) + compact(data) + self.tags


def page(mappings=(), next_offset=0, committed=100, error=0, topic=TOPIC,
         index=INDEX, partition=0, epoch=1, hw=50, pending=False, version=1,
         throttle=0, message=None, tags=b'\0'):
    payload = pack('ih', throttle, error) + compact(message) + topic + index
    payload += pack('iiqqq', partition, epoch, hw, committed, next_offset)
    payload += uv(len(mappings) + 1) + b''.join(m.encode() for m in mappings)
    return payload + (bytes([pending]) if version >= 1 else b'') + tags


class Reader:
    def __init__(self, b):
        self.b, self.p = b, 0
    def read(self, n):
        b = self.b[self.p:self.p+n]
        assert len(b) == n
        self.p += n
        return b
    def int(self, fmt):
        return struct.unpack('>' + fmt, self.read(struct.calcsize('>' + fmt)))[0]
    def uv(self):
        n, s = 0, 0
        while True:
            v = self.int('B')
            n |= (v & 127) << s
            if not v & 128:
                return n
            s += 7
    def compact(self):
        n = self.uv()
        return self.read(n-1) if n else None
    def tags(self):
        for _ in range(self.uv()):
            self.uv()
            self.read(self.uv())


class Broker:
    def __init__(self, response=None, max_version=1, min_version=0, topic=TOPIC,
                 metadata_error=0, metadata_version=12, delay=0, metadata_delay=0):
        self.response = response or (lambda req: page(next_offset=req['start'], committed=0,
                                                      version=req['version']))
        self.max_version, self.min_version = max_version, min_version
        self.topic, self.metadata_error = topic, metadata_error
        self.metadata_version = metadata_version
        self.delay, self.metadata_delay = delay, metadata_delay
        self.fetches, self.requests, self.failures = [], [], []
        self.stop = threading.Event()
        self.server = socket.socket()
        self.server.bind(('127.0.0.1', 0))
        self.server.listen()
        self.port = self.server.getsockname()[1]
        self.server.settimeout(.1)
        self.connections, self.workers = [], []
        self.thread = threading.Thread(target=self.accept, daemon=True)
    def __enter__(self):
        self.thread.start()
        return self
    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(2)
        self.server.close()
        for connection in self.connections:
            connection.close()
        for worker in self.workers:
            worker.join(2)
        if not args[0] and self.failures:
            raise AssertionError(self.failures)
    def accept(self):
        while not self.stop.is_set():
            try:
                c, _ = self.server.accept()
                self.connections.append(c)
                worker = threading.Thread(target=self.serve, args=(c,), daemon=True)
                self.workers.append(worker)
                worker.start()
            except socket.timeout:
                pass
    @staticmethod
    def receive(c, n):
        data = bytearray()
        while len(data) < n:
            b = c.recv(n-len(data))
            if not b:
                raise EOFError()
            data.extend(b)
        return bytes(data)
    def serve(self, c):
        try:
            while not self.stop.is_set():
                length = struct.unpack('>i', self.receive(c, 4))[0]
                r = Reader(self.receive(c, length))
                api, version, corr = r.int('h'), r.int('h'), r.int('i')
                # ClientId uses int16 even in a flexible request header.
                client_len = r.int('h')
                r.read(max(0, client_len))
                flexible = (api == 18 and version >= 3) or (api == 3 and version >= 9) or api == 98
                if flexible:
                    r.tags()
                self.requests.append((api, version))
                if api == 18:
                    apis = [(0, 0, 9), (1, 0, 16), (3, 0, self.metadata_version), (18, 0, 3)]
                    if self.max_version >= 0:
                        apis.append((98, self.min_version, self.max_version))
                    payload = pack('h', 0) + (uv(len(apis) + 1) if version >= 3 else pack('i', len(apis)))
                    payload += b''.join(pack('hhh', *a) + (b'\0' if version >= 3 else b'') for a in apis)
                    if version >= 1:
                        payload += pack('i', 0)
                    if version >= 3:
                        payload += b'\0'
                    header = b''  # ApiVersions response header is always v0.
                elif api == 3:
                    self.stop.wait(self.metadata_delay)
                    n = r.uv() - 1
                    names = []
                    for _ in range(max(0, n)):
                        if version >= 10:
                            r.read(16)
                        names.append(r.compact())
                        r.tags()
                    auto_create = r.int('b') if version >= 4 else 0
                    assert not auto_create, 'metadata must disable topic auto-creation'
                    # Metadata v9..12: broker, cluster, controller, topic.
                    payload = pack('i', 0) + uv(2) + pack('i', 1) + compact(b'127.0.0.1')
                    payload += pack('i', self.port) + compact(None) + b'\0' + compact(b'cluster') + pack('i', 1)
                    payload += uv(len(names) + 1)
                    for name in names:
                        payload += pack('h', self.metadata_error) + compact(name)
                        if version >= 10:
                            payload += self.topic
                        payload += b'\0' + uv(1) + pack('i', 0) + b'\0'
                    if version <= 10:
                        payload += pack('i', 0)
                    payload += b'\0'
                    header = b'\0'
                elif api == 98:
                    req = {'topic': r.read(16), 'start': r.int('q'), 'end': r.int('q'),
                           'max_batches': r.int('i'), 'timeout': r.int('i'),
                           'max_bytes': r.int('i'), 'isolation': r.int('b'), 'version': version}
                    r.tags()
                    assert r.p == len(r.b), 'extra or missing request bytes'
                    assert req['topic'] == self.topic
                    assert 1 <= req['timeout'] <= 30000
                    assert req['isolation'] == 0 or version >= 1
                    self.fetches.append(req)
                    self.stop.wait(self.delay)
                    payload = self.response(req)
                    if payload is None:  # Simulate a transport failure.
                        return
                    header = b'\0'
                else:
                    raise AssertionError('unexpected API %s' % api)
                b = pack('i', corr) + header + payload
                c.sendall(pack('i', len(b)) + b)
        except (EOFError, ConnectionError, OSError):
            pass
        except Exception as exc:
            self.failures.append(repr(exc))
        finally:
            c.close()


class Options(C.Structure):
    _fields_ = [('max_batches', C.c_int32), ('max_bytes', C.c_int32), ('isolation_level', C.c_int), ('check_crcs', C.c_int)]


class Info(C.Structure):
    _fields_ = [('topic_id', C.c_void_p), ('index_topic_id', C.c_void_p),
                ('index_partition', C.c_int32), ('coordinator_leader_epoch', C.c_int32),
                ('index_high_watermark', C.c_int64), ('committed_global_end_offset', C.c_int64),
                ('next_global_offset', C.c_int64), ('transaction_pending', C.c_int),
                ('has_snapshot', C.c_int), ('api_version', C.c_int16),
                ('throttle_time_ms', C.c_int32), ('error', C.c_int), ('error_string', C.c_char_p)]


class Record(C.Structure):
    _fields_ = [('global_offset', C.c_int64), ('physical_partition', C.c_int32),
                ('physical_offset', C.c_int64), ('leader_epoch', C.c_int32),
                ('timestamp', C.c_int64), ('timestamp_type', C.c_int),
                ('key', C.c_void_p), ('key_len', C.c_size_t), ('payload', C.c_void_p),
                ('len', C.c_size_t), ('headers', C.c_void_p)]


def setup_library(path):
    lib = C.CDLL(path)
    def fn(name, result, args):
        f = getattr(lib, 'rd_kafka_' + name)
        f.restype, f.argtypes = result, args
        return f
    ptr, integer = C.c_void_p, C.c_int
    fn('conf_new', ptr, [])
    fn('conf_destroy', None, [ptr])
    fn('conf_get', integer, [ptr, C.c_char_p, C.c_char_p, C.POINTER(C.c_size_t)])
    fn('conf_set', integer, [ptr, C.c_char_p, C.c_char_p, C.c_char_p, C.c_size_t])
    fn('new', ptr, [integer, ptr, C.c_char_p, C.c_size_t])
    fn('destroy', None, [ptr])
    fn('global_sequence_fetch_options_init', None, [C.POINTER(Options)])
    fn('global_sequence_fetch', ptr, [ptr, C.c_char_p, ptr, C.c_int64, C.c_int64,
                                    C.POINTER(Options), C.POINTER(ptr), integer])
    fn('global_sequence_page_info', C.POINTER(Info), [ptr])
    fn('global_sequence_page_records', C.POINTER(Record), [ptr, C.POINTER(C.c_size_t)])
    fn('global_sequence_page_destroy', None, [ptr])
    fn('error_code', integer, [ptr])
    fn('error_string', C.c_char_p, [ptr])
    fn('error_destroy', None, [ptr])
    fn('Uuid_new', ptr, [C.c_int64, C.c_int64])
    fn('Uuid_copy', ptr, [ptr])
    fn('Uuid_destroy', None, [ptr])
    fn('Uuid_most_significant_bits', C.c_int64, [ptr])
    fn('header_get_all', integer, [ptr, C.c_size_t, C.POINTER(C.c_char_p), C.POINTER(ptr), C.POINTER(C.c_size_t)])
    return lib


class Client:
    def __init__(self, broker, **config):
        self.lib = LIB
        conf = LIB.rd_kafka_conf_new()
        settings = {'bootstrap.servers': '127.0.0.1:%d' % broker.port,
                    'allow.auto.create.topics': 'false', 'retry.backoff.ms': '5',
                    'socket.timeout.ms': '1000', 'log_level': '0', **config}
        for name, value in settings.items():
            err = C.create_string_buffer(512)
            assert LIB.rd_kafka_conf_set(conf, name.encode(), value.encode(), err, len(err)) == 0, err.value
        err = C.create_string_buffer(512)
        self.rk = LIB.rd_kafka_new(0, conf, err, len(err))
        assert self.rk, err.value
    def __enter__(self):
        return self
    def __exit__(self, *args):
        LIB.rd_kafka_destroy(self.rk)
    def fetch(self, start=0, end=3, expected=None, timeout=2000, **options):
        opts = Options()
        LIB.rd_kafka_global_sequence_fetch_options_init(C.byref(opts))
        for name, value in options.items():
            setattr(opts, name, value)
        page_ptr = C.c_void_p()
        err = LIB.rd_kafka_global_sequence_fetch(self.rk, b'test', expected, start, end,
                                               C.byref(opts), C.byref(page_ptr), timeout)
        if err:
            result = (LIB.rd_kafka_error_code(err), LIB.rd_kafka_error_string(err).decode())
            LIB.rd_kafka_error_destroy(err)
            assert not page_ptr
            return result, None, []
        assert page_ptr
        info = LIB.rd_kafka_global_sequence_page_info(page_ptr).contents
        n = C.c_size_t()
        records = LIB.rd_kafka_global_sequence_page_records(page_ptr, C.byref(n))
        return page_ptr, info, [records[i] for i in range(n.value)]


class Tests(unittest.TestCase):
    def good(self, wire, start=0, end=3, **opts):
        with Broker(lambda r: wire) as broker, Client(broker) as client:
            p, info, records = client.fetch(start, end, **opts)
            self.assertIsNotNone(info, (p, broker.requests, broker.fetches, broker.failures))
            result = (info.next_global_offset, info.error, info.transaction_pending,
                      [(r.global_offset, r.physical_partition, r.physical_offset,
                        C.string_at(r.payload, r.len), r.timestamp, r.leader_epoch) for r in records])
            LIB.rd_kafka_global_sequence_page_destroy(p)
            return result
    def bad(self, wire, start=0, end=3, code=-199, **opts):
        with Broker(lambda r: wire) as broker, Client(broker) as client:
            err, info, records = client.fetch(start, end, **opts)
            self.assertIsNone(info, 'invalid response accepted')
            self.assertEqual(err[0], code, err)
            return err
    def test_codecs_and_mid_batch_selection(self):
        conf = LIB.rd_kafka_conf_new()
        features, size = C.create_string_buffer(1024), C.c_size_t(1024)
        self.assertEqual(LIB.rd_kafka_conf_get(conf, b'builtin.features', features, C.byref(size)), 0)
        LIB.rd_kafka_conf_destroy(conf)
        supported = features.value.decode().split(',')
        for codec in range(6):
            with self.subTest(codec=codec):
                wire = page([Mapping(selected_start=1, selected_end=2, data=batch(codec=codec))], next_offset=2)
                name = {1:'gzip', 2:'snappy', 3:'lz4', 4:'zstd', 5:'snappy'}.get(codec)
                if name and name not in supported:
                    self.bad(wire, 1, 2, max_bytes=1, code=-170)
                    continue
                result = self.good(wire, 1, 2, max_bytes=1)
                self.assertEqual(result, (2, 0, 0, [(1, 0, 11, b'value1', 1001, 2)]))
    def test_multiple_partitions_and_partial_error(self):
        mappings = [Mapping(), Mapping(3, 2, 40, 2, 3, 5)]
        result = self.good(page(mappings, next_offset=5, error=6, message=b'leader moved'), end=8)
        self.assertEqual(result[:3], (5, 6, 0))
        self.assertEqual([(r[0], r[1], r[2]) for r in result[3]],
                         [(0,0,10), (1,0,11), (2,0,12), (3,2,40), (4,2,41)])
    def test_pagination_uuid_and_lifetime(self):
        def response(req):
            start = req['start']
            return page([Mapping(start, 2, 40+start, 3, start, min(start+3, req['end']))],
                        next_offset=min(start+3, req['end']))
        with Broker(response) as broker:
            with Client(broker) as client:
                p, info, _ = client.fetch(0, 5)
                expected = LIB.rd_kafka_Uuid_copy(info.topic_id)
                LIB.rd_kafka_global_sequence_page_destroy(p)
                p, info, records = client.fetch(3, 5, expected=expected)
                LIB.rd_kafka_Uuid_destroy(expected)
            # The result must outlive the client and response/network buffers.
            self.assertEqual(C.string_at(records[0].payload, records[0].len), b'value0')
            self.assertEqual([r.global_offset for r in records], [3,4])
            self.assertEqual(LIB.rd_kafka_Uuid_most_significant_bits(info.topic_id), 123)
            LIB.rd_kafka_global_sequence_page_destroy(p)
    def test_version_zero(self):
        with Broker(lambda req: page([Mapping()], next_offset=3, version=0), max_version=0) as broker, Client(broker) as client:
            p, info, records = client.fetch()
            self.assertIsNotNone(info, (p, broker.requests, broker.fetches, broker.failures))
            self.assertEqual(info.api_version, 0)
            self.assertEqual(len(records), 3)
            self.assertEqual(broker.fetches[0]['isolation'], 0)
            LIB.rd_kafka_global_sequence_page_destroy(p)
            err, info, _ = client.fetch(isolation_level=1)
            self.assertIsNone(info)
            self.assertEqual(err[0], -165)
            self.assertEqual(len(broker.fetches), 1)
    def test_unavailable_api_and_metadata_uuid(self):
        for kwargs in ({'max_version': -1}, {'min_version': 2, 'max_version': 2},
                       {'topic': bytes(16)}, {'metadata_version': 9}):
            with self.subTest(kwargs=kwargs), Broker(**kwargs) as broker, Client(broker) as client:
                err, info, _ = client.fetch()
                self.assertIsNone(info)
                self.assertEqual(err[0], -165, err)
                self.assertFalse(broker.fetches)
    def test_topic_recreation(self):
        with Broker() as broker, Client(broker) as client:
            expected = LIB.rd_kafka_Uuid_new(9, 9)
            err, info, _ = client.fetch(expected=expected)
            LIB.rd_kafka_Uuid_destroy(expected)
            self.assertIsNone(info)
            self.assertEqual(err[0], -149)
            self.assertFalse(broker.fetches)
        self.bad(page(topic=OTHER, committed=0))
    def test_read_committed_skips_and_pending(self):
        self.assertEqual(self.good(page(next_offset=3), isolation_level=1)[:3], (3,0,0))
        mappings = [Mapping(1, 0, 10, 1, 1, 2), Mapping(4, 2, 30, 1, 4, 5)]
        result = self.good(page(mappings, next_offset=6, pending=True), end=9, isolation_level=1)
        self.assertEqual(result[:3], (6,0,1))
        self.assertEqual([r[0] for r in result[3]], [1,4])
        self.assertEqual(self.good(page(next_offset=0, pending=True), isolation_level=1), (0,0,1,[]))
        # Progress can contain no records AND an error: it must not retry.
        self.assertEqual(self.good(page(next_offset=2, error=6), isolation_level=1), (2,6,0,[]))
    def test_early_errors_and_empty_ranges(self):
        with Broker(lambda r: page(index=bytes(16), next_offset=-1, error=29,
                                  partition=-1, epoch=-1, hw=-1, committed=-1)) as broker, Client(broker) as client:
            p, info, records = client.fetch()
            self.assertIsNotNone(info, (p, broker.requests, broker.fetches, broker.failures))
            self.assertEqual((info.has_snapshot, info.next_global_offset, info.error), (0,0,29))
            LIB.rd_kafka_global_sequence_page_destroy(p)
        self.assertEqual(self.good(page(committed=0), end=0), (0,0,0,[]))
        self.assertEqual(self.good(page(next_offset=10, committed=3), 10, 11), (10,0,0,[]))
    def test_invalid_pages(self):
        good_batch = batch()
        corrupt = good_batch[:21] + bytes([good_batch[21] ^ 1]) + good_batch[22:]
        cases = [page(index=bytes(16)), page(index=bytes(16), error=29, next_offset=1),
                 page(index=bytes(16), error=29, pending=True), page(topic=OTHER),
                 page(partition=-1), page(epoch=-1), page(hw=-1), page(committed=-1),
                 page(next_offset=-1), page(next_offset=4), page(next_offset=3, committed=2),
                 page(next_offset=1), page(pending=True), page(error=6, pending=True),
                 page(next_offset=3, committed=3, pending=True), page(),
                 page([Mapping(selected_start=1)], next_offset=3),
                 page([Mapping(last=13)], next_offset=3),
                 page([Mapping(partition=-1)], next_offset=3),
                 page([Mapping(global_base=2**63-2, count=3)], next_offset=3),
                 page([Mapping(data=corrupt)], next_offset=3),
                 page([Mapping(data=good_batch[:-1])], next_offset=3),
                 page([Mapping(data=good_batch+good_batch)], next_offset=3),
                 page([Mapping(data=batch(base=20))], next_offset=3),
                 page([Mapping(data=batch(count=2))], next_offset=3),
                 page([Mapping(data=batch(attributes=32))], next_offset=3),
                 page([Mapping(data=batch(epoch=-2))], next_offset=3),
                 page([Mapping(data=batch(records=record(0)+record(1, delta=2)+record(2)))], next_offset=3),
                 page([Mapping(data=batch(records=record(0)+record(1)))], next_offset=3),
                 page([Mapping(data=batch(records=record(0)+record(1)+record(2)+b'x'))], next_offset=3),
                 page([Mapping(), Mapping(1, 1, 20, 2, 1, 3)], next_offset=3),
                 page([Mapping(1, 1, 20, 2, 1, 3)], next_offset=3),
                 page([Mapping()], next_offset=2), page([Mapping()], next_offset=3, committed=2),
                 page([Mapping(tags=b'\x02\x01\x00\x01\x00')], next_offset=3),
                 page([Mapping()], next_offset=3, tags=b'\x01\x01\xff'),
                 page([Mapping()], next_offset=3)+b'x']
        for i, wire in enumerate(cases):
            with self.subTest(case=i):
                self.bad(wire)
    def test_limits_and_crc_option(self):
        mappings = [Mapping(), Mapping(3, 1, 40, 2, 3, 5)]
        self.bad(page(mappings, next_offset=5), end=5, max_bytes=1)
        self.bad(page(mappings, next_offset=5), end=5, max_batches=1)
        data = bytearray(batch())
        data[17] ^= 1  # Only CRC field: disabled CRC must still decode structurally.
        wire = page([Mapping(data=bytes(data))], next_offset=3)
        with Broker(lambda r: wire) as broker, Client(broker, **{'check.crcs': 'false'}) as client:
            p, info, records = client.fetch(check_crcs=0)
            self.assertIsNotNone(info, (p, broker.requests, broker.fetches, broker.failures))
            self.assertEqual(len(records), 3)
            LIB.rd_kafka_global_sequence_page_destroy(p)
    def test_unknown_tags(self):
        tags = b'\x01\x05\x03abc'
        self.assertEqual(self.good(page([Mapping(tags=tags)], next_offset=3, tags=tags))[0], 3)
    def test_retry_before_progress_only(self):
        calls = []
        def response(req):
            calls.append(time.monotonic())
            if len(calls) <= 3:
                return page(index=bytes(16), error=(3, 100, 15)[len(calls)-1], throttle=20)
            return page([Mapping()], next_offset=3, error=6)
        with Broker(response) as broker, Client(broker) as client:
            p, info, records = client.fetch()
            self.assertIsNotNone(info, (p, broker.requests, broker.fetches, broker.failures))
            self.assertEqual(info.error, 6)
            self.assertEqual(len(records), 3)
            self.assertEqual(len(calls), 4)
            self.assertGreater(calls[1] - calls[0], .015)
            LIB.rd_kafka_global_sequence_page_destroy(p)
    def test_deadlines_and_late_response(self):
        for kwargs in ({'delay': .15}, {'metadata_delay': .15}):
            with self.subTest(kwargs=kwargs), Broker(**kwargs) as broker, Client(broker) as client:
                before = time.monotonic()
                err, info, _ = client.fetch(timeout=80)
                elapsed = time.monotonic() - before
                self.assertIsNone(info)
                self.assertEqual(err[0], -185, err)
                self.assertLess(elapsed, .25)
                time.sleep(.2)  # Late callback after the stack opaque is gone.
    def test_null_empty_headers_and_log_append_time(self):
        data = record(0, key=None, value=None, headers=((b'x', None), (b'y', b'')))
        data += record(1, key=b'', value=b'', headers=()) + record(2)
        wire = page([Mapping(data=batch(attributes=8, records=data))], next_offset=3)
        with Broker(lambda req: wire) as broker, Client(broker) as client:
            p, info, records = client.fetch()
            self.assertIsNotNone(info, p)
            self.assertFalse(records[0].key)
            self.assertFalse(records[0].payload)
            self.assertTrue(records[1].key)
            self.assertTrue(records[1].payload)
            self.assertEqual((records[1].key_len, records[1].len), (0, 0))
            self.assertEqual([(r.timestamp, r.timestamp_type) for r in records], [(2000,2)]*3)
            name, value, size = C.c_char_p(), C.c_void_p(), C.c_size_t()
            self.assertEqual(LIB.rd_kafka_header_get_all(records[0].headers, 0, C.byref(name), C.byref(value), C.byref(size)), 0)
            self.assertEqual(name.value, b'x')
            self.assertFalse(value)
            self.assertEqual(LIB.rd_kafka_header_get_all(records[0].headers, 1, C.byref(name), C.byref(value), C.byref(size)), 0)
            self.assertEqual(name.value, b'y')
            self.assertTrue(value)
            self.assertEqual(size.value, 0)
            LIB.rd_kafka_global_sequence_page_destroy(p)
    def test_truncated_and_oversized_responses(self):
        full = page([Mapping()], next_offset=3)
        current = [b'']
        with Broker(lambda req: current[0]) as broker, Client(broker) as client:
            for cut in range(len(full)):
                current[0] = full[:cut]
                err, info, _ = client.fetch()
                self.assertIsNone(info, 'accepted truncation at %d' % cut)
                self.assertEqual(err[0], -199, (cut, err))
        self.bad(page([Mapping(data=b'x' * (8388608+1))], next_offset=3))
    def test_transport_retry_and_metadata_errors(self):
        calls = []
        def response(req):
            calls.append(req)
            return None if len(calls) == 1 else page([Mapping()], next_offset=3)
        with Broker(response) as broker, Client(broker) as client:
            p, info, records = client.fetch()
            self.assertIsNotNone(info, p)
            self.assertEqual(len(calls), 2)
            LIB.rd_kafka_global_sequence_page_destroy(p)
        for code in (3, 29):
            with Broker(metadata_error=code) as broker, Client(broker) as client:
                err, info, _ = client.fetch()
                self.assertIsNone(info)
                self.assertEqual(err[0], code)
                self.assertFalse(broker.fetches)
    def test_throttle_across_pages_and_retry_deadline(self):
        calls = []
        def response(req):
            calls.append(time.monotonic())
            return page(next_offset=req['start'], committed=0, throttle=80)
        with Broker(response) as broker, Client(broker) as client:
            for _ in range(2):
                p, info, _ = client.fetch()
                self.assertIsNotNone(info, p)
                LIB.rd_kafka_global_sequence_page_destroy(p)
            self.assertGreaterEqual(calls[1] - calls[0], .075)
        with Broker(lambda req: page(error=15, throttle=1000)) as broker, Client(broker) as client:
            start = time.monotonic()
            err, info, _ = client.fetch(timeout=100)
            self.assertIsNone(info)
            self.assertEqual(err[0], -185)
            self.assertLess(time.monotonic() - start, .25)
            self.assertEqual(len(broker.fetches), 1)
    def test_compiled_examples(self):
        ran = False
        for key in ('GST_C_EXAMPLE', 'GST_CPP_EXAMPLE'):
            example = os.environ.get(key)
            if not example:
                continue
            ran = True
            def response(req):
                start = req['start']
                return page([Mapping(start, 0, 10+start, 3, start, min(start+3, req['end']))],
                            next_offset=min(start+3, req['end']))
            with Broker(response) as broker:
                result = subprocess.run([example, '127.0.0.1:%d' % broker.port, 'test', '0', '5'],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(result.stdout.splitlines()), 3 if key == 'GST_C_EXAMPLE' else 5)
        if not ran:
            self.skipTest('set GST_C_EXAMPLE/GST_CPP_EXAMPLE to test compiled examples')
    def test_invalid_arguments(self):
        for args in ({'start': -1}, {'start': 2, 'end': 1}, {'timeout': 0},
                     {'max_batches': 0}, {'max_batches': 1001}, {'max_bytes': 0},
                     {'max_bytes': 16646145}, {'isolation_level': 2}, {'check_crcs': 2}):
            with self.subTest(args=args), Broker() as broker, Client(broker) as client:
                err, info, _ = client.fetch(**args)
                self.assertIsNone(info)
                self.assertEqual(err[0], -186)
                self.assertFalse(broker.fetches)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    LIB = setup_library(sys.argv.pop(1))
    unittest.main(verbosity=2)
