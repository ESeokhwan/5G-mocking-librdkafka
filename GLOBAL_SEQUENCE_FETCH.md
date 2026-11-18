# Global Sequence Fetch in this fork

This extension targets `kafka-gst` commit
`fcb463f99ce7078825b410fe30cc366e70672903`. It sends **API 98**, flexible
versions **0–1**, to an available Kafka broker. The broker routes the index and
physical reads; the client does not discover an index coordinator.
Standard Kafka brokers do not implement this API.

## Build and run

```sh
cmake -S . -B build-gst -DCMAKE_BUILD_TYPE=Debug \
  -DRDKAFKA_BUILD_EXAMPLES=ON -DRDKAFKA_BUILD_TESTS=ON
cmake --build build-gst -j6

# C: one page of [0, 1000)
build-gst/examples/global_sequence_fetch localhost:9092 my-gst 0 1000

# C++: all pages of [0, 1000), read_uncommitted
build-gst/examples/global_sequence_consumer localhost:9092 my-gst 0 1000

# C++: read_committed, at most 10 batches, soft 1 MiB per page
build-gst/examples/global_sequence_consumer localhost:9092 my-gst 0 1000 1 10 1048576
```

Set `CMAKE_PREFIX_PATH` to the dependency installation if needed (for example
`-DCMAKE_PREFIX_PATH=/opt/homebrew` on Apple Silicon). Gzip, Snappy, LZ4 and Zstd
support follows the library build. The C++ example requires C++11 or later.
The configure/Makefile build also includes both examples.

The topic must already exist with GST enabled, and the requested data must have
been indexed. These examples do not create topics or produce data. The C++
example prints tab-separated global offset, physical partition, physical
offset, timestamp, key hex and value hex; `null` distinguishes null from empty
bytes. Page state is printed to stderr. It waits and polls the same cursor when
the requested end is not yet indexed or a transaction is pending. It exits with
code 2 after delivering a partial page that has a broker error. These examples
are correctness/usage tools, not throughput benchmarks.

## Public C interface and C++ integration

Declarations are in `src/rdkafka.h`. The parent `rdkafka_testbed` already uses
`add_subdirectory(librdkafka)` and links `rdkafka++` / `rdkafka`, so rebuilding
those local targets includes this extension. Its virtual include directory
already exposes the modified headers:

```cpp
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

rd_kafka_global_sequence_fetch_options_t opts;
rd_kafka_global_sequence_fetch_options_init(&opts);
opts.max_batches = 100;
opts.max_bytes = 1024 * 1024;
opts.isolation_level = 0;  // 1 requires API v1
opts.check_crcs = 1;       // default; independent of partition check.crcs

rd_kafka_global_sequence_page_t *page = nullptr;
rd_kafka_error_t *error = rd_kafka_global_sequence_fetch(
    handle->c_ptr(), topic, expected_uuid, start, end_exclusive,
    &opts, &page, 10000);
```

`handle` can be a `RdKafka::Producer` or `RdKafka::KafkaConsumer`. In C, pass an
`rd_kafka_t *` directly. This uses the existing network transport, configured
security, ApiVersions, Metadata, broker connections and custom networking hooks.
No new socket stack is introduced. Link this fork's rebuilt library, not an
installed upstream library that lacks these symbols. There is no additional
virtual method in the C++ wrapper.

A validated response returns `error == NULL` and an owned `page`. **A broker
error is in the page**, including an early error with no snapshot:

```c
const rd_kafka_global_sequence_page_info_t *info =
    rd_kafka_global_sequence_page_info(page);
size_t count;
const rd_kafka_global_sequence_record_t *records =
    rd_kafka_global_sequence_page_records(page, &count);
/* Deliver records[0..count), then persist info->next_global_offset. */
/* Inspect info->error and info->transaction_pending before the next call. */
```

A local failure returns an owned `rd_kafka_error_t *` and `page == NULL`.
Destroy the error using `rd_kafka_error_destroy()`. Local failures include
invalid arguments, unsupported versions, metadata errors, a recreated topic,
malformed wire data, CRC/decompression errors and deadline expiry. They expose
no records and no advanced cursor. Unknown broker error codes are preserved in
the page as numeric `rd_kafka_resp_err_t` values.

`rd_kafka_global_sequence_page_destroy(page)` releases all page storage. Until
then, records, key/value bytes, decoded headers, UUIDs and error strings are
valid, even after the transport handle is destroyed. Pointers are borrowed;
do not destroy individual records, headers or page UUIDs. Use
`rd_kafka_header_get_all()` to inspect record headers. A null key/value pointer
means Kafka null; a nonnull pointer with length zero means empty bytes.
`leader_epoch`, timestamps and physical offsets retain their source values.

These structs and symbols are experimental extensions of this research fork;
recompile callers when updating this interface.

## UUID pinning and page state

Pass `expected_uuid == NULL` only for the initial read of a topic incarnation.
Copy `info->topic_id` with `rd_kafka_Uuid_copy()` before destroying the first
page. Pass that copy for **every subsequent page and retry**, then destroy the
copy with `rd_kafka_Uuid_destroy()` when finished. If saving a cursor across
process restarts, save the UUID's two 64-bit parts too and reconstruct it with
`rd_kafka_Uuid_new()`.

Every call resolves fresh named metadata with topic auto-creation disabled.
A nonzero expected UUID is compared to metadata; a mismatch returns
`RD_KAFKA_RESP_ERR__INCONSISTENT` before sending API 98. Once resolved, the UUID
stays fixed across internal retries. The response TopicId must also match;
a mismatching response is `RD_KAFKA_RESP_ERR__BAD_MSG`. Metadata without a topic
UUID returns `RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE`.

`next_global_offset` is the next **unprocessed** global position, not a physical
Kafka offset. A partial page may contain valid records followed by an error at
this position. Deliver the prefix once, then retry from this cursor with the
same UUID. Do not throw away the prefix and restart the old page automatically.

The page also exposes index topic UUID, index partition, coordinator leader
epoch, exclusive index HW, committed global end, API version, throttle duration,
and `transaction_pending`. With `has_snapshot == 0`, an early broker error has
no valid snapshot coordinates; its `-1` wire cursor is normalized to the
requested start. No-snapshot pages must be empty and must have an error.

## Isolation, limits and validation

| Item | Contract |
| --- | --- |
| API v0 | read_uncommitted; IsolationLevel byte is still present |
| API v1 | read_uncommitted or read_committed; adds TransactionPending |
| Range | `0 <= start <= end_exclusive`, explicit and stateless |
| MaxBatches | 1–1000, default 100; counts batches, not records |
| MaxBytes | 1–16,646,144, default 1,048,576; soft limit |
| Encoded batch | At most 8,388,608 bytes, exactly one full magic v2 batch |
| Encoded page Records sum | At most 16,646,144 bytes |
| CRC | CRC32C checked by default; `opts.check_crcs=0` disables only CRC checks |
| Call timeout | Positive milliseconds, one deadline for the whole operation |
| Wire TimeoutMs | Remaining deadline, capped at 30,000 ms |

One complete first batch may exceed requested MaxBytes. A response containing
multiple batches may not. Encoded limits apply before decompression and do not
bound decompressed/application memory.

All mappings must have valid nonnegative coordinates, a consistent physical
range and count, no global overlap or overflow, and an end below the committed
snapshot boundary. Selected ranges must be the exact intersections with the
request. Complete batches retain their original physical base/last offsets;
the global offset is `mapping.global_base + record_ordinal`. The decoder checks
magic, length, epoch, count, control-batch exclusion, CRC, record boundaries,
all physical record offsets, and decoded headers, including unselected records.
A malformed page is discarded as a whole; it does not advance a caller's cursor.

For read_uncommitted, selected ranges form a contiguous prefix starting at the
requested start, and next equals the last selected end. Empty pages do not
advance. For read_committed, aborted batches may be omitted, so gaps and an
empty page with an advanced cursor are valid. A pending transaction may follow
a valid prefix; resume at next. Pending is never valid with an error or at/after
`min(requested_end, committed_end)`. An empty page without progress beyond the
current committed end is not EOF for live data. This API is not a long poll.

Retriable metadata/transport/broker errors before progress may be retried within
the deadline, with retry backoff and server throttle. A validated page whose
cursor advanced is returned immediately even if it has an error and no records.
Terminal broker errors are returned as pages without retry. Valid server
throttle is reported through the existing throttle callback and observed on
subsequent API 98 requests to that broker. Connection negotiation and socket
timeouts use the existing librdkafka transport. Late replies after a timeout
cannot access the expired caller state. Decompression is synchronous; deadline
checks occur before/between records and after decoding, and cannot preempt an
individual codec call or a blocking user-supplied transport hook.

The API does not subscribe, assign, join a group, store or commit offsets, or
call application callbacks from its private result queue. The application must
keep the handle alive during the call and continue ordinary polling as needed
for other work on a shared handle.

## Tests and integration status

```sh
ctest --test-dir build-gst -R '^GlobalSequenceFetch$' --output-on-failure
TESTS=0000 build-gst/tests/test-runner -p1
# Or run the fixture suite directly (Linux: use librdkafka.so):
python3 tests/global_sequence_mock.py build-gst/src/librdkafka.dylib
# Makefile builds:
make -C tests global-sequence GST_LIBRARY=../src/librdkafka.so
```

The Python test has no third-party Python dependencies. It runs an ephemeral
loopback TCP broker implementing the actual wire schemas independently of the
C encoder/decoder. Optional system liblz4/libzstd generate those fixtures;
codecs disabled in the library build are checked for an unsupported-codec error. CTest also runs
the compiled C and C++ examples when examples are enabled. Direct runs can set
`GST_C_EXAMPLE` and `GST_CPP_EXAMPLE` to their executable paths.

Coverage includes v0/v1 and rejection of missing/incompatible API versions,
metadata UUID absence/mismatch, topic recreation, multiple partitions/pages,
selection within a batch, uncompressed/gzip/raw and Xerial Snappy/LZ4/Zstd,
CRC on/off, null/empty bytes and headers, append/create timestamps, partial
errors, no-snapshot errors, committed gaps/empty advances/pending, malformed
mappings/offsets/counts/tags, every byte truncation of a valid response, size
limits, transport retry, throttle, timeouts, late replies and result ownership.

On 2026-11-13, on macOS arm64:

- CMake Debug shared and static libraries and both examples built successfully.
- CTest `GlobalSequenceFetch`: all 19 test methods passed, including compiled
  C/C++ examples, all 245 byte truncations of a valid response, and dozens of
  malformed-page variants.
- Existing `TESTS=0000` built-in unit suite passed.
- The same wire/decoder suite passed with AddressSanitizer and
  UndefinedBehaviorSanitizer (18 methods passed; the compiled-example method
  was skipped in that sanitizer-only library build). No sanitizer error was
  reported. The statically linked C/C++ examples also passed the mock test.

**No live kafka-gst broker or Java/C result comparison has been run**:
the target checkout had no built broker/client jars and no identified running
GST cluster. The wire fixture is not a server integration or performance test.

For live comparison, build/start the exact target Kafka revision, create and
populate a GST with deterministic keys/values, wait for indexing, then read the
same explicit range and UUID with the Java GlobalSequenceConsumer and this C++
example. Compare global position, source partition/offset and payload/key bytes
across pages; repeat with compression, page limits and read_committed. No server
or parent testbed application files are changed by this client implementation.

## Changed files

- Public API and implementation: `src/rdkafka.h`, `src/rdkafka_global_sequence.c`.
- Broker throttle and API registry: `src/rdkafka_broker.h`,
  `src/rdkafka_protocol.h`, `src/rdkafka_proto.h`.
- Library build registration: `src/CMakeLists.txt`, `src/Makefile`,
  `win32/librdkafka.vcxproj` (Windows project updated, not executed here).
- Examples: `examples/global_sequence_fetch.c`,
  `examples/global_sequence_consumer.cpp`, `examples/CMakeLists.txt`,
  `examples/Makefile`, `examples/README.md`.
- Tests: `tests/global_sequence_mock.py`, `tests/CMakeLists.txt`, `tests/Makefile`.
- Documentation/build output exclusions: `README.md`, `GLOBAL_SEQUENCE_FETCH.md`,
  `.gitignore`.
