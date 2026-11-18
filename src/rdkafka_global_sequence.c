/*
 * Global Sequence Fetch extension for the kafka-gst fork.
 * Copyright (c) 2026. Distributed under the librdkafka BSD license.
 * Wire contract: kafka-gst fcb463f99ce7078825b410fe30cc366e70672903.
 */
#include "rdkafka_int.h"
#include "rdkafka_broker.h"
#include "rdkafka_header.h"
#include "rdkafka_lz4.h"
#include "crc32c.h"
#if WITH_ZLIB
#include "rdgz.h"
#endif
#if WITH_SNAPPY
#include "snappy.h"
#endif
#if WITH_ZSTD
#include "rdkafka_zstd.h"
#endif

#define GS_MAX_BATCH_BYTES   (8 * 1024 * 1024)
#define GS_MAX_PAYLOAD_BYTES (16 * 1024 * 1024 - 128 * 1024)

struct rd_kafka_global_sequence_page_s {
        rd_kafka_global_sequence_page_info_t info;
        rd_kafka_Uuid_t topic_id, index_topic_id;
        char *error_string;
        rd_kafka_global_sequence_record_t *records;
        size_t count, capacity;
        rd_list_t buffers;
};

/* A bounded reader is also used for decompressed records. Unlike ordinary
 * partition Fetch, this API requires exactly one COMPLETE batch per mapping;
 * truncated records must never be silently accepted as a shorter page. */
typedef struct gs_reader_s {
        const unsigned char *p;
        size_t left;
        int bad;
} gs_reader_t;

static const void *gs_take(gs_reader_t *r, size_t n) {
        const void *p = r->p;
        if (r->bad || n > r->left) {
                r->bad = 1;
                return NULL;
        }
        r->p += n;
        r->left -= n;
        return p;
}

static uint64_t gs_int(gs_reader_t *r, unsigned n) {
        const unsigned char *p = gs_take(r, n);
        uint64_t v             = 0;
        unsigned i;
        if (!p)
                return 0;
        for (i = 0; i < n; i++)
                v = (v << 8) | p[i];
        return v;
}

static uint64_t gs_uvar(gs_reader_t *r, unsigned bits) {
        uint64_t v = 0;
        unsigned shift;
        for (shift = 0; shift < bits; shift += 7) {
                unsigned b = (unsigned)gs_int(r, 1);
                if (r->bad || (shift + 7 > bits && (b >> (bits - shift)))) {
                        r->bad = 1;
                        return 0;
                }
                v |= (uint64_t)(b & 127) << shift;
                if (!(b & 128))
                        return v;
        }
        r->bad = 1;
        return 0;
}

static int64_t gs_var(gs_reader_t *r, unsigned bits) {
        uint64_t v = gs_uvar(r, bits);
        return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
}

static void gs_uuid(gs_reader_t *r, rd_kafka_Uuid_t *id) {
        *id                        = RD_KAFKA_UUID_ZERO;
        id->most_significant_bits  = (int64_t)gs_int(r, 8);
        id->least_significant_bits = (int64_t)gs_int(r, 8);
}

static void gs_tags(gs_reader_t *r) {
        uint64_t count = gs_uvar(r, 32), i, previous = 0;
        if (count > r->left / 2) {
                r->bad = 1;
                return;
        }
        for (i = 0; i < count && !r->bad; i++) {
                uint64_t tag = gs_uvar(r, 32);
                size_t len   = (size_t)gs_uvar(r, 32);
                if (i && tag <= previous)
                        r->bad = 1;
                gs_take(r, len);
                previous = tag;
        }
}

void rd_kafka_global_sequence_fetch_options_init(
    rd_kafka_global_sequence_fetch_options_t *options) {
        memset(options, 0, sizeof(*options));
        options->max_batches = 100;
        options->max_bytes   = 1024 * 1024;
        options->check_crcs  = 1;
}

const rd_kafka_global_sequence_page_info_t *rd_kafka_global_sequence_page_info(
    const rd_kafka_global_sequence_page_t *page) {
        return &page->info;
}

const rd_kafka_global_sequence_record_t *rd_kafka_global_sequence_page_records(
    const rd_kafka_global_sequence_page_t *page,
    size_t *countp) {
        *countp = page->count;
        return page->records;
}

void rd_kafka_global_sequence_page_destroy(
    rd_kafka_global_sequence_page_t *page) {
        size_t i;
        if (!page)
                return;
        for (i = 0; i < page->count; i++)
                rd_kafka_headers_destroy(
                    (rd_kafka_headers_t *)page->records[i].headers);
        rd_list_destroy(&page->buffers);
        rd_free(page->records);
        rd_free(page->error_string);
        rd_free(page);
}

/* Reuse the same codec implementations as the partition message-set reader.
 * Codec availability follows the library build. No physical offsets or CRC
 * covered bytes are rewritten. The broker has already filtered transactions. */
static rd_kafka_resp_err_t gs_decompress(rd_kafka_broker_t *rkb,
                                         int codec,
                                         int64_t offset,
                                         const void *data,
                                         size_t size,
                                         void **out,
                                         size_t *len) {
        *out = NULL;
        *len = 0;
        switch (codec) {
        case 0:
                *out = rd_memdup(data, size);
                *len = size;
                break;
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP: {
                uint64_t n = 0;
                *out       = rd_gz_decompress(data, (int)size, &n);
                *len       = (size_t)n;
                break;
        }
#endif
#if WITH_SNAPPY
        case RD_KAFKA_COMPRESSION_SNAPPY: {
                static const unsigned char magic[] = {0x82, 'S', 'N', 'A',
                                                      'P',  'P', 'Y', 0};
                if (size > 20 && !memcmp(data, magic, sizeof(magic))) {
                        char errstr[128];
                        *out = rd_kafka_snappy_java_uncompress(
                            (const char *)data + 16, size - 16, len, errstr,
                            sizeof(errstr));
                } else if (rd_kafka_snappy_uncompressed_length(data, size,
                                                               len)) {
                        *out = rd_malloc(RD_MAX(*len, 1));
                        if (rd_kafka_snappy_uncompress(data, size, *out)) {
                                rd_free(*out);
                                *out = NULL;
                        }
                }
                break;
        }
#endif
        case RD_KAFKA_COMPRESSION_LZ4:
                return rd_kafka_lz4_decompress(rkb, 1, offset, (char *)data,
                                               size, out, len);
#if WITH_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD:
                return rd_kafka_zstd_decompress(rkb, (char *)data, size, out,
                                                len);
#endif
        default:
                return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
        }
        return *out ? RD_KAFKA_RESP_ERR_NO_ERROR
                    : RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
}

/* Decode and validate ALL records, including the unselected ends of a batch.
 * Return no records from any malformed page. Pointer storage belongs to page.
 */
static rd_kafka_error_t *gs_batch(rd_kafka_broker_t *rkb,
                                  rd_kafka_global_sequence_page_t *page,
                                  const void *bytes,
                                  size_t size,
                                  int64_t global_base,
                                  int32_t partition,
                                  int64_t physical_base,
                                  int64_t physical_last,
                                  int32_t count,
                                  int64_t selected_start,
                                  int64_t selected_end,
                                  int check_crcs,
                                  rd_ts_t deadline) {
        gs_reader_t b = {bytes, size, 0}, records;
        int64_t base, timestamp_base, timestamp_max;
        int32_t length, epoch, delta, record_count;
        unsigned magic, attributes;
        uint32_t crc;
        void *data = NULL;
        size_t len;
        int32_t i;
        const char *detail = "physical batch did not match its mapping";
        rd_kafka_resp_err_t err;
        base   = (int64_t)gs_int(&b, 8);
        length = (int32_t)gs_int(&b, 4);
        epoch  = (int32_t)gs_int(&b, 4);
        magic  = (unsigned)gs_int(&b, 1);
        crc    = (uint32_t)gs_int(&b, 4);
        if (b.bad || size < RD_KAFKAP_MSGSET_V2_SIZE ||
            length < RD_KAFKAP_MSGSET_V2_SIZE - 12 ||
            (size_t)length != size - 12 || magic != 2 || epoch < -1)
                goto bad;
        if (check_crcs &&
            crc != rd_crc32c(0, (const unsigned char *)bytes + 21, size - 21)) {
                detail = "physical batch CRC32C mismatch";
                goto bad;
        }
        attributes     = (unsigned)gs_int(&b, 2);
        delta          = (int32_t)gs_int(&b, 4);
        timestamp_base = (int64_t)gs_int(&b, 8);
        timestamp_max  = (int64_t)gs_int(&b, 8);
        gs_take(&b, 8 + 2 + 4); /* producer ID, epoch, base sequence */
        record_count = (int32_t)gs_int(&b, 4);
        if (b.bad || (attributes & RD_KAFKA_MSGSET_V2_ATTR_CONTROL) ||
            base != physical_base || delta != physical_last - physical_base ||
            record_count != count)
                goto bad;
        err =
            gs_decompress(rkb, attributes & 7, base, b.p, b.left, &data, &len);
        if (err)
                return rd_kafka_error_new(
                    err, "Global sequence batch decompression failed: %s",
                    rd_kafka_err2str(err));
        rd_list_add(&page->buffers, data);
        records = (gs_reader_t) {data, len, 0};
        /* A record needs at least seven bytes (length, attributes, timestamp,
         * offset, key length, value length, header count). Bound hostile counts
         * before allocating or iterating. */
        if ((uint64_t)count > len / 7) {
                detail = "record count exceeded the decoded batch length";
                goto bad;
        }
        for (i = 0; i < count; i++) {
                int64_t record_len, timestamp_delta, offset_delta, key_len,
                    value_len, header_count, j;
                const void *key, *value, *record_data;
                gs_reader_t r;
                rd_kafka_global_sequence_record_t *record = NULL;
                rd_kafka_headers_t *headers               = NULL;
                int64_t global_offset                     = global_base + i;
                if (rd_clock() >= deadline)
                        return rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__TIMED_OUT,
                            "Global sequence decode deadline expired");
                record_len = gs_var(&records, 32);
                if (records.bad || record_len < 0)
                        goto bad;
                record_data = gs_take(&records, (size_t)record_len);
                if (records.bad)
                        goto bad;
                r = (gs_reader_t) {record_data, (size_t)record_len, 0};
                gs_int(&r, 1); /* record attributes */
                timestamp_delta = gs_var(&r, 64);
                offset_delta    = gs_var(&r, 32);
                key_len         = gs_var(&r, 32);
                key       = key_len < 0 ? NULL : gs_take(&r, (size_t)key_len);
                value_len = gs_var(&r, 32);
                value = value_len < 0 ? NULL : gs_take(&r, (size_t)value_len);
                header_count = gs_var(&r, 32);
                if (r.bad || key_len < -1 || value_len < -1 ||
                    offset_delta != i || header_count < 0 ||
                    (uint64_t)header_count > r.left / 2) {
                        detail =
                            "invalid record offsets, lengths or header count";
                        goto bad;
                }
                if (!(attributes & RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME) &&
                    ((timestamp_delta > 0 &&
                      timestamp_base > INT64_MAX - timestamp_delta) ||
                     (timestamp_delta < 0 &&
                      timestamp_base < INT64_MIN - timestamp_delta))) {
                        detail = "record timestamp overflow";
                        goto bad;
                }
                if (global_offset >= selected_start &&
                    global_offset < selected_end) {
                        if (page->count == page->capacity) {
                                page->capacity = RD_MAX(page->capacity * 2, 32);
                                page->records  = rd_realloc(
                                    page->records,
                                    page->capacity * sizeof(*page->records));
                        }
                        record = &page->records[page->count++];
                        memset(record, 0, sizeof(*record));
                        headers = rd_kafka_headers_new((size_t)header_count);
                        record->headers            = headers;
                        record->global_offset      = global_offset;
                        record->physical_partition = partition;
                        record->physical_offset    = physical_base + i;
                        record->leader_epoch       = epoch;
                        record->timestamp_type =
                            attributes & RD_KAFKA_MSG_ATTR_LOG_APPEND_TIME
                                ? RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME
                                : RD_KAFKA_TIMESTAMP_CREATE_TIME;
                        record->timestamp =
                            record->timestamp_type ==
                                    RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME
                                ? timestamp_max
                                : timestamp_base + timestamp_delta;
                        record->key     = key;
                        record->key_len = (size_t)RD_MAX(0, key_len);
                        record->payload = value;
                        record->len     = (size_t)RD_MAX(0, value_len);
                }
                for (j = 0; j < header_count; j++) {
                        int64_t name_len = gs_var(&r, 32), header_len;
                        const void *name =
                            name_len < 0 ? NULL : gs_take(&r, (size_t)name_len);
                        const void *header;
                        header_len = gs_var(&r, 32);
                        header     = header_len < 0
                                         ? NULL
                                         : gs_take(&r, (size_t)header_len);
                        if (r.bad || name_len < 0 || header_len < -1)
                                goto bad;
                        if (headers)
                                rd_kafka_header_add(headers, name,
                                                    (ssize_t)name_len, header,
                                                    (ssize_t)header_len);
                }
                if (r.bad || r.left) {
                        detail = "record length did not match its contents";
                        goto bad;
                }
        }
        if (records.bad || records.left) {
                detail =
                    "decoded batch did not contain exactly recordCount records";
                goto bad;
        }
        return NULL;
bad:
        return rd_kafka_error_new(RD_KAFKA_RESP_ERR__BAD_MSG,
                                  "Invalid global sequence page: %s", detail);
}

/* Mirrors GlobalSequencePageDecoder: complete physical mappings, exact request
 * intersections, contiguous read_uncommitted prefixes, and read_committed gaps
 * / empty cursor advances for aborted transactions. No snapshot is a distinct
 * early-error case and its -1 cursor is not an ordinary page cursor. */
static rd_kafka_error_t *
gs_decode(rd_kafka_broker_t *rkb,
          rd_kafka_buf_t *reply,
          const rd_kafka_Uuid_t *topic_id,
          int64_t start,
          int64_t end,
          const rd_kafka_global_sequence_fetch_options_t *options,
          int16_t version,
          rd_ts_t deadline,
          rd_kafka_global_sequence_page_t **pagep) {
        rd_kafka_global_sequence_page_t *page = rd_calloc(1, sizeof(*page));
        rd_kafka_global_sequence_page_info_t *info = &page->info;
        rd_kafka_error_t *error                    = NULL;
        size_t wire_size = rd_slice_remains(&reply->rkbuf_reader);
        const void *wire =
            rd_slice_ensure_contig(&reply->rkbuf_reader, wire_size);
        gs_reader_t r = {wire, wire_size, 0};
        uint64_t n, i, encoded = 0;
        int64_t previous_end = -1, first_selected = -1, last_selected = -1;
        const char *detail = "truncated or malformed response";
        rd_list_init(&page->buffers, 0, rd_free);
        info->topic_id       = &page->topic_id;
        info->index_topic_id = &page->index_topic_id;
        info->api_version    = version;
#define GS_CHECK(expr, why)                                                    \
        do {                                                                   \
                if (!(expr)) {                                                 \
                        detail = why;                                          \
                        goto bad;                                              \
                }                                                              \
        } while (0)
        GS_CHECK(wire && wire_size <= GS_MAX_PAYLOAD_BYTES + 1024 * 1024,
                 "response exceeded the global wire bound");
        info->throttle_time_ms = (int32_t)gs_int(&r, 4);
        info->error            = (rd_kafka_resp_err_t)(int16_t)gs_int(&r, 2);
        n                      = gs_uvar(&r, 32);
        GS_CHECK(!r.bad && n <= r.left + 1, "invalid error string length");
        if (n) {
                page->error_string =
                    rd_strndup(gs_take(&r, (size_t)n - 1), (size_t)n - 1);
                info->error_string = page->error_string;
        }
        gs_uuid(&r, &page->topic_id);
        gs_uuid(&r, &page->index_topic_id);
        info->index_partition             = (int32_t)gs_int(&r, 4);
        info->coordinator_leader_epoch    = (int32_t)gs_int(&r, 4);
        info->index_high_watermark        = (int64_t)gs_int(&r, 8);
        info->committed_global_end_offset = (int64_t)gs_int(&r, 8);
        info->next_global_offset          = (int64_t)gs_int(&r, 8);
        n                                 = gs_uvar(&r, 32);
        GS_CHECK(!r.bad && n && n - 1 <= (uint64_t)options->max_batches,
                 "null batch array or too many batches");
        n--;
        GS_CHECK(!rd_kafka_Uuid_cmp(page->topic_id, *topic_id),
                 "response topic UUID mismatch");
        info->has_snapshot = !RD_KAFKA_UUID_IS_ZERO(page->index_topic_id);
        if (info->has_snapshot) {
                GS_CHECK(info->index_partition >= 0 &&
                             info->coordinator_leader_epoch >= 0 &&
                             info->index_high_watermark >= 0 &&
                             info->committed_global_end_offset >= 0,
                         "negative snapshot partition, epoch or offset");
                GS_CHECK(info->next_global_offset >= start &&
                             info->next_global_offset <= end,
                         "cursor outside the requested range");
                GS_CHECK(info->next_global_offset <=
                                 info->committed_global_end_offset ||
                             (info->next_global_offset == start &&
                              start >= info->committed_global_end_offset && !n),
                         "cursor advanced beyond the committed end");
        } else {
                GS_CHECK(!n && info->error &&
                             (info->next_global_offset == -1 ||
                              info->next_global_offset == start),
                         "response without a snapshot carried page data or no "
                         "error");
        }
        for (i = 0; i < n; i++) {
                int64_t global_base    = (int64_t)gs_int(&r, 8);
                int32_t partition      = (int32_t)gs_int(&r, 4);
                int64_t physical_base  = (int64_t)gs_int(&r, 8);
                int64_t physical_last  = (int64_t)gs_int(&r, 8);
                int32_t count          = (int32_t)gs_int(&r, 4);
                int64_t selected_start = (int64_t)gs_int(&r, 8);
                int64_t selected_end   = (int64_t)gs_int(&r, 8);
                int64_t global_end;
                uint64_t size_plus_one = gs_uvar(&r, 32);
                const void *bytes;
                GS_CHECK(!r.bad && global_base >= 0 && partition >= 0 &&
                             physical_base >= 0 &&
                             physical_last >= physical_base && count > 0 &&
                             physical_last - physical_base ==
                                 (int64_t)count - 1 &&
                             global_base <= INT64_MAX - count,
                         "invalid or overflowing batch mapping");
                global_end = global_base + count;
                GS_CHECK(global_end <= info->committed_global_end_offset &&
                             previous_end <= global_base,
                         "batch mappings overlapped or exceeded committed end");
                GS_CHECK(
                    selected_start == RD_MAX(start, global_base) &&
                        selected_end == RD_MIN(end, global_end) &&
                        selected_start < selected_end,
                    "selected range differed from the request intersection");
                GS_CHECK(options->isolation_level || last_selected < 0 ||
                             selected_start == last_selected,
                         "uncommitted page contained a gap");
                GS_CHECK(size_plus_one > 1 &&
                             size_plus_one - 1 <= GS_MAX_BATCH_BYTES,
                         "missing or oversized complete batch");
                encoded += size_plus_one - 1;
                GS_CHECK(
                    encoded <= GS_MAX_PAYLOAD_BYTES &&
                        (n == 1 || encoded <= (uint64_t)options->max_bytes),
                    "encoded batches exceeded the page byte limit");
                bytes = gs_take(&r, (size_t)size_plus_one - 1);
                GS_CHECK(!r.bad, "truncated physical batch");
                error = gs_batch(rkb, page, bytes, (size_t)size_plus_one - 1,
                                 global_base, partition, physical_base,
                                 physical_last, count, selected_start,
                                 selected_end, options->check_crcs, deadline);
                if (error)
                        goto fail;
                gs_tags(&r);
                if (first_selected < 0)
                        first_selected = selected_start;
                previous_end  = global_end;
                last_selected = selected_end;
        }
        if (version >= 1) {
                unsigned pending = (unsigned)gs_int(&r, 1);
                GS_CHECK(pending <= 1, "invalid TransactionPending boolean");
                info->transaction_pending = (int)pending;
        }
        gs_tags(&r);
        GS_CHECK(!r.bad && !r.left,
                 "truncated response, invalid tags or trailing data");
        GS_CHECK(!info->transaction_pending ||
                     (info->has_snapshot && options->isolation_level &&
                      !info->error &&
                      info->next_global_offset <
                          RD_MIN(end, info->committed_global_end_offset)),
                 "invalid transaction pending state");
        if (info->has_snapshot) {
                GS_CHECK(last_selected <= info->next_global_offset,
                         "cursor preceded returned records");
                if (!options->isolation_level) {
                        GS_CHECK(
                            first_selected < 0
                                ? info->next_global_offset == start
                                : (first_selected == start &&
                                   last_selected == info->next_global_offset),
                            "uncommitted cursor skipped a global range");
                }
                GS_CHECK(
                    info->error || info->transaction_pending ||
                        info->next_global_offset != start ||
                        start >= RD_MIN(end, info->committed_global_end_offset),
                    "successful page made no progress inside committed range");
        } else {
                info->next_global_offset = start;
        }
        if (rd_clock() >= deadline) {
                error = rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__TIMED_OUT,
                    "Global sequence decode deadline expired");
                goto fail;
        }
        rd_kafka_op_throttle_time(rkb, rkb->rkb_rk->rk_rep,
                                  info->throttle_time_ms);
        if (info->throttle_time_ms > 0) {
                rd_ts_t until =
                    rd_clock() + (rd_ts_t)info->throttle_time_ms * 1000;
                rd_kafka_broker_lock(rkb);
                rkb->rkb_ts_global_sequence_throttle =
                    RD_MAX(rkb->rkb_ts_global_sequence_throttle, until);
                rd_kafka_broker_unlock(rkb);
        }
        *pagep = page;
        return NULL;
bad:
        error = rd_kafka_error_new(RD_KAFKA_RESP_ERR__BAD_MSG,
                                   "Invalid global sequence page: %s", detail);
fail:
        rd_kafka_global_sequence_page_destroy(page);
        return error;
#undef GS_CHECK
}

static int gs_retryable(rd_kafka_resp_err_t err) {
        switch (err) {
        case RD_KAFKA_RESP_ERR__TRANSPORT:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
        case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
        case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
        case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
        case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID:
        case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS:
        case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND:
        case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR:
        case RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH:
        case RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED:
                return 1;
        default:
                return 0;
        }
}

static void gs_sleep_until(rd_ts_t until) {
        rd_ts_t now;
        /* rd_usleep takes an int number of microseconds on POSIX. Chunk long
         * server throttles instead of overflowing that argument. */
        while ((now = rd_clock()) < until)
                rd_usleep((int)RD_MIN(until - now, 1000000), NULL);
}

static void gs_backoff(rd_kafka_t *rk, rd_ts_t deadline, int throttle_ms) {
        int ms = RD_MAX(rk->rk_conf.retry_backoff_ms, throttle_ms);
        gs_sleep_until(RD_MIN(deadline, rd_clock() + (rd_ts_t)ms * 1000));
}

/* Always resolve fresh metadata. No auto-creation and no stale topic-ID cache.
 * DescribeTopics uses the existing Metadata transport/parser and admin
 * deadline. */
static rd_kafka_error_t *gs_resolve(rd_kafka_t *rk,
                                    const char *topic,
                                    const rd_kafka_Uuid_t *expected,
                                    rd_ts_t deadline,
                                    rd_kafka_Uuid_t *id) {
        rd_kafka_error_t *error;
        do {
                rd_kafka_AdminOptions_t *options;
                rd_kafka_TopicCollection_t *topics;
                rd_kafka_queue_t *queue;
                rd_kafka_event_t *event;
                const rd_kafka_TopicDescription_t **descriptions;
                const rd_kafka_error_t *topic_error;
                size_t n = 0;
                char errstr[256];
                int remains = rd_timeout_remains(deadline);
                if (remains <= 0)
                        break;
                options = rd_kafka_AdminOptions_new(
                    rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);
                rd_kafka_AdminOptions_set_request_timeout(
                    options, remains, errstr, sizeof(errstr));
                topics = rd_kafka_TopicCollection_of_topic_names(&topic, 1);
                queue  = rd_kafka_queue_new(rk);
                rd_kafka_DescribeTopics(rk, topics, options, queue);
                rd_kafka_TopicCollection_destroy(topics);
                rd_kafka_AdminOptions_destroy(options);
                event =
                    rd_kafka_queue_poll(queue, rd_timeout_remains(deadline));
                error = NULL;
                if (!event) {
                        error = rd_kafka_error_new(
                            RD_KAFKA_RESP_ERR__TIMED_OUT,
                            "Global sequence metadata deadline expired");
                } else if (rd_kafka_event_error(event)) {
                        error = rd_kafka_error_new(
                            rd_kafka_event_error(event),
                            "Global sequence metadata: %s",
                            rd_kafka_event_error_string(event));
                } else {
                        descriptions = rd_kafka_DescribeTopics_result_topics(
                            rd_kafka_event_DescribeTopics_result(event), &n);
                        if (n != 1 || !descriptions[0]) {
                                error = rd_kafka_error_new(
                                    RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
                                    "Global sequence topic absent from "
                                    "metadata");
                        } else if ((topic_error =
                                        rd_kafka_TopicDescription_error(
                                            descriptions[0]))) {
                                error = rd_kafka_error_copy(topic_error);
                        } else {
                                *id = *rd_kafka_TopicDescription_topic_id(
                                    descriptions[0]);
                                if (RD_KAFKA_UUID_IS_ZERO(*id))
                                        error = rd_kafka_error_new(
                                            RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                                            "Global sequence metadata requires "
                                            "a nonzero topic UUID (Metadata "
                                            "v10+)");
                                else if (expected &&
                                         rd_kafka_Uuid_cmp(*expected, *id))
                                        error = rd_kafka_error_new(
                                            RD_KAFKA_RESP_ERR__INCONSISTENT,
                                            "Global sequence topic was "
                                            "recreated: expected %s, metadata "
                                            "returned %s",
                                            rd_kafka_Uuid_base64str(expected),
                                            rd_kafka_Uuid_base64str(id));
                        }
                }
                rd_kafka_event_destroy(event);
                rd_kafka_queue_destroy(queue);
                if (!error ||
                    rd_kafka_error_code(error) ==
                        RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART ||
                    !gs_retryable(rd_kafka_error_code(error)))
                        return error;
                rd_kafka_error_destroy(error);
                gs_backoff(rk, deadline, 0);
        } while (rd_timeout_remains(deadline) > 0);
        return rd_kafka_error_new_retriable(
            RD_KAFKA_RESP_ERR__TIMED_OUT,
            "Global sequence metadata deadline expired");
}

typedef struct gs_response_s {
        rd_kafka_resp_err_t error;
        rd_kafka_buf_t *reply;
} gs_response_t;

/* Non-destroy callbacks run only while the private reply queue is served by
 * the calling thread. On timeout, destroying/disabling that queue turns late
 * responses into DESTROY callbacks. Do not dereference the expired stack
 * opaque in that path. No broker thread decodes a page or owns caller memory.
 */
static void gs_response(rd_kafka_t *rk,
                        rd_kafka_broker_t *rkb,
                        rd_kafka_resp_err_t err,
                        rd_kafka_buf_t *reply,
                        rd_kafka_buf_t *request,
                        void *opaque) {
        gs_response_t *result;
        if (err == RD_KAFKA_RESP_ERR__DESTROY)
                return;
        result        = opaque;
        result->error = err;
        if (reply) {
                rd_kafka_buf_keep(reply);
                result->reply = reply;
        }
}

/* Called with the broker lock held by broker_any_up(). */
static int gs_filter(rd_kafka_broker_t *rkb, void *opaque) {
        int min_version = *(int *)opaque;
        return rd_kafka_broker_ApiVersion_supported0(
                   rkb, RD_KAFKAP_FetchGlobalSequence, min_version, 1, NULL,
                   rd_false) < 0;
}

rd_kafka_error_t *rd_kafka_global_sequence_fetch(
    rd_kafka_t *rk,
    const char *topic,
    const rd_kafka_Uuid_t *expected_topic_id,
    int64_t start,
    int64_t end_exclusive,
    const rd_kafka_global_sequence_fetch_options_t *user_options,
    rd_kafka_global_sequence_page_t **pagep,
    int timeout_ms) {
        rd_kafka_global_sequence_fetch_options_t options;
        rd_kafka_Uuid_t topic_id = RD_KAFKA_UUID_ZERO;
        rd_kafka_error_t *error;
        rd_ts_t deadline;
        if (pagep)
                *pagep = NULL;
        rd_kafka_global_sequence_fetch_options_init(&options);
        if (user_options)
                options = *user_options;
        if (!rk || !topic || !*topic || !pagep || start < 0 ||
            end_exclusive < start || timeout_ms <= 0 ||
            options.max_batches < 1 || options.max_batches > 1000 ||
            options.max_bytes < 1 || options.max_bytes > GS_MAX_PAYLOAD_BYTES ||
            (options.isolation_level != 0 && options.isolation_level != 1) ||
            (options.check_crcs != 0 && options.check_crcs != 1) ||
            (expected_topic_id && RD_KAFKA_UUID_IS_ZERO(*expected_topic_id)))
                return rd_kafka_error_new(
                    RD_KAFKA_RESP_ERR__INVALID_ARG,
                    "Invalid Global Sequence Fetch topic, UUID, range, options "
                    "or timeout");
        deadline = rd_timeout_init(timeout_ms);
        error = gs_resolve(rk, topic, expected_topic_id, deadline, &topic_id);
        if (error)
                return error;
        while (rd_timeout_remains(deadline) > 0) {
                rd_kafka_broker_t *rkb;
                rd_kafka_buf_t *request;
                rd_kafka_q_t *queue;
                gs_response_t response = {RD_KAFKA_RESP_ERR__TIMED_OUT, NULL};
                int16_t version;
                int min_version = options.isolation_level;
                int remains;
                rd_ts_t throttle_until;
                rkb = rd_kafka_broker_any_usable(
                    rk, rd_timeout_remains(deadline), RD_DO_LOCK, 0,
                    "global sequence fetch");
                if (!rkb)
                        break;
                version = rd_kafka_broker_ApiVersion_supported(
                    rkb, RD_KAFKAP_FetchGlobalSequence, min_version, 1, NULL);
                if (version < 0) {
                        rd_kafka_broker_destroy(rkb);
                        rkb = rd_kafka_broker_any_up(
                            rk, NULL, gs_filter, &min_version,
                            "global sequence fetch supported broker");
                        if (!rkb)
                                return rd_kafka_error_new(
                                    RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                                    "No connected broker supports API 98 "
                                    "v%d..1",
                                    min_version);
                        version = rd_kafka_broker_ApiVersion_supported(
                            rkb, RD_KAFKAP_FetchGlobalSequence, min_version, 1,
                            NULL);
                }
                /* ApiVersions may have changed during a reconnect between
                 * selection and this check. Never serialize version -1. */
                if (version < 0) {
                        rd_kafka_broker_destroy(rkb);
                        gs_backoff(rk, deadline, 0);
                        continue;
                }
                rd_kafka_broker_lock(rkb);
                throttle_until = rkb->rkb_ts_global_sequence_throttle;
                rd_kafka_broker_unlock(rkb);
                gs_sleep_until(RD_MIN(throttle_until, deadline));
                remains = rd_timeout_remains(deadline);
                if (remains <= 0) {
                        rd_kafka_broker_destroy(rkb);
                        break;
                }
                request = rd_kafka_buf_new_flexver_request(
                    rkb, RD_KAFKAP_FetchGlobalSequence, 1, 64, rd_true);
                rd_kafka_buf_ApiVersion_set(request, version, 0);
                rd_kafka_buf_write_uuid(request, &topic_id);
                rd_kafka_buf_write_i64(request, start);
                rd_kafka_buf_write_i64(request, end_exclusive);
                rd_kafka_buf_write_i32(request, options.max_batches);
                rd_kafka_buf_write_i32(request, RD_MIN(30000, remains));
                rd_kafka_buf_write_i32(request, options.max_bytes);
                /* Present in BOTH v0 and v1; v0 only allows value 0. */
                rd_kafka_buf_write_i8(request, (int8_t)options.isolation_level);
                /* The standard flexible request finalizer appends body tags. */
                request->rkbuf_max_retries = RD_KAFKA_REQUEST_NO_RETRIES;
                rd_kafka_buf_set_abs_timeout(request, remains, 0);
                queue = rd_kafka_q_new(rk);
                rd_kafka_broker_buf_enq_replyq(rkb, request,
                                               RD_KAFKA_REPLYQ(queue, 0),
                                               gs_response, &response);
                rd_kafka_q_serve(queue, rd_timeout_remains(deadline), 1,
                                 RD_KAFKA_Q_CB_RETURN, NULL, NULL);
                rd_kafka_q_destroy_owner(queue);
                if (response.error) {
                        error = rd_kafka_error_new(
                            response.error, "Global sequence request: %s",
                            rd_kafka_err2str(response.error));
                } else {
                        error = gs_decode(rkb, response.reply, &topic_id, start,
                                          end_exclusive, &options, version,
                                          deadline, pagep);
                }
                if (response.reply)
                        rd_kafka_buf_destroy(response.reply);
                rd_kafka_broker_destroy(rkb);
                if (*pagep) {
                        rd_kafka_global_sequence_page_info_t *info =
                            &(*pagep)->info;
                        if (!info->error || info->next_global_offset > start ||
                            !gs_retryable(info->error))
                                return NULL;
                        /* Never hide a valid prefix, even if its payload is
                         * empty after read_committed filtering. */
                        remains = info->throttle_time_ms;
                        rd_kafka_global_sequence_page_destroy(*pagep);
                        *pagep = NULL;
                        gs_backoff(rk, deadline, remains);
                } else {
                        if (!gs_retryable(rd_kafka_error_code(error)))
                                return error;
                        rd_kafka_error_destroy(error);
                        gs_backoff(rk, deadline, 0);
                }
        }
        return rd_kafka_error_new_retriable(
            RD_KAFKA_RESP_ERR__TIMED_OUT,
            "Global Sequence Fetch deadline expired for topic %s", topic);
}
