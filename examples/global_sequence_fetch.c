/* One-page C example. Distributed under the librdkafka BSD license. */
#include "rdkafka.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_error_t *error;
        rd_kafka_global_sequence_page_t *page = NULL;
        const rd_kafka_global_sequence_page_info_t *info;
        const rd_kafka_global_sequence_record_t *records;
        rd_kafka_global_sequence_fetch_options_t options;
        char errstr[512], *tail;
        int64_t start, end;
        size_t i, count;
        int status = 0;
        if (argc != 5) {
                fprintf(stderr, "Usage: %s brokers topic start end_exclusive\n",
                        argv[0]);
                return 1;
        }
        start = strtoll(argv[3], &tail, 10);
        if (!*argv[3] || *tail)
                return 1;
        end = strtoll(argv[4], &tail, 10);
        if (!*argv[4] || *tail)
                return 1;
        conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "bootstrap.servers", argv[1], errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        rd_kafka_global_sequence_fetch_options_init(&options);
        error = rd_kafka_global_sequence_fetch(rk, argv[2], NULL, start, end,
                                               &options, &page, 10000);
        if (error) {
                fprintf(stderr, "%s\n", rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                rd_kafka_destroy(rk);
                return 1;
        }
        info    = rd_kafka_global_sequence_page_info(page);
        records = rd_kafka_global_sequence_page_records(page, &count);
        for (i = 0; i < count; i++)
                printf("global=%" PRId64 " partition=%" PRId32
                       " physical=%" PRId64 " bytes=%zu\n",
                       records[i].global_offset, records[i].physical_partition,
                       records[i].physical_offset, records[i].len);
        fprintf(stderr,
                "topic_id=%s next=%" PRId64 " committed_end=%" PRId64
                " pending=%d\n",
                rd_kafka_Uuid_base64str(info->topic_id),
                info->next_global_offset, info->committed_global_end_offset,
                info->transaction_pending);
        if (info->error) {
                fprintf(stderr, "Page error at %" PRId64 ": %s\n",
                        info->next_global_offset,
                        info->error_string ? info->error_string
                                           : rd_kafka_err2str(info->error));
                status = 2;
        }
        /* For subsequent pages/retries, copy info->topic_id BEFORE freeing
         * the page and pass that expected UUID with info->next_global_offset.
         * See global_sequence_consumer.cpp for a complete paginated loop. */
        rd_kafka_global_sequence_page_destroy(page);
        rd_kafka_destroy(rk);
        return status;
}
