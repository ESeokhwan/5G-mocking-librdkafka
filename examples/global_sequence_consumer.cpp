/* Global Sequence Fetch example for the kafka-gst fork (API 98).
 * Distributed under the librdkafka BSD license. */
#include "rdkafkacpp.h"
#include "rdkafka.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

using Page = std::unique_ptr<rd_kafka_global_sequence_page_t,
                             decltype(&rd_kafka_global_sequence_page_destroy)>;
using Uuid = std::unique_ptr<rd_kafka_Uuid_t, decltype(&rd_kafka_Uuid_destroy)>;

static void hex(const void *bytes, size_t size) {
  if (!bytes) {
    std::cout << "null";
    return;
  }
  const auto *p = static_cast<const unsigned char *>(bytes);
  for (size_t i = 0; i < size; ++i)
    std::cout << std::hex << std::setw(2) << std::setfill('0')
              << static_cast<unsigned>(p[i]);
  std::cout << std::dec;
}

int main(int argc, char **argv) {
  if (argc < 5 || argc > 8) {
    std::cerr << "Usage: " << argv[0]
              << " brokers topic start end_exclusive [isolation=0|1]"
                 " [max_batches=100] [max_bytes=1048576]\n"
                 "Records: global_offset<TAB>partition<TAB>physical_offset"
                 "<TAB>timestamp<TAB>key_hex<TAB>payload_hex\n";
    return 1;
  }
  try {
    int64_t next      = std::stoll(argv[3]);
    const int64_t end = std::stoll(argv[4]);
    rd_kafka_global_sequence_fetch_options_t options;
    rd_kafka_global_sequence_fetch_options_init(&options);
    if (argc > 5)
      options.isolation_level = std::stoi(argv[5]);
    if (argc > 6)
      options.max_batches = std::stoi(argv[6]);
    if (argc > 7)
      options.max_bytes = std::stoi(argv[7]);
    if (next < 0 || end < next)
      throw std::runtime_error("Invalid global range");

    std::string error;
    std::unique_ptr<RdKafka::Conf> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (conf->set("bootstrap.servers", argv[1], error) !=
            RdKafka::Conf::CONF_OK ||
        conf->set("allow.auto.create.topics", "false", error) !=
            RdKafka::Conf::CONF_OK)
      throw std::runtime_error(error);
    // A producer handle supplies the common network transport; no group joins
    // or partition assignments are needed. An existing consumer also works.
    std::unique_ptr<RdKafka::Producer> transport(
        RdKafka::Producer::create(conf.get(), error));
    if (!transport)
      throw std::runtime_error(error);
    Uuid expected(nullptr, rd_kafka_Uuid_destroy);
    while (next < end) {
      rd_kafka_global_sequence_page_t *raw = nullptr;
      rd_kafka_error_t *failure            = rd_kafka_global_sequence_fetch(
          transport->c_ptr(), argv[2], expected.get(), next, end, &options,
          &raw, 10000);
      if (failure) {
        const std::string reason = rd_kafka_error_string(failure);
        rd_kafka_error_destroy(failure);
        throw std::runtime_error(reason);
      }
      Page page(raw, rd_kafka_global_sequence_page_destroy);
      const auto *info = rd_kafka_global_sequence_page_info(page.get());
      if (!expected) {
        expected.reset(rd_kafka_Uuid_copy(info->topic_id));
        std::cerr << "topic_id=" << rd_kafka_Uuid_base64str(expected.get())
                  << '\n';
      }
      size_t count;
      const auto *records =
          rd_kafka_global_sequence_page_records(page.get(), &count);
      for (size_t i = 0; i < count; ++i) {
        const auto &r = records[i];
        std::cout << r.global_offset << '\t' << r.physical_partition << '\t'
                  << r.physical_offset << '\t' << r.timestamp << '\t';
        hex(r.key, r.key_len);
        std::cout << '\t';
        hex(r.payload, r.len);
        std::cout << '\n';
      }
      const int64_t previous = next;
      next =
          info->next_global_offset;  // Also advance for empty committed pages.
      std::cerr << "next=" << next
                << " committed_end=" << info->committed_global_end_offset
                << " records=" << count
                << " pending=" << info->transaction_pending << '\n';
      if (info->error) {
        // The valid prefix was already delivered. Keep next and the expected
        // UUID if adding application-level retries; never restart at previous.
        std::cerr << "Page error at global " << next << ": "
                  << (info->error_string ? info->error_string
                                         : rd_kafka_err2str(info->error))
                  << '\n';
        return 2;
      }
      // For live data or pending transactions this bounded API returns
      // promptly. Poll the same cursor again; an empty page alone does not mean
      // EOF.
      if (next == previous)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
  return 0;
}
