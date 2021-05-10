//"Copyright [year] <Copyright Owner>"
//
// Created by mrbgn on 5/5/21.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include <string>

#include "PicoSHA.hpp"
#include "ThreadPool.hpp"
#include "rocksdb/db.h"

#ifndef TEMPLATE_DBCS_H
#define TEMPLATE_DBCS_H

/**
 * @brief Struct to forward key-value pairs to consumer
 * @param key Key from key-value pair
 * @param value value from key-value pair
 */

struct CycledList {
  uint32_t length;
  uint32_t position;
  CycledList(uint32_t len) : length(len) { position = 0; }
  uint32_t NextIndex() {
    position = (position + 1) % length;
    return position;
  }
};

/**
 * @brief Class to operate with existed ( created by time ) rocksdb DataBase
 * @class Dbcs
 *
 * @param log_level Log level to Boost_log; if log_level is unexpected it's set
 * to fatal
 * @param thread_count Quantity of threads to operate with db that is equal to
 * quantity of families in output db
 * @param output Link to output db
 * @param input Link to input db
 *
 */
class Dbcs {
 public:
  Dbcs(std::string &log_level, uint32_t &thread_count, std::string &output,
       std::string &input);

  /**
   * @brief void to read all families
   */
  void ReadDatabaseFamilies();
  /**
   * @brief void to read from exact family
   * @param iterator to First pair in exact family
   */
  void ReadFromFamily(rocksdb::Iterator *iterator);
  /**
   * @brief void to create new db with thread_count quanity of families
   */
  void CreateOutputDb();
  /**
   * @brief main void to read all data from input db
   */
  void ReadData();
  /**
   * @brief void to hash pairs and add it to output db
   * @param key key string
   * @param value value string
   * @param handle_number number of family to put pair in
   */
  void HashPair(std::string key, std::string value, uint32_t handle_number);
  /**
   * @brief void to enable logging via Bosst::Log
   */
  void EnableLogging();
/**
 * @brief void to log messages
 * @param message string to log via Boost::Log
 */
  void Log(std::string message);

 private:
  std::string _logLevel;
  uint32_t _threadWriteCount;
  std::string _inputPath;
  std::string _outputPath;
  ThreadPool _poolWrite;
  std::vector<std::string> _families;
};

#endif  // TEMPLATE_DBCS_H
