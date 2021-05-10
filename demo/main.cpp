//"Copyright [year] <Copyright Owner>"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <iostream>
#include <string>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <boost/program_options.hpp>

#include "Dbcs.hpp"


using namespace boost::program_options;
using namespace rocksdb;

int main(int argc, char** argv) {
  std::string usage =\
 R"(Usage:

  dbcs [options] <path/to/input/storage.db>

  Options:

  --log-level <string>          = "info"|"warning"|"error"
                                = default: "error"
  --thread-count <number>       =
                                = default: count of logical core
  --output <path>               = <path/to/output/storage.db>
                                = default: <path/to/input/dbcs-storage.db>
  --input <path>                = <path/to/input/storage.db>
                                = default: <path/to/input/dbcs.db>)";

  options_description desc("General options");
  std::string task_type;
  desc.add_options()
      ("help", usage.c_str())
      ("create-test-db", "creating test db")
      ("log-level", value<std::string>()->default_value("error"))
      ("thread-count", value<uint32_t>()->default_value(std::thread::hardware_concurrency()))
      ("output", value<std::string>()->default_value("../dbcs-storage.db"))
      ("input", value<std::string>()->default_value("../dbcs.db"));
  variables_map vm;
  try {
    parsed_options parsed = command_line_parser(argc, argv).options(desc).allow_unregistered().run();
    store(parsed, vm);

    if (vm.count("help")) {
      std::cout << usage << std::endl;
      return 0;
    }

    std::string log_level = vm["log-level"].as<std::string>();
    uint32_t thread_count = vm["thread-count"].as<uint32_t>();
    std::string output = vm["output"].as<std::string>();
    std::string input = vm["input"].as<std::string>();

    Dbcs db(log_level, thread_count, output, input);

  }
  catch(std::exception& ex) {
    std::cout << usage << std::endl;
    return 1;
  }

}