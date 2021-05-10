//"Copyright [year] <Copyright Owner>"
//
// Created by mrbgn on 5/5/21.
//
#include <Dbcs.hpp>
using namespace rocksdb;
namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;

Dbcs::Dbcs(std::string& log_level, uint32_t& thread_count, std::string& output,
           std::string& input)
    : _logLevel(log_level),
      _threadWriteCount(thread_count),
      _inputPath(input),
      _outputPath(output),
      _poolWrite(thread_count) {
  try {
    EnableLogging();
    ReadDatabaseFamilies();
    ReadData();
  } catch (const std::exception& e) {
    Log(e.what());
  }
}
void Dbcs::ReadDatabaseFamilies() {
  DB* dbRead;
  Options options;
  options.create_if_missing = false;
  rocksdb::Status status =
      rocksdb::DB::OpenForReadOnly(options, _inputPath, &dbRead);
  if (!status.ok()) {
    Log(status.ToString());
    throw std::runtime_error("BD ERROR");
  }
  dbRead->ListColumnFamilies(options, _inputPath, &_families);
  delete dbRead;
  CreateOutputDb();
}

void Dbcs::CreateOutputDb() {
  DB* dbWrite;
  Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;

  rocksdb::Status status = rocksdb::DB::Open(options, _outputPath, &dbWrite);
  if (!status.ok()) std::cerr << status.ToString() << std::endl;

  for (int i = 0; i < _threadWriteCount; ++i) {
    rocksdb::ColumnFamilyHandle* cf;
    status = dbWrite->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                         "family_" + std::to_string(i), &cf);
    if (!status.ok()) {
      Log(status.ToString());
      throw std::runtime_error("BD ERROR");
    }
    dbWrite->DestroyColumnFamilyHandle(cf);
  }
  delete dbWrite;
}
void Dbcs::ReadData() {
  DB* dbRead;
  Options options;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

  for (auto& family : _families) {
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        family, rocksdb::ColumnFamilyOptions()));
  }

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status status = rocksdb::DB::Open(
      rocksdb::DBOptions(), _inputPath, column_families, &handles, &dbRead);

  if (!status.ok()) {
    Log(status.ToString());
    throw std::runtime_error("BD ERROR");
  }

  ThreadPool ReadingPool(_families.size());

  for (auto& handle : handles) {
    rocksdb::Iterator* it = dbRead->NewIterator(rocksdb::ReadOptions(), handle);
    it->SeekToFirst();
    ReadingPool.enqueue(&Dbcs::ReadFromFamily, std::ref(*this), it);
  }

  for (auto handle : handles) {
    status = dbRead->DestroyColumnFamilyHandle(handle);
    if (!status.ok()) {
      Log(status.ToString());
      throw std::runtime_error("BD ERROR");
    }
  }

  delete dbRead;
}
void Dbcs::ReadFromFamily(rocksdb::Iterator* iterator) {
  CycledList list(_threadWriteCount);
  for (iterator; iterator->Valid(); iterator->Next()) {
    _poolWrite.enqueue(&Dbcs::HashPair, std::ref(*this),
                       iterator->key().ToString(), iterator->value().ToString(),
                       list.NextIndex());
  }
  delete iterator;
}
void Dbcs::HashPair(std::string key, std::string value,
                    uint32_t handle_number) {
  DB* dbWrite;
  Options options;
  std::vector<ColumnFamilyDescriptor> column_families;
  std::vector<ColumnFamilyHandle*> writeHandles;

  Status status = rocksdb::DB::OpenForReadOnly(options, _outputPath, &dbWrite);
  if (!status.ok()) {
    Log(status.ToString());
    throw std::runtime_error("BD ERROR");
  }
  std::vector<std::string> families;
  dbWrite->ListColumnFamilies(options, _outputPath, &families);
  delete dbWrite;

  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

  status = DB::Open(rocksdb::DBOptions(), _outputPath, column_families,
                    &writeHandles, &dbWrite);
  if (!status.ok()) {
    Log(status.ToString());
    throw std::runtime_error("BD ERROR");
  }

  status = dbWrite->Put(WriteOptions(), writeHandles[handle_number], Slice(key),
                        Slice(value));
  if (!status.ok()) {
    Log(status.ToString());
    throw std::runtime_error("BD ERROR");
  }

  std::string success =
      "Successfully added pair [key : " + key + "; value: " + value + " ]";
  Log(success);

  delete dbWrite;
}
void Dbcs::EnableLogging() {
  const std::string format = "%TimeStamp% <%Severity%> (%ThreadID%): %Message%";
  logging::add_console_log(
      std::cout, keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
      keywords::auto_flush = true);
  logging::add_common_attributes();
}
void Dbcs::Log(std::string message) {
  if (_logLevel == "trace") {
    BOOST_LOG_TRIVIAL(trace) << std ::endl << message << std::endl;
  } else if (_logLevel == "error") {
    BOOST_LOG_TRIVIAL(error) << std ::endl << message << std::endl;
  } else if (_logLevel == "info") {
    BOOST_LOG_TRIVIAL(info) << std ::endl << message << std::endl;
  } else {
    BOOST_LOG_TRIVIAL(fatal) << std ::endl << message << std::endl;
  }
}
