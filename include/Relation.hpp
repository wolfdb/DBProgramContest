#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include "Consts.hpp"

using RelationId = unsigned;
struct FilterInfo;
//---------------------------------------------------------------------------
class Relation {
  private:
  /// Owns memory (false if it was mmaped)
  bool ownsMemory;
  /// Loads data from a file
  void loadRelation(const char* fileName);

  public:
  /// The number of tuples
  uint64_t rowCount;
  /// The join column containing the keys
  std::vector<uint64_t*> columns;

  /// Sample count
  uint64_t sampleCount;
  bool sampleAll = false;
  uint32_t sampleStep;

  /// Sampled min max, set on the first scan
  std::vector<uint64_t> sample_maxs;
  std::vector<uint64_t> sample_mins;

  /// Sampled distinct values, set on second scan(if order is false)
  std::vector<std::unordered_map<uint64_t, uint64_t>> sample_ndvs;
  /// Compress the column if ndv < rowCount / 3
  std::vector<std::vector<uint64_t*>> compressColumns;
  std::vector<bool> needCompress;

  /// Stores a relation into a file (binary)
  void storeRelation(const std::string& fileName);
  /// Stores a relation into a file (csv)
  void storeRelationCSV(const std::string& fileName);
  /// Dump SQL: Create and load table (PostgreSQL)
  void dumpSQL(const std::string& fileName,unsigned relationId);
  /// Calculate the sample count
  void calcSampleCount();
  /// build histogram for column i
  void buildHistogram(int idx);
    /// build concurrent hash map for column i, with range [start, end)
  void buildConcurrentHashMap(int idx, uint32_t start, uint32_t end);

  /// Calculate the estimate cost
  void calThenSetEstimateCost(FilterInfo &filter);

  /// Constructor without mmap
  Relation(uint64_t rowCount,std::vector<uint64_t*>&& columns) : ownsMemory(true), rowCount(rowCount), columns(columns) {}
  /// Constructor using mmap
  Relation(const char* fileName);
  /// Delete copy constructor
  Relation(const Relation& other)=delete;
  /// Move constructor
  Relation(Relation&& other)=default;
  /// The destructor
  ~Relation();
};
//---------------------------------------------------------------------------
