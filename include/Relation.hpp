#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_set>
#include <tbb/concurrent_unordered_map.h>
#include <boost/histogram.hpp>
#include "Consts.hpp"

using RelationId = unsigned;
struct FilterInfo;
//---------------------------------------------------------------------------
struct HolderOfStaticHistogram {
  // put axis types here
  using axes_t = std::tuple<
    boost::histogram::axis::regular<>
  >;
  using hist_t = boost::histogram::histogram<axes_t>;
  hist_t hist_;
};
//---------------------------------------------------------------------------
class Relation {
  private:
  /// Owns memory (false if it was mmaped)
  bool ownsMemory;
  /// Loads data from a file
  void loadRelation(const char* fileName);

  public:
  enum Sorted : char { True, False, Likely };
  enum Order : char { ASC, DESC };
  /// The number of tuples, this must be uint64_t
  uint64_t rowCount;
  /// The join column containing the keys
  std::vector<uint64_t*> columns;

  /// Sample count
  uint64_t sampleCount;

  /// Sampled min max, set on the first scan
  std::vector<uint64_t> sample_maxs;
  std::vector<uint64_t> sample_mins;
  std::vector<int64_t> sample_distinct_count;

  /// Sampled distinct values, set on second scan(if order is false)
  std::vector<std::unordered_set<uint64_t>> sample_distinctVals;

  /// Sample Histogram
  std::vector<HolderOfStaticHistogram> sample_histograms;

  /// Is columns sorted, sample set this first, and a later sequentail may change it
  std::vector<Sorted> sorts;
  std::vector<Order> orders;

  /// Real min/max & histogram
  std::vector<uint64_t> maxs;
  std::vector<uint64_t> mins;
  std::vector<int64_t> distinct_count;
  // value -> index map
  // std::vector<std::unordered_multimap<uint64_t, uint64_t>> columnHmap;
  std::vector<oneapi::tbb::concurrent_unordered_multimap<uint64_t, uint32_t>> columnHmap;
  std::vector<bool> hasHmapBuilt;

  /// Real Histogram
  std::vector<HolderOfStaticHistogram> histograms;

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
  /// print histogram of column i
  void printHistogram(int idx);
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
