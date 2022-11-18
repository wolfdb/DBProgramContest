#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include <map>
#include <array>
#include "Relation.hpp"
#include "Parser.hpp"
#include "Column.hpp"

class Joiner;
//---------------------------------------------------------------------------
enum class OperatorType { Scan, FilterScan, Join, SelfJoin, Checksum};
class Operator {
  /// Operators materialize their entire result
  bool isParentSumOperator = false;
  friend Joiner;
  protected:
  /// Mapping from select info to data
  std::unordered_map<SelectInfo,unsigned> select2ResultColId;
  /// The materialized results
  std::vector<Column<uint64_t>> results;

  public:
  /// Require a column and add it to results
  virtual bool require(SelectInfo &info) = 0;
  /// Resolves a column
  unsigned resolve(const SelectInfo &info) { assert(select2ResultColId.find(info)!=select2ResultColId.end()); return select2ResultColId[info]; }
  /// Run
  virtual void run() = 0;
  /// Get  materialized results
  virtual std::vector<Column<uint64_t>>& getResults();
  /// Get memory consumption
  virtual uint64_t getResultsSize();
  /// The result size
  uint64_t resultSize=0;
  /// estimate row count
  uint64_t eCost;
  int counted = 0; // 0 - no counting, 1 - make new counting, 2 - follow child op'counting

  virtual bool isParentSum() {
    return isParentSumOperator;
  }
  virtual void setParentSum() {
    isParentSumOperator = true;
  }

  std::set<unsigned> bindings;
  /// The destructor
  virtual ~Operator() {};

  virtual std::string getname() = 0;
  // virtual OperatorType getType() = 0;
};
//---------------------------------------------------------------------------
class Scan : public Operator {
  protected:
  /// The relation
  Relation& relation;
  /// The name of the relation in the query
  unsigned relationBinding;
  /// required info
  std::vector<SelectInfo> infos;

  public:
  /// The constructor
  Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {
    this->eCost = r.rowCount;
    this->bindings.insert(relationBinding);
  };
  /// Require a column and add it to results
  bool require(SelectInfo &info) override;
  /// Run
  void run() override;
  virtual uint64_t getResultsSize() override;

  std::string getname() override { return "Scan"; };
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
  /// The filter info
  std::vector<FilterInfo> filters;
  /// The input data
  std::vector<uint64_t*> inputData;
  /// tmpResults[filterid][col][val]
  std::vector<std::vector<std::vector<uint64_t>>> tmpResults;
  /// Apply filter
  bool applyFilter(uint64_t id,FilterInfo& f);

  public:
  /// The constructor
  FilterScan(Relation& r,std::vector<FilterInfo> filters) : Scan(r,filters[0].filterColumn.binding), filters(filters)  {
    if (filters.size() == 1) {
      this->eCost = filters[0].eCost;
    } else {
      double estimate = 1.0;
      for (size_t i = 1; i < filters.size(); i++) {
        auto &filter = filters[i];
        estimate *= static_cast<double>(filter.eCost) / static_cast<double>(filter.rowCount);
      }
      this->eCost = static_cast<uint64_t>(filters[0].eCost * estimate);
    }
  };
  /// The constructor
  FilterScan(Relation& r,FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {};
  /// Require a column and add it to results
  bool require(SelectInfo &info) override;
  /// Run
  void run() override;

  std::string getname() override { return "FilterScan"; };
};
//---------------------------------------------------------------------------
class Join : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// tmpResults[partition][probingTaskIndex][col][val]
  std::vector<std::vector<std::vector<std::vector<uint64_t>>>> tmpResults;
  std::vector<uint64_t> finalResults;
  /// Create mapping for bindings
  void createMappingForBindings();

  std::array<std::vector<uint64_t>, 2> partitionTable;
  const uint64_t partitionSize = L2_CACHE_SIZE / 8;
  uint64_t partitionCnt;

  std::vector<unsigned> probingCnt; // determined in histogramTask
  std::vector<uint64_t> probingLength;
  std::vector<unsigned> ProbingRemain;
  std::vector<unsigned> resultIndex;

  unsigned taskCnt[2];
  uint64_t taskLength[2];
  uint64_t taskRemain[2];
  std::vector<std::vector<uint64_t>> histograms[2];
  std::vector<std::vector<uint64_t*>> partition[2];
  std::vector<uint64_t> partitionLength[2];
  unsigned colId[2];

  using HT=std::unordered_multimap<uint64_t,uint64_t>;

  std::vector<HT*> hashTableIndices;  // for using thread local storage
  std::vector<std::unordered_map<uint64_t, uint64_t>*> hashTableCnts; // for using thread local storage
  bool cntBuilding = false;

#if USE_PARALLEL_BUILD_HASH_TABLE
  /// The hash table for the join
  std::vector<HT> hashTables;
#else
  /// The hash table for the join
  HT hashTable;
#endif
  /// Columns that have to be materialized
  std::unordered_set<SelectInfo> requestedColumns;
  /// Left/right columns that have been requested
  std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


  /// The entire input data of left and right
  std::vector<Column<uint64_t>> leftInputData,rightInputData;
  /// The input data that has to be copied
  // std::vector<Column<uint64_t>>copyLeftData,copyRightData;

  public:
  /// The constructor
  Join(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo &info) override;
  /// Run
  void run() override;
  /// Partition Left
  void partitionLeft();
  /// Partition Right
  void partitionRight();
  /// Build and probe for bucket
  void buildAndProbe(unsigned bucket);

  std::string getname() override { return "Join"; };
};
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> input;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// tmpResults[partition][col][val]
  std::vector<std::vector<std::vector<uint64_t>>> tmpResults;
  /// Copy tuple to result
  void copy2Result(uint64_t id);
  void copy2ResultToSum(std::vector<uint64_t> &sum, uint64_t id);
  /// The required IUs
  std::set<SelectInfo> requiredIUs;

  /// The input data that has to be copied
  std::vector<Column<uint64_t>*>copyData;

  public:
  /// The constructor
  SelfJoin(std::unique_ptr<Operator>&& input,PredicateInfo& pInfo) : input(std::move(input)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo& info) override;
  /// Run
  void run() override;

  std::string getname() override { return "SelfJoin"; };
};
//---------------------------------------------------------------------------
class Checksum : public Operator {
  /// The input operator
  std::unique_ptr<Operator> input;
  /// The join predicate info
  std::vector<SelectInfo>& colInfo;

  public:
  std::vector<uint64_t> checkSums;
  /// The constructor
  Checksum(std::unique_ptr<Operator>&& input,std::vector<SelectInfo>& colInfo) : input(std::move(input)), colInfo(colInfo) {};
  /// Request a column and add it to results
  bool require(SelectInfo &info) override { throw; /* check sum is always on the highest level and thus should never request anything */ }
  /// Run
  void run() override;

  std::string getname() override { return "Checksum"; };
};
//---------------------------------------------------------------------------
