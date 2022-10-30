#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include <map>
#include "Relation.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
enum class OperatorType { Scan, FilterScan, Join, SelfJoin, Checksum};
class Operator {
  /// Operators materialize their entire result

  protected:
  /// map from binding to relation
  std::map<unsigned, Relation*> binding2Relations;
  /// map from binding to linenos
  std::vector<unsigned> bindingOfIntermediateResults;
  /// The intermediate results, each bind has one
  /// We assume there will be no more that 2^32 lines of data for each table
  std::vector<std::vector<uint32_t>> intermediateResults;

  public:
  /// Resolves a column
  virtual unsigned resolve(const SelectInfo &info) {
    for (auto i = 0; i < bindingOfIntermediateResults.size(); i++) {
      if (info.binding == bindingOfIntermediateResults[i]) {
        return i;
      }
    }
    assert(false);  // must find the binding
    return -1;
  }
  /// Run
  virtual void run() = 0;
  /// Get Relation
  virtual Relation* getRelation(unsigned binding) {
    assert(binding2Relations.find(binding) != binding2Relations.end());
    return binding2Relations[binding];
  }

  /// Get the bindings
  virtual std::vector<unsigned>& getBindings() {
    return bindingOfIntermediateResults;
  }
  /// Get intermediate results
  std::vector<std::vector<uint32_t>>& getResults() {
    return intermediateResults;
  }

  /// The result size
  uint64_t resultSize=0;
  /// estimate row count
  uint64_t eCost;

  /// Is the result from this operator are a range
  virtual bool isRangeResult() = 0;

  /// The destructor
  virtual ~Operator() {};
};
//---------------------------------------------------------------------------
class Scan : public Operator {
  protected:
  /// The relation
  Relation& relation;
  /// The name of the relation in the query
  unsigned relationBinding;

  public:
  /// The constructor
  Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {
    this->eCost = r.rowCount;
    this->intermediateResults.reserve(1);
    this->bindingOfIntermediateResults.push_back(relationBinding);
    binding2Relations[relationBinding] = &relation;
  };
  /// Run
  void run() override;

  /// Is the result from this operator are a range
  bool isRangeResult() override { return true; };
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
  /// The filter info
  std::vector<FilterInfo> filters;
  /// Is the index a range
  bool isRange;
  /// Apply filter
  bool applyFilter(uint64_t id, const FilterInfo& f);

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
  FilterScan(Relation& r, FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {};
  /// Run
  void run() override;

  bool isRangeResult() override { return isRange; }
};
//---------------------------------------------------------------------------
class Join : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2ResultLR(uint32_t leftId, uint32_t rightId);
  void copy2ResultL(uint32_t leftId, uint32_t rightId);
  void copy2ResultR(uint32_t leftId, uint32_t rightId);
  void copy2Result(uint32_t leftId, uint32_t rightId);
  /// Create mapping for bindings
  void createMappingForBindings();

  // value to line no multimap
  using HT=std::unordered_multimap<uint64_t,uint32_t>;

  /// The hash table for the join
  HT hashTable;

  public:
  /// The constructor
  Join(std::unique_ptr<Operator>&& left, std::unique_ptr<Operator>&& right,
          PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
  /// Run
  void run() override;

  bool isRangeResult() override { return false; }
};
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> input;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint32_t id);

  public:
  /// The constructor
  SelfJoin(std::unique_ptr<Operator>&& input,PredicateInfo& pInfo) : input(std::move(input)), pInfo(pInfo) {};
  /// Resolves a column
  unsigned resolve(const SelectInfo &info) override {
    return input->resolve(info);
  }
  /// Get Relation
  Relation* getRelation(unsigned binding) override {
    return input->getRelation(binding);
  }
  /// Get the bindings
  std::vector<unsigned>& getBindings() override {
    return input->getBindings();
  }
  /// Run
  void run() override;

  bool isRangeResult() override { return false; }
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
  /// Run
  void run() override;

  bool isRangeResult() override { return false; }
};
//---------------------------------------------------------------------------
