#pragma once
#include <vector>
#include <cstdint>
#include <set>
#include "Operators.hpp"
#include "Relation.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
class Joiner {

  /// Add scan to query
  std::unique_ptr<Operator> addScan(std::set<unsigned>& usedRelations,SelectInfo& info,QueryInfo& query);

  public:
  /// The relations that might be joined
  std::vector<Relation> relations;
  /// Add relation
  void addRelation(const char* fileName);
  /// Get relation
  Relation& getRelation(unsigned id);
  /// Joins a given set of relations
  std::string join(QueryInfo& i);
  /// Build histograms and indexs
  void buildHistogram();

  /// estimate the cost of SelectInfo
  uint64_t estimateCost(const SelectInfo &info, const QueryInfo& query);

  /// the orignal left-deep join tree
  std::unique_ptr<Operator> buildPlanTree(QueryInfo& query);

  /// my left-deep join tree
  std::unique_ptr<Operator> buildMyPlanTree(QueryInfo& query);
  static int query_count;
};
//---------------------------------------------------------------------------
