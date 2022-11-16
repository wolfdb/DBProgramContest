#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include <chrono>
#include <thread>
#include <cmath>
#include <algorithm>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include "Parser.hpp"
#include "Log.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace std::chrono;
//---------------------------------------------------------------------------
int Joiner::query_count = 0;
int Joiner::batch = 0;

void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
  relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
void Joiner::buildHistogram()
{
  // milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  boost::asio::thread_pool pool(std::thread::hardware_concurrency());
  for (auto &relation : relations) {
    for (int i = 0; i < relation.columns.size(); i++) {
      // for each column, build histogram
      boost::asio::post(pool, [&relation, i]() {
        relation.buildHistogram(i);
      });
    }
  }
  pool.join();
  // milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  // cerr << "build histograms run time: " << end.count() - start.count() << " ms\n";
  // int tmpcnt = 0;
  // for (auto &relation : relations) {
  //   if (tmpcnt < 15) {
  //   cerr << "relation: " << tmpcnt << "; sample cnt: " << relation.sampleCount << endl;
  //   for (int i = 0; i < relation.columns.size(); i++) {
  //       relation.printHistogram(i);
  //   }
  //   }
  //   tmpcnt ++;
  // }
}
//---------------------------------------------------------------------------
void Relation::printHistogram(int idx)
  // print histogram of column idx
{
  cerr << "column " << idx << ", max: " << sample_maxs[idx] << ", min: " << sample_mins[idx] << ", ndv: " << sample_distinctVals[idx].size() << endl;
}
//---------------------------------------------------------------------------
Relation& Joiner::getRelation(unsigned relationId)
// Loads a relation from disk
{
  if (relationId >= relations.size()) {
    cerr << "Relation with id: " << relationId << " does not exist" << endl;
    throw;
  }
  return relations[relationId];
}
//---------------------------------------------------------------------------
unique_ptr<Operator> Joiner::addScan(set<unsigned>& usedRelations,SelectInfo& info,QueryInfo& query)
  // Add scan to query
{
  usedRelations.emplace(info.binding);
  vector<FilterInfo> filters;
    for (auto& f : query.filters) {
      if (f.filterColumn.binding==info.binding) {
        filters.emplace_back(f);
      }
    }
  return filters.size()?make_unique<FilterScan>(getRelation(info.relId),filters):make_unique<Scan>(getRelation(info.relId),info.binding);
}
//---------------------------------------------------------------------------
uint64_t Joiner::estimateCost(const SelectInfo &info, const QueryInfo& query)
  // estimate the cost of SelectInfo
{
  auto relId = query.relationIds[info.binding];
  auto count = getRelation(relId).rowCount;
  auto scount = getRelation(relId).sampleCount;
  auto ndv = getRelation(relId).sample_distinctVals[info.colId].size();
  // auto &hist = getRelation(relId).sample_histograms[info.colId];
  auto smin = getRelation(relId).sample_mins[info.colId];
  auto smax = getRelation(relId).sample_maxs[info.colId];
  double sample_factor = static_cast<double>(count) / static_cast<double>(scount);
  if (query.filterMap.find(info) != query.filterMap.end()) {
    auto &filters = query.filterMap.at(info);
    assert(filters.size() <= 2);
    if (filters.size() == 1) {
      auto constant = filters[0].constant;
      switch (filters[0].comparison)
      {
      case FilterInfo::Comparison::Less: {
        if (constant < smin) {
          return 0;
        } else if (constant > smax) {
          return count;
        } else {
          return static_cast<uint64_t>(((constant - smin) / (double)(smax - smin)) * count);
        }
      }
      case FilterInfo::Comparison::Greater: {
        if (constant > smax) {
          return 0;
        } else if (constant < smin) {
          return count;
        } else {
          return static_cast<uint64_t>(((smax - constant) / (double)(smax - smin)) * count);
        }
      }
      case FilterInfo::Comparison::Equal: {
        if (constant > smax || constant < smin) {
          return 0;
        } else if (scount == ndv) {
          return 1;
        } else {
          return scount / ndv;
        }
      }
      default:
        break;
      }
    } else {
      auto constant1 = filters[0].constant;
      auto constant2 = filters[1].constant;
      auto left = std::min(constant1, constant2);
      auto right = std::max(constant1, constant2);
      if (left > smax) {
        return 0;
      }
      if (right < smin) {
        return 0;
      }
      left = std::max(left, smin);
      right = std::min(right, smax);
      return static_cast<uint64_t>(((right - left) / (double)(smax - smin)) * count);
    }
  } else {
    return count;
  }
}
//---------------------------------------------------------------------------
enum QueryGraphProvides {  Left, Right, Both, None };
//---------------------------------------------------------------------------
static QueryGraphProvides analyzeInputOfJoin(set<unsigned>& usedRelations,SelectInfo& leftInfo,SelectInfo& rightInfo)
  // Analyzes inputs of join
{
  bool usedLeft=usedRelations.count(leftInfo.binding);
  bool usedRight=usedRelations.count(rightInfo.binding);

  if (usedLeft^usedRight)
    return usedLeft?QueryGraphProvides::Left:QueryGraphProvides::Right;
  if (usedLeft&&usedRight)
    return QueryGraphProvides::Both;
  return QueryGraphProvides::None;
}
//---------------------------------------------------------------------------
unique_ptr<Operator> Joiner::buildPlanTree(QueryInfo& query)
  // the orignal left-deep join tree
{
  set<unsigned> usedRelations;
  
  // We always start with the first join predicate and append the other joins to it (--> left-deep join trees)
  // You might want to choose a smarter join ordering ...
  auto& firstJoin=query.predicates[0];
  auto left=addScan(usedRelations,firstJoin.left,query);
  auto right=addScan(usedRelations,firstJoin.right,query);
  unique_ptr<Operator> root=make_unique<Join>(move(left),move(right),firstJoin);

  for (unsigned i=1;i<query.predicates.size();++i) {
    auto& pInfo=query.predicates[i];
    auto& leftInfo=pInfo.left;
    auto& rightInfo=pInfo.right;
    unique_ptr<Operator> left, right;
    switch(analyzeInputOfJoin(usedRelations,leftInfo,rightInfo)) {
      case QueryGraphProvides::Left:
        left=move(root);
        right=addScan(usedRelations,rightInfo,query);
        root=make_unique<Join>(move(left),move(right),pInfo);
        break;
      case QueryGraphProvides::Right:
        left=addScan(usedRelations,leftInfo,query);
        right=move(root);
        root=make_unique<Join>(move(left),move(right),pInfo);
        break;
      case QueryGraphProvides::Both:
        // All relations of this join are already used somewhere else in the query.
        // Thus, we have either a cycle in our join graph or more than one join predicate per join.
        root=make_unique<SelfJoin>(move(root),pInfo);
        break;
      case QueryGraphProvides::None:
        // Process this predicate later when we can connect it to the other joins
        // We never have cross products
        query.predicates.push_back(pInfo);
        break;
    };
  }

  return root;
}
//---------------------------------------------------------------------------
unique_ptr<Operator> Joiner::buildMyPlanTree(QueryInfo& query)
  // my left-deep join tree
{
  // Second for every Predicate(i.e. Join), add the operator to a PQ
  auto cmp = [](PredicateInfo &left, PredicateInfo &right) { return left.eCost < right.eCost; };
  for (auto &pInfo : query.predicates) {
    auto leftCost = estimateCost(pInfo.left, query);
    auto rightCost = estimateCost(pInfo.right, query);
    if (pInfo.left.binding == pInfo.right.binding) {
      pInfo.eCost = static_cast<uint64_t>(::sqrt(leftCost + rightCost));
    } else {
      pInfo.eCost = leftCost + rightCost;
    }
  }
  // sort the predicates
  std::sort(query.predicates.begin(), query.predicates.end(), cmp);
  for (auto &predicate : query.predicates) {
#if PRINT_LOG
    log_print("predicate {}, eCost:{}\n", predicate.dumpText(), predicate.eCost);
#endif
  }

  // following is copied from buildPlanTree
  set<unsigned> usedRelations;

  auto& firstJoin = query.predicates[0];

  unique_ptr<Operator> root = nullptr;
  if (firstJoin.left.binding == firstJoin.right.binding) {
    auto left = addScan(usedRelations, firstJoin.left, query);
    root = make_unique<SelfJoin>(move(left), firstJoin);
  } else {
    auto left = addScan(usedRelations, firstJoin.left, query);
    auto right = addScan(usedRelations, firstJoin.right, query);
    root = make_unique<Join>(move(left), move(right), firstJoin);
  }

  std::vector<bool> predicateUsed(query.predicates.size(), false);
  auto cnt = query.predicates.size() - 1;

  while (cnt > 0) {
    for (unsigned i = 1; i < query.predicates.size(); ++i) {
      if (predicateUsed[i]) {
        continue;
      }
      auto& pInfo = query.predicates[i];
      auto& leftInfo = pInfo.left;
      auto& rightInfo = pInfo.right;
      unique_ptr<Operator> left, right;
      switch (analyzeInputOfJoin(usedRelations, leftInfo, rightInfo)) {
        case QueryGraphProvides::Left:
          left = move(root);
          right = addScan(usedRelations, rightInfo, query);
          root = make_unique<Join>(move(left), move(right), pInfo);
          predicateUsed[i] = true;
          cnt --;
          break;
        case QueryGraphProvides::Right:
          left = addScan(usedRelations, leftInfo, query);
          right = move(root);
          root = make_unique<Join>(move(left), move(right), pInfo);
          predicateUsed[i] = true;
          cnt --;
          break;
        case QueryGraphProvides::Both:
          // All relations of this join are already used somewhere else in the query.
          // Thus, we have either a cycle in our join graph or more than one join predicate per join.
          root = make_unique<SelfJoin>(move(root),pInfo);
          predicateUsed[i] = true;
          cnt --;
          break;
        case QueryGraphProvides::None:
          // Process this predicate later when we can connect it to the other joins
          // We never have cross products
          break;
      }
    }
  }

  return root;
}
//---------------------------------------------------------------------------
string Joiner::join(QueryInfo& query)
  // Executes a join query
{
#if PRINT_LOG
  log_print("query {}: {}\n", Joiner::query_count, query.dumpSQL());
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
  Joiner::query_count ++;
  if (query.impossible) {
    stringstream ss;
    for (unsigned i = 0; i < query.selections.size(); i++) {
      ss << "NULL";
      if (i < query.selections.size() - 1) {
        ss << " ";
      }
    }
    ss << "\n";
    return ss.str();
  }

  // The original left-deep tree
  // unique_ptr<Operator> root = buildPlanTree(query);
  // My left-deep tree, seems not necessary to build a bushy tree?
  unique_ptr<Operator> root = buildMyPlanTree(query);
  
  root->setParentSum();
  Checksum checkSum(move(root),query.selections);
  checkSum.run();

#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("query run time: {} ms\n\n", end.count() - start.count());
#endif

  stringstream ss;
  auto& results=checkSum.checkSums;
  for (unsigned i=0;i<results.size();++i) {
    ss << (results[i]==0?"NULL":to_string(results[i]));
    if (i<results.size()-1)
      ss << " ";
  }
  ss << "\n";
  return ss.str();
}
//---------------------------------------------------------------------------
