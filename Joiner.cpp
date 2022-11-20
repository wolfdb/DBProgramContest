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

std::vector<Relation> Joiner::relations;
void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
  relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
void Joiner::buildHistogram()
{
#if PRINT_LOG
  log_print("max concurrency: {}\n", std::thread::hardware_concurrency());
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
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
#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("build histograms run time: {} ms\n\n", end.count() - start.count());
#endif
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
void Joiner::estimateCost(QueryInfo& query)
  // estimate the cost, and update relationCosts
{
  auto &rCosts = query.relationCosts;
  for (auto &ele : query.filterMap) {
    auto binding = ele.first.binding;
    auto colId = ele.first.colId;
    auto relId = query.relationIds[binding];
    auto &rel = getRelation(relId);
    auto &filters = ele.second;
    uint64_t tmax = rel.sample_maxs[colId];
    uint64_t tmin = rel.sample_mins[colId];
    assert(filters.size() < 3);
    uint64_t eCost = 0;
    if (filters.size() == 1) {
      auto &filter = filters[0];
      switch (filter.comparison)
      {
        case FilterInfo::Comparison::Equal: {
          eCost = 4;
          break;
        }
        case FilterInfo::Comparison::Less: {
          eCost = (filter.constant - tmin) * rel.rowCount / (tmax - tmin);
          break;
        }
        case FilterInfo::Comparison::Greater: {
          eCost = (tmax - filter.constant) * rel.rowCount / (tmax - tmin);
          break;
        }
      }
    } else {
      auto &fil0 = filters[0];
      auto &fil1 = filters[1];
      auto left = std::min(fil0.constant, fil1.constant);
      auto right = std::max(fil0.constant, fil1.constant);
      left = std::max(tmin, left);
      right = std::min(tmax, right);
      eCost = (right - left) * rel.rowCount / (tmax - tmin);
    }
    // cerr << "rel " << relId << ", binding " << binding << ", colId " << colId << ": eCost " << eCost << "/" << rel.rowCount << endl;
    if (rCosts.find(binding) != rCosts.end()) {
      rCosts[binding] = std::min(rCosts[binding], eCost);
    } else {
      rCosts[binding] = std::min(eCost, rel.rowCount);
    }
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
  // First calculate estimate row count for the Filter
//   for (auto &filter: query.filters) {
//     auto relId = filter.filterColumn.relId;
//     getRelation(relId).calThenSetEstimateCost(filter);
// #if PRINT_LOG
//     log_print("filter:{}, eCost:{}, rowCount:{}\n",
//       filter.comparison == FilterInfo::Comparison::Equal ? "=" : filter.comparison == FilterInfo::Comparison::Greater ? ">" : "<",
//       filter.eCost, filter.rowCount);
// #endif
//   }

  estimateCost(query);

  // Second for every Predicate(i.e. Join), add the operator to a PQ
  auto cmp = [](PredicateInfo &left, PredicateInfo &right) { return left.eCost < right.eCost; };
  for (auto &pInfo : query.predicates) {
    auto leftRelId = query.relationIds[pInfo.left.binding];
    auto leftCost = getRelation(leftRelId).rowCount;
    auto rightRelId = query.relationIds[pInfo.right.binding];
    auto rightCost = getRelation(rightRelId).rowCount;
    if (pInfo.left.binding == pInfo.right.binding) {
      if (query.relationCosts.find(pInfo.left.binding) != query.relationCosts.end()) {
        leftCost = query.relationCosts.at(pInfo.left.binding);
      }
      pInfo.eCost = static_cast<uint64_t>(::sqrt(leftCost));
    } else {
      if (query.relationCosts.find(pInfo.left.binding) != query.relationCosts.end()) {
        leftCost = query.relationCosts.at(pInfo.left.binding);
      }
      if (query.relationCosts.find(pInfo.right.binding) != query.relationCosts.end()) {
        rightCost = query.relationCosts.at(pInfo.right.binding);
      }
      pInfo.eCost = std::min(leftCost, rightCost);
    }
  }
  // sort the predicates
  std::sort(query.predicates.begin(), query.predicates.end(), cmp);
#if PRINT_LOG
  for (auto &predicate : query.predicates) {
    log_print("predicate {}, eCost:{}\n", predicate.dumpText(), predicate.eCost);
  }
#endif


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
