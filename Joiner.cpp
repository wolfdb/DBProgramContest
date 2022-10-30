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

void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
  relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
void Joiner::buildHistogram()
{
  int tmpcnt = 0;
  log_print("max concurrency: {}\n", std::thread::hardware_concurrency());
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
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
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("build histograms run time: {} ms\n\n", end.count() - start.count());
  for (auto &relation : relations) {
    for (int i = 0; i < relation.columns.size(); i++) {
      log_print("build histgram for relation: {}, column {}\n", tmpcnt, i);
      relation.printHistogram(i);
    }
    tmpcnt ++;
  }
}
//---------------------------------------------------------------------------
void Joiner::buildIndex(const std::vector<QueryInfo> &qq)
  // concurrently build indexs for Equal filter
{
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  boost::asio::thread_pool pool(std::thread::hardware_concurrency());
  for (auto &qi: qq) {
    for (auto &filter: qi.filters) {
      if (filter.comparison == FilterInfo::Comparison::Equal) {
        auto &filterColumn = filter.filterColumn;
        auto &relation = getRelation(filterColumn.relId);
        if (relation.sorts[filterColumn.colId] == Relation::Sorted::Likely ||   // do not build hash map for sorted column
            relation.hasHmapBuilt[filterColumn.colId] == true) {
          continue;
        }
        // call reserve to avoid rehashing
        relation.columnHmap[filterColumn.colId].reserve(relation.rowCount);
        uint64_t step = std::max(MIN_SCAN_ITEM_COUNT, relation.rowCount / std::thread::hardware_concurrency());
        uint64_t start = 0;
        for (; start + step < relation.rowCount; start += step) {
          boost::asio::post(pool, [&relation, colId = filterColumn.colId, start, end = start + step]() {
            relation.buildConcurrentHashMap(colId, static_cast<uint32_t>(start), static_cast<uint32_t>(end));
          });
          log_print("build map for relation {}, column {}, range [{}, {})\n", filterColumn.relId, filterColumn.colId, start, start + step);
        }
        boost::asio::post(pool, [&relation, colId = filterColumn.colId, start, end = relation.rowCount]() {
          relation.buildConcurrentHashMap(colId, static_cast<uint32_t>(start), static_cast<uint32_t>(end));
        });
        log_print("build map for relation {}, column {}, range [{}, {})\n", filterColumn.relId, filterColumn.colId, start, relation.rowCount);
        relation.hasHmapBuilt[filterColumn.colId] = true;
        // log_print("hash bucket count: {} load factor {}\n", relation.columnHmap[filterColumn.colId].unsafe_bucket_count(), relation.columnHmap[filterColumn.colId].load_factor());
      }
    }
  }
  pool.join();
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("build index run time: {} ms\n\n", end.count() - start.count());
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
  uint64_t cost = getRelation(relId).rowCount;
  for (auto& f : query.filters) {
    if (f.filterColumn.binding==info.binding) {
      cost = std::min(cost, f.eCost);
    }
  }
  return cost;
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
  for (auto &filter: query.filters) {
    auto relId = filter.filterColumn.relId;
    getRelation(relId).calThenSetEstimateCost(filter);
    log_print("filter:{}, eCost:{}, rowCount:{}, sorted:{}\n",
      filter.comparison == FilterInfo::Comparison::Equal ? "=" : filter.comparison == FilterInfo::Comparison::Greater ? ">" : "<",
      filter.eCost, filter.rowCount, filter.sorted);
  }

  // Second for every Predicate(i.e. Join), add the operator to a PQ
  auto cmp = [](PredicateInfo &left, PredicateInfo &right) { return left.eCost < right.eCost; };
  for (auto &pInfo : query.predicates) {
    if (pInfo.left.binding == pInfo.right.binding) {
      pInfo.eCost = estimateCost(pInfo.left, query);
      pInfo.eCost = static_cast<uint64_t>(::sqrt(pInfo.eCost));
    } else {
      pInfo.eCost = std::min(estimateCost(pInfo.left, query), estimateCost(pInfo.right, query));
    }
  }

  // sort the predicates
  std::sort(query.predicates.begin(), query.predicates.end(), cmp);
  for (auto &predicate : query.predicates) {
    log_print("predicate {}, eCost:{}\n", predicate.dumpText(), predicate.eCost);
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
  log_print("query {}: {}\n", Joiner::query_count++, query.dumpSQL());
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());

  // The original left-deep tree
  // unique_ptr<Operator> root = buildPlanTree(query);
  // My left-deep tree, seems not necessary to build a bushy tree?
  unique_ptr<Operator> root = buildMyPlanTree(query);
  
  Checksum checkSum(move(root),query.selections);
  checkSum.run();

  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("query run time: {} ms\n\n", end.count() - start.count());

  stringstream out;
  auto& results=checkSum.checkSums;
  for (unsigned i=0;i<results.size();++i) {
    out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
    if (i<results.size()-1)
      out << " ";
  }
  out << "\n";
  return out.str();
}
//---------------------------------------------------------------------------
