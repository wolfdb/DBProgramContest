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
#include <queue>
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
  out.print("max concurrency: {}\n", std::thread::hardware_concurrency());
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
  out.print("build histograms run time: {} ms\n\n", end.count() - start.count());
  for (auto &relation : relations) {
    for (int i = 0; i < relation.columns.size(); i++) {
      out.print("build histgram for relation: {}, column {}\n", tmpcnt, i);
      relation.printHistogram(i);
    }
    tmpcnt ++;
  }
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
    auto relId = query.relationIds[filter.filterColumn.binding];
    getRelation(relId).calThenSetEstimateCost(filter);
    out.print("filter:{}, eCost:{}, rowCount:{}, sorted:{}\n",
      filter.comparison == FilterInfo::Comparison::Equal ? "=" : filter.comparison == FilterInfo::Comparison::Greater ? ">" : "<",
      filter.eCost, filter.rowCount, filter.sorted);
  }

  // Second for every Predicate(i.e. Join), add the operator to a PQ
  auto cmp = [](PredicateInfo &left, PredicateInfo &right) { return left.eCost > right.eCost; };
  std::priority_queue<PredicateInfo, std::vector<PredicateInfo>, decltype(cmp)> pq(cmp);
  for (auto &pInfo : query.predicates) {
    pInfo.eCost = std::min(estimateCost(pInfo.left, query), estimateCost(pInfo.right, query));
    pq.push(pInfo);
  }

  // construct a queue from priority queue
  std::vector<PredicateInfo> predicates;
  while (!pq.empty()) {
    auto &predicate = pq.top();
    out.print("eCost:{}\n", predicate.eCost);
    predicates.emplace_back(std::move(predicate));
    pq.pop();
  }

  // swap to avoid Use-After-Free exception
  std::swap(predicates, query.predicates);

  // following is copied from buildPlanTree
  set<unsigned> usedRelations;

  auto& firstJoin = query.predicates[0];
  auto left = addScan(usedRelations, firstJoin.left, query);
  auto right = addScan(usedRelations, firstJoin.right, query);
  unique_ptr<Operator> root=make_unique<Join>(move(left), move(right), firstJoin);

  for (unsigned i = 1; i < query.predicates.size(); ++i) {
    auto& pInfo = query.predicates[i];
    auto& leftInfo = pInfo.left;
    auto& rightInfo = pInfo.right;
    unique_ptr<Operator> left, right;
    switch (analyzeInputOfJoin(usedRelations, leftInfo, rightInfo)) {
      case QueryGraphProvides::Left:
        left = move(root);
        right = addScan(usedRelations, rightInfo, query);
        root = make_unique<Join>(move(left), move(right), pInfo);
        break;
      case QueryGraphProvides::Right:
        left = addScan(usedRelations, leftInfo, query);
        right = move(root);
        root = make_unique<Join>(move(left), move(right), pInfo);
        break;
      case QueryGraphProvides::Both:
        // All relations of this join are already used somewhere else in the query.
        // Thus, we have either a cycle in our join graph or more than one join predicate per join.
        root = make_unique<SelfJoin>(move(root),pInfo);
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
string Joiner::join(QueryInfo& query)
  // Executes a join query
{
  out.print("query {}: {}\n", Joiner::query_count++, query.dumpSQL());
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());

  // The original left-deep tree
  // unique_ptr<Operator> root = buildPlanTree(query);
  // My left-deep tree, seems not necessary to build a bushy tree?
  unique_ptr<Operator> root = buildMyPlanTree(query);
  
  Checksum checkSum(move(root),query.selections);
  checkSum.run();

  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  out.print("query run time: {} ms\n\n", end.count() - start.count());

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
