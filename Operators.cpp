#include <cassert>
#include <iostream>
#include <queue>
#include <algorithm>
#include <future>
// #include <boost/asio/thread_pool.hpp>
// #include <boost/asio/post.hpp>
#include "Operators.hpp"
#include "Consts.hpp"
#if PRINT_LOG
#include "Log.hpp"
#endif
//---------------------------------------------------------------------------
using namespace std;
using namespace std::chrono;
//---------------------------------------------------------------------------
void Scan::run()
  // Run
{
#if PRINT_LOG
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
  resultSize=relation.rowCount;
  intermediateResults.push_back({0, static_cast<uint32_t>(relation.rowCount)});
#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("scan resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i, const FilterInfo& f)
  // Apply filter
{
  auto compareCol=relation.columns[f.filterColumn.colId];
  auto constant=f.constant;
  switch (f.comparison) {
    case FilterInfo::Comparison::Equal:
      return compareCol[i]==constant;
    case FilterInfo::Comparison::Greater:
      return compareCol[i]>constant;
    case FilterInfo::Comparison::Less:
      return compareCol[i]<constant;
  };
  return false;
}
//---------------------------------------------------------------------------
void FilterScan::run()
  // Run
{
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  // do the apply for each filter
  std::sort(filters.begin(), filters.end(), [](const FilterInfo &left, const FilterInfo &right) { return left.eCost < right.eCost; });
  vector<uint32_t> index{0, static_cast<uint32_t>(relation.rowCount)};
  vector<uint32_t> tmp;
  bool isRange = true;
  bool nomatch = false;
  for (auto &f : filters) {
    if (nomatch) {
      break;
    }
    auto &fc = f.filterColumn;
    uint64_t *column = relation.columns[fc.colId];
    if (isRange && relation.sorts[fc.colId] == Relation::Sorted::Likely) {
      // use binary search for sorted column
      uint64_t *left = column + index[0];
      uint64_t *right = column + index[1];
      uint64_t distance = right - left;
      switch (f.comparison)
      {
      case FilterInfo::Comparison::Equal:
      {
        if (relation.orders[fc.colId] == Relation::Order::ASC) {
          auto lower = std::lower_bound(left, right, f.constant) - left;
          if (lower == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            auto index0 = index[0];
            index[0] = index0 + lower;
            auto upper = std::upper_bound(left, right, f.constant) - left;
            if (upper == distance) {
              index[1] = index[0] + 1;
            } else {
              index[1] = index0 + upper;
            }
          }
        } else {
          auto lower = std::lower_bound(left, right, f.constant, std::greater<uint64_t>()) -  left;
          if (lower == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            auto index0 = index[0];
            index[0] = index[0] + lower;
            auto upper = std::upper_bound(left, right, f.constant, std::greater<uint64_t>()) - left;
            if (upper == distance) {
              index[1] = index[0] + 1;
            } else {
              index[1] = index0 + upper;
            }
          }
        }
        break;
      }
      case FilterInfo::Comparison::Greater:
      {
        if (relation.orders[fc.colId] == Relation::Order::ASC) {
          auto upper = std::upper_bound(left, right, f.constant) - left;
          if (upper == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            index[0] = index[0] + upper;
          }
        } else {
          auto lower = std::lower_bound(left, right, f.constant, std::greater<uint64_t>()) - left;
          if (lower == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            index[1] = index[0] + lower;
          }
        }
        break;
      }
      case FilterInfo::Comparison::Less:
      {
        if (relation.orders[fc.colId] == Relation::Order::ASC) {
          auto lower = std::lower_bound(left, right, f.constant) - left;
          if (lower == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            index[1] = index[0] + lower;
          }
        } else {
          auto upper = std::upper_bound(left, right, f.constant, std::greater<uint64_t>()) - left;
          if (upper == distance) {
            nomatch = true;
            isRange = false;
            index.clear();
          } else {
            index[0] = index[0] + upper;
          }
        }
        break;
      }
      default:
        assert(false);
      }
    } else {
      switch (f.comparison)
      {
      case FilterInfo::Comparison::Equal:
      {
        if (isRange) {
          assert(index.size() == 2);
          tmp.reserve(index[1] - index[0]);
          // we can use simd intrinsic for the following range
          for (uint32_t i = index[0]; i < index[1]; i++) {
            if (column[i] == f.constant) {
              tmp.push_back(i);
            }
          }
          isRange = false;
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        } else {
          tmp.reserve(index.size());
          // can not use simd here
          for (auto ii : index) {
            if (column[ii] == f.constant) {
              tmp.push_back(ii);
            }
          }
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        }
        break;
      }
      case FilterInfo::Comparison::Greater:
      {
        if (isRange) {
          assert(index.size() == 2);
          tmp.reserve(index[1] - index[0]);
          // we can use simd intrinsic for the following range
          for (uint32_t i = index[0]; i < index[1]; i++) {
            if (column[i] > f.constant) {
              tmp.push_back(i);
            }
          }
          isRange = false;
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        } else {
          tmp.reserve(index.size());
          // can not use simd here
          for (auto ii : index) {
            if (column[ii] > f.constant) {
              tmp.push_back(ii);
            }
          }
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        }
        break;
      }
      case FilterInfo::Comparison::Less:
      {
        if (isRange) {
          assert(index.size() == 2);
          tmp.reserve(index[1] - index[0]);
          // we can use simd intrinsic for the following range
          for (uint32_t i = index[0]; i < index[1]; i++) {
            if (column[i] < f.constant) {
              tmp.push_back(i);
            }
          }
          isRange = false;
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        } else {
          tmp.reserve(index.size());
          // can not use simd here
          for (auto ii : index) {
            if (column[ii] < f.constant) {
              tmp.push_back(ii);
            }
          }
          index.swap(tmp);
          tmp.clear();
          if (index.size() == 0) {
            nomatch = true;
          }
        }
        break;
      }
      default:
        assert(false);
      }
    }
  }

  this->isRange = isRange;
  // log_print("index : {}\n", fmt::join(index, ", "));
  // copy the data for test now
  // if (isRange) {
  //   for (uint64_t i = index[0]; i < index[1]; i++) {
  //     copy2Result(i);
  //   }
  // } else {
  //   for (auto i : index) {
  //     copy2Result(i);
  //   }
  // }
  this->resultSize = isRange ? index[1] - index[0] : index.size();
  this->intermediateResults.emplace_back(std::move(index));
#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("filter scan resultSize: {}, isRange :{}, run time {} ms\n", resultSize, isRange, end.count() - start.count());
#endif
  // vector<uint64_t> idxvec;
  // vector<uint64_t> tmp2;
  // idxvec.reserve(relation.rowCount);
  // tmp2.reserve(relation.rowCount);
  // bool first = true;
  // for (auto &f : filters) {
  //   if (first) {
  //     for (uint64_t i=0;i<relation.rowCount;++i) {
  //       if (applyFilter(i,f)) {
  //         idxvec.push_back(i);
  //       }
  //     }
  //     first = false;
  //   } else {
  //     for (auto i : idxvec) {
  //       if (applyFilter(i,f)) {
  //         tmp2.push_back(i);
  //       }
  //     }
  //     std::swap(idxvec, tmp2);
  //     tmp2.clear();
  //   }
  // }

  // log_print("index2 : {}\n", fmt::join(idxvec, ", "));

  // for (auto i : idxvec) {
  //   copy2Result(i);
  // }

  // copy the result
  // for (uint64_t i=0;i<relation.rowCount;++i) {
  //   bool pass=true;
  //   for (auto& f : filters) {
  //     pass&=applyFilter(i,f);
  //   }
  //   if (pass)
  //     copy2Result(i);
  // }
}
//---------------------------------------------------------------------------
void Join::copy2ResultLR(uint32_t leftId, uint32_t rightId)
{
  assert(intermediateResults.size() == 2);
  intermediateResults[0].push_back(leftId);
  intermediateResults[1].push_back(rightId);
  ++ resultSize;
}
void Join::copy2ResultLRP(std::vector<std::vector<uint32_t>> &result, uint32_t leftId, uint32_t rightId)
{
  assert(result.size() == 2);
  result[0].push_back(leftId);
  result[1].push_back(rightId);
}
void Join::copy2ResultL(uint32_t leftId, uint32_t rightId)
{
  auto &rightResults = right->getResults();
  intermediateResults[0].push_back(leftId);
  unsigned index = 1;
  for (unsigned cId = 0; cId < rightResults.size(); ++cId) {
    intermediateResults[index++].push_back(rightResults[cId][rightId]);
  }
  ++ resultSize;
}
void Join::copy2ResultLP(std::vector<std::vector<uint32_t>> &result, uint32_t leftId, uint32_t rightId)
{
  auto &rightResults = right->getResults();
  result[0].push_back(leftId);
  unsigned index = 1;
  for (unsigned cId = 0; cId < rightResults.size(); ++cId) {
    result[index++].push_back(rightResults[cId][rightId]);
  }
}
void Join::copy2ResultR(uint32_t leftId, uint32_t rightId)
{
  auto &leftResults = left->getResults();
  unsigned index = 0;
  for (unsigned cId = 0; cId < leftResults.size(); ++cId) {
    intermediateResults[index++].push_back(leftResults[cId][leftId]);
  }
  intermediateResults[index].push_back(rightId);
  ++ resultSize;
}
void Join::copy2ResultRP(std::vector<std::vector<uint32_t>> &result, uint32_t leftId, uint32_t rightId)
{
  auto &leftResults = left->getResults();
  unsigned index = 0;
  for (unsigned cId = 0; cId < leftResults.size(); ++cId) {
    result[index++].push_back(leftResults[cId][leftId]);
  }
  result[index].push_back(rightId);
}
void Join::copy2Result(uint32_t leftId, uint32_t rightId)
  // Copy to result
{
  auto &leftResults = left->getResults();
  auto &rightResults = right->getResults();
  unsigned index=0;

  for (unsigned cId = 0; cId < leftResults.size(); ++cId) {
    intermediateResults[index++].push_back(leftResults[cId][leftId]);
  }

  for (unsigned cId = 0; cId < rightResults.size(); ++cId) {
    intermediateResults[index++].push_back(rightResults[cId][rightId]);
  }
  ++resultSize;
}
void Join::copy2ResultP(std::vector<std::vector<uint32_t>> &result, uint32_t leftId, uint32_t rightId)
  // Copy to result
{
  auto &leftResults = left->getResults();
  auto &rightResults = right->getResults();
  unsigned index=0;

  for (unsigned cId = 0; cId < leftResults.size(); ++cId) {
    result[index++].push_back(leftResults[cId][leftId]);
  }

  for (unsigned cId = 0; cId < rightResults.size(); ++cId) {
    result[index++].push_back(rightResults[cId][rightId]);
  }
}
//---------------------------------------------------------------------------
void Join::run()
  // Run
{
#if USE_ASYNC_JOIN
  auto al = std::async(std::launch::async , [this]() { left->run(); });
  right->run();
  al.get();
#else
  left->run();
  right->run();
#endif
  // if (left->isFilterScan()) {
  //   std::thread tl([this]() {
  //     left->run();
  //   });
  //   right->run();
  //   if (right->resultSize == 0) {
  //     tl.detach();
  //   } else {
  //     tl.join();
  //   }
  // } else if (right->isFilterScan()) {
  //   std::thread tr([this]() {
  //     right->run();
  //   });
  //   left->run();
  //   if (left->resultSize == 0) {
  //     tr.detach();
  //   } else {
  //     tr.join();
  //   }
  // } else {
  //   left->run();
  //   right->run();
  // }

#if PRINT_LOG
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  milliseconds end, tmpms;
#endif
  // Use smaller input for build
  if (left->resultSize>right->resultSize) {
    swap(left,right);
    swap(pInfo.left,pInfo.right);
  }

  auto &leftResults = left->getResults();
  auto &rightResults = right->getResults();
  auto &leftBindings = left->getBindings();
  auto &rightBindings = right->getBindings();

  // Resolve the input column
  unsigned resColId = 0;
  intermediateResults.resize(leftResults.size() + rightResults.size());
  bindingOfIntermediateResults.resize(leftBindings.size() + rightBindings.size());
  assert(leftResults.size() + rightResults.size() == leftBindings.size() + rightBindings.size());
  for (auto binding : leftBindings) {
    bindingOfIntermediateResults[resColId++] = binding;
    assert(binding2Relations.find(binding) == binding2Relations.end());
    binding2Relations[binding] = left->getRelation(binding);
  }
  for (auto binding : rightBindings) {
    bindingOfIntermediateResults[resColId++] = binding;
    assert(binding2Relations.find(binding) == binding2Relations.end());
    binding2Relations[binding] = right->getRelation(binding);
  }

  if (left->resultSize == 0) {
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
    return;
  }

  auto leftColId = left->resolve(pInfo.left);
  auto leftBinding = leftBindings[leftColId];
  auto rightColId = right->resolve(pInfo.right);
  auto rightBinding = rightBindings[rightColId];
  uint64_t *leftColumn = left->getRelation(leftBinding)->columns[pInfo.left.colId];
  uint64_t *rightColumn = right->getRelation(rightBinding)->columns[pInfo.right.colId];
  bool leftSorted = left->getRelation(leftBinding)->sorts[pInfo.left.colId] == Relation::Sorted::Likely;
  bool rightSorted = right->getRelation(rightBinding)->sorts[pInfo.right.colId] == Relation::Sorted::Likely;

#if PRINT_LOG
  // log_print("left column addr {}, right column addr {}\n", fmt::ptr(leftColumn), fmt::ptr(rightColumn));
  log_print("leftBinding: {}, leftColId: {}, left sorted: {}  rightBinding: {}, rightColId: {}, right sorted: {}\n", leftBinding, pInfo.left.colId, leftSorted, rightBinding, pInfo.right.colId, rightSorted);
#endif

  // If left resultSize == 1, no not need to build hash table
  if (left->resultSize == 1) {
    if (left->isRangeResult()) {
      assert(leftResults.size() == 1);
      uint32_t index = leftResults[0][0];
      uint64_t value = leftColumn[index];
      if (right->isRangeResult()) {
        auto &rightResult = rightResults[0];
        for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
          if (value == rightColumn[i]) {
            copy2ResultLR(index, i);
          }
        }
      } else {
        auto rightResult = rightResults[rightColId];
#if PRINT_LOG
        log_print("right result size: {}\n", rightResult.size());
#endif
        for (uint32_t i = 0; i < rightResult.size(); i++) {
          if (value == rightColumn[rightResult[i]]) {
            copy2ResultL(index, i);
          }
        }
      }
    } else {
      auto leftResult = leftResults[leftColId];
      assert(leftResult.size() == 1);
      uint32_t index = leftResult[0];
      uint64_t value = leftColumn[index];
      if (right->isRangeResult()) {
        assert(rightResults.size() == 1);
        auto &rightResult = rightResults[0];
        for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
          if (value == rightColumn[i]) {
            copy2ResultR(0, i);
          }
        }
      } else {
        auto rightResult = rightResults[rightColId];
#if PRINT_LOG
        log_print("right result size: {}\n", rightResult.size());
#endif
        for (uint32_t i = 0; i < rightResult.size(); i++) {
          if (value == rightColumn[rightResult[i]]) {
            copy2Result(0, i);
          }
        }
      }
    }

#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("left resultSize == 1, join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
    return;
  }

#if USE_MERGE_JOIN
  // merge join
  if (left->isRangeResult() && right->isRangeResult() && leftSorted && rightSorted) {
    assert(leftResults.size() == 1);
    assert(rightResults.size() == 1);
    uint32_t leftStart = leftResults[0][0];
    uint32_t leftEnd = leftResults[0][1];
    uint32_t rightStart = rightResults[0][0];
    uint32_t rightEnd = rightResults[0][1];
    uint32_t bt_cnt = 0;  // backtrace cnt
    uint64_t pre = std::numeric_limits<uint64_t>::max();  // previous value

    // TODO: assume both are asc ordered now, add other logic later
    while (leftStart < leftEnd && rightStart < rightEnd) {
#if PRINT_LOG
      log_print("left pos: {}, value: {} right pos: {}, value: {}\n", leftStart, leftColumn[leftStart], rightStart, rightColumn[rightStart]);
#endif
      if (leftColumn[leftStart] == rightColumn[rightStart]) {
        copy2ResultLR(leftStart, rightStart);
        bt_cnt ++;
        rightStart ++;
      } else if (leftColumn[leftStart] < rightColumn[rightStart]) {
        pre = leftColumn[leftStart];
        leftStart ++;
        if (leftStart < leftEnd && leftColumn[leftStart] == pre) {
          // backtrace
          rightStart -= bt_cnt;
          bt_cnt = 0;
        }
      } else if (leftColumn[leftStart] > rightColumn[rightStart]) {
        rightStart ++;
      }
    }

#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("Left result size: {}, right result size: {}, merge join result size: {} time {} ms\n", leftEnd - leftResults[0][0], rightEnd - rightResults[0][0], resultSize, end.count() - start.count());
#endif
    return;
  }
#endif

  if (left->isRangeResult()) {
    // Build phase
#if PRINT_LOG
    tmpms = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
    assert(leftResults.size() == 1);
    auto &leftResult = leftResults[0];
    assert(left->resultSize == leftResult[1] - leftResult[0]);
#if USE_PARALLEL_BUILD_HASH_TABLE
    if (leftResult[1] - leftResult[0] > (MIN_BUILD_ITEM_CNT << 1)) {
      uint32_t build_cnt = (leftResult[1] - leftResult[0]) / MIN_BUILD_ITEM_CNT;
      build_cnt = std::min(build_cnt, MAX_BUILD_TASK_CNT);
      hashTables.resize(build_cnt);
      size_t bstep = (leftResult[1] - leftResult[0] + build_cnt - 1) / build_cnt;
      size_t bstart = leftResult[0] + bstep;
      int buildid = 0;
      std::vector<std::future<void>> bvf;
      while (bstart < leftResult[1]) {
        buildid ++;
        // log_print("split task {} for the probe, start: {}\n", taskid, start);
        if (bstart + bstep >= leftResult[1]) {
          bvf.push_back(std::async(std::launch::async , [this, &hashTable = hashTables[buildid], leftColumn, bstart, bend = leftResult[1]]() {
            hashTable.reserve((bend - bstart) * 2);
            for (uint32_t i = bstart; i < bend; i++) {
              hashTable.emplace(leftColumn[i], i);
            }
          }));
          break;
        }
        bvf.push_back(std::async(std::launch::async , [this, &hashTable = hashTables[buildid], leftColumn, bstart, bend = bstart + bstep]() {
          hashTable.reserve((bend - bstart) * 2);
          for (uint32_t i = bstart; i < bend; i++) {
            hashTable.emplace(leftColumn[i], i);
          }
        }));
      }
      hashTables[0].reserve(bstep * 2);
      for (uint32_t i = leftResult[0]; i < bstep; i++) {
        hashTables[0].emplace(leftColumn[i], i);
      }

      for_each(bvf.begin(), bvf.end(), [](future<void> &x) { x.wait(); });
    } else {
      hashTables.resize(1);
      hashTables[0].reserve(left->resultSize * 2);
      for (uint32_t i = leftResult[0]; i < leftResult[1]; i++) {
        // simd ?
        // if (leftBinding == 2) {
        //   log_print(" emplace {}, {}\n", leftColumn[i], i);
        // }
        // log_print(" emplace {}, {}\n", leftColumn[i], i);
        hashTables[0].emplace(leftColumn[i], i);
      }
    }
#else
    hashTable.reserve(left->resultSize * 2);
    // log_print("range [{}, {})\n", leftResult[0], leftResult[1]);
    for (uint32_t i = leftResult[0]; i < leftResult[1]; i++) {
      // simd ?
      // if (leftBinding == 2) {
      //   log_print(" emplace {}, {}\n", leftColumn[i], i);
      // }
      // log_print(" emplace {}, {}\n", leftColumn[i], i);
      hashTable.emplace(leftColumn[i], i);
    }
#endif
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("Left result size: {}, is range: true, build hashmap time {} ms\n", leftResult[1] - leftResult[0], end.count() - tmpms.count());
    tmpms = end;
#endif
    // Probe phase
    if (right->isRangeResult()) {
      assert(rightResults.size() == 1);
      auto &rightResult = rightResults[0];
#if PRINT_LOG
      log_print("right result size: {}, is range: true\n", rightResult[1] - rightResult[0]);
#endif
#if USE_PARALLEL_PROBE
      if (rightResult[1] - rightResult[0] > (MIN_PROBE_ITEM_CNT << 1)) {
        uint32_t task_cnt = (rightResult[1] - rightResult[0]) / MIN_PROBE_ITEM_CNT;
        task_cnt = std::min(task_cnt, MAX_PROBE_TASK_CNT);
        size_t step = (rightResult[1] - rightResult[0] + task_cnt - 1) / task_cnt;
        size_t start = rightResult[0] + step;
        int taskid = 0;
        std::vector<std::vector<std::vector<uint32_t>>> parallelResults(task_cnt, std::vector<std::vector<uint32_t>>(intermediateResults.size()));

        std::vector<std::future<size_t>> vf;
        while (start < rightResult[1]) {
          taskid ++;
          // log_print("split task {} for the probe, start: {}\n", taskid, start);
          if (start + step >= rightResult[1]) {
            vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, start, end = rightResult[1]]() {
              for (auto &result : results) {
                result.reserve(end - start + 1);
              }
              for (uint32_t i = start; i < end; i++) {
                auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
                for (auto &hashTable: hashTables) {
                  auto range = hashTable.equal_range(rightKey);
                  for (auto iter = range.first; iter != range.second; ++iter) {
                    copy2ResultLRP(results, iter->second, i);
                  }
                }
#else
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultLRP(results, iter->second, i);
                }
#endif
              }
              return results[0].size();
            }));
            break;
          }
          vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, start, end = start + step]() {
            for (auto &result : results) {
              result.reserve(end - start + 1);
            }
            for (uint32_t i = start; i < end; i++) {
              auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
              for (auto &hashTable: hashTables) {
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultLRP(results, iter->second, i);
                }
              }
#else
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultLRP(results, iter->second, i);
              }
#endif
            }
            return results[0].size();
          }));
          start += step;
        }
        for (auto &result : intermediateResults) {
          result.reserve(step);
        }
        for (uint32_t i = rightResult[0]; i < rightResult[0] + step; i++) {
          auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultLR(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultLR(iter->second, i);
          }
#endif
        }
        // wait for the results
        size_t pcnt = 0;
        for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
          pcnt += x.get();
          // log_print("subtasks probe get {} results\n", pcnt);
        });
        resultSize += pcnt;
#if PRINT_LOG
        // end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
        // log_print("before merge time {} ms, intermediate cnt: {}\n", end.count() - tmpms.count(), intermediateResults[0].size());
        // tmpms = end;
#endif
        // merge the results
        for (int i = 0; i < intermediateResults.size(); i++) {
          intermediateResults[i].reserve(resultSize);
          for (int j = 1; j < parallelResults.size(); j++) {
            intermediateResults[i].insert(intermediateResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
          }
        }
      } else {
        for (auto &result : intermediateResults) {
          result.reserve(rightResult[1] - rightResult[0] + 1);
        }
        for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
          auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultLR(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultLR(iter->second, i);
          }
#endif
        }
      }
#else   // USE_PARALLEL_PROBE
      for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
        auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
        for (auto &hashTable: hashTables) {
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultLR(iter->second, i);
          }
        }
#else
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2ResultLR(iter->second, i);
        }
#endif
      }
#endif  // USE_PARALLEL_PROBE
#if PRINT_LOG
      end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
      log_print("probe phase time {} ms\n", end.count() - tmpms.count());
#endif
    } else {  // right range: false
      auto &rightResult = rightResults[rightColId];
#if PRINT_LOG
      log_print("right result size: {}, is range: false\n", rightResult.size());
#endif
#if USE_PARALLEL_PROBE
      if (rightResult.size() > (MIN_PROBE_ITEM_CNT << 1)) {
        uint32_t task_cnt = rightResult.size() / MIN_PROBE_ITEM_CNT;
        task_cnt = std::min(task_cnt, MAX_PROBE_TASK_CNT);
        size_t step = (rightResult.size() + task_cnt - 1) / task_cnt;
        size_t start = step;
        int taskid = 0;
        std::vector<std::vector<std::vector<uint32_t>>> parallelResults(task_cnt, std::vector<std::vector<uint32_t>>(intermediateResults.size()));

        std::vector<std::future<size_t>> vf;
        while (start < rightResult.size()) {
          taskid ++;
          // log_print("split task {} for the probe, start: {}\n", taskid, start);
          if (start + step >= rightResult.size()) {
            vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, &rightResult, start, end = rightResult.size()]() {
              for (auto &result : results) {
                result.reserve(end - start + 1);
              }
              for (uint32_t i = start; i < end; i++) {
                auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
                for (auto &hashTable: hashTables) {
                  auto range = hashTable.equal_range(rightKey);
                  for (auto iter = range.first; iter != range.second; ++iter) {
                    copy2ResultLP(results, iter->second, i);
                  }
                }
#else
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultLP(results, iter->second, i);
                }
#endif
              }
              return results[0].size();
            }));
            break;
          }
          vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, &rightResult, start, end = start + step]() {
            for (auto &result : results) {
              result.reserve(end - start + 1);
            }
            for (uint32_t i = start; i < end; i++) {
              auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
              for (auto &hashTable: hashTables) {
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultLP(results, iter->second, i);
                }
              }
#else
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultLP(results, iter->second, i);
              }
#endif
            }
            return results[0].size();
          }));
          start += step;
        }
        for (auto &result : intermediateResults) {
          result.reserve(step);
        }
        for (uint32_t i = 0; i < step; i++) {
          auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultL(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultL(iter->second, i);
          }
#endif
        }
        // wait for the results
        size_t pcnt = 0;
        for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
          pcnt += x.get();
          // log_print("subtasks probe get {} results\n", pcnt);
        });
        resultSize += pcnt;
#if PRINT_LOG
        // end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
        // log_print("before merge time {} ms, intermediate cnt: {}\n", end.count() - tmpms.count(), intermediateResults[0].size());
        // tmpms = end;
#endif
        // merge the results
        for (int i = 0; i < intermediateResults.size(); i++) {
          intermediateResults[i].reserve(resultSize);
          for (int j = 1; j < parallelResults.size(); j++) {
            intermediateResults[i].insert(intermediateResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
          }
        }
      } else {
        for (uint32_t i = 0; i < rightResult.size(); i++) {
          auto rightKey = rightColumn[rightResult[i]];
          // log_print("  rightKey: {}\n", rightKey);
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultL(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            // log_print("  hash key {}, copy {}, {}\n", iter->first, iter->second, i);
            copy2ResultL(iter->second, i);
          }
#endif
        }
      }
#else     // USE_PARALLEL_PROBE
      for (uint32_t i = 0; i < rightResult.size(); i++) {
        auto rightKey = rightColumn[rightResult[i]];
        // log_print("  rightKey: {}\n", rightKey);
#if USE_PARALLEL_BUILD_HASH_TABLE
        for (auto &hashTable: hashTables) {
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultL(iter->second, i);
          }
        }
#else
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          // log_print("  hash key {}, copy {}, {}\n", iter->first, iter->second, i);
          copy2ResultL(iter->second, i);
        }
#endif
      }
#endif    // USE_PARALLEL_PROBE
#if PRINT_LOG
      end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
      log_print("probe phase time {} ms\n", end.count() - tmpms.count());
#endif
    }
  } else {  // left range: false
    // Build phase
#if PRINT_LOG
    tmpms = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
    auto leftResult = leftResults[leftColId];
#if USE_PARALLEL_BUILD_HASH_TABLE
    if (leftResult.size() > (MIN_BUILD_ITEM_CNT << 1)) {
      uint32_t build_cnt = (leftResult.size()) / MIN_BUILD_ITEM_CNT;
      build_cnt = std::min(build_cnt, MAX_BUILD_TASK_CNT);
      hashTables.resize(build_cnt);
      size_t bstep = (leftResult[1] - leftResult[0] + build_cnt - 1) / build_cnt;
      size_t bstart = leftResult[0] + bstep;
      int buildid = 0;
      std::vector<std::future<void>> bvf;
      while (bstart < leftResult.size()) {
        buildid ++;
        if (bstart + bstep >= leftResult.size()) {
          bvf.push_back(std::async(std::launch::async , [this, &hashTable = hashTables[buildid], leftColumn, &leftResult, bstart, bend = leftResult.size()]() {
            hashTable.reserve((bend - bstart) * 2);
            for (uint32_t i = bstart; i < bend; i++) {
              hashTable.emplace(leftColumn[leftResult[i]], i);
            }
          }));
          break;
        }
        bvf.push_back(std::async(std::launch::async , [this, &hashTable = hashTables[buildid], leftColumn, &leftResult, bstart, bend = bstart + bstep]() {
          hashTable.reserve((bend - bstart) * 2);
          for (uint32_t i = bstart; i < bend; i++) {
            hashTable.emplace(leftColumn[leftResult[i]], i);
          }
        }));
        bstart += bstep;
      }
      hashTables[0].reserve(bstep * 2);
      for (uint32_t i = 0; i < bstep; i++) {
        hashTables[0].emplace(leftColumn[leftResult[i]], i);
      }

      for_each(bvf.begin(), bvf.end(), [](future<void> &x) { x.wait(); });
    } else {
      hashTables.resize(1);
      hashTables[0].reserve(leftResult.size() * 2);
      for (uint32_t i = 0; i < leftResult.size(); i++) {
        hashTables[0].emplace(leftColumn[leftResult[i]], i);
      }
    }
#else
    hashTable.reserve(left->resultSize * 2);
    for (uint32_t i = 0; i < leftResult.size(); i++) {
      // multi threads
      hashTable.emplace(leftColumn[leftResult[i]], i);
    }
#endif
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("Left result size: {}, is range: false, build hashmap time {} ms\n", leftResult.size(), end.count() - tmpms.count());
    tmpms = end;
#endif
    // Probe phase
    if (right->isRangeResult()) {
      assert(rightResults.size() == 1);
      auto &rightResult = rightResults[0];
#if PRINT_LOG
      log_print("right result size: {}, is range: true\n", rightResult[1] - rightResult[0]);
#endif
#if USE_PARALLEL_PROBE
      if (rightResult[1] - rightResult[0] > (MIN_PROBE_ITEM_CNT << 1)) {
        uint32_t task_cnt = (rightResult[1] - rightResult[0]) / MIN_PROBE_ITEM_CNT;
        task_cnt = std::min(task_cnt, MAX_PROBE_TASK_CNT);
        size_t step = (rightResult[1] - rightResult[0] + task_cnt - 1) / task_cnt;
        size_t start = rightResult[0] + step;
        int taskid = 0;
        std::vector<std::vector<std::vector<uint32_t>>> parallelResults(task_cnt, std::vector<std::vector<uint32_t>>(intermediateResults.size()));

        std::vector<std::future<size_t>> vf;
        while (start < rightResult[1]) {
          taskid ++;
          // log_print("split task {} for the probe, start: {}\n", taskid, start);
          if (start + step >= rightResult[1]) {
            vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, start, end = rightResult[1]]() {
              for (auto &result : results) {
                result.reserve(end - start + 1);
              }
              for (uint32_t i = start; i < end; i++) {
                auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
                for (auto &hashTable: hashTables) {
                  auto range = hashTable.equal_range(rightKey);
                  for (auto iter = range.first; iter != range.second; ++iter) {
                    copy2ResultRP(results, iter->second, i);
                  }
                }
#else
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultRP(results, iter->second, i);
                }
#endif
              }
              return results[0].size();
            }));
            break;
          }
          vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, start, end = start + step]() {
            for (auto &result : results) {
              result.reserve(end - start + 1);
            }
            for (uint32_t i = start; i < end; i++) {
              auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
                for (auto &hashTable: hashTables) {
                  auto range = hashTable.equal_range(rightKey);
                  for (auto iter = range.first; iter != range.second; ++iter) {
                    copy2ResultRP(results, iter->second, i);
                  }
                }
#else
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultRP(results, iter->second, i);
              }
#endif
            }
            return results[0].size();
          }));
          start += step;
        }
        for (auto &result : intermediateResults) {
          result.reserve(step);
        }
        for (uint32_t i = rightResult[0]; i < rightResult[0] + step; i++) {
          auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultR(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultR(iter->second, i);
          }
#endif
        }
        // wait for the results
        size_t pcnt = 0;
        for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
          pcnt += x.get();
          // log_print("subtasks probe get {} results\n", pcnt);
        });
        resultSize += pcnt;
#if PRINT_LOG
        // end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
        // log_print("before merge time {} ms, intermediate cnt: {}\n", end.count() - tmpms.count(), intermediateResults[0].size());
        // tmpms = end;
#endif
        // merge the results
        for (int i = 0; i < intermediateResults.size(); i++) {
          intermediateResults[i].reserve(resultSize);
          for (int j = 1; j < parallelResults.size(); j++) {
            intermediateResults[i].insert(intermediateResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
          }
        }
      } else {
        for (auto &result : intermediateResults) {
          result.reserve(rightResult[1] - rightResult[0] + 1);
        }
        for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
          auto rightKey = rightColumn[i];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultR(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultR(iter->second, i);
          }
#endif
        }
      }
#else
      for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
        auto rightKey = rightColumn[i];
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2ResultR(iter->second, i);
        }
      }
#endif
    } else {
      auto &rightResult = rightResults[rightColId];
#if PRINT_LOG
      log_print("right result size: {}, is range: false\n", rightResult.size());
#endif
#if USE_PARALLEL_PROBE
      if (rightResult.size() > (MIN_PROBE_ITEM_CNT << 1)) {
        uint32_t task_cnt = rightResult.size() / MIN_PROBE_ITEM_CNT;
        task_cnt = std::min(task_cnt, MAX_PROBE_TASK_CNT);
        size_t step = (rightResult.size() + task_cnt - 1) / task_cnt;
        size_t start = step;
        int taskid = 0;
        std::vector<std::vector<std::vector<uint32_t>>> parallelResults(task_cnt, std::vector<std::vector<uint32_t>>(intermediateResults.size()));

        std::vector<std::future<size_t>> vf;
        while (start < rightResult.size()) {
          taskid ++;
          // log_print("split task {} for the probe, start: {}\n", taskid, start);
          if (start + step >= rightResult.size()) {
            vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, &rightResult, start, end = rightResult.size()]() {
              for (auto &result : results) {
                result.reserve(end - start + 1);
              }
              for (uint32_t i = start; i < end; i++) {
                auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
                for (auto &hashTable: hashTables) {
                  auto range = hashTable.equal_range(rightKey);
                  for (auto iter = range.first; iter != range.second; ++iter) {
                    copy2ResultP(results, iter->second, i);
                  }
                }
#else
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultP(results, iter->second, i);
                }
#endif
              }
              return results[0].size();
            }));
            break;
          }
          vf.push_back(std::async(std::launch::async , [this, &results = parallelResults[taskid], rightColumn, &rightResult, start, end = start + step]() {
            for (auto &result : results) {
              result.reserve(end - start + 1);
            }
            for (uint32_t i = start; i < end; i++) {
              auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
              for (auto &hashTable: hashTables) {
                auto range = hashTable.equal_range(rightKey);
                for (auto iter = range.first; iter != range.second; ++iter) {
                  copy2ResultP(results, iter->second, i);
                }
              }
#else
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultP(results, iter->second, i);
              }
#endif
            }
            return results[0].size();
          }));
          start += step;
        }
        for (auto &result : intermediateResults) {
          result.reserve(step);
        }
        for (uint32_t i = 0; i < step; i++) {
          auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2Result(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2Result(iter->second, i);
          }
#endif
        }
        // wait for the results
        size_t pcnt = 0;
        for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
          pcnt += x.get();
          // log_print("subtasks probe get {} results\n", pcnt);
        });
        resultSize += pcnt;
#if PRINT_LOG
        // end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
        // log_print("before merge time {} ms, intermediate cnt: {}\n", end.count() - tmpms.count(), intermediateResults[0].size());
        // tmpms = end;
#endif
        // merge the results
        for (int i = 0; i < intermediateResults.size(); i++) {
          intermediateResults[i].reserve(resultSize);
          for (int j = 1; j < parallelResults.size(); j++) {
            intermediateResults[i].insert(intermediateResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
          }
        }
      } else {
        for (uint32_t i = 0; i < rightResult.size(); i++) {
          auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
          for (auto &hashTable: hashTables) {
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2Result(iter->second, i);
            }
          }
#else
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2Result(iter->second, i);
          }
#endif
        }
      }
#else
      for (uint32_t i = 0; i < rightResult.size(); i++) {
        auto rightKey = rightColumn[rightResult[i]];
#if USE_PARALLEL_BUILD_HASH_TABLE
        for (auto &hashTable: hashTables) {
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2Result(iter->second, i);
          }
        }
#else
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2Result(iter->second, i);
        }
#endif
      }
#endif
    }
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("probe phase time {} ms\n", end.count() - tmpms.count());
#endif
  }
#if PRINT_LOG
  end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
}
//---------------------------------------------------------------------------
void SelfJoin::copy2Result(uint32_t id)
  // Copy to result
{
  auto &inputResults = input->getResults();
  for (unsigned cId = 0; cId < inputResults.size(); ++cId)
    intermediateResults[cId].push_back(inputResults[cId][id]);
  ++resultSize;
}
//---------------------------------------------------------------------------
void SelfJoin::run()
  // Run
{
  input->run();
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  auto &inputResults = input->getResults();
  intermediateResults.resize(inputResults.size());

  auto leftColId=input->resolve(pInfo.left);
  auto rightColId=input->resolve(pInfo.right);

  uint64_t *leftColumn = input->getRelation(pInfo.left.binding)->columns[pInfo.left.colId];
  uint64_t *rightColumn = input->getRelation(pInfo.right.binding)->columns[pInfo.right.colId];

  auto leftCol=inputResults[leftColId];
  auto rightCol=inputResults[rightColId];
  if (input->isRangeResult()) {
    assert(inputResults.size() == 1);
    for (uint32_t i = inputResults[0][0]; i < inputResults[0][1]; i++) {
      if (leftColumn[i] == rightColumn[i]) {
        intermediateResults[0].push_back(i);
        ++resultSize;
      }
    }
  } else {
    for (uint32_t i = 0; i < input->resultSize; ++i) {
      if (leftColumn[leftCol[i]] == rightColumn[rightCol[i]])
        copy2Result(i);
    }
  }
#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("self join resultSize: {}, isRange: {}, run time {} ms\n", resultSize, input->isRangeResult(), end.count() - start.count());
#endif
}
//---------------------------------------------------------------------------
void Checksum::run()
  // Run
{
  input->run();
#if PRINT_LOG
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
  auto &results = input->getResults();
  resultSize = input->resultSize;
  if (resultSize == 0) {
    checkSums.resize(colInfo.size());
#if PRINT_LOG
    milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("checksum result size {}, is range: {}, run time {} ms\n", resultSize,input->isRangeResult(), end.count() - start.count());
#endif
    return;
  }

  if (input->isRangeResult()) {
    assert(results.size() == 1);
    for (auto &sInfo : colInfo) {
      uint64_t sum = 0;
      uint64_t *column = input->getRelation(sInfo.binding)->columns[sInfo.colId];
      for (uint32_t i = results[0][0]; i < results[0][1]; i++) {
        sum += column[i];
      }
      checkSums.push_back(sum);
    }
  } else {
    // first build cache
    // boost::asio::thread_pool pool(std::min(SUM_CACHE_MAX_POOL_SIZE, static_cast<int>(colInfo.size())));
    std::vector<std::future<void>> vf;
    for (auto &sInfo : colInfo) {
      if (sumsCache.find(sInfo) != sumsCache.end()) {
        continue;
      }
      sumsCache.insert({sInfo, 0});
      auto colId = input->resolve(sInfo);
      auto &resulti = results[colId];
      uint64_t *column = input->getRelation(sInfo.binding)->columns[sInfo.colId];
      // log_print("result size num: {}, column addr: {}, colId: {}\n", results[0].size(), fmt::ptr(column), colId);

      vf.push_back(std::async(std::launch::async , [&sInfo, &resulti, column, this]() {
        uint64_t sum = 0;
        for (auto i : resulti) {
          sum += column[i];
        }
        sumsCache[sInfo] = sum;
      }));
      // boost::asio::post(pool, [&sInfo, &resulti, column, this]() {
      //   uint64_t sum = 0;
      //   for (auto i : resulti) {
      //     sum += column[i];
      //   }
      //   sumsCache[sInfo] = sum;
      // });
    }
    for_each(vf.begin(), vf.end(), [](future<void> &x) {
      x.wait();
    });
    // pool.join();
    // then check cache for sum result
    for (auto &sInfo : colInfo) {
      checkSums.push_back(sumsCache[sInfo]);
    }
  }
#if PRINT_LOG
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("checksum result size {}, is range: {}, run time {} ms\n", resultSize,input->isRangeResult(), end.count() - start.count());
#endif
}
//---------------------------------------------------------------------------
