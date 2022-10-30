#include <Operators.hpp>
#include <cassert>
#include <iostream>
#include <queue>
#include <algorithm>
#include "Log.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace std::chrono;
//---------------------------------------------------------------------------
void Scan::run()
  // Run
{
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  resultSize=relation.rowCount;
  intermediateResults.push_back({0, static_cast<uint32_t>(relation.rowCount)});

  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("scan resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
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
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("filter scan resultSize: {}, isRange :{}, run time {} ms\n", resultSize, isRange, end.count() - start.count());

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
//---------------------------------------------------------------------------
void Join::run()
  // Run
{
  left->run();
  right->run();
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
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
    milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
    return;
  }

  auto leftColId = left->resolve(pInfo.left);
  auto leftBinding = leftBindings[leftColId];
  auto rightColId = right->resolve(pInfo.right);
  auto rightBinding = rightBindings[rightColId];
  uint64_t *leftColumn = left->getRelation(leftBinding)->columns[pInfo.left.colId];
  uint64_t *rightColumn = right->getRelation(rightBinding)->columns[pInfo.right.colId];

  // log_print("left column addr {}, right column addr {}\n", fmt::ptr(leftColumn), fmt::ptr(rightColumn));

  hashTable.reserve(left->resultSize * 2);
  if (left->isRangeResult()) {
    // Build phase
    assert(leftResults.size() == 1);
    log_print("leftColId: {} leftBinding: {}, rightColId: {}, rightBinding: {}\n", leftColId, leftBinding, rightColId, rightBinding);
    auto &leftResult = leftResults[0];

    // log_print("range [{}, {})\n", leftResult[0], leftResult[1]);
    for (uint32_t i = leftResult[0]; i < leftResult[1]; i++) {
      // simd ?
      // if (leftBinding == 2) {
      //   log_print(" emplace {}, {}\n", leftColumn[i], i);
      // }
      // log_print(" emplace {}, {}\n", leftColumn[i], i);
      hashTable.emplace(leftColumn[i], i);
    }
    // Probe phase
    if (right->isRangeResult()) {
      assert(rightResults.size() == 1);
      auto &rightResult = rightResults[0];
      for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
        auto rightKey = rightColumn[i];
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2ResultLR(iter->second, i);
        }
      }
    } else {
      auto rightResult = rightResults[rightColId];

      log_print("right result size: {}\n", rightResult.size());
      for (uint32_t i = 0; i < rightResult.size(); i++) {
        auto rightKey = rightColumn[rightResult[i]];
        // log_print("  rightKey: {}\n", rightKey);
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          // log_print("  hash key {}, copy {}, {}\n", iter->first, iter->second, i);
          copy2ResultL(iter->second, i);
        }
      }
    }
  } else {
    // Build phase
    auto leftResult = leftResults[leftColId];
    for (uint32_t i = 0; i < leftResult.size(); i++) {
      // multi threads
      hashTable.emplace(leftColumn[leftResult[i]], i);
    }
    // Probe phase
    if (right->isRangeResult()) {
      assert(rightResults.size() == 1);
      auto &rightResult = rightResults[0];
      for (uint32_t i = rightResult[0]; i < rightResult[1]; i++) {
        auto rightKey = rightColumn[i];
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2ResultR(iter->second, i);
        }
      }
    } else {
      auto &rightResult = rightResults[rightColId];
      for (uint32_t i = 0; i < rightResult.size(); i++) {
        auto rightKey = rightColumn[rightResult[i]];
        auto range = hashTable.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
          copy2Result(iter->second, i);
        }
      }
    }
  }
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
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

  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("self join resultSize: {}, isRange: {}, run time {} ms\n", resultSize, input->isRangeResult(), end.count() - start.count());
}
//---------------------------------------------------------------------------
void Checksum::run()
  // Run
{
  input->run();
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  auto &results = input->getResults();
  resultSize = input->resultSize;

  log_print("is range: {}, result size {}\n", input->isRangeResult(), resultSize);
  if (resultSize == 0) {
    checkSums.resize(colInfo.size());
    milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("checksum result size {}, is range: {}, run time {} ms\n", resultSize,input->isRangeResult(), end.count() - start.count());
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
    for (auto &sInfo : colInfo) {
      uint64_t sum = 0;
      uint64_t *column = input->getRelation(sInfo.binding)->columns[sInfo.colId];
      auto colId = input->resolve(sInfo);
      // log_print("result size num: {}, column addr: {}, colId: {}\n", results[0].size(), fmt::ptr(column), colId);
      auto &resulti = results[colId];
      for (auto i : resulti) {
        sum += column[i];
      }
      checkSums.push_back(sum);
    }
  }
  milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("checksum result size {}, is range: {}, run time {} ms\n", resultSize,input->isRangeResult(), end.count() - start.count());
}
//---------------------------------------------------------------------------
