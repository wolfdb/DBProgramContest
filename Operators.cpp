#include <cassert>
#include <iostream>
#include <future>
#include "Operators.hpp"
#include "Log.hpp"
#include "Consts.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace std::chrono;
//---------------------------------------------------------------------------
bool Scan::require(SelectInfo info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  resultColumns.push_back(relation.columns[info.colId]);
  select2ResultColId[info]=resultColumns.size()-1;
  return true;
}
//---------------------------------------------------------------------------
void Scan::run()
  // Run
{
  // Nothing to do
  resultSize=relation.rowCount;
  log_print("scan resultSize: {}\n", resultSize);
}
//---------------------------------------------------------------------------
vector<uint64_t*> Scan::getResults()
  // Get materialized results
{
  return resultColumns;
}
//---------------------------------------------------------------------------
bool FilterScan::require(SelectInfo info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  if (select2ResultColId.find(info)==select2ResultColId.end()) {
    // Add to results
    inputData.push_back(relation.columns[info.colId]);
    tmpResults.emplace_back();
    unsigned colId=tmpResults.size()-1;
    select2ResultColId[info]=colId;
  }
  return true;
}
//---------------------------------------------------------------------------
void FilterScan::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<inputData.size();++cId)
    tmpResults[cId].push_back(inputData[cId][id]);
  ++resultSize;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
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
  for (uint64_t i=0;i<relation.rowCount;++i) {
    bool pass=true;
    for (auto& f : filters) {
      pass&=applyFilter(i,f);
    }
    if (pass)
      copy2Result(i);
  }
}
//---------------------------------------------------------------------------
vector<uint64_t*> Operator::getResults()
  // Get materialized results
{
  vector<uint64_t*> resultVector;
  for (auto& c : tmpResults) {
    resultVector.push_back(c.data());
  }
  return resultVector;
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requestedColumns.count(info)==0) {
    bool success=false;
    if(left->require(info)) {
      requestedColumnsLeft.emplace_back(info);
      success=true;
    } else if (right->require(info)) {
      success=true;
      requestedColumnsRight.emplace_back(info);
    }
    if (!success)
      return false;

    tmpResults.emplace_back();
    requestedColumns.emplace(info);
  }
  return true;
}
//---------------------------------------------------------------------------
void Join::copy2Result(uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId)
    tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);

  for (unsigned cId=0;cId<copyRightData.size();++cId)
    tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
  ++resultSize;
}
void Join::copy2ResultP(std::vector<std::vector<uint64_t>> &results, uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId)
    results[relColId++].push_back(copyLeftData[cId][leftId]);

  for (unsigned cId=0;cId<copyRightData.size();++cId)
    results[relColId++].push_back(copyRightData[cId][rightId]);
}
//---------------------------------------------------------------------------
void Join::run()
  // Run
{
  left->require(pInfo.left);
  right->require(pInfo.right);
#if USE_ASYNC_JOIN
  auto al = std::async(std::launch::async | std::launch::deferred , [this]() { left->run(); });
  right->run();
  al.get();
#else
  left->run();
  right->run();
#endif

#if PRINT_LOG
  milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  milliseconds end, tmpms;
#endif
  // Use smaller input for build
  if (left->resultSize>right->resultSize) {
    swap(left,right);
    swap(pInfo.left,pInfo.right);
    swap(requestedColumnsLeft,requestedColumnsRight);
  }

  auto leftInputData=left->getResults();
  auto rightInputData=right->getResults();

  // Resolve the input columns
  unsigned resColId=0;
  for (auto& info : requestedColumnsLeft) {
    copyLeftData.push_back(leftInputData[left->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }
  for (auto& info : requestedColumnsRight) {
    copyRightData.push_back(rightInputData[right->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }

  if (left->resultSize == 0) {
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
    return;
  }

  log_print("leftBinding: {}, leftColId: {}  rightBinding: {}, rightColId: {}\n", pInfo.left.binding, pInfo.left.colId, pInfo.right.binding, pInfo.right.colId);

  auto leftColId=left->resolve(pInfo.left);
  auto rightColId=right->resolve(pInfo.right);
  auto leftKeyColumn=leftInputData[leftColId];
  auto rightKeyColumn=rightInputData[rightColId];
  auto leftResultSize = left->resultSize;
  auto rightResultSize = right->resultSize;

  // If left resultSize == 1, no not need to build hash table
  if (leftResultSize == 1) {
    auto leftKey = leftKeyColumn[0];
    for (uint64_t i=0; i< rightResultSize; ++i) {
      if (leftKey == rightKeyColumn[i]) {
        copy2Result(0, i);
      }
    }

#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("left resultSize == 1, join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
    return;
  }

  // Build phase
#if PRINT_LOG
  tmpms = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
#endif
  uint32_t build_cnt = 1;
#if USE_PARALLEL_BUILD_HASH_TABLE
  if (leftResultSize > (MIN_BUILD_ITEM_CNT << 1)) {
    build_cnt = std::min((uint32_t)(leftResultSize / MIN_BUILD_ITEM_CNT), MAX_BUILD_TASK_CNT);
    hashTables.resize(build_cnt);
    size_t bstep = (leftResultSize + build_cnt - 1) / build_cnt;
    size_t bstart = bstep;
    int buildid = 0;
    std::vector<std::future<void>> bvf;
    while (bstart < leftResultSize) {
      buildid ++;
      if (bstart + bstep >= leftResultSize) {
        bvf.push_back(std::async(std::launch::async | std::launch::deferred , [&hashTable = hashTables[buildid], leftKeyColumn, bstart, bend = leftResultSize]() {
          hashTable.reserve((bend - bstart) * 2);
          for (uint32_t i = bstart; i < bend; i++) {
            hashTable.emplace(leftKeyColumn[i], i);
          }
        }));
        break;
      }
      bvf.push_back(std::async(std::launch::async | std::launch::deferred , [&hashTable = hashTables[buildid], leftKeyColumn, bstart, bend = bstart + bstep]() {
        hashTable.reserve((bend - bstart) * 2);
        for (uint32_t i = bstart; i < bend; i++) {
          hashTable.emplace(leftKeyColumn[i], i);
        }
      }));
      bstart += bstep;
    }
    hashTables[0].reserve(bstep * 2);
    for (uint32_t i = 0; i < bstep; i++) {
      hashTables[0].emplace(leftKeyColumn[i], i);
    }
    for_each(bvf.begin(), bvf.end(), [](future<void> &x) { x.wait(); });
  } else {
    hashTables.resize(1);
    hashTables[0].reserve(leftResultSize * 2);
    for (uint32_t i = 0; i < leftResultSize; i++) {
      hashTables[0].emplace(leftKeyColumn[i], i);
    }
  }
#else
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }
#endif
#if PRINT_LOG
  end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("Left result size: {} build hashmap time {} ms\n", leftResultSize, end.count() - tmpms.count());
  tmpms = end;
#endif
  // Probe phase
#if PRINT_LOG
  log_print("right result size: {}, is range: false\n", rightResultSize);
#endif

  if (rightResultSize > (MIN_PROBE_ITEM_CNT << 1)) {
    uint32_t task_cnt = (32 + build_cnt - 1) / build_cnt;
    task_cnt = std::min(task_cnt, MAX_PROBE_TASK_CNT);
    size_t step = (rightResultSize + task_cnt - 1) / task_cnt;
    size_t start = step;
    int taskid = 0;
    std::vector<std::vector<std::vector<uint64_t>>> parallelResults(task_cnt * build_cnt, std::vector<std::vector<uint64_t>>(tmpResults.size()));
    std::vector<std::future<size_t>> vf;
#if USE_PARALLEL_BUILD_HASH_TABLE
    while (start < rightResultSize) {
      taskid ++;
      if (start + step >= rightResultSize) {
        for (uint32_t ii = 0; ii < build_cnt; ii++) {
          vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &hashTable = hashTables[ii], &results = parallelResults[taskid * build_cnt + ii], rightKeyColumn, start, end = rightResultSize]() {
            for (auto &result: results) {
              result.reserve(end - start + 1);
            }
            for (uint64_t i = start; i < end; i++) {
              auto rightKey = rightKeyColumn[i];
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultP(results, iter->second, i);
              }
            }
            return results[0].size();
          }));
        }
        break;
      }
      for (uint32_t ii = 0; ii < build_cnt; ii++) {
        vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &hashTable = hashTables[ii], &results = parallelResults[taskid * build_cnt + ii], rightKeyColumn, start, end = start + step]() {
          for (auto &result: results) {
              result.reserve(end - start + 1);
            }
            for (uint32_t i = start; i < end; i++) {
              auto rightKey = rightKeyColumn[i];
              auto range = hashTable.equal_range(rightKey);
              for (auto iter = range.first; iter != range.second; ++iter) {
                copy2ResultP(results, iter->second, i);
              }
            }
            return results[0].size();
        }));
      }
      start += step;
    }
    for (uint32_t ii = 1; ii < build_cnt; ii++) {
      vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &hashTable = hashTables[ii], &results = parallelResults[ii], rightKeyColumn, start = 0, end = step]() {
        for (auto &result: results) {
          result.reserve(end - start + 1);
        }
        for (uint32_t i = start; i < end; i++) {
          auto rightKey = rightKeyColumn[i];
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultP(results, iter->second, i);
          }
        }
        return results[0].size();
      }));
    }
    for (auto &result : tmpResults) {
      result.reserve(step);
    }
    for (uint32_t i = 0; i < step; i++) {
      auto rightKey = rightKeyColumn[i];
      auto range = hashTables[0].equal_range(rightKey);
      for (auto iter = range.first; iter != range.second; ++iter) {
        copy2Result(iter->second, i);
      }
    }
#else // USE_PARALLEL_BUILD_HASH_TABLE
    while (start < rightResultSize) {
      taskid ++;
      if (start + step >= rightResultSize) {
        vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &results = parallelResults[taskid], rightKeyColumn, start, end = rightResultSize]() {
          for (auto &result : results) {
            result.reserve(end - start + 1);
          }
          for (uint32_t i = start; i < end; i++) {
            auto rightKey = rightKeyColumn[i];
            auto range = hashTable.equal_range(rightKey);
            for (auto iter = range.first; iter != range.second; ++iter) {
              copy2ResultP(results, iter->second, i);
            }
          }
          return results[0].size();
        }));
        break;
      }
      vf.push_back(std::async(std::launch::async | std::launch::deferred , [this, &results = parallelResults[taskid], rightKeyColumn, start, end = start + step]() {
        for (auto &result : results) {
          result.reserve(end - start + 1);
        }
        for (uint32_t i = start; i < end; i++) {
          auto rightKey = rightKeyColumn[i];
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultP(results, iter->second, i);
          }
        }
        return results[0].size();
      }));
      start += step;
    }
    for (auto &result : tmpResults) {
      result.reserve(step);
    }
    for (uint32_t i = 0; i < step; i++) {
      auto rightKey = rightKeyColumn[i];
      auto range = hashTable.equal_range(rightKey);
      for (auto iter = range.first; iter != range.second; ++iter) {
        copy2Result(iter->second, i);
      }
    }
#endif
    // wait for the results
    size_t pcnt = 0;
    for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
      pcnt += x.get();
      // log_print("subtasks probe get {} results\n", pcnt);
    });
    resultSize += pcnt;
    // merge the results
    for (int i = 0; i < tmpResults.size(); i++) {
      tmpResults[i].reserve(resultSize);
      for (int j = 1; j < parallelResults.size(); j++) {
        tmpResults[i].insert(tmpResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
      }
    }
  } else {    // MIN_PROBE_ITEM_CNT
#if USE_PARALLEL_BUILD_HASH_TABLE
    std::vector<std::vector<std::vector<uint64_t>>> parallelResults(build_cnt, std::vector<std::vector<uint64_t>>(tmpResults.size()));
    std::vector<std::future<size_t>> vf;
    for (uint32_t ii = 1; ii < build_cnt; ii++) {
      vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &hashTable = hashTables[ii], &results = parallelResults[ii], rightKeyColumn, start = 0, end = rightResultSize]() {
        for (auto &result: results) {
          result.reserve(end - start + 1);
        }
        for (uint32_t i = start; i < end; i++) {
          auto rightKey = rightKeyColumn[i];
          auto range = hashTable.equal_range(rightKey);
          for (auto iter = range.first; iter != range.second; ++iter) {
            copy2ResultP(results, iter->second, i);
          }
        }
        return results[0].size();
      }));
    }
    for (auto &result : tmpResults) {
      result.reserve(rightResultSize);
    }
    for (uint32_t i = 0; i < rightResultSize; i++) {
      auto rightKey = rightKeyColumn[i];
      auto range = hashTables[0].equal_range(rightKey);
      for (auto iter = range.first; iter != range.second; ++iter) {
        copy2Result(iter->second, i);
      }
    }
    // merge results
    size_t pcnt = 0;
    for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
      pcnt += x.get();
      // log_print("subtasks probe get {} results\n", pcnt);
    });
    resultSize += pcnt;
    for (int i = 0; i < tmpResults.size(); i++) {
      tmpResults[i].reserve(resultSize);
      for (int j = 1; j < parallelResults.size(); j++) {
        tmpResults[i].insert(tmpResults[i].begin(), parallelResults[j][i].begin(), parallelResults[j][i].end());
      }
    }
#else
    for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
      auto rightKey=rightKeyColumn[i];
      auto range=hashTable.equal_range(rightKey);
      for (auto iter=range.first;iter!=range.second;++iter) {
        copy2Result(iter->second,i);
      }
    }
#endif
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
void SelfJoin::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<copyData.size();++cId)
    tmpResults[cId].push_back(copyData[cId][id]);
  ++resultSize;
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requiredIUs.count(info))
    return true;
  if(input->require(info)) {
    tmpResults.emplace_back();
    requiredIUs.emplace(info);
    return true;
  }
  return false;
}
//---------------------------------------------------------------------------
void SelfJoin::run()
  // Run
{
  input->require(pInfo.left);
  input->require(pInfo.right);
  input->run();
  inputData=input->getResults();

  for (auto& iu : requiredIUs) {
    auto id=input->resolve(iu);
    copyData.emplace_back(inputData[id]);
    select2ResultColId.emplace(iu,copyData.size()-1);
  }

  auto leftColId=input->resolve(pInfo.left);
  auto rightColId=input->resolve(pInfo.right);

  auto leftCol=inputData[leftColId];
  auto rightCol=inputData[rightColId];
  for (uint64_t i=0;i<input->resultSize;++i) {
    if (leftCol[i]==rightCol[i])
      copy2Result(i);
  }
}
//---------------------------------------------------------------------------
void Checksum::run()
  // Run
{
  for (auto& sInfo : colInfo) {
    input->require(sInfo);
  }
  input->run();
  auto results=input->getResults();

  for (auto& sInfo : colInfo) {
    auto colId=input->resolve(sInfo);
    auto resultCol=results[colId];
    uint64_t sum=0;
    resultSize=input->resultSize;
    for (auto iter=resultCol,limit=iter+input->resultSize;iter!=limit;++iter)
      sum+=*iter;
    checkSums.push_back(sum);
  }
}
//---------------------------------------------------------------------------
