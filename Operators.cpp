#include <cassert>
#include <iostream>
#include <future>
#include <array>
#include "Joiner.hpp"
#include "Operators.hpp"
#include "Log.hpp"
#include "Consts.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace std::chrono;
//---------------------------------------------------------------------------
vector<Column<uint64_t>>& Operator::getResults()
// Get materialized results
{
  return results;
}
//---------------------------------------------------------------------------
uint64_t Operator::getResultsSize() {
  return resultSize*results.size();
}
//---------------------------------------------------------------------------
bool Scan::require(SelectInfo &info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  if (select2ResultColId.find(info) == select2ResultColId.end()) {
    results.emplace_back(1);
    infos.push_back(info);
    select2ResultColId[info] = results.size() - 1;
  }
  return true;
}
//---------------------------------------------------------------------------
uint64_t Scan::getResultsSize() {
  return results.size()*relation.rowCount; 
}
//---------------------------------------------------------------------------
void Scan::run()
  // Run
{
  // return compressed data
  if (infos.size() == 1 && !relation.compressColumns[infos[0].colId].empty()) {
    uint64_t *data = relation.compressColumns[infos[0].colId][0];
    uint64_t *length = relation.compressColumns[infos[0].colId][1];
    resultSize = length - data;
    results[0].addTuples(0, data, resultSize);
    results[0].fix();
    results.emplace_back(1);
    results[1].addTuples(0, length, resultSize);
    results[1].fix();
    counted = 2;
  } else {
    resultSize = relation.rowCount;
    for (int i = 0; i < infos.size(); i++) {
      results[i].addTuples(0, relation.columns[infos[i].colId], resultSize);
      results[i].fix();
    }
  }
#if PRINT_LOG
  // log_print("scan resultSize: {}\n", resultSize);
#endif
}
//---------------------------------------------------------------------------
bool FilterScan::require(SelectInfo &info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  if (select2ResultColId.find(info)==select2ResultColId.end()) {
    // Add to results
    infos.push_back(info);
    // inputData.push_back(relation.columns[info.colId]);
    // tmpResults.emplace_back();
    unsigned colId=infos.size()-1;
    select2ResultColId[info]=colId;
  }
  return true;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
  // Apply filter
{
  auto compareCol = counted == 2 ? relation.compressColumns[f.filterColumn.colId][0] : relation.columns[f.filterColumn.colId];
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
  uint64_t size = relation.rowCount;
  if (infos.size() == 1) {
    bool pass = true;
    for (auto &f: filters) {
      if (f.filterColumn.colId != infos[0].colId) {
        pass = false;
        break;
      }
    }
    if (pass && !relation.compressColumns[infos[0].colId].empty()) {
      counted = 2;
      size = relation.compressColumns[infos[0].colId][1] - relation.compressColumns[infos[0].colId][0];
    }
  }

  uint32_t filter_cnt = std::min((uint32_t)(size / MIN_FILTER_ITEM_CNT), MAX_FILTER_TAKS_CNT);
  if (filter_cnt == 0) {
    filter_cnt = 1;
  }
  size_t step = size / filter_cnt;
  size_t remain = size % filter_cnt;

  if (counted == 2) {
    inputData.emplace_back(relation.compressColumns[infos[0].colId][0]);
    inputData.emplace_back(relation.compressColumns[infos[0].colId][1]);
    results.emplace_back(filter_cnt);
    results.emplace_back(filter_cnt);
  } else {
    for (auto &sinfo: infos) {
      inputData.emplace_back(relation.columns[sinfo.colId]);
      results.emplace_back(filter_cnt);
    }
    if (counted) {
      results.emplace_back(filter_cnt);
    }
  }
  tmpResults.resize(filter_cnt);
  std::vector<std::future<size_t>> vf;
  uint64_t start = 0;
  for (unsigned i = 0; i < filter_cnt; i++) {
    uint64_t len = step;
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, filterid = i, start, end = start + len]() {
      auto &localResults = tmpResults[filterid];
      auto colSize = inputData.size();
      for (int j = 0; j < inputData.size(); j++) {
        localResults.emplace_back();
      }
      if (counted) {
        localResults.emplace_back();
      }
      if (filters.size() == 1) {
        auto &f = filters[0];
        auto constant=f.constant;
        auto compareCol = counted == 2 ? relation.compressColumns[f.filterColumn.colId][0] : relation.columns[f.filterColumn.colId];
        switch (f.comparison) {
          case FilterInfo::Comparison::Equal: {
            for (uint64_t i = start; i < end; i++) {
              if (compareCol[i]==constant) {
                for (unsigned cId = 0; cId < colSize; cId++) {
                  localResults[cId].push_back(inputData[cId][i]);
                }
              }
            }
            break;
          }
          case FilterInfo::Comparison::Greater: {
            for (uint64_t i = start; i < end; i++) {
              if (compareCol[i] > constant) {
                for (unsigned cId = 0; cId < colSize; cId++) {
                  localResults[cId].push_back(inputData[cId][i]);
                }
              }
            }
            break;
          }
          case FilterInfo::Comparison::Less: {
            for (uint64_t i = start; i < end; i++) {
              if (compareCol[i] < constant) {
                for (unsigned cId = 0; cId < colSize; cId++) {
                  localResults[cId].push_back(inputData[cId][i]);
                }
              }
            }
            break;
          }
        }
      } else {
        bool pass=true;
        for (uint64_t i = start; i < end; i++) {
          for (auto& f : filters) {
            pass&=applyFilter(i,f);
          }
          if (pass) {
            for (unsigned cId = 0; cId < colSize; cId++) {
              localResults[cId].push_back(inputData[cId][i]);
            }
          }
          pass = true;
        }
      }
      // add to results
      for (unsigned cId = 0; cId < colSize; cId++) {
        results[cId].addTuples(filterid, localResults[cId].data(), localResults[cId].size());
      }
      return localResults[0].size();
    }));
    start += len;
  }

  size_t pcnt = 0;
  for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
    pcnt += x.get();
  });
  resultSize = pcnt;
  for (unsigned cId = 0; cId < inputData.size(); cId++) {
    results[cId].fix();
  }
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo &info)
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

    requestedColumns.emplace(info);
  }
  return true;
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

  auto &leftInputData=left->getResults();
  auto &rightInputData=right->getResults();

  // Resolve the input columns
  unsigned resColId=0;
  for (auto& info : requestedColumnsLeft) {
    select2ResultColId[info]=resColId++;
  }
  for (auto& info : requestedColumnsRight) {
    select2ResultColId[info]=resColId++;
  }

  if (left->resultSize == 0) {
#if PRINT_LOG
    end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
    return;
  }

  if (requestedColumnsLeft.size() == 0 || 
      (requestedColumnsLeft.size() == 1 && requestedColumnsLeft[0] == pInfo.left)) {
    cntBuilding = true;
  }

  if (requestedColumnsLeft.size() + requestedColumnsRight.size() == 1) {
    auto &sInfo = requestedColumnsLeft.size() == 1 ? requestedColumnsLeft[0] : requestedColumnsRight[0];
    counted = Joiner::relations[sInfo.relId].needCompress[sInfo.colId];
  }

  if ((left->counted || right->counted || cntBuilding) && !counted) {
    counted = 2;
  }

  // log_print("leftBinding: {}, leftColId: {}  rightBinding: {}, rightColId: {}\n", pInfo.left.binding, pInfo.left.colId, pInfo.right.binding, pInfo.right.colId);

  colId[0]=left->resolve(pInfo.left);
  colId[1]=right->resolve(pInfo.right);
  auto leftResultSize = left->resultSize;
  auto rightResultSize = right->resultSize;

  // use nest loop join directly
  if (leftResultSize <= 8) {
    tmpResults.emplace_back();
    std::vector<std::vector<uint64_t>> leftData(leftInputData.size(), std::vector<uint64_t>(leftResultSize));
    std::vector<Column<uint64_t>::Iterator> colIt;
    for (unsigned i = 0; i < leftInputData.size(); i++) {
      colIt.push_back(leftInputData[i].begin(0));
    }

    uint64_t offset = 0;
    for (uint64_t i = 0; i < leftResultSize; i++) {
      for (unsigned j = 0; j < leftInputData.size(); j++) {
        leftData[j][offset] = *(colIt[j]);
        ++(colIt[j]);
      }
      offset ++;
    }

    // cerr << leftResultSize << endl;
    auto task_cnt = std::min((uint32_t)(rightResultSize / MIN_PARTITION_TABLE_SIZE), MAX_TASK_CNT);
    if (task_cnt == 0) task_cnt = 1;
    tmpResults[0].resize(task_cnt);
    for (int i = 0; i < requestedColumns.size(); i++) {
      results.emplace_back(task_cnt);
    }
    if (counted) {
      results.emplace_back(task_cnt);
    }
    uint64_t step = rightResultSize / task_cnt;
    uint64_t remain = rightResultSize % task_cnt;
    std::vector<std::future<size_t>> vf;
    uint64_t start = 0;
    for (int i = 0; i < task_cnt; i++) {
      uint64_t len = step;
      if (remain) {
        len++;
        remain--;
      }
      vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, &leftData, leftResultSize, taskid = i, start, end = start+len]() -> size_t {
        uint64_t *leftKeyColumn = leftData[colId[0]].data();
        auto &rightData = right->getResults();
        auto rightIter = rightData[colId[1]].begin(start);
        std::vector<Column<uint64_t>::Iterator> copyRightIters;
        vector<uint64_t*> copyLeftData;
        vector<vector<uint64_t>> &localResults = tmpResults[0][taskid];
        unsigned leftColSize = requestedColumnsLeft.size();
        unsigned rightColSize = requestedColumnsRight.size();
        unsigned resultColSize = requestedColumns.size();

        for (unsigned j = 0; j < requestedColumns.size(); j++) {
          localResults.emplace_back();
        }
        if (counted) {
          localResults.emplace_back();
        }

        for (auto &info: requestedColumnsLeft) {
          copyLeftData.push_back(leftData[left->resolve(info)].data());
        }
        if (left->counted) {
          copyLeftData.push_back(leftData.back().data());
        }

        for (auto &info: requestedColumnsRight) {
          copyRightIters.push_back(rightData[right->resolve(info)].begin(start));
        }
        if (right->counted) {
          copyRightIters.push_back(rightData.back().begin(start));
        }

        if (!isParentSum()) {
          for (uint64_t j = start; j < end; j++, ++rightIter) {
            for (uint64_t k = 0; k < leftResultSize; k++) {
              if (leftKeyColumn[k] == *rightIter) {
                unsigned relColId=0;
                for (unsigned cId=0;cId<leftColSize;++cId)
                  localResults[relColId++].push_back(copyLeftData[cId][k]);
                for (unsigned cId=0;cId<rightColSize;++cId)
                  localResults[relColId++].push_back(*copyRightIters[cId]);
                if (counted) {
                  uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][k] : 1;
                  uint64_t rightCnt= right->counted ? *copyRightIters[rightColSize] : 1;
                  localResults[relColId].push_back(leftCnt * rightCnt);
                }
              }
            }
            for (unsigned k = 0; k < copyRightIters.size(); k++) {
              ++(copyRightIters[k]);
            }
          }
        } else {  // push down
          unsigned relColId=0;
          for (unsigned cId=0;cId<leftColSize;++cId)
            localResults[relColId++].push_back(0);
          for (unsigned cId=0;cId<rightColSize;++cId)
            localResults[relColId++].push_back(0);
          if (counted) {
            localResults[relColId].push_back(1);
          }

          for (uint64_t j = start; j < end; j++, ++rightIter) {
            for (uint64_t k = 0; k < leftResultSize; k++) {
              if (leftKeyColumn[k] == *rightIter) {
                unsigned relColId=0;
                if (counted) {
                  uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][k] : 1;
                  uint64_t rightCnt= right->counted ? *copyRightIters[rightColSize] : 1;
                  uint64_t factor = leftCnt * rightCnt;
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][k] * factor;
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += *copyRightIters[cId] * factor;
                } else {
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][k];
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += *copyRightIters[cId];
                }
              }
            }
            for (unsigned k = 0; k < copyRightIters.size(); k++) {
              ++(copyRightIters[k]);
            }
          }
        }

        if (localResults[0].size() == 0) {
          return 0;
        }
        for (unsigned i = 0; i < resultColSize; i++) {
          results[i].addTuples(taskid, localResults[i].data(), localResults[i].size());
        }
        if (counted) {
          results[resultColSize].addTuples(taskid, localResults[resultColSize].data(), localResults[resultColSize].size());
        }
        return localResults[0].size();
      }));
      start += len;
    }

    size_t pcnt = 0;
    for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
      pcnt += x.get();
    });
    resultSize = pcnt;

    for (unsigned cId=0;cId<requestedColumns.size();++cId) {
      results[cId].fix();
    }
    if (counted) {
      results[requestedColumns.size()].fix();
    }

    return;
  }

  // cerr << "leftResultSize: " << leftResultSize << "; rightResultSize: " << rightResultSize << endl;

  partitionCnt = CNT_PARTITIONS(left->resultSize * 8 * 2, PARTITION_SIZE);  // uint64*2(key, value)
  if (partitionCnt < 32) {
    partitionCnt = 32;
  }
  // partitionCnt = next_pow_of_2(partitionCnt);

  partitionTable[0].reserve(left->getResultsSize());
  partitionTable[1].reserve(right->getResultsSize());

  for (uint64_t i = 0; i < partitionCnt; i++) {
    partition[0].emplace_back();
    partition[1].emplace_back();
    for (unsigned j = 0; j < leftInputData.size(); j++) {
      partition[0][i].emplace_back();
    }
    for (unsigned j = 0; j < rightInputData.size(); j++) {
      partition[1][i].emplace_back();
    }
    tmpResults.emplace_back();
  }

  auto leftTaskCnt = std::min((uint32_t)(leftResultSize / MIN_PARTITION_TABLE_SIZE), MAX_TASK_CNT);
  auto rightTaskCnt = std::min((uint32_t)(rightResultSize / MIN_PARTITION_TABLE_SIZE), MAX_TASK_CNT);
  if (leftTaskCnt == 0) leftTaskCnt = 1;
  if (rightTaskCnt == 0) rightTaskCnt = 1;
  taskCnt[0] = leftTaskCnt;
  taskCnt[1] = rightTaskCnt;
  taskLength[0] = leftResultSize / leftTaskCnt;
  taskLength[1] = rightResultSize / rightTaskCnt;
  taskRemain[0] = leftResultSize % leftTaskCnt;
  taskRemain[1] = rightResultSize % rightTaskCnt;
  histograms[0].resize(leftTaskCnt);
  histograms[1].resize(rightTaskCnt);

  // Partition Phase
  std::vector<std::future<void>> vf;
  vf.push_back(std::async(std::launch::async | std::launch::deferred, [this]() {
    this->partitionLeft();
  }));
  vf.push_back(std::async(std::launch::async | std::launch::deferred, [this](){
    this->partitionRight();
  }));
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });

  // Determain sub joins
  vector<unsigned> subJoins;
  for (int i = 0; i < partitionCnt; i++) {
    if (partitionLength[0][i] != 0 && partitionLength[1][i] != 0) {
      // cerr << "partition " << i << endl;
      subJoins.push_back(i);
    }
  }
  if (subJoins.size() == 0) {
    for (unsigned cId=0; cId < requestedColumns.size(); cId++) {
      results[cId].fix();
    }
    if (counted) {
      results.back().fix();
    }
    return;
  }

  if (cntBuilding) {
    hashTableCnts.resize(partitionCnt, NULL);
  } else {
    hashTableIndices.resize(partitionCnt, NULL);
  }

  // Do build and probe for each bucket
  vf.clear();
  for (auto idx: subJoins) {
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, idx]() {
      this->buildAndProbe(idx);
    }));
  }
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });

  for (unsigned cId=0;cId<requestedColumns.size();++cId) {
    results[cId].fix();
  }
  if (counted) {
    results[requestedColumns.size()].fix();
  }

#if PRINT_LOG
  end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
  log_print("join resultSize: {}, run time {} ms\n", resultSize, end.count() - start.count());
#endif
}
//---------------------------------------------------------------------------
void Join::partitionLeft()
  // Partition Left
{
  uint64_t start = 0;
  uint64_t remain = taskRemain[0];
  auto task_cnt = taskCnt[0];
  std::vector<std::future<void>> vf;
  for (unsigned i = 0; i < task_cnt; i++) {
    uint64_t len = taskLength[0];
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, taskid = i, start, end = start + len]() {
      auto &inputData = left->getResults();
      auto &keyColumn = inputData[colId[0]];
      histograms[0][taskid].resize(partitionCnt);
      auto it = keyColumn.begin(start);
      for (uint64_t i = start; i < end; i++, ++it) {
        // cerr << "value: " << *it << ", bucket: " << RADIX_HASH(*it, partitionCnt) << endl;
        histograms[0][taskid][RADIX_HASH(*it, partitionCnt)]++;
      }
    }));
    start += len;
  }
  // wait the statistics
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });

  for (int i = 0; i < partitionCnt; i++) {
    partitionLength[0].push_back(0);
    for (int j = 0; j < task_cnt; j++) {
      partitionLength[0][i] += histograms[0][j][i];
      if (j != 0) { // accumulate the counts
        histograms[0][j][i] += histograms[0][j-1][i];
      }
    }
  }

  // split the partition
  auto columnCnt = left->getResults().size();
  uint64_t* partAddress = partitionTable[0].data();
  for (uint64_t i = 0; i < partitionCnt; i++) {
    uint64_t tupleCnt = partitionLength[0][i];
    // cerr << "tuple cnt: " << tupleCnt << endl;
    for (unsigned j = 0; j < columnCnt; j++) {
      partition[0][i][j] = partAddress + j * tupleCnt;
    }
    partAddress += columnCnt * tupleCnt;
  }
  // put the data to its bucket
  vf.clear();
  start = 0;
  remain = taskRemain[0];
  for (unsigned i = 0; i < task_cnt; i++) {
    uint64_t len = taskLength[0];
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, taskid = i, start, end = start + len]() {
      auto &inputData = left->getResults();
      auto &keyColumn = inputData[colId[0]];
      auto it = keyColumn.begin(start);
      vector<Column<uint64_t>::Iterator> colIt;
      vector<uint64_t> insertOffs(partitionCnt, 0);
      for (unsigned i = 0; i < inputData.size(); i++) {
        colIt.push_back(inputData[i].begin(start));
      }

      for (uint64_t i = start; i < end; i++, ++it) {
        uint64_t hashResult = RADIX_HASH(*it, partitionCnt);
        uint64_t insertBase;
        uint64_t insertOff = insertOffs[hashResult]++;
        if (UNLIKELY(taskid == 0))
          insertBase = 0;
        else
          insertBase = histograms[0][taskid - 1][hashResult];

        for (unsigned j = 0; j < inputData.size(); j++) {
          partition[0][hashResult][j][insertBase + insertOff] = *(colIt[j]);
          ++(colIt[j]);
        }
      }
    }));
    start += len;
  }
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });
}
//---------------------------------------------------------------------------
void Join::partitionRight()
  // Partition Right
{
  uint64_t start = 0;
  uint64_t remain = taskRemain[1];
  auto task_cnt = taskCnt[1];
  std::vector<std::future<void>> vf;
  for (unsigned i = 0; i < task_cnt; i++) {
    uint64_t len = taskLength[1];
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, taskid = i, start, end = start + len]() {
      auto &inputData = right->getResults();
      auto &keyColumn = inputData[colId[1]];
      histograms[1][taskid].resize(partitionCnt);
      auto it = keyColumn.begin(start);
      for (uint64_t i = start; i < end; i++, ++it) {
        histograms[1][taskid][RADIX_HASH(*it, partitionCnt)]++;
      }
    }));
    start += len;
  }
  // wait the statistics
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });

  for (int i = 0; i < partitionCnt; i++) {
    partitionLength[1].push_back(0);
    for (int j = 0; j < task_cnt; j++) {
      partitionLength[1][i] += histograms[1][j][i];
      if (j != 0) { // accumulate the counts
        histograms[1][j][i] += histograms[1][j-1][i];
      }
    }
  }

  resultIndex.push_back(0);
  for (int i = 0; i < partitionCnt; i++) {
    uint64_t limitRight = partitionLength[1][i];
    uint32_t probe_cnt = std::min((uint32_t)(limitRight / MIN_PARTITION_TABLE_SIZE), MAX_TASK_CNT);
    if (probe_cnt == 0) probe_cnt = 1;
    uint64_t probe_len = limitRight / probe_cnt;
    uint64_t probe_remain = limitRight % probe_cnt;
    probingCnt.push_back(probe_cnt);
    probingLength.push_back(probe_len);
    ProbingRemain.push_back(probe_remain);
    resultIndex.push_back(resultIndex.back() + probe_cnt);
  }
  unsigned probingResultCnt = resultIndex[partitionCnt];
  for (int i = 0; i < requestedColumns.size(); i++) {
    results.emplace_back(probingResultCnt);
  }
  if (counted) {
    results.emplace_back(probingResultCnt);
  }

  // split the partition
  auto columnCnt = right->getResults().size();
  uint64_t* partAddress = partitionTable[1].data();
  for (uint64_t i = 0; i < partitionCnt; i++) {
    uint64_t tupleCnt = partitionLength[1][i];
    for (unsigned j = 0; j < columnCnt; j++) {
      partition[1][i][j] = partAddress + j * tupleCnt;
    }
    partAddress += columnCnt * tupleCnt;
  }
  vf.clear();
  start = 0;
  remain = taskRemain[1];
  for (unsigned i = 0; i < task_cnt; i++) {
    uint64_t len = taskLength[1];
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, taskid = i, start, end = start + len]() {
      auto &inputData = right->getResults();
      auto &keyColumn = inputData[colId[1]];
      auto it = keyColumn.begin(start);
      vector<Column<uint64_t>::Iterator> colIt;
      vector<uint64_t> insertOffs(partitionCnt, 0);
      for (unsigned i = 0; i < inputData.size(); i++) {
        colIt.push_back(inputData[i].begin(start));
      }

      for (uint64_t i = start; i < end; i++, ++it) {
        uint64_t hashResult = RADIX_HASH(*it, partitionCnt);
        uint64_t insertBase;
        uint64_t insertOff = insertOffs[hashResult]++;
        if (UNLIKELY(taskid == 0))
          insertBase = 0;
        else
          insertBase = histograms[1][taskid - 1][hashResult];

        for (unsigned j = 0; j < inputData.size(); j++) {
          partition[1][hashResult][j][insertBase + insertOff] = *(colIt[j]);
          ++(colIt[j]);
        }
      }
    }));
    start += len;
  }
  for_each(vf.begin(), vf.end(), [](future<void> &x) {
    x.wait();
  });
}
//---------------------------------------------------------------------------
void Join::buildAndProbe(unsigned bucket)
  // Build and probe for bucket
{
  auto &leftPart = partition[0][bucket];
  auto leftPartLen = partitionLength[0][bucket];
  auto &rightPart = partition[1][bucket];
  // auto rightPartLen = partitionLength[1][bucket];
  if (leftPartLen > HASH_THRESHOLD) {
    uint64_t *leftKeyColumn = leftPart[colId[0]];
    if (cntBuilding) {
      hashTableCnts[bucket] = new unordered_map<uint64_t, uint64_t>();
      auto hashTable = hashTableCnts[bucket];
      hashTable->reserve(leftPartLen * 2);
      for (uint64_t i = 0; i < leftPartLen; i++) {
        if (left->counted) {
          (*hashTable)[leftKeyColumn[i]] += leftPart[1][i];
        } else {
          (*hashTable)[leftKeyColumn[i]] ++;
        }
      }
    } else {
      hashTableIndices[bucket] = new HT();
      auto hashTable = hashTableIndices[bucket];
      hashTable->reserve(leftPartLen * 2);
      for (uint64_t i = 0; i < leftPartLen; i++) {
        hashTable->emplace(std::make_pair(leftKeyColumn[i], i));
      }
    }
  }

  unsigned task_cnt = probingCnt[bucket];
  uint64_t len = probingLength[bucket];
  unsigned remain = ProbingRemain[bucket];
  tmpResults[bucket].resize(task_cnt);
  std::vector<std::future<size_t>> vf;
  uint64_t start = 0;
  for (int i = 0; i < task_cnt; i++) {
    uint64_t step = len;
    if (remain) {
      step ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, bucket, probeid = i, &leftPart, leftPartLen, &rightPart, start, end = start + step]() -> size_t {
      uint64_t *leftKeyColumn = leftPart[colId[0]];
      uint64_t *rightKeyColumn = rightPart[colId[1]];
      vector<uint64_t*> copyLeftData, copyRightData;
      vector<vector<uint64_t>> &localResults = tmpResults[bucket][probeid];
      unsigned leftColSize = requestedColumnsLeft.size();
      unsigned rightColSize = requestedColumnsRight.size();
      unsigned resultColSize = requestedColumns.size();
      unordered_map<uint64_t, uint64_t>* hashTableC = NULL;
      unordered_multimap<uint64_t, uint64_t>* hashTable = NULL;

      if (cntBuilding) {
        hashTableC = this->hashTableCnts[bucket];
      } else {
        hashTable = this->hashTableIndices[bucket];
      }

      for (unsigned j = 0; j < requestedColumns.size(); j++) {
        localResults.emplace_back();
      }
      if (counted) {
        localResults.emplace_back();
      }

      for (auto &info: requestedColumnsLeft) {
        copyLeftData.push_back(leftPart[left->resolve(info)]);
      }
      if (left->counted) {
        copyLeftData.push_back(leftPart.back());
      }

      for (auto &info: requestedColumnsRight) {
        copyRightData.push_back(rightPart[right->resolve(info)]);
      }
      if (right->counted) {
        copyRightData.push_back(rightPart.back());
      }

      if (!isParentSum()) {
        if (leftPartLen > HASH_THRESHOLD) {
          if (cntBuilding) {
            for (uint64_t i = start; i < end; i++) {
              auto rightKey = rightKeyColumn[i];
              if (hashTableC->find(rightKey) == hashTableC->end())
                continue;
              uint64_t leftCnt = hashTableC->at(rightKey);
              uint64_t rightCnt = right->counted ? copyRightData.back()[i] : 1;
              if (counted == 1) {
                auto data = (leftColSize == 1) ? rightKey : copyRightData[0][i];
                localResults[0].push_back(data);
                localResults[1].push_back(leftCnt * rightCnt);
              } else {
                unsigned relColId = 0;
                for (unsigned cId=0;cId<leftColSize;++cId)
                  localResults[relColId++].push_back(rightKey);
                for (unsigned cId=0;cId<rightColSize;++cId)
                  localResults[relColId++].push_back(copyRightData[cId][i]);
                if (counted){
                  localResults[relColId].push_back(leftCnt * rightCnt);
                }
              }
            }
          } else {
            for (uint64_t i = start; i < end; i++) {
              auto rightKey=rightKeyColumn[i];
              auto range=hashTable->equal_range(rightKey);
              for (auto iter=range.first;iter!=range.second;++iter) {
                unsigned relColId=0;
                for (unsigned cId=0;cId<leftColSize;++cId)
                  localResults[relColId++].push_back(copyLeftData[cId][iter->second]);
                for (unsigned cId=0;cId<rightColSize;++cId)
                  localResults[relColId++].push_back(copyRightData[cId][i]);
                if (counted){
                  uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][iter->second] : 1;
                  uint64_t rightCnt = right->counted ? copyRightData[rightColSize][i] : 1;
                  localResults[relColId].push_back(leftCnt * rightCnt);
                }
              }
            }
          }
        } else {  // nested loop join
          for (uint64_t i = 0; i < leftPartLen; i++) {
            for (uint64_t j = start; j < end; j++) {
              if (leftKeyColumn[i] == rightKeyColumn[j]) {
                unsigned relColId=0;
                for (unsigned cId=0;cId<leftColSize;++cId)
                  localResults[relColId++].push_back(copyLeftData[cId][i]);
                for (unsigned cId=0;cId<rightColSize;++cId)
                  localResults[relColId++].push_back(copyRightData[cId][j]);
                if (counted){
                  uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
                  uint64_t rightCnt= right->counted ? copyRightData[rightColSize][j] : 1;
                  localResults[relColId].push_back(leftCnt * rightCnt);
                }
              }
            }
          }
        }
      } else {  // push_down
        if (leftPartLen > HASH_THRESHOLD) {
          if (cntBuilding) {
            if (counted == 1) {
              localResults[0].push_back(0);
              localResults[1].push_back(1);
            } else {
              unsigned relColId = 0;
              for (unsigned cId=0;cId<leftColSize;++cId)
                localResults[relColId++].push_back(0);
              for (unsigned cId=0;cId<rightColSize;++cId)
                localResults[relColId++].push_back(0);
              if (counted) {
                localResults[relColId].push_back(1);
              }
            }
            for (uint64_t i = start; i < end; i++) {
              auto rightKey = rightKeyColumn[i];
              if (hashTableC->find(rightKey) == hashTableC->end())
                continue;
              uint64_t leftCnt = hashTableC->at(rightKey);
              uint64_t rightCnt = right->counted ? copyRightData.back()[i] : 1;
              uint64_t factor = leftCnt * rightCnt;
              if (counted == 1) {
                auto data = (leftColSize == 1) ? rightKey : copyRightData[0][i];
                localResults[0][0] += data * factor;
              } else {
                unsigned relColId = 0;
                if (counted) {
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += rightKey * factor;
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += copyRightData[cId][i] * factor;
                } else {
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += rightKey;
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += copyRightData[cId][i];
                }
                
              }
            }
          } else {
            unsigned relColId=0;
            for (unsigned cId=0;cId<leftColSize;++cId)
              localResults[relColId++].push_back(0);
            for (unsigned cId=0;cId<rightColSize;++cId)
              localResults[relColId++].push_back(0);
            if (counted) {
              localResults[relColId].push_back(1);
            }
            for (uint64_t i = start; i < end; i++) {
              auto rightKey = rightKeyColumn[i];
              auto range = hashTable->equal_range(rightKey);
              uint64_t rightCnt = right->counted ? copyRightData[rightColSize][i] : 1;
              for (auto it = range.first; it != range.second; ++it) {
                relColId=0;
                if (counted) {
                  uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][it->second] : 1;
                  uint64_t factor = leftCnt * rightCnt;
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][it->second] * factor;
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += copyRightData[cId][i] * factor;
                } else {
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][it->second];
                  for (unsigned cId=0;cId<rightColSize;++cId)
                    localResults[relColId++][0] += copyRightData[cId][i];
                }
              }
            }
          }
        } else {  // nest loop join
          unsigned relColId=0;
          for (unsigned cId=0;cId<leftColSize;++cId)
            localResults[relColId++].push_back(0);
          for (unsigned cId=0;cId<rightColSize;++cId)
            localResults[relColId++].push_back(0);
          if (counted) {
            localResults[relColId].push_back(1);
          }
          for (uint64_t i = 0; i < leftPartLen; i++) {
            uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
            for (uint64_t j = start; j < end; j++) {
              if (leftKeyColumn[i] == rightKeyColumn[j]) {
                relColId=0;
                if (counted) {
                  uint64_t rightCnt= right->counted ? copyRightData[rightColSize][j] : 1;
                  uint64_t factor = leftCnt * rightCnt;
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][i] * factor;
                  for (unsigned cId=0;cId<rightColSize;++cId) {
                    localResults[relColId++][0] += copyRightData[cId][j] * factor;
                  }
                } else {
                  for (unsigned cId=0;cId<leftColSize;++cId)
                    localResults[relColId++][0] += copyLeftData[cId][i];
                  for (unsigned cId=0;cId<rightColSize;++cId) {
                    localResults[relColId++][0] += copyRightData[cId][j];
                  }
                }
              }
            }
          }
        }
      }
      
      if (localResults[0].size() == 0) {
        return 0;
      }
      for (unsigned i = 0; i < resultColSize; i++) {
        results[i].addTuples(resultIndex[bucket] + probeid, localResults[i].data(), localResults[i].size());
      }
      if (counted) {
        results[resultColSize].addTuples(resultIndex[bucket] + probeid, localResults[resultColSize].data(), localResults[resultColSize].size());
      }
      return localResults[0].size();
    }));
    start += step;
  }
  size_t pcnt = 0;
  for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
    pcnt += x.get();
  });
  __sync_fetch_and_add(&resultSize, pcnt);
  // cerr << "join result size: " << resultSize << endl;
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo &info)
  // Require a column and add it to results
{
  if (requiredIUs.count(info))
    return true;
  if(input->require(info)) {
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
  auto &inputData = input->getResults();
  if (input->resultSize == 0) {
    return;
  }
  auto inputResultSize = input->resultSize;
  uint32_t task_cnt = std::min((uint32_t)(inputResultSize / MIN_PROBE_ITEM_CNT), MAX_BUILD_TASK_CNT);
  if (task_cnt == 0) task_cnt = 1;
  uint32_t step = inputResultSize / task_cnt;
  uint32_t remain = inputResultSize % task_cnt;

  // log_print("SelfJoin: input->resultSize {}\n", inputResultSize);

  for (auto& iu : requiredIUs) {
    auto id=input->resolve(iu);
    copyData.emplace_back(&inputData[id]);
    select2ResultColId.emplace(iu,copyData.size()-1);
    results.emplace_back(task_cnt);
  }

  if (input->counted){
    if (!counted){
      counted = 2;
    }
    copyData.emplace_back(&inputData.back());
  }
  if (counted){
    results.emplace_back(task_cnt);
  }

  tmpResults.resize(task_cnt);
  std::vector<std::future<size_t>> vf;
  uint64_t start = 0;
  for (int i = 0; i < task_cnt; i++) {
    uint64_t len = step;
    if (remain) {
      len ++;
      remain --;
    }
    vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, taskid = i, start, end = start + len]() {
      auto& inputData=input->getResults();
      vector<vector<uint64_t>>& localResults = tmpResults[taskid];
      auto leftColId=input->resolve(pInfo.left);
      auto rightColId=input->resolve(pInfo.right);

      auto leftColIt=inputData[leftColId].begin(start);;
      auto rightColIt=inputData[rightColId].begin(start);;

      vector<Column<uint64_t>::Iterator> colIt;
      unsigned colSize = copyData.size();
      for (int j=0; j<colSize; j++) {
        localResults.emplace_back();
      }
      if (counted && !input->counted){
          localResults.emplace_back();
      }
      for (unsigned i=0; i<colSize; i++) {
        colIt.push_back(copyData[i]->begin(start));
      }
      std::unordered_map<uint64_t, uint64_t> cntMap;
      if (!this->isParentSum()) {
        for (uint64_t i = start; i < end; i++) {
          if (*leftColIt==*rightColIt) {
            if (counted == 1) {
              auto dup = cntMap.find(*(colIt[0]));
              uint64_t dupCnt = (input->counted) ? *(colIt[1]) : 1;
              if (dup != cntMap.end()){
                localResults[1][dup->second] += dupCnt;
              } else {
                localResults[0].push_back(*(colIt[0]));
                localResults[1].push_back(dupCnt);
                cntMap.insert(dup, pair<uint64_t, uint64_t>(*(colIt[0]), localResults[1].size()-1));
              }
            } else {
              for (unsigned cId=0;cId<colSize;++cId) {
                localResults[cId].push_back(*(colIt[cId]));
              }
            }
          }
          ++leftColIt;
          ++rightColIt;
          for (unsigned j=0; j<colSize; j++) {
            ++colIt[j];
          }
        }
      } else {
        if (counted == 1) {
          localResults[0].push_back(0);
          localResults[1].push_back(1);
        } else {
          for (unsigned cId=0;cId<colSize;++cId) {
            localResults[cId].push_back(0);
          }
        }

        for (uint64_t i = start; i < end; i++, ++leftColIt, ++rightColIt) {
          if (*leftColIt == *rightColIt) {
            // log_print("SelfJoin: leftKe: {} rightkey: {}\n", *leftColIt, *rightColIt);
            if (counted == 1) {
              uint64_t dupCnt = (input->counted) ? *(colIt[1]) : 1;
              localResults[0][0] += (*(colIt[0])) * dupCnt;
            } else {
              for (unsigned cId=0;cId<colSize;++cId) {
                localResults[cId][0] += *(colIt[cId]);
              }
            }
          }
          for (unsigned j=0; j<colSize; j++) {
            ++colIt[j];
          }
        }
      }
      for (int i=0; i<colSize; i++) {
        results[i].addTuples(taskid, localResults[i].data(), localResults[i].size());
      }
      if (counted && !input->counted){
        results[colSize].addTuples(taskid, localResults[colSize].data(), localResults[colSize].size());
      }
      return localResults[0].size();
    }));
    start += len;
  }
  size_t pcnt = 0;
  for_each(vf.begin(), vf.end(), [&pcnt](future<size_t> &x) {
    pcnt += x.get();
  });
  resultSize = pcnt;
  for (unsigned cId=0;cId<copyData.size();++cId) {
    results[cId].fix();
  }
  if (counted && !input->counted) {
    results[copyData.size()].fix();
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
  checkSums.resize(colInfo.size(), 0);
  // cerr << "checksum resultSize: " << input->resultSize << endl;

  if (input->resultSize == 0) {
    return;
  }

  auto& inputData=input->getResults();
  int sumIndex = 0;
  for (auto &sInfo: colInfo) {
    auto colId = input->resolve(sInfo);
    auto inputColIt = inputData[colId].begin(0);
    uint64_t sum = 0;
    for (uint64_t i= 0; i< input->resultSize; i++, ++inputColIt) {
      sum += (*inputColIt);
    }
    checkSums[sumIndex++] = sum;
  }
  // if (input->resultSize > MIN_PROBE_ITEM_CNT) {
  //   cerr << "resultSize: " << input->resultSize << endl;
  // }
  // uint64_t task_cnt = std::min((uint32_t)(input->resultSize / MIN_PROBE_ITEM_CNT), MAX_TASK_CNT);
  // if (task_cnt == 0) task_cnt = 1;
  // uint64_t step = input->resultSize / task_cnt;
  // uint64_t remain = input->resultSize % task_cnt;

  // uint64_t start = 0;
  // std::vector<std::future<void>> vf;
  // for (int i = 0; i < task_cnt; i++) {
  //   uint64_t len = step;
  //   if (remain) {
  //     remain --;
  //     len ++;
  //   }
  //   vf.push_back(std::async(std::launch::async | std::launch::deferred, [this, start, end = start + len]() {
  //     auto& inputData=input->getResults();
  //     int sumIndex = 0;
  //     for (auto &sInfo: colInfo) {
  //       auto colId = input->resolve(sInfo);
  //       auto inputColIt = inputData[colId].begin(start);
  //       uint64_t sum = 0;
  //       for (uint64_t i= start; i< end; i++, ++inputColIt) {
  //         sum += (*inputColIt);
  //       }
  //       // cerr << input->counted << endl;
  //       // if (input->counted) {
  //       //   auto countColIt = inputData.back().begin(start);
  //       //   for (int i = start; i < end; i++, ++inputColIt, ++countColIt) {
  //       //     sum += (*inputColIt) * (*countColIt);
  //       //   }
  //       // } else {
  //       //   for (int i=start; i < end; i++,++inputColIt){
  //       //     sum += (*inputColIt);
  //       //   }
  //       // }
  //       __sync_fetch_and_add(&checkSums[sumIndex++], sum);
  //     }
  //   }));
  //   start += len;
  // }

  // for_each(vf.begin(), vf.end(), [](future<void> &x) {
  //   x.wait();
  // });
}
//---------------------------------------------------------------------------
