#include <Operators.hpp>
#include <cassert>
#include <iostream>
#include <queue>
#include <algorithm>
#include "Log.hpp"
//---------------------------------------------------------------------------
using namespace std;
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
bool FilterScan::isRangeResult() {
  return isRange;
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
  // do the apply for each filter
  std::sort(filters.begin(), filters.end(), [](const FilterInfo &left, const FilterInfo &right) { return left.eCost < right.eCost; });
  vector<uint64_t> index{0, relation.rowCount};
  vector<uint64_t> tmp;
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
          for (int64_t i = index[0]; i < index[1]; i++) {
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
          for (int64_t i = index[0]; i < index[1]; i++) {
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
          for (int64_t i = index[0]; i < index[1]; i++) {
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
  // out.print("index : {}\n", fmt::join(index, ", "));
  // copy the data for test now
  if (isRange) {
    for (uint64_t i = index[0]; i < index[1]; i++) {
      copy2Result(i);
    }
  } else {
    for (auto i : index) {
      copy2Result(i);
    }
  }
  this->resultSize = isRange ? index[1] - index[0] : index.size();
  this->index.swap(index);


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

  // out.print("index2 : {}\n", fmt::join(idxvec, ", "));

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
//---------------------------------------------------------------------------
void Join::run()
  // Run
{
  left->require(pInfo.left);
  right->require(pInfo.right);
  left->run();
  right->run();


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

  auto leftColId=left->resolve(pInfo.left);
  auto rightColId=right->resolve(pInfo.right);

  // Build phase
  auto leftKeyColumn=leftInputData[leftColId];
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }
  // Probe phase
  auto rightKeyColumn=rightInputData[rightColId];
  for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
    auto rightKey=rightKeyColumn[i];
    auto range=hashTable.equal_range(rightKey);
    for (auto iter=range.first;iter!=range.second;++iter) {
      copy2Result(iter->second,i);
    }
  }
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
