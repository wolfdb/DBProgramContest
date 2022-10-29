#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include "Relation.hpp"
#include "Parser.hpp"
#include "Log.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace boost::histogram;
//---------------------------------------------------------------------------
void Relation::storeRelation(const string& fileName)
  // Stores a relation into a binary file
{
  ofstream outFile;
  outFile.open(fileName,ios::out|ios::binary);
  outFile.write((char*)&rowCount,sizeof(rowCount));
  auto numColumns=columns.size();
  outFile.write((char*)&numColumns,sizeof(size_t));
  for (auto c : columns) {
    outFile.write((char*)c,rowCount*sizeof(uint64_t));
  }
  outFile.close();
}
//---------------------------------------------------------------------------
void Relation::storeRelationCSV(const string& fileName)
  // Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
{
  ofstream outFile;
  outFile.open(fileName+".tbl",ios::out);
  for (uint64_t i=0;i<rowCount;++i) {
    for (auto& c : columns) {
      outFile << c[i] << '|';
    }
    outFile << "\n";
  }
}
//---------------------------------------------------------------------------
void Relation::dumpSQL(const string& fileName,unsigned relationId)
  // Dump SQL: Create and load table (PostgreSQL)
{
  ofstream outFile;
  outFile.open(fileName+".sql",ios::out);
  // Create table statement
  outFile << "CREATE TABLE r" << relationId << " (";
  for (unsigned cId=0;cId<columns.size();++cId) {
    outFile << "c" << cId << " bigint" << (cId<columns.size()-1?",":"");
  }
  outFile << ");\n";
  // Load from csv statement
  outFile << "copy r" << relationId << " from 'r" << relationId << ".tbl' delimiter '|';\n";
}
//---------------------------------------------------------------------------
void Relation::calcSampleCount()
  // Calculate the sample count
{
  if (this->rowCount < DBCONTEST_PAGE_ITEM_COUNT) {
    this->sampleCount = this->rowCount;
    return;
  }

  if ((this->rowCount >> 4) < DBCONTEST_PAGE_ITEM_COUNT) {
    this->sampleCount = DBCONTEST_PAGE_ITEM_COUNT;
    return;
  }

  // sample 1/16 of the data or 1MB, which is small
  uint64_t sampleCount = this->rowCount >> 4;
  sampleCount = DBCONTEST_ITEM_COUNT_ALIGN(sampleCount);
  this->sampleCount = std::min(sampleCount, MAX_SAMPLE_ITEM_COUNT);
}
//---------------------------------------------------------------------------
void Relation::buildHistogram(int idx)
  // build histogram for column idx
{
  assert(idx < columns.size());
  uint64_t *column = this->columns[idx];
  uint64_t tmp_max = column[0];
  uint64_t tmp_min = column[0];
  uint64_t pre = column[0];
  uint64_t cnt = 1;
  bool sorted = true;
  bool asc = false;
  bool desc = false;
  for (int i = 1; i < this->sampleCount; i++) {
    uint64_t val = column[i];
    tmp_max = std::max(tmp_max, val);
    tmp_min = std::min(tmp_min, val);

    if (sorted) {
      if (val > pre) {
        asc = true;
      } else if (val < pre) {
        desc = true;
      }
      if (asc && desc) {
        sorted = false;
      }
      if (val != pre) {
        cnt ++;
      }
      pre = val;
    }
  }

  this->sample_maxs[idx] = tmp_max;
  this->sample_mins[idx] = tmp_min;

  if (sorted) {
    uint64_t lastVal = column[this->rowCount - 1];
    this->sorts[idx] = Sorted::Likely;
    if (asc) {
      this->orders[idx] = Order::ASC;
      this->maxs[idx] = lastVal;
    } else {
      this->orders[idx] = Order::DESC;
      this->mins[idx] = lastVal;
    }
    this->sample_distinct_count[idx] = cnt;
  } else {
    this->sorts[idx] = Sorted::False;
  }

#if 0
  // second scan, build histogram
  if (this->sorts[idx] == Sorted::Likely) {
    uint64_t lastVal = column[this->rowCount - 1];
    pre = column[0];
    cnt = 1;
    if (this->orders[idx] == Order::ASC) {
      assert(lastVal > this->sample_maxs[idx]);
      auto h = make_histogram(axis::regular<>(HISTOGRAM_BUCKET_CNT, this->sample_mins[idx], lastVal));
      h(pre);
      for (int i = 1; i < this->rowCount; i++) {
        uint64_t val = column[i];
        if (val < pre) {
          sorted = false;
          break;
        }
        h(column[i]);
        if (val > pre) {
          cnt ++;
        }
        pre = val;
      }
      this->histograms[idx].hist_ = std::move(h);
    } else {
      assert(lastVal < this->sample_mins[idx]);
      auto h = make_histogram(axis::regular<>(HISTOGRAM_BUCKET_CNT, lastVal, this->sample_maxs[idx]));
      h (pre);
      for (int i = 0; i < this->rowCount; i++) {
        uint64_t val = column[i];
        if (val > pre) {
          sorted = false;
          break;
        }
        h(column[i]);
        if (val < pre) {
          cnt ++;
        }
        pre = val;
      }
    }
    if (sorted == false) {
      this->sorts[idx] = Sorted::False;
    } else {
      this->sorts[idx] = Sorted::True;
      this->distinct_count[idx] = cnt;
      if (this->orders[idx] == Order::ASC) {
        this->mins[idx] = this->sample_mins[idx];
        this->maxs[idx] = lastVal;
      } else {
        this->mins[idx] = lastVal;
        this->maxs[idx] = this->sample_maxs[idx];
      }
    }
  }
#endif

  if (this->sorts[idx] == Sorted::False) {
    auto h = make_histogram(axis::regular<>(HISTOGRAM_BUCKET_CNT, this->sample_mins[idx], this->sample_maxs[idx]));
    auto &sample_distinctVal = this->sample_distinctVals[idx];
    for (int i = 0; i < this->sampleCount; i++) {
      h(column[i]);
      sample_distinctVal.insert(column[i]);
    }
    this->sample_histograms[idx].hist_ = std::move(h);
  }
}
//---------------------------------------------------------------------------
void Relation::printHistogram(int idx)
  // print histogram of column idx
{
  if (this->sorts[idx] == Sorted::True) {
    log_print("Max: {}, Min: {}\n", this->maxs[idx], this->mins[idx]);
    log_print("Sorted, order: {}\n", this->orders[idx] == Order::ASC ? "ASC" : "DESC");
    log_print("  distinct values cnt: {}\n", this->distinct_count[idx]);
    // auto &h = this->histograms[idx].hist_;
    // for (auto&& x : indexed(h, coverage::all)) {
    //   log_print("bin {} [{}, {}): {}\n", x.index(), x.bin().lower(), x.bin().upper(), *x);
    // }
  } else if (this->sorts[idx] == Sorted::Likely) {
    int tmpMax = this->orders[idx] == Order::ASC ? this->maxs[idx] : this->sample_maxs[idx];
    int tmpMin = this->orders[idx] == Order::ASC ? this->sample_mins[idx] : this->mins[idx];
    log_print("Max: {}, Min: {}\n", tmpMax, tmpMin);
    log_print(" order: {}\n", this->orders[idx] == Order::ASC ? "ASC" : "DESC");
    log_print("  distinct values cnt: {}\n", this->sample_distinct_count[idx]);
  } else {
    log_print("Max: {}, Min: {}\n", this->sample_maxs[idx], this->sample_mins[idx]);
    log_print("Sorted: {}\n", "False");
    log_print("  distinct values cnt: {}\n", this->sample_distinctVals[idx].size());
    // auto &h = this->sample_histograms[idx].hist_;
    // for (auto&& x : indexed(h, coverage::all)) {
    //   log_print("bin {} [{}, {}): {}\n", x.index(), x.bin().lower(), x.bin().upper(), *x);
    // }
  }
}
//---------------------------------------------------------------------------
void Relation::buildConcurrentHashMap(int idx, uint64_t start, uint64_t end)
  // build concurrent hash map for column i, with range [start, end)
{
  assert(idx < columns.size());
  auto &hashmap = this->columnHmap[idx];
  uint64_t *column = this->columns[idx];
  // unrolling later to see the performance gain
  for (uint64_t i = start; i < end; i++) {
    hashmap.insert(std::make_pair(column[i], i));
  }
}
//---------------------------------------------------------------------------
void Relation::calThenSetEstimateCost(FilterInfo &filter)
  // Calculate the estimate cost
{
  unsigned idx = filter.filterColumn.colId;
  auto sorted = sorts[idx];
  auto ordered = orders[idx];
  auto count = this->rowCount;
  auto scount = this->sampleCount;
  double sample_factor = static_cast<double>(count) / static_cast<double>(scount);
  double fraction = 0.;
  filter.rowCount = count;
  filter.sorted = sorted == Sorted::Likely;
  // log_print("colIdx:{}, rowCount:{}, sampleCount:{}, sample_max:{}, sample_min:{}, max:{}, min:{}\n", idx, count, scount, sample_maxs[idx], sample_mins[idx], maxs[idx], mins[idx]);
  switch (filter.comparison)
  {
  case FilterInfo::Comparison::Equal:
    if (sorted == Sorted::Likely) {
      filter.eCost = (scount / this->sample_distinct_count[idx]);
    } else {
      // build concurrent unordered multimap?
      filter.eCost = static_cast<uint64_t>((static_cast<double>(scount) / this->sample_distinctVals[idx].size()) * sample_factor);
    }
    break;
  case FilterInfo::Comparison::Greater:
    if (sorted == Sorted::Likely) {
      if (ordered == Order::ASC) {
        fraction = static_cast<double>(maxs[idx] - filter.constant) / static_cast<double>(maxs[idx] - sample_mins[idx]);
      } else {
        fraction = static_cast<double>(sample_maxs[idx] - filter.constant) / static_cast<double>(sample_maxs[idx] - mins[idx]);
      }
      filter.eCost = static_cast<uint64_t>(count * fraction);
    } else {
      // build concurrent multimap?
      if (filter.constant > sample_maxs[idx] || filter.constant < sample_mins[idx]) {
        filter.eCost = count;
      } else {
        fraction = static_cast<double>(sample_maxs[idx] - filter.constant) / static_cast<double>(sample_maxs[idx] - sample_mins[idx]);
        filter.eCost = static_cast<uint64_t>(count * fraction);
      }
    }
    break;
  case FilterInfo::Comparison::Less:
    if (sorted == Sorted::Likely) {
      if (ordered == Order::ASC) {
        fraction = static_cast<double>(filter.constant - sample_mins[idx]) / static_cast<double>(maxs[idx] - sample_mins[idx]);
      } else {
        fraction = static_cast<double>(filter.constant - mins[idx]) / static_cast<double>(sample_maxs[idx] - mins[idx]);
      }
      filter.eCost = static_cast<uint64_t>(count * fraction);
    } else {
      // build concurrent multimap?
      if (filter.constant > sample_maxs[idx] || filter.constant < sample_mins[idx]) {
        filter.eCost = count;
      } else {
        fraction = static_cast<double>(filter.constant - sample_mins[idx]) / static_cast<double>(sample_maxs[idx] - sample_mins[idx]);
        filter.eCost = static_cast<uint64_t>(count * fraction);
      }
    }
    break;
  default:
    assert(false);
    break;
  }
}
//---------------------------------------------------------------------------
void Relation::loadRelation(const char* fileName)
{
  int fd = open(fileName, O_RDONLY);
  if (fd==-1) {
    cerr << "cannot open " << fileName << endl;
    throw;
  }

  // Obtain file size
  struct stat sb;
  if (fstat(fd,&sb)==-1)
    cerr << "fstat\n";

  auto length=sb.st_size;

  char* addr=static_cast<char*>(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
  if (addr==MAP_FAILED) {
    cerr << "cannot mmap " << fileName << " of length " << length << endl;
    throw;
  }

  if (length<16) {
    cerr << "relation file " << fileName << " does not contain a valid header" << endl;
    throw;
  }

  this->rowCount=*reinterpret_cast<uint64_t*>(addr);
  addr+=sizeof(rowCount);
  auto numColumns=*reinterpret_cast<size_t*>(addr);
  addr+=sizeof(size_t);
  for (unsigned i=0;i<numColumns;++i) {
    this->columns.push_back(reinterpret_cast<uint64_t*>(addr));
    addr+=rowCount*sizeof(uint64_t);
  }

  // resize sample vectors
  this->sample_maxs.resize(numColumns, std::numeric_limits<uint64_t>::min());
  this->sample_mins.resize(numColumns, std::numeric_limits<uint64_t>::max());
  this->sample_distinct_count.resize(numColumns, 0);
  this->sample_distinctVals.resize(numColumns);
  this->sample_histograms.resize(numColumns);

  // resize sort and order vector
  this->sorts.resize(numColumns);
  this->orders.resize(numColumns);

  // resize real vectors
  this->maxs.resize(numColumns, std::numeric_limits<uint64_t>::min());
  this->mins.resize(numColumns, std::numeric_limits<uint64_t>::max());
  this->distinct_count.resize(numColumns);
  this->columnHmap.resize(numColumns);
  this->hasHmapBuilt.resize(numColumns, false);
  this->histograms.resize(numColumns);

  // calculate sample count we should read
  this->calcSampleCount();
  assert(this->sampleCount == this->rowCount || this->sampleCount % DBCONTEST_PAGE_ITEM_COUNT == 0);
  log_print("{}: sample {}/{} items for each column\n", fileName, this->sampleCount, this->rowCount);
}
//---------------------------------------------------------------------------
Relation::Relation(const char* fileName) : ownsMemory(false)
  // Constructor that loads relation from disk
{
  loadRelation(fileName);
}
//---------------------------------------------------------------------------
Relation::~Relation()
  // Destructor
{
  if (ownsMemory) {
    for (auto c : columns)
      delete[] c;
  }
}
//---------------------------------------------------------------------------
