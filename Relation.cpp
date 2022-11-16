#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>
#include <cmath>
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
  if (this->rowCount < MAX_SAMPLE_ITEM_COUNT) {
    this->sampleCount = this->rowCount;
    this->sampleAll = true;
    return;
  }

  // sample 1/16 of the data or 1MB, which is small
  uint64_t sampleCount = this->rowCount >> 4;
  this->sampleCount = std::min(sampleCount, MAX_SAMPLE_ITEM_COUNT);
  this->sampleAll = false;
}
//---------------------------------------------------------------------------
void Relation::buildHistogram(int idx)
  // build histogram for column idx
{
  assert(idx < columns.size());
  uint64_t *column = this->columns[idx];
  auto &distinctValue = this->sample_distinctVals[idx];
  distinctValue.reserve(this->sampleCount);
  uint64_t smax = std::numeric_limits<uint64_t>::min();
  uint64_t smin = std::numeric_limits<uint64_t>::max();
  vector<uint64_t> sampleData;
  if (sampleAll) {
    for (int i = 0; i < this->sampleCount; i++) {
      smax = std::max(smax, column[i]);
      smin = std::min(smin, column[i]);
      distinctValue[column[i]]++;
    }
  } else {
    sampleData.reserve(this->sampleCount + 100);
    uint64_t step = this->rowCount / this->sampleCount * 2;
    for (int i = 0; i < this->rowCount; i += step) {
      sampleData.push_back(column[i]);
      sampleData.push_back(column[i+1]);
      distinctValue[column[i]]++;
      distinctValue[column[i+1]]++;
      uint64_t tmpmax = std::max(column[i], column[i+1]);
      uint64_t tmpmin = std::min(column[i], column[i+1]);
      smax = std::max(smax, tmpmax);
      smin = std::min(smin, tmpmin);
    }
    this->sampleCount = sampleData.size();
  }

  this->sample_maxs[idx] = smax;
  this->sample_mins[idx] = smin;

  // build histgram
  // auto h = make_histogram(axis::regular<>(HISTOGRAM_BUCKET_CNT, this->sample_mins[idx], this->sample_maxs[idx]));
  // if (sampleAll) {
  //   for (int i = 0; i < this->sampleCount; i++) {
  //     h(column[i]);
  //   }
  // } else {
  //   for (auto ele: sampleData) {
  //     h(ele);
  //   }
  // }
  // this->sample_histograms[idx].hist_ = std::move(h);
}
//---------------------------------------------------------------------------
void Relation::calThenSetEstimateCost(FilterInfo &filter)
  // Calculate the estimate cost
{
  unsigned idx = filter.filterColumn.colId;
  auto count = this->rowCount;
  auto scount = this->sampleCount;
  double sample_factor = static_cast<double>(count) / static_cast<double>(scount);
  double fraction = 0.;
  filter.rowCount = count;
  switch (filter.comparison)
  {
  case FilterInfo::Comparison::Equal:
    // build concurrent unordered multimap?
    // filter.eCost = static_cast<uint64_t>((static_cast<double>(scount) / this->sample_distinctVals[idx].size()) * sample_factor);
    filter.eCost = std::log(count);
    break;
  case FilterInfo::Comparison::Greater:
    // build concurrent multimap?
    // if (filter.constant > sample_maxs[idx] || filter.constant < sample_mins[idx]) {
    //   filter.eCost = count;
    // } else {
    //   fraction = static_cast<double>(sample_maxs[idx] - filter.constant) / static_cast<double>(sample_maxs[idx] - sample_mins[idx]);
    //   filter.eCost = static_cast<uint64_t>(count * fraction);
    // }
    filter.eCost = count / 2;
    break;
  case FilterInfo::Comparison::Less:
    // build concurrent multimap?
    // if (filter.constant > sample_maxs[idx] || filter.constant < sample_mins[idx]) {
    //   filter.eCost = count;
    // } else {
    //   fraction = static_cast<double>(filter.constant - sample_mins[idx]) / static_cast<double>(sample_maxs[idx] - sample_mins[idx]);
    //   filter.eCost = static_cast<uint64_t>(count * fraction);
    // }
    filter.eCost = count / 2;
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
  this->sample_maxs.resize(numColumns);
  this->sample_mins.resize(numColumns);
  this->sample_distinctVals.resize(numColumns);
  this->sample_histograms.resize(numColumns);

  // calculate sample count we should read
  this->calcSampleCount();
  // cerr << "relation file: " << fileName << ": sample " << this->sampleCount  << "/" << this->rowCount << " rows for each column" << endl;
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
