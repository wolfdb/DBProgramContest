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
  if ((this->rowCount >> 1) <= MAX_SAMPLE_COUNT) {
    this->sampleCount = this->rowCount;
    this->sampleAll = true;
    this->sampleStep = 1;
    return;
  }

  this->sampleCount = MAX_SAMPLE_COUNT;
  this->sampleAll = false;
  this->sampleStep = this->rowCount / this->sampleCount;
}
//---------------------------------------------------------------------------
void Relation::buildHistogram(int idx)
  // build histogram for column idx
{
  assert(idx < columns.size());
  uint64_t *column = this->columns[idx];
  auto &ndv = this->sample_ndvs[idx];
  uint64_t tmp_max = std::numeric_limits<uint64_t>::min();
  uint64_t tmp_min = std::numeric_limits<uint64_t>::max();
  const auto step = this->sampleStep;

  ndv.reserve(sampleCount * 2);
  uint64_t pos = 0;
  for (int i = 0; i < this->sampleCount; i++) {
    uint64_t val = column[pos];
    tmp_max = std::max(tmp_max, val);
    tmp_min = std::min(tmp_min, val);
    ndv[val] ++;
    pos += step;
  }

  // compress the data
  if ((sampleCount / ndv.size()) > 3) {
    this->needCompress[idx] = true;
    if (!sampleAll) {
      ndv.clear();
      ndv.reserve(rowCount);
      for (unsigned i = 0; i < rowCount; ++i) {
        uint64_t val = column[i];
        tmp_max = std::max(tmp_max, val);
        tmp_min = std::min(tmp_min, val);
        ndv[val]++;
      }
    }
    uint64_t *vals= new uint64_t[ndv.size() * 2];
    uint64_t *cnts= vals + ndv.size();

    uint64_t index = 0;
    for (auto &it: ndv) {
      vals[index] = it.first;
      cnts[index++] = it.second;
    }
    compressColumns[idx].emplace_back(vals);
    compressColumns[idx].emplace_back(cnts);
  }

  this->sample_maxs[idx] = tmp_max;
  this->sample_mins[idx] = tmp_min;
}
//---------------------------------------------------------------------------
void Relation::calThenSetEstimateCost(FilterInfo &filter)
  // Calculate the estimate cost
{
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
  this->sample_maxs.resize(numColumns, std::numeric_limits<uint64_t>::min());
  this->sample_mins.resize(numColumns, std::numeric_limits<uint64_t>::max());
  this->sample_ndvs.resize(numColumns);

  // resize compress columns
  this->compressColumns.resize(numColumns);
  this->needCompress.resize(numColumns, false);

  // calculate sample count we should read
  this->calcSampleCount();
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
