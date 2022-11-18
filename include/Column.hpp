#pragma once
#include <iostream>
#include <vector>
#include "Utils.hpp"

using namespace std;

template <typename T>
class Column {
  vector<T*> tuples;
  vector<uint64_t> tupleLength;
  vector<uint64_t> offsets;

  bool fixed = false;

public:
  Column(unsigned taskCnt) {
    tuples.resize(taskCnt);
    tupleLength.resize(taskCnt, 0);
    offsets.resize(taskCnt + 1, 0);
  }

  void addTuples(unsigned pos, uint64_t* data, uint64_t length) {
    this->tuples[pos] = data;
    this->tupleLength[pos] = length;
  }

  void fix() {
    for (unsigned i = 1; i < offsets.size(); i++) {
      offsets[i] = offsets[i-1] + tupleLength[i-1];
    }
    fixed = true;
  }

  class Iterator;

  Iterator begin(uint64_t index) {
    assert(fixed);
    return Iterator(*this, index);
  }

  class Iterator {
    unsigned localIndex;
    uint64_t localOffset;
    Column<T> col;

  public:
    Iterator(Column<T>&col, uint64_t start) : col(col) {
      if (col.tuples.size() == 0)
        return;
      auto it = lower_bound(col.offsets.begin(), col.offsets.end(), start);
      localIndex = it - col.offsets.begin();
      assert(localIndex < col.offsets.size());
      if (col.offsets[localIndex] != start)
        localIndex --;
      localOffset = start - col.offsets[localIndex];
      while (col.tupleLength[localIndex] == 0) {
        localIndex++;
        localOffset = 0;
      }
    }

    inline T& operator*() {
      assert(localIndex < col.tupleLength.size());
      return col.tuples[localIndex][localOffset];
    }
    inline Iterator& operator++() {
      localOffset ++;
      while (UNLIKELY(localOffset >= col.tupleLength[localIndex])) {
        localIndex ++;
        localOffset = 0;
        if (UNLIKELY(localIndex == col.tupleLength.size())) {
          break;
        }
      }
      return *this;
    }
  };
};