#pragma once
#include <fstream>
#include "Relation.hpp"

#define LIKELY(x) __builtin_expect((x),1)
#define UNLIKELY(x) __builtin_expect((x),0)

#define CNT_PARTITIONS(WHOLE, PART) (((WHOLE)+((PART)-1))/(PART))
#define RADIX_HASH(value, base) ((value)&(base-1))

static inline bool is_pow_of_2(uint64_t x) {
  return !(x & (x-1));
}
static inline uint64_t next_pow_of_2(uint64_t x) {
  if (is_pow_of_2(x)) return x;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
#if BITS_PER_LONG == 64
  x |= x >> 32;
#endif
  return x + 1;
}
//---------------------------------------------------------------------------
class Utils {
 public:
  /// Create a dummy relation
  static Relation createRelation(uint64_t size,uint64_t numColumns);

  /// Store a relation in all formats
  static void storeRelation(std::ofstream& out,Relation& r,unsigned i);
};
//---------------------------------------------------------------------------
