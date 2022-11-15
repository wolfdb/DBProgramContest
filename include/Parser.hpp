#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <map>
#include <set>
#include "Relation.hpp"
//---------------------------------------------------------------------------
struct SelectInfo {
   /// Relation id
   RelationId relId;
   /// Binding for the relation
   unsigned binding;
   /// Column id
   unsigned colId;
   /// The constructor
   SelectInfo(RelationId relId,unsigned b,unsigned colId) : relId(relId), binding(b), colId(colId) {};
   /// The constructor if relation id does not matter
   SelectInfo(unsigned b,unsigned colId) : SelectInfo(-1,b,colId) {};
   /// Copy constructor
   SelectInfo(const SelectInfo &info) : relId(info.relId), binding(info.binding), colId(info.colId) {};

   /// Equality operator
   inline bool operator==(const SelectInfo& o) const {
     return o.relId == relId && o.binding == binding && o.colId == colId;
   }
   /// Less Operator
   inline bool operator<(const SelectInfo& o) const {
     if (binding < o.binding) {
       return true;
     }
     if (binding > o.binding) {
       return false;
     }
     return colId < o.colId;
   }

   /// Greater Operator
   inline bool operator>(const SelectInfo& o) const {
     if (binding > o.binding) {
       return true;
     }
     if (binding < o.binding) {
       return false;
     }
     return colId > o.colId;
   }

   /// Dump text format
   std::string dumpText() const;
   /// Dump SQL
   std::string dumpSQL(bool addSUM=false) const;

   /// The delimiter used in our text format
   static const char delimiter=' ';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=", ";
};
//---------------------------------------------------------------------------
struct FilterInfo {
   enum Comparison : char { Less='<', Greater='>', Equal='=' };
   /// Filter Column
   SelectInfo filterColumn;
   /// Constant
   uint64_t constant;
   /// Comparison type
   Comparison comparison;
   /// Dump SQL
   std::string dumpSQL();

   /// Estimate cost, row count
   uint64_t eCost;
   /// Total row count for this scan
   uint64_t rowCount;

   /// The constructor
   FilterInfo(SelectInfo filterColumn,uint64_t constant,Comparison comparison) : filterColumn(filterColumn), constant(constant), comparison(comparison) {};
   /// Dump text format
   std::string dumpText();

   /// The delimiter used in our text format
   static const char delimiter='&';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=" and ";
};
static const std::vector<FilterInfo::Comparison> comparisonTypes { FilterInfo::Comparison::Less, FilterInfo::Comparison::Greater, FilterInfo::Comparison::Equal};
//---------------------------------------------------------------------------
struct PredicateInfo {
   /// Left
   SelectInfo left;
   /// Right
   SelectInfo right;
   /// The constructor
   PredicateInfo(SelectInfo left, SelectInfo right) : left(left), right(right){};
   /// Dump text format
   std::string dumpText() const;
   /// Dump SQL
   std::string dumpSQL() const;

   uint64_t eCost;

   /// Equality operator
   inline bool operator==(const PredicateInfo& o) const {
     return (left == o.left && right == o.right) || 
            (left == o.right && right == o.left);
   }

   /// Less Operator
   inline bool operator<(const PredicateInfo& o) const {
     auto &ll = left < right ? left : right;
     auto &lr = left < right ? right : left;

     auto &rl = o.left < o.right ? o.left : o.right;
     auto &rr = o.left < o.right ? o.right : o.left;

     if (ll < rl) 
     {
      return true;
     }
     if (ll > rl) {
      return false;
     }
     return lr < rr;
   }

   /// The delimiter used in our text format
   static const char delimiter='&';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=" and ";
};
//---------------------------------------------------------------------------
namespace std {
  /// Simple hash function to enable use with unordered_map
  template<> struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const& s) const noexcept { return s.binding ^ (s.colId << 5); }
  };

  template<> struct hash<PredicateInfo> {
    std::size_t operator()(PredicateInfo const& s) const noexcept {
      return (s.left.binding ^ (s.left.colId) << 5) ^ ((s.right.binding ^ (s.right.colId << 5)) << 10);
    }
  };

  template<> struct equal_to<PredicateInfo> {
   constexpr bool operator()(const PredicateInfo& lhs, const PredicateInfo& rhs ) const {
      return (lhs.left == rhs.left && lhs.right == rhs.right) || 
            (lhs.left == rhs.right && lhs.right == rhs.left);
   }
  };
};
//---------------------------------------------------------------------------
class QueryInfo {
   public:
   /// The relation ids
   std::vector<RelationId> relationIds;
   /// The predicates
   std::vector<PredicateInfo> predicates;
   /// The selection info pair
   ///                                     binding
   std::unordered_map<SelectInfo, std::map<unsigned, std::set<SelectInfo>>> selectInfoMap;
   std::unordered_map<SelectInfo, std::vector<FilterInfo>> filterMap;
   std::set<PredicateInfo> predicatesSet;
   /// The filters
   std::vector<FilterInfo> filters;
   /// The selections
   std::vector<SelectInfo> selections;
   /// Is the query impossible
   bool impossible = false;
   /// Reset query info
   void clear();

   private:
   /// Parse a single predicate
   void parsePredicate(std::string& rawPredicate);
   /// Resolve bindings of relation ids
   void resolveRelationIds();

   public:
   /// Parse relation ids <r1> <r2> ...
   void parseRelationIds(std::string& rawRelations);
   /// Parse predicates r1.a=r2.b&r1.b=r3.c...
   void parsePredicates(std::string& rawPredicates);
   /// Parse selections r1.a r1.b r3.c...
   void parseSelections(std::string& rawSelections);
   /// Parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
   void parseQuery(std::string& rawQuery);
   void addFilter(SelectInfo &si, uint64_t constant, char compType);
   /// Dump text format
   std::string dumpText();
   /// Dump SQL
   std::string dumpSQL();
   /// The empty constructor
   QueryInfo() {}
   /// The constructor that parses a query
   QueryInfo(std::string rawQuery);
};
//---------------------------------------------------------------------------
