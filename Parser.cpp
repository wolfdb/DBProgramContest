#include <cassert>
#include <iostream>
#include <utility>
#include <sstream>
#include "Parser.hpp"
#if PRINT_LOG
#include "Log.hpp"
#endif
#include "Operators.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static void splitString(string& line,vector<unsigned>& result,const char delimiter)
  // Split a line into numbers
{
  stringstream ss(line);
  string token;
  while (getline(ss,token,delimiter)) {
    result.push_back(stoul(token));
  }
}
//---------------------------------------------------------------------------
static void splitString(string& line,vector<string>& result,const char delimiter)
  // Parse a line into strings
{
  stringstream ss(line);
  string token;
  while (getline(ss,token,delimiter)) {
    result.push_back(token);
  }
}
//---------------------------------------------------------------------------
static void splitPredicates(string& line,vector<string>& result)
  // Split a line into predicate strings
{
  // Determine predicate type
  for (auto cT : comparisonTypes) {
    if (line.find(cT)!=string::npos) {
      splitString(line,result,cT);
      break;
    }
  }
}
//---------------------------------------------------------------------------
void QueryInfo::parseRelationIds(string& rawRelations)
  // Parse a string of relation ids
{
  splitString(rawRelations,relationIds,' ');
}
//---------------------------------------------------------------------------
static SelectInfo parseRelColPair(string& raw)
{
  vector<unsigned> ids;
  splitString(raw,ids,'.');
  return SelectInfo(0,ids[0],ids[1]);
}
//---------------------------------------------------------------------------
inline static bool isConstant(string& raw) { return raw.find('.')==string::npos; }
//---------------------------------------------------------------------------
void QueryInfo::parsePredicate(string& rawPredicate)
  // Parse a single predicate: join "r1Id.col1Id=r2Id.col2Id" or "r1Id.col1Id=constant" filter
{
  vector<string> relCols;
  splitPredicates(rawPredicate,relCols);
  assert(relCols.size()==2);
  assert(!isConstant(relCols[0])&&"left side of a predicate is always a SelectInfo");
  auto leftSelect=parseRelColPair(relCols[0]);
  if (isConstant(relCols[1])) {
    uint64_t constant=stoul(relCols[1]);
    char compType=rawPredicate[relCols[0].size()];
    filters.emplace_back(leftSelect,constant,FilterInfo::Comparison(compType));
  } else {
    auto rightSelect = parseRelColPair(relCols[1]);
    if (leftSelect.binding == rightSelect.binding && leftSelect.colId == rightSelect.colId) {
#if PRINT_LOG
      log_print("removed predicate: {}\n", rawPredicate);
#endif
      return;
    }
    selectInfoMap[leftSelect][rightSelect.binding].insert(rightSelect);
    selectInfoMap[rightSelect][leftSelect.binding].insert(leftSelect);
    // log_print("parsePredicate: left {}, right {}, map size {}\n", leftSelect.dumpText(), rightSelect.dumpText(), selectInfoMap.size());
  }
}
//---------------------------------------------------------------------------
void QueryInfo::parsePredicates(string& text)
  // Parse predicates
{
  vector<string> predicateStrings;
  splitString(text,predicateStrings,'&');
  for (auto& rawPredicate : predicateStrings) {
    parsePredicate(rawPredicate);
  }

  // resemble predicates
  for (auto &selectInfo: selectInfoMap) {
    auto left = selectInfo.first;
    // log_print("left select: ({},{})\n", left.binding, left.colId);
    for (auto &selectSet : selectInfo.second) {
      if (selectSet.second.size() == 1) {
        auto right = *(selectSet.second.begin());
        if (left < right) {
          // log_print("left {},{} < right {},{}\n", left.binding, left.colId, right.binding, right.colId);
          predicatesSet.insert({left, right});
        } // let the other half handle to avoid duplicate
      } else {
        auto tleft = *(selectSet.second.begin());
        for (auto tright : selectSet.second) {
          // log_print("  left select: ({},{})\n", left.binding, left.colId);
          if (tleft < tright) {
            // log_print("   right select: ({},{})\n", tright.binding, tright.colId);
            predicatesSet.insert({tleft, tright});
          }
        }
        if (left < tleft) {
          // log_print(" right select: ({},{})\n", tleft.binding, tleft.colId);
          predicatesSet.insert({left, tleft});
        }
      }
    }
  }

  for (auto &predicate : predicatesSet) {
    // log_print("dump: {}\n", predicate.dumpText());
    predicates.push_back(predicate);
  }
}
//---------------------------------------------------------------------------
void QueryInfo::parseSelections(string& rawSelections)
 // Parse selections
{
  vector<string> selectionStrings;
  splitString(rawSelections,selectionStrings,' ');
  for (auto& rawSelect : selectionStrings) {
    selections.emplace_back(parseRelColPair(rawSelect));
  }
}
//---------------------------------------------------------------------------
static void resolveIds(vector<unsigned>& relationIds,SelectInfo& selectInfo)
  // Resolve relation id
{
  selectInfo.relId=relationIds[selectInfo.binding];
}
//---------------------------------------------------------------------------
void QueryInfo::resolveRelationIds()
  // Resolve relation ids
{
  // Selections
  for (auto& sInfo : selections) {
    resolveIds(relationIds,sInfo);
  }
  // Predicates
  for (auto& pInfo : predicates) {
    resolveIds(relationIds,pInfo.left);
    resolveIds(relationIds,pInfo.right);
  }
  // Filters
  for (auto& fInfo : filters) {
    resolveIds(relationIds,fInfo.filterColumn);
  }
}
//---------------------------------------------------------------------------
void QueryInfo::parseQuery(string& rawQuery)
  // Parse query [RELATIONS]|[PREDICATES]|[SELECTS]
{
  clear();
  vector<string> queryParts;
  splitString(rawQuery,queryParts,'|');
  assert(queryParts.size()==3);
  parseRelationIds(queryParts[0]);
  parsePredicates(queryParts[1]);
  parseSelections(queryParts[2]);
  resolveRelationIds();
  // log_print("original query: {}\nrewrite query: {}\n", rawQuery, dumpText());
}
//---------------------------------------------------------------------------
void QueryInfo::clear()
  // Reset query info
{
  relationIds.clear();
  predicates.clear();
  filters.clear();
  selections.clear();
  selectInfoMap.clear();
  predicatesSet.clear();
}
//---------------------------------------------------------------------------
static string wrapRelationName(uint64_t id)
  // Wraps relation id into quotes to be a SQL compliant string
{
  return "\""+to_string(id)+"\"";
}
//---------------------------------------------------------------------------
string SelectInfo::dumpSQL(bool addSUM) const
  // Appends a selection info to the stream
{
  auto innerPart=wrapRelationName(binding)+".c"+to_string(colId);
  return addSUM?"SUM("+innerPart+")":innerPart;
}
//---------------------------------------------------------------------------
string SelectInfo::dumpText() const
  // Dump text format
{
  return to_string(binding)+"."+to_string(colId);
}
//---------------------------------------------------------------------------
string FilterInfo::dumpText()
  // Dump text format
{
  return filterColumn.dumpText()+static_cast<char>(comparison)+to_string(constant);
}
//---------------------------------------------------------------------------
string FilterInfo::dumpSQL()
  // Dump text format
{
  return filterColumn.dumpSQL()+static_cast<char>(comparison)+to_string(constant);
}
//---------------------------------------------------------------------------
string PredicateInfo::dumpText() const
  // Dump text format
{
  return left.dumpText()+'='+right.dumpText();
}
//---------------------------------------------------------------------------
string PredicateInfo::dumpSQL() const
  // Dump text format
{
  return left.dumpSQL()+'='+right.dumpSQL();
}
//---------------------------------------------------------------------------
template <typename T>
static void dumpPart(stringstream& ss,vector<T> elements)
{
  for (unsigned i=0;i<elements.size();++i) {
    ss << elements[i].dumpText();
    if (i<elements.size()-1)
      ss << T::delimiter;
  }
}
//---------------------------------------------------------------------------
template <typename T>
static void dumpPartSQL(stringstream& ss,vector<T> elements)
{
  for (unsigned i=0;i<elements.size();++i) {
    ss << elements[i].dumpSQL();
    if (i<elements.size()-1)
      ss << T::delimiterSQL;
  }
}
//---------------------------------------------------------------------------
string QueryInfo::dumpText()
  // Dump text format
{
  stringstream text;
  // Relations
  for (unsigned i=0;i<relationIds.size();++i) {
    text << relationIds[i];
    if (i<relationIds.size()-1)
      text << " ";
  }
  text << "|";

  dumpPart(text,predicates);
  if (predicates.size()&&filters.size())
    text << PredicateInfo::delimiter;
  dumpPart(text,filters);
  text << "|";
  dumpPart(text,selections);

  return text.str();
}
//---------------------------------------------------------------------------
string QueryInfo::dumpSQL()
  // Dump SQL
{
  stringstream sql;
  sql << "SELECT ";
  for (unsigned i=0;i<selections.size();++i) {
    sql << selections[i].dumpSQL(true);
    if (i<selections.size()-1)
      sql << ", ";
  }

  sql << " FROM ";
  for (unsigned i=0;i<relationIds.size();++i) {
    sql << "r" << relationIds[i] << " " << wrapRelationName(i);
    if (i<relationIds.size()-1)
      sql << ", ";
  }

  sql << " WHERE ";
  dumpPartSQL(sql,predicates);
  if (predicates.size()&&filters.size())
    sql << " and ";
  dumpPartSQL(sql,filters);

  sql << ";";

  return sql.str();
}
//---------------------------------------------------------------------------
QueryInfo::QueryInfo(string rawQuery) { parseQuery(rawQuery); }
//---------------------------------------------------------------------------
