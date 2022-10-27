#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
   Joiner joiner;
   // Read join relations
   string line;
   while (getline(cin, line)) {
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   //
   joiner.buildHistogram();

   vector<QueryInfo> qq;
   while (getline(cin, line)) {
      if (line == "F") { // End of a batch
         // build concurrent_unorder_multimap for unsorted columns that with '=' comparator
         joiner.buildIndex(qq);
         for (auto &i : qq) {
            cout << joiner.join(i);
         }
         qq.clear();
      } else {
         QueryInfo i;
         i.parseQuery(line);
         qq.push_back(i);
      }
   }
   return 0;
}
