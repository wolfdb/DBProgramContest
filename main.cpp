#include <iostream>
#include <future>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Log.hpp"
#include "Consts.hpp"

using namespace std;

struct QueryInfoWrapper {
   QueryInfo query;
};

//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
   Joiner joiner;
   // Read join relations
   string line;
   while (getline(cin, line)) {
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }
   // Debug: print relation column address
   // for (int i = 0; i < joiner.relations.size(); i++) {
   //    auto &rel = joiner.relations[i];
   //    out.print("relation :{}\n", i);
   //    for (int j = 0; j < rel.columns.size(); j++) {
   //       out.print("  column {} addr {}\n", j, fmt::ptr(rel.columns[j]));
   //    }
   // }

   // Preparation phase (not timed)
   // Build histograms, indexes,...
   //
   joiner.buildHistogram();

#if USE_PARALLEL_QUERY
   std::vector<std::unique_ptr<QueryInfoWrapper>> qq;
   while (getline(cin, line)) {
      if (line == "F") { // End of a batch
         // build concurrent_unorder_multimap for unsorted columns that with '=' comparator
         // joiner.buildIndex(qq);
         std::vector<std::future<string>> vf;
         for (auto it = qq.begin(); it < qq.end(); it++) {
            // std::unique_ptr<QueryInfoWrapper> iw = std::move(*it);
            // cout << joiner.join(iw->query);
            vf.push_back(async([&joiner, iw = std::move(*it)]() {
               return joiner.join(iw->query);
            }));
         }
         for_each(vf.begin(), vf.end(), [](future<string> &x) { cout << x.get(); });
         qq.clear();
      } else {
         std::unique_ptr<QueryInfoWrapper> iw = std::make_unique<QueryInfoWrapper>();
         iw->query.parseQuery(line);
         qq.push_back(std::move(iw));
      }
   }
#else

   QueryInfo i;
   while (getline(cin, line)) {
      if (line == "F") continue; // End of a batch
      i.parseQuery(line);
      cout << joiner.join(i);
   }
#endif
   return 0;
}
