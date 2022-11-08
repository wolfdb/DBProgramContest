#include <iostream>
#include <future>
#include <memory>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Log.hpp"
#include "Consts.hpp"

using namespace std;
using namespace std::chrono;

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
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   //
   joiner.buildHistogram();

#if USE_PARALLEL_QUERY
   std::vector<std::unique_ptr<QueryInfoWrapper>> qq;
   while (getline(cin, line)) {
      if (line == "F") { // End of a batch
         std::vector<std::future<string>> vf;
         for (auto it = qq.begin(); it < qq.end(); it++) {
            // std::unique_ptr<QueryInfoWrapper> iw = std::move(*it);
            // cout << joiner.join(iw->query);
            vf.push_back(async(std::launch::async | std::launch::deferred , [&joiner, iw = std::move(*it)]() {
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
   work_load = joiner.relations[0].rowCount;
   int32_t batch = 0;
   milliseconds start = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
   while (getline(cin, line)) {
      if (line == "F") {
         batch ++;
         continue; // End of a batch
      }
      actually_query = Joiner::query_count;
      milliseconds end = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
      log_print("batch: {}, before this query, execution time: {} ms\n", batch, end.count() - start.count());
      i.parseQuery(line);
      cout << joiner.join(i);
   }
#endif
   return 0;
}
