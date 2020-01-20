//
// Created by xuxiyan on 2020/1/20.
//

#include <cstdio>
#include <iostream>
#include <chrono>
#include <vector>
#include <set>
#include <string>
#include "pmgd.h"
#include "util.h"

#include "log_util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;

static const char ID_STR[] = "pmgd.loader.id";
static StringID ID;

// 获取一个点的所有了label在label array中的边的数量

void usage() {
    printf("Usage: program [db_name] [ldbc/freebase/twitter/graph500]\n");
    exit(0);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage();
    }

    // init log
    initLog();

    char* dbName = argv[1];

    try {
        Graph db(dbName, Graph::ReadOnly);

        string dataset = argv[2];
        vector<long long> ids;
        set<string> labels;
        if (dataset == "ldbc") {
            ids = {871871,562439,1022837,842648,399882,519531,1148153,449638,411950,1257602};
            labels = {"hasInterest","likes","hasType","hasTag","hasCreator","replyOf","containerOf","studyAt","isLocatedIn","isSubclassOf"};
        } else if (dataset == "freebase") {
            ids = {90775171808048LL,95575470755348LL,81707782495348LL,80669087845748LL,48525481515448LL,57676777527448LL,57505683687148LL,499084754948LL,68875188845748LL,505274954948LL};
            labels = {"/m/04ykw93","/m/04ykw7r","/m/0bf4xgr","/m/0wz7dh2","/m/03z9618","/m/01xs0hs","/m/02kb2_q","/m/07w8036","/m/01xxxz7","/m/05h9s4_"};
        } else if (dataset == "twitter") {

        } else if (dataset == "graph500") {

        } else if (dataset == "sample") {
            ids = {1};
            labels = {"knows"};
        } else {
            usage();
        }

        double total = 0;
        for (int i = 0;i < ids.size(); i++) {
            auto start_t = system_clock::now();
            Transaction tx(db, Transaction::ReadOnly);
            Node &n = *(db.get_nodes(0, PropertyPredicate(StringID(ID_STR), PropertyPredicate::Eq, (long long) ids[i])));
            // 获取所有的边，看是否在set中
            long long count = 0;
            for (auto &tag : labels) {
                EdgeIterator edges = n.get_edges(StringID(tag.c_str()));
                while (edges) {
                    // auto edge = &*edges;
                    // long long next_id = edge->get_property(StringID(ID_STR)).int_value();
                    // LOG_DEBUG_WRITE("console", "edge:{}", next_id)
                    edges.next();
                    count++;
                }
            }
            auto end_t = system_clock::now();
            auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
            total += (double(duration.count()) * microseconds::period::num / microseconds::period::den);
            LOG_DEBUG_WRITE("console", "source id: {}, NN both filtered count is {}, duration is {} microseconds ≈ {} s",
                            ids[i], count, double(duration.count()),
                            double(duration.count()) * microseconds::period::num / microseconds::period::den)
        }
        LOG_DEBUG_WRITE("console", "total source id is {}, total duration is {} s, {} s for each record",
                        ids.size(),
                        total,
                        total / ids.size())

    } catch (Exception e) {
        print_exception(e);
        return 1;
    }
}
