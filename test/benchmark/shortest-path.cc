//
// Created by xuxiyan on 2020/1/20.
//

#include <cstdio>
#include <iostream>
#include <chrono>
#include <vector>
#include <set>
#include <queue>
#include "pmgd.h"
#include "util.h"

#include "log_util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;

static const char ID_STR[] = "pmgd.loader.id";
static StringID ID;

void usage() {
    printf("Usage: program [db_name] [ldbc/freebase/twitter/graph500]\n");
    exit(0);
}

int shortest(Graph &db, long long src, long long dst, int maxDep) {
    // Transaction tx(db, Transaction::ReadWrite);
    std::set<long long> visted;
    std::queue<long long> que;
    std::queue<long long> que2;
    visted.insert(src);
    que.push(src);
    int currentDep = 1;
    while(!que.empty() && currentDep < maxDep) {
        long long id = que.front();
        que.pop();

        Node &n = *(db.get_nodes(0, PropertyPredicate(StringID(ID_STR), PropertyPredicate::Eq, (long long)id)));
        for (EdgeIterator it = n.get_edges(); it; it.next()) {
            // LOG_DEBUG_WRITE("console", "{} -> {}", (*it).get_source().get_property(StringID(ID_STR)).int_value(), (*it).get_destination().get_property(StringID(ID_STR)).int_value())
            Node &next = (*it).get_destination();
            long long next_id = next.get_property(StringID(ID_STR)).int_value();
            if (next_id == id) {
                Node &pre = (*it).get_source();
                next_id = pre.get_property(StringID(ID_STR)).int_value();
            }
            if (!visted.count(next_id)) {
                if (next_id == dst) {
                    // found dst
                    return currentDep;
                }
                // LOG_DEBUG_WRITE("console", "level:{} : {} -> {}", currentDep, id, next_id);
                // std::cout << it->SecondNode << " ";
                visted.insert(next_id);
                que2.push(next_id);
            }
        }
        if (que.empty()) {
            currentDep++;
            if (currentDep >= maxDep) {
                return -1;
                // LOG_DEBUG_WRITE("console", "from id:{} to id:{} shortest path length is {}", src, dst, -1)
                // continue; // break;
            }
            // LOG_DEBUG_WRITE("console", "node in depth:{} is {}", currentDep, que2.size())
            while(!que2.empty()) {
                que.push(que2.front());
                que2.pop();
            }
            // std::cout << std::endl;
        }
    }
    return -2;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage();
    }

    // init log
    initLog();

    char* dbName = argv[1];
    string dataset = argv[2];

    int maxDepth = 10;

    vector<long long> ids;
    if (dataset == "ldbc") {
        ids = {871871,562439,1022837,842648,399882,519531,1148153,449638,411950,1257602};
    } else if (dataset == "freebase") {
        ids = {90775171808048LL,95575470755348LL,81707782495348LL,80669087845748LL,48525481515448LL,57676777527448LL,57505683687148LL,499084754948LL,68875188845748LL,505274954948LL};
    } else if (dataset == "twitter") {
        ids = {239261,7713390,6838192,5378052,3581806,4422251,3303191,7771631,5901685,8395972};
    } else if (dataset == "graph500") {

    } else if (dataset == "sample") {
        if (argc < 6) {
            printf("please apply [src] [dst] [maxDepth] to the end of params.\n");
            exit(0);
        }
        ids = {stoi(argv[3]), stoi(argv[4])};
        maxDepth = stoi(argv[5]);

    } else {
        usage();
    }

    try {
        LOG_INFO_WRITE("console", "len <= 10, len = -1 means reach max depth {}, len = -2 means not route to dst node.", maxDepth)
        Graph db(dbName, Graph::ReadOnly);
        Transaction tx(db, Transaction::ReadOnly);

        double total = 0;
        for (int i = 0;i < ids.size(); i++) {
            auto start_t = system_clock::now();
            int j = (i + 1) % ids.size();
            long long src = ids[i];
            long long dst = ids[j];
            // from src to dst
            int len = shortest(db, src, dst, maxDepth);
            auto end_t = system_clock::now();
            auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
            total += (double(duration.count()) * microseconds::period::num / microseconds::period::den);
            LOG_DEBUG_WRITE("console",
                            "from id:{} to id:{} shortest path length is {}, duration is {} microseconds ≈ {} s", src,
                            dst, len, double(duration.count()),
                            double(duration.count()) * microseconds::period::num / microseconds::period::den)
        }
    } catch (Exception e) {
        print_exception(e);
        return 1;
    }





}