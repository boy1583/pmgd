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
    printf("Usage: program [db_name] [ldbc/freebase/mico/yeast]\n");
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
        ids = {168062,126101,2900,164352,95984,115510,18809,103863,97894,32702};
    } else if (dataset == "freebase") {
        ids = {1484158,63957,624478,1005153,1845599,1160813,1554886,1385143,147440,650644};
    } else if (dataset == "mico") {
        ids = {10693,59896,10464,33317,95783,79938,92858,86993,14637,94686};
    } else if (dataset == "yeast") {
        ids = {517,831,541,313,1941,2263,1079,1883,349,1850};
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