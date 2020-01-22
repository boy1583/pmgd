//
// Created by xuxiyan on 2020/1/20.
//

#include <cstdio>
#include <iostream>
#include <chrono>
#include <vector>
#include "pmgd.h"
#include "util.h"

#include "log_util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;

static const char ID_STR[] = "pmgd.loader.id";
static StringID ID;

bool debug = false;

void usage() {
    printf("Usage: program [db_name] [-d]\n");
    exit(0);
}

// 获取顶点所有边数量大于等于K的所有顶点数量

int main(int argc, char* argv[]) {
    if (argc < 2) {
        usage();
    }

    if (argc >= 3 && !strcmp(argv[2], "-d")) {
        debug = true;
    }

    // init log
    initLog();

    char* dbName = argv[1];

    Graph db(dbName, Graph::ReadOnly);

    string dataset = argv[1];
    // test suite 中是50
    const long long K = 50;

    try {
        Transaction tx(db, Transaction::ReadOnly);
        auto start_t = system_clock::now();
        NodeIterator nodes = db.get_nodes();
        long long total = 0;
        while(nodes) {
            Node &node = (*nodes);
            EdgeIterator edges = node.get_edges();
            long long count = 0;
            while(edges) {
                count++;
                edges.next();
            }
            if (count >= K) {
                total++;
                if (debug)
                    LOG_DEBUG_WRITE("console", "source id: {}, count: {}", node.get_property(StringID(ID_STR)).int_value(), count);
            }
            nodes.next();
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console", "k ({}) degree both filtered count is {}, duration is {} microseconds ≈ {} s",
                        K, total, double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den)
    } catch (Exception e) {
        print_exception(e);
        return 1;
    }

}