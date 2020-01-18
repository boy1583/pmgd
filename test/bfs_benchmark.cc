//
// Created by xuxiyan on 2020/1/18.
//
#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <queue>
#include <chrono>
#include <vector>

#include <rapidjson/filewritestream.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/writer.h>
#include <rapidjson/document.h>

#include <cstdio>
#include <iostream>

#include "pmgd.h"
#include "util.h"

#include "log_util.h"

using namespace std;
using namespace chrono;
using namespace PMGD;

static const char ID_STR[] = "pmgd.loader.id";
static StringID ID;

void bfs(Graph &db, StringID tag, int maxDep) {
    Transaction tx(db, Transaction::ReadOnly);
    std::set<StringID> visted;
    std::queue<StringID> que;
    std::queue<StringID> que2;
    visted.insert(tag);
    que.push(tag);
    int currentDep = 1;
    while(!que.empty() && currentDep < maxDep) {
        StringID id = que.front();
        que.pop();

        Node &n = *(db.get_nodes(id));
        for (EdgeIterator it = n.get_edges(Direction::Outgoing); it; it.next()) {
            Node &next = (*it).get_destination();
            if (!visted.count(next.get_tag())) {
                LOG_DEBUG_WRITE("console", "level:{} : {} -> {}", currentDep, n.get_tag().name(), next.get_tag().name());
                // std::cout << it->SecondNode << " ";
                visted.insert(next.get_tag());
                que2.push(next.get_tag());
            }
        }
        if (que.empty()) {
            currentDep++;
            if (currentDep >= maxDep)
                continue;
            // LOG_DEBUG_WRITE("console", "node in depth:{} is {}", currentDep, que2.size())
            while(!que2.empty()) {
                que.push(que2.front());
                que2.pop();
            }

            // std::cout << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    // load json data into DB
    if (argc < 3) {
        printf("Usage: program [db path] [ldbc/freebase/twitter/graph500]\n");
        return -1;
    }
    initLog();
    string dbName = argv[1];
    // 先选点
    vector<long long> sources;
    if (!strcmp(argv[2], "ldbc")) {
        LOG_DEBUG_WRITE("console", "use ldbc dataset lid...")
        vector<long long> tmp = {146099,119049,51670,152169,153302,107604,112505,51921,171483,156751}; //ldbc json2
        sources = tmp;
    } else if (!strcmp(argv[2], "freebase")) {
        LOG_DEBUG_WRITE("console", "use freebase dataset lid...")
        vector<long long> tmp = {1465101,1202829,842389,882134,115187,1434038,1709605,1671993,1473105,372467}; //freebase json2
        sources = tmp;
    } else if (!strcmp(argv[2], "twitter")) {
        LOG_DEBUG_WRITE("console", "use twitter dataset lid...")
        vector<long long> tmp = {6560807,1803319,34194,4624064,3909079,7893588,3110229,2217947,989555,5465226};
        sources = tmp;
    } else if (!strcmp(argv[2], "graph500")) {
        LOG_DEBUG_WRITE("console", "use graph500 dataset lid...")
        vector<long long> tmp = {966892,68103,505800,194272,218438,66102,460371,696911,346984,1189161};
        sources = tmp;
    } else {
        printf("unknow dataset lids, use 1 as source node, create sample graph.\n");
        vector<long long> tmp = {1};
        sources = tmp;

        dbName = "sample";
        try {
            Graph db(dbName.c_str(), Graph::Create);
            Transaction tx(db, Transaction::ReadWrite);
            Node &n1 = db.add_node(1);
            Node &n149 = db.add_node(149);
            Node &n434 = db.add_node(434);
            Node &n977 = db.add_node(977);
            Node &n411 = db.add_node(411);
            Node &n249 = db.add_node(249);
            Node &n641 = db.add_node(641);

            db.add_edge(n1, n434, 723);
            db.add_edge(n1, n977, 626);
            db.add_edge(n411, n1, 339);
            db.add_edge(n1, n641, 700);
            db.add_edge(n149, n1, 130);
            db.add_edge(n434, n249, 551);
            db.add_edge(n249, n977, 390);
            db.add_edge(n1, n434, 241);
            tx.commit();
        } catch (Exception e) {
            print_exception(e);
            return 1;
        }
    }

    try {
        Graph db(dbName.c_str(), Graph::ReadOnly);

        // 确定点是否存在
        vector<StringID> ss;
        {
            Transaction tx(db, Transaction::ReadOnly);
            ID = StringID(ID_STR);
            for (auto s : sources) {
                NodeIterator nodes = db.get_nodes(0, PropertyPredicate(ID, PropertyPredicate::Eq, s));
                if (nodes) {
                    ss.emplace_back((*nodes).get_tag());
                    LOG_DEBUG_WRITE("console", "node id:{} => {}", s, true)
                } else {
                    LOG_ERROR_WRITE("console", "node id:{} => {}", s, true)
                    exit(0);
                }
            }
            LOG_DEBUG_WRITE("console", "select {} nodes as below:", sources.size())
            string tmp;
            for (auto s : sources) {
                tmp += std::to_string(s);
                tmp += ", ";
            }
            LOG_DEBUG_WRITE("console", "{}", tmp)
        }

        for (int dep = 2; dep <= 5; dep++) {
            double totalSeconds = 0;
            for (int i = 0;i < ss.size(); i++) {
                auto &tag = ss[i];
                auto start_t = system_clock::now();
                bfs(db, tag, dep);
                auto end_t = system_clock::now();
                auto duration = duration_cast<microseconds>(end_t - start_t);
                // auto ms = duration_cast<std::chrono::milliseconds>(end_t - start_t);
                LOG_DEBUG_WRITE("console", "source node is {} (StringID:{}), BFS dep is {}, duration is {} microseconds ≈ {} s",
                                sources[i], tag.name(), dep, double(duration.count()),
                                double(duration.count()) * microseconds::period::num / microseconds::period::den);
                totalSeconds += double(duration.count()) * microseconds::period::num / microseconds::period::den;
            }
            LOG_DEBUG_WRITE("console", "===> BFS dep is {}, total duration is {} s, {} s per node. <===", dep, totalSeconds, totalSeconds / sources.size())
        }

    } catch (Exception e) {
        print_exception(e);
        return 1;
    }

}