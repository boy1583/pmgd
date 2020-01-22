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

void bfs(Graph &db, uint64_t tag, int maxDep) {
    // Transaction tx(db, Transaction::ReadWrite);
    std::set<uint64_t> visted;
    std::queue<uint64_t> que;
    std::queue<uint64_t> que2;
    visted.insert(tag);
    que.push(tag);
    int currentDep = 1;
    while(!que.empty() && currentDep < maxDep) {
        uint64_t id = que.front();
        que.pop();

        Node &n = *(db.get_nodes(0, PropertyPredicate(StringID(ID_STR), PropertyPredicate::Eq, (long long)id)));
        for (EdgeIterator it = n.get_edges(); it; it.next()) {
            // get both direct edge
            Node &next = (*it).get_destination();
            long long next_id = next.get_property(StringID(ID_STR)).int_value();
            if (next_id == id) {
                Node &pre = (*it).get_source();
                next_id = pre.get_property(StringID(ID_STR)).int_value();
            }
            if (!visted.count(next_id)) {
                visted.insert(next_id);
                que2.push(next_id);
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
        printf("Usage: program [db_name] [ldbc/freebase/mico/yeast]\n");
        return -1;
    }
    initLog();
    string dbName = argv[1];
    // 先选点
    vector<long long> sources;
    if (!strcmp(argv[2], "ldbc")) {
        LOG_DEBUG_WRITE("console", "use ldbc dataset lid...")
        vector<long long> tmp = {168062,126101,2900,164352,95984,115510,18809,103863,97894,32702}; //ldbc json2
        sources = tmp;
    } else if (!strcmp(argv[2], "freebase")) {
        LOG_DEBUG_WRITE("console", "use freebase dataset lid...")
        vector<long long> tmp = {1484158,63957,624478,1005153,1845599,1160813,1554886,1385143,147440,650644}; //freebase json2
        sources = tmp;
    } else if (!strcmp(argv[2], "mico")) {
        LOG_DEBUG_WRITE("console", "use twitter dataset lid...")
        vector<long long> tmp = {10693,59896,10464,33317,95783,79938,92858,86993,14637,94686};
        sources = tmp;
    } else if (!strcmp(argv[2], "yeast")) {
        LOG_DEBUG_WRITE("console", "use graph500 dataset lid...")
        vector<long long> tmp = {517,831,541,313,1941,2263,1079,1883,349,1850};
        sources = tmp;
    } else if (!strcmp(argv[2], "sample")) {
        LOG_DEBUG_WRITE("console", "use sample dataset lid...(only 1)")
        vector<long long> tmp = {1};
        sources = tmp;
    } else {
        printf("create sample graph...\n");
        vector<long long> tmp = {1};
        sources = tmp;
        dbName = "sample";
        try {
            Graph db(dbName.c_str(), Graph::Create);
            {
                Transaction tx(db, Transaction::ReadWrite);
                ID = StringID(ID_STR);
                db.create_index(Graph::NodeIndex, 0, ID_STR, PropertyType::Integer);
                db.create_index(Graph::EdgeIndex, 0, ID_STR, PropertyType::Integer);
                tx.commit();
            }
            {
                Transaction tx(db, Transaction::ReadWrite);
                Node &n1 = db.add_node("person");
                n1.set_property(ID, 1LL);
                 n1.set_property("name", "marko");
                 n1.set_property("age", 29);

                Node &n2 = db.add_node("person");
                n2.set_property(ID, 2LL);
                n2.set_property("name", "vadas");
                n2.set_property("age", 27);

                Node &n3 = db.add_node("software");
                n3.set_property(ID, 3LL);
                n3.set_property("name", "lop");
                n3.set_property("lang", "java");

                Node &n4 = db.add_node("person");
                n4.set_property(ID, 4LL);
                n4.set_property("name", "josh");
                n4.set_property("age", 32);

                Node &n5 = db.add_node("software");
                n5.set_property(ID, 5LL);
                n5.set_property("name", "ripple");
                n5.set_property("lang", "java");

                Node &n6 = db.add_node("person");
                n6.set_property(ID, 6LL);
                n6.set_property("name", "peter");
                n6.set_property("age", 35);

                Edge &e7 = db.add_edge(n1, n2, "knows");
                e7.set_property(ID, 7LL);
                e7.set_property("weight", 0.5);

                Edge &e8 = db.add_edge(n1, n4, "knows");
                e8.set_property(ID, 8LL);
                e8.set_property("weight", 1.0);

                Edge &e9 = db.add_edge(n1, n3, "created");
                e9.set_property(ID, 9LL);
                e9.set_property("weight", 0.4);

                Edge &e10 = db.add_edge(n4, n5, "created");
                e10.set_property(ID, 10LL);
                e10.set_property("weight", 1.0);

                Edge &e11 = db.add_edge(n4, n3, "created");
                e11.set_property(ID, 11LL);
                e11.set_property("weight", 0.4);

                Edge &e12 = db.add_edge(n6, n3, "created");
                e12.set_property(ID, 12LL);
                e12.set_property("weight", 0.2);

                tx.commit();
            }
            exit(0);
        } catch (Exception e) {
            print_exception(e);
            return 1;
        }
    }

    try {
        Graph db(dbName.c_str(), Graph::ReadOnly);

        // 确定点是否存在
        {
            Transaction tx(db, Transaction::ReadOnly);
            // vector<uint64_t> ss;
            for (auto s : sources) {
                NodeIterator nodes = db.get_nodes(0, PropertyPredicate(StringID(ID_STR), PropertyPredicate::Eq, s));
                if (nodes) {
                    // ss.emplace_back((*nodes).get_tag());
                    LOG_DEBUG_WRITE("console", "node id:{} => {}", s, true)
                } else {
                    LOG_ERROR_WRITE("console", "node id:{} => {}", s, false)
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

            for (int dep = 2; dep <= 5; dep++) {
                double totalSeconds = 0;
                for (int i = 0;i < sources.size(); i++) {
                    auto start_t = system_clock::now();
                    bfs(db, sources[i], dep);
                    auto end_t = system_clock::now();
                    auto duration = duration_cast<microseconds>(end_t - start_t);
                    // auto ms = duration_cast<std::chrono::milliseconds>(end_t - start_t);
                    LOG_DEBUG_WRITE("console", "source node is {}, BFS dep is {}, duration is {} microseconds ≈ {} s",
                                    sources[i], dep, double(duration.count()),
                                    double(duration.count()) * microseconds::period::num / microseconds::period::den);
                    totalSeconds += double(duration.count()) * microseconds::period::num / microseconds::period::den;
                }
                LOG_DEBUG_WRITE("console", "===> BFS dep is {}, total duration is {} s, {} s per node. <===", dep, totalSeconds, totalSeconds / sources.size())
            }
        }
    } catch (Exception e) {
        print_exception(e);
        return 1;
    }

}