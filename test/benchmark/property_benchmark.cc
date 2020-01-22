//
// Created by xxy on 2020/1/18.
//

#include <map>
#include <string>
#include <chrono>
#include <map>

#include "log_util.h"

#include "pmgd.h"
#include "util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;

std::map<std::string, std::string> datas;

static std::string gen_random(const int len) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    std::string res(len, '\0');
    for (int i = 0; i < len; ++i) {
        res[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return res;
}

// 200w buckets, 800 entry cap
// 100w random test each time
// 10 times
// int times = 1;
const uint64_t dataSize = 20000;
const uint64_t keySize = 10;
const uint64_t valSize = 20;

void generatePropertyArray() {
    datas.clear();
    for (int i = 0;i < dataSize; i++) {
        std::string key = gen_random(keySize);
        std::string val = gen_random(valSize);
        if (datas.count(key)) {
            i--;
            continue;
        }
        datas[key] = val;
    }
    assert(datas.size() == dataSize);
    LOG_INFO_WRITE("console", "generated {} properties, length of each key is {}, length of each value is {}",
                   dataSize, keySize, valSize)
}

void nodePropertyBenchmark(Graph &db) {
    Node *n = 0;
    // create a node
    {
        Transaction tx(db, Transaction::ReadWrite);
        Node &node = db.add_node(0);
        n = &node;
    }

    // insert
    {
        auto start_t = system_clock::now();
        Transaction tx(db, Transaction::ReadWrite);
        for (auto &kv : datas) {
            n->set_property(kv.first.c_str(), kv.second.c_str());
        }
        tx.commit();
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone insert test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }


    // get
    {
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadOnly);
            for (auto &kv : datas) {
                n->get_property(kv.first.c_str()).string_value();
            }
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t);
        LOG_DEBUG_WRITE("console",
                        "node property zone get test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

    // update
    {
        // change all value
        LOG_DEBUG_WRITE("console", "modify all value in map...")
        for (auto &kv : datas) {
            kv.second = gen_random(valSize);
        }
        LOG_DEBUG_WRITE("console", "modify finished...")
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadWrite);
            for (auto &kv : datas) {
                n->set_property(kv.first.c_str(), kv.second.c_str());
            }
            tx.commit();
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone update test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

    // delete
    {
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadWrite);
            for (auto &kv : datas) {
                n->remove_property(kv.first.c_str());
            }
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone update test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

}

void edgePropertyBenchmark(Graph &db) {
    // create a node
    Edge *e = 0;
    {
        Transaction tx(db, Transaction::ReadWrite);
        Node &n = db.add_node(1);
        Node &m = db.add_node(2);
        Edge &edge = db.add_edge(n, m, 0);
        e = &edge;
    }

    // insert
    {
        auto start_t = system_clock::now();
        Transaction tx(db, Transaction::ReadWrite);
        for (auto &kv : datas) {
            e->set_property(kv.first.c_str(), kv.second.c_str());
        }
        tx.commit();
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone insert test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }


    // get
    {
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadOnly);
            for (auto &kv : datas) {
                e->get_property(kv.first.c_str()).string_value();
            }
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t);
        LOG_DEBUG_WRITE("console",
                        "node property zone get test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

    // update
    {
        // change all value
        LOG_DEBUG_WRITE("console", "modify all value in map...")
        for (auto &kv : datas) {
            kv.second = gen_random(valSize);
        }
        LOG_DEBUG_WRITE("console", "modify finished...")
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadWrite);
            for (auto &kv : datas) {
                e->set_property(kv.first.c_str(), kv.second.c_str());
            }
            tx.commit();
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone update test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

    // delete
    {
        auto start_t = system_clock::now();
        {
            Transaction tx(db, Transaction::ReadWrite);
            for (auto &kv : datas) {
                e->remove_property(kv.first.c_str());
            }
        }
        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t); // μs 微妙
        LOG_DEBUG_WRITE("console",
                        "node property zone update test => key size: {} bytes, value size: {} bytes, number of record: {}",
                        keySize, valSize, dataSize)
        LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den,
                        double(duration.count()) / dataSize)
    }

}


int main(int argc, char* argv[]) {
//    if (argc < 2) {
//        printf("Usage: program [devdax path]\n");
//        return -1;
//    }
    // char *PATH = argv[1];

    initLog();

    Graph db("property_db", Graph::Create);

    LOG_DEBUG_WRITE("console", "create new zone succeeded.")

    // 0. generate
    generatePropertyArray();

    LOG_INFO_WRITE("console", "begin node property benchmark...")

    // 1. insert node property
    // 2. get node property
    // 3. update node property
    // 4. delete node property
    nodePropertyBenchmark(db);

    LOG_INFO_WRITE("console", "finish node property benchmark...")

    LOG_INFO_WRITE("console", "begin edge property benchmark...")

    // 1. insert edge property
    // 2. get edge property
    // 3. update edge property
    // 4. delete edge property
    edgePropertyBenchmark(db);

    LOG_DEBUG_WRITE("console", "close pool finished.")
}