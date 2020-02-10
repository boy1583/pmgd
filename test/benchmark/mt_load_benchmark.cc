//
// Created by xxy on 2020/2/6.
//

#include <iostream>
#include <inttypes.h>
#include <stdlib.h>
#include <thread>
#include <atomic>
#include <mutex>
#include <fstream>
#include <sstream>
#include <set>
#include <queue>
#include <chrono>
#include <vector>
#include <map>

#include <rapidjson/filewritestream.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/writer.h>
#include <rapidjson/document.h>

#include "pmgd.h"
#include "util.h"
#include "log_util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;
using namespace rapidjson;

class LNode {
public:
    long long        _id;
    string          _label;
    std::vector<std::pair<std::string, std::string>> ps;
};

class LEdge {
public:
    long long    _id;
    long long    _inV;
    long long    _outV;
    string      _label;
    std::vector<std::pair<std::string, std::string>>    _ps;
};


// 1. 加载点和边至内存中

// 2. 创建点

// 3. 启动多线程创建边

// 4. join所有线程，查看结果

class MTLoadBenchmark {
private:
    /**
     * data
     */
    static vector<LNode> nodes;
    map<long long, Node*> nodeRefs;

    static vector<LEdge> edges;

    // mutex
    std::mutex mtx;
    volatile long long index;

    /**
     * db
     */
    Graph _db;
    Graph::Config _config;
public:
    MTLoadBenchmark() :
    // init_config(&_config)
        _db("graph_mt_load", Graph::Create) {
    }

    ~MTLoadBenchmark() {
    }

    /**
     * load ldbc dataset
     * @param path
     */
    static void loadLDBCDataset(const char* json2_path);

    // void createLDBCIndex();

    /**
     * load freebase dataset
     * @param path
     */
    static void loadFreebaseDataset(const char* path);

    // void createFreebaseIndex();

    /**
     * load graph500 dataset
     * @param path
     */
    void loadGraph500Dataset(const char* path);

    // void createGraph500Index();

    /**
     * load twitter dataset
     * @param path
     */
    static void loadTwitterDataset(const char* path);

    // void createTwitterIndex();


    /**
     * test insert edges in multiple threads
     */
     void test1(int nthread) {
         // multiple thread
         vector<thread> threads;

        for (int i = 0;i < nthread; i++) {
            threads.push_back(thread(&MTLoadBenchmark::insertNodeThread, this, nthread, i));
        }

        for (auto &t : threads) {
            t.join();
        }

        threads.clear();

        index = 0;

        auto start_t = system_clock::now();

         for (int i = 0;i < nthread; i++) {
             threads.push_back(thread(&MTLoadBenchmark::insertEdgeThread, this, nthread, i));
         }

         for (auto &t : threads) {
             t.join();
         }

        auto end_t = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t);
        LOG_DEBUG_WRITE("console", "{} thread(s) joined. duration is {} microseconds ≈ {} s",
                        nthread,
                        double(duration.count()),
                        double(duration.count()) * microseconds::period::num / microseconds::period::den);

         threads.clear();
     }

private:
    void insertEdgeThread(int nthread, int tindex);

     void insertNodeThread(int nthread, int tindex);

    Graph::Config& init_config(Graph::Config &config) {
        config.allocator_region_size = 104857600;  // 100MB
        printf("hardware_concurrency is %u\n", std::thread::hardware_concurrency());
        config.num_allocators = 64;
        return config;
    }
};

int main(int argc, char* argv[]) {
    initLog();

    if (argc < 5) {
        printf("Usage: program [data_type:ldbc/freebase/twitter] [dataset_path] [min_thread_num] [max_thread_num]\n");
        return 0;
    }

    const char* dataType = argv[1];
    const char* dataPath = argv[2];
    int min_thread = atoi(argv[3]);
    int max_thread = atoi(argv[4]);

    LOG_DEBUG_WRITE("console", "dataset path: {}, min number of thread: {}, max number of thread: {}",
            dataPath, min_thread, max_thread);

    vector<int> as = {1, 8, 16, 24, 32, 40};

    if (!strcmp(dataType, "ldbc")) {
        /**
     * ldbc
     */
        MTLoadBenchmark::loadLDBCDataset(dataPath);
        try {
            for (auto n : as) {
                if (system("rm -rf ./graph_mt_load") < 0)
                    exit(-1);
                MTLoadBenchmark mt;
                if (n <= max_thread && n >= min_thread) {
                    mt.test1(n);
                }
            }
        } catch (Exception &e) {
            print_exception(e);
        }
    } else if (!strcmp(dataType, "freebase")) {
        /**
     * freebase
     */
        MTLoadBenchmark::loadFreebaseDataset(dataPath);
        try {
            for (auto n : as) {
                if (system("rm -rf ./graph_mt_load") < 0)
                    exit(-1);
                MTLoadBenchmark mt;
                if (n <= max_thread && n >= min_thread) {
                    mt.test1(n);
                }
            }
        } catch (Exception &e) {
            print_exception(e);
        }
    } else if (!strcmp(dataType, "twitter")) {
        /**
     * twitter
     */
        MTLoadBenchmark::loadTwitterDataset(dataPath);
        try {
            for (auto n : as) {
                if (system("rm -rf ./graph_mt_load") < 0)
                    exit(-1);
                MTLoadBenchmark mt;
                if (n <= max_thread && n >= min_thread) {
                    mt.test1(n);
                }
            }
        } catch (Exception &e) {
            print_exception(e);
        }
    } else {
        LOG_ERROR_WRITE("console", "dataType mistake in [ldbc,freebase,twitter]")
        return 0;
    }
}

void MTLoadBenchmark::loadLDBCDataset(const char *json2_path) {
    FILE *fp = fopen(json2_path, "r");
    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    Document d;
    d.ParseStream<0, UTF8<>, FileReadStream>(is);
    long long maxE = 0, maxV = 0;
    for (auto &v : d["vertices"].GetArray()) {
        LNode node;
        node._id = v["_id"].GetInt64();
        maxV = max(maxV, node._id);
        node._label = v["xlabel"]["value"].GetString();
        auto &empty = node.ps;

        if (v.HasMember("iid")) empty.emplace_back(make_pair("iid", v["iid"]["value"].GetString()));
        if (v.HasMember("birthday")) empty.emplace_back(make_pair("birthday", v["birthday"]["value"].GetString()));
        if (v.HasMember("lastName")) empty.emplace_back(make_pair("lastName", v["lastName"]["value"].GetString()));
        if (v.HasMember("gender")) empty.emplace_back(make_pair("gender", v["gender"]["value"].GetString()));
        if (v.HasMember("length")) empty.emplace_back(make_pair("length", v["length"]["value"].GetString()));
        if (v.HasMember("language")) empty.emplace_back(make_pair("language", v["language"]["value"].GetString()));
        if (v.HasMember("creationDate")) empty.emplace_back(make_pair("creationDate", v["creationDate"]["value"].GetString()));
        if (v.HasMember("xname")) empty.emplace_back(make_pair("xname", v["xname"]["value"].GetString()));
        if (v.HasMember("url")) empty.emplace_back(make_pair("url", v["url"]["value"].GetString()));
        if (v.HasMember("firstName")) empty.emplace_back(make_pair("firstName", v["firstName"]["value"].GetString()));
        if (v.HasMember("imageFile")) empty.emplace_back(make_pair("imageFile", v["imageFile"]["value"].GetString()));
        if (v.HasMember("locationIP")) empty.emplace_back(make_pair("locationIP", v["locationIP"]["value"].GetString()));
        if (v.HasMember("email")) empty.emplace_back(make_pair("email", v["email"]["value"].GetString()));

        nodes.emplace_back(node);
    }
    LOG_INFO_WRITE("console", "Load {} nodes. maxNodeId is {}", nodes.size(), maxV)

    for (auto &e : d["edges"].GetArray()) {
        LEdge edge;
        edge._id = e["_id"].GetInt64();
        // assert(edge._id != 0);
        maxE = max(maxE, edge._id);
        edge._inV = e["_inV"].GetInt64();
        edge._outV = e["_outV"].GetInt64();
        edge._label = e["_label"].GetString();
        edges.emplace_back(edge);
    }
    LOG_INFO_WRITE("console", "Load {} edges. maxEdgeId is {}", edges.size(), maxE)
    fclose(fp);
}

void MTLoadBenchmark::loadFreebaseDataset(const char *json2_path) {
    FILE* fp = fopen(json2_path, "r");

    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));

    Document d;
    d.ParseStream<0, UTF8<>, FileReadStream>(is);

    std::string mode = d["mode"].GetString();
    if (mode != "EXTENDED") {
        std::cerr << "ldbc dataset file must be EXTENDED format!";
        exit(0);
    }

    // 点 freebaseid _type mid _id
    for (auto& v : d["vertices"].GetArray()) {
        LNode node;
        node._id = v["_id"].GetUint64();
        node._label = v["_type"].GetString();
        node.ps.emplace_back(make_pair("freebaseid", std::to_string(v["freebaseid"]["value"].GetUint64())));
        node.ps.emplace_back(make_pair("mid", v["mid"]["value"].GetString()));
        // if (vn > 1000) break;
        nodes.emplace_back(node);
    }
    LOG_INFO_WRITE("console", "Load {} nodes.", nodes.size())

    // 边 _type _outV _id _inV _label
    for (auto& n : d["edges"].GetArray()) {
        LEdge edge;
        edge._id = n["_id"].GetUint64();
        edge._outV = n["_outV"].GetUint64();
        edge._inV = n["_inV"].GetUint64();
        edge._label = n["_label"].GetString();
        // if (nn > 1000) break;
        edges.emplace_back(edge);
    }
    LOG_INFO_WRITE("console", "Load {} edges.", edges.size())
    fclose(fp);
}

void MTLoadBenchmark::loadTwitterDataset(const char *json2_path) {
    std::ifstream ifile(json2_path);
    uint64_t maxE = 11316811ULL;
    nodes.resize(maxE);
    for (uint64_t e = 1; e <= maxE; e++) {
        nodes[e - 1]._id = e;
        nodes[e - 1]._label = "labelV";
    }
    LOG_INFO_WRITE("console", "Load {} nodes.", nodes.size())

    std::string buffer;
    uint64_t from, to;
    // std::set<uint64_t> es;
    // uint64_t countE = 0, countV = 0;
    uint64_t index = 1;
    while(std::getline(ifile, buffer, (char)(0x0A))) {
        std::istringstream sin(buffer);
        sin >> from >> to;
        LEdge edge;
        edge._id = index++;
        edge._outV = from;
        edge._inV = to;
        edge._label = "labelE";
        edges.emplace_back(edge);
    }
    LOG_INFO_WRITE("console", "Load {} edges.", edges.size())
}

void MTLoadBenchmark::insertEdgeThread(int nthread, int tindex) {
    long long count = 0;
    const long long total = edges.size();
    long long begin, end;
    while(true) {
        mtx.lock();
        begin = index;
        index += 10000;
        end = index;
        mtx.unlock();
        if (begin >= total)
            break;
        if (end > total)
            end = total;
        count += (end - begin);
        try {
            for (long long i = begin; i < end; i++) {
                Transaction tx(_db, Transaction::ReadWrite);
                // insert edge
                auto &e = edges[i];
                Edge &edge = _db.add_edge(*nodeRefs[e._outV], *nodeRefs[e._inV], e._label.c_str());
                edge.set_property("_id", e._id);
                for (auto &p : e._ps) {
                    edge.set_property(p.first.c_str(), p.second.c_str());
                }
                tx.commit();
            }
        } catch (Exception &e) {
            print_exception(e);
        }
    }
    long long part = total / nthread;

    LOG_DEBUG_WRITE("console", "thread_{} finish handled {} records ≈ {}%", tindex, count, (100.0 * count / part))
}

void MTLoadBenchmark::insertNodeThread(int nthread, int tindex) {
    // insert node
    long long total = nodes.size();
    long long part = (total + nthread - 1) / nthread;
    long long begin = tindex * part;
    long long end = begin + part;
    if (end > total) end = total;
    try {
        Transaction tx(_db, Transaction::ReadWrite);
        for (long long i = begin; i < end; i++) {
            auto &node = nodes[i];
            Node &n = _db.add_node(node._label.c_str());
            n.set_property("_id", node._id);
            for (auto &p : node.ps) {
                n.set_property(p.first.c_str(), p.second.c_str());
            }
            nodeRefs[node._id] = &n;
        }
        tx.commit();

    } catch (Exception &e) {
        print_exception(e);
    }
    LOG_DEBUG_WRITE("console", "add all node finished, count is {}", nodeRefs.size())
}


std::vector<LNode> MTLoadBenchmark::nodes;
std::vector<LEdge> MTLoadBenchmark::edges;