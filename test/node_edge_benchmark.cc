//
// Created by xxy on 2020/1/17.
//
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <queue>
#include <chrono>
#include <vector>
#include <map>
#include <string>

#include <rapidjson/filewritestream.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/writer.h>
#include <rapidjson/document.h>

#include <cstdio>
#include <iostream>

#include "log_util.h"

#include "pmgd.h"
#include "util.h"

using namespace PMGD;

static const char ID_STR[] = "pmgd.loader.id";
static StringID ID;

using namespace std;
using namespace chrono;
using namespace rapidjson;

long long num_nodes = 0;
long long num_edges = 0;

void node_added(Node &);
void edge_added(Edge &);

static Node &get_node(Graph &db, long long id,
                      std::function<void(Node &)> node_func);

static Edge &get_edge(Graph &db, long long id,
                      std::function<void(Edge &)> edge_func);

// typedef std::vector<std::pair<std::string, std::string>> PropertyList;

class LDBCNode {
public:
    long long        _id;
    string          xlabel;
    std::vector<std::pair<std::string, std::string>>    ps;
};

class LDBCEdge {
public:
    long long    _id;
    long long    _inV;
    long long    _outV;
    string      _label;
};

std::vector<LDBCNode> nodes;
std::vector<LDBCEdge> edges;

void loadLdbcDataSet(const char* ldbcPath) {
    FILE* fp = fopen(ldbcPath, "r");

    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));

    Document d;
    d.ParseStream<0, UTF8<>, FileReadStream>(is);

    long long maxE = 0, maxV = 0;

    // std::cout << d["mode"].GetString();
    for (auto& v : d["vertices"].GetArray()) {
        LDBCNode node;
        node._id = v["_id"].GetInt64() + 1ULL;
        maxV = max(maxV, node._id);
        node.xlabel = v["xlabel"].GetString();
        auto &empty = node.ps;

        if (v.HasMember("iid")) empty.emplace_back(make_pair("iid", v["iid"].GetString()));
        if (v.HasMember("birthday")) empty.emplace_back(make_pair("birthday", v["birthday"].GetString()));
        if (v.HasMember("lastName")) empty.emplace_back(make_pair("lastName", v["lastName"].GetString()));
        if (v.HasMember("gender")) empty.emplace_back(make_pair("gender", v["gender"].GetString()));
        if (v.HasMember("length")) empty.emplace_back(make_pair("length", v["length"].GetString()));
        if (v.HasMember("language")) empty.emplace_back(make_pair("language", v["language"].GetString()));
        if (v.HasMember("creationDate")) empty.emplace_back(make_pair("creationDate", v["creationDate"].GetString()));
        if (v.HasMember("xname")) empty.emplace_back(make_pair("xname", v["xname"].GetString()));
        if (v.HasMember("url")) empty.emplace_back(make_pair("url", v["url"].GetString()));
        if (v.HasMember("firstName")) empty.emplace_back(make_pair("firstName", v["firstName"].GetString()));
        if (v.HasMember("imageFile")) empty.emplace_back(make_pair("imageFile", v["imageFile"].GetString()));
        if (v.HasMember("locationIP")) empty.emplace_back(make_pair("locationIP", v["locationIP"].GetString()));
        if (v.HasMember("email")) empty.emplace_back(make_pair("email", v["email"].GetString()));

        nodes.emplace_back(node);

//        cout << v["_id"].GetUint64() << endl;
        // _id xlabel iid  oid
        // birthday lastName gender browserUsed length language creationDate
        // title type xname content url firstName imageFile locationIP email
        // because euler id from 1, ldbc from zero, need + 1
    }
    LOG_INFO_WRITE("console", "Load {} nodes. maxNodeId is {}", nodes.size(), maxV)

    for (auto& e : d["edges"].GetArray()) {
        LDBCEdge edge;
        edge._id = e["_id"].GetInt64();
        assert(edge._id != 0);
        maxE = max(maxE, edge._id);
        edge._inV = e["_inV"].GetInt64() + 1ULL;
        edge._outV = e["_outV"].GetInt64() + 1ULL;
        edge._label = e["_label"].GetString();

        // _outV _id _inV _label
        edges.emplace_back(edge);
    }
    LOG_INFO_WRITE("console", "Load {} edges. maxEdgeId is {}", edges.size(), maxE)

    fclose(fp);
}

// 插入点
void insertNodeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &n : nodes) {
        Transaction tx(db, Transaction::ReadWrite);
        Node &node = db.add_node(StringID(n.xlabel.c_str()));
        for (auto &p : n.ps) {
            node.set_property(StringID(p.first.c_str()), p.second);
        }
        tx.commit();
    }

    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node insert test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / nodes.size())
}

// 查询点
void getNodeBenchmark(Graph  &db) {
    auto start_t = system_clock::now();
    for (auto &node : nodes) {
        Transaction tx(db, Transaction::ReadOnly);
        get_node(db, node._id, nullptr);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node get test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / nodes.size())
}

/*
// 修改点(label)
void updateNodeBenchmark(euler_db &db) {
    std::string label = "newlabel";
    auto start_t = system_clock::now();
    for (auto &node : nodes) {
        db.UpdateNode(node._id, label, node.ps);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node update test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / nodes.size())
}

// 删除点
void deleteNodeBenchmark(euler_db &db) {
    auto start_t = system_clock::now();
    for (auto &node : nodes) {
        db.DeleteNode(node._id);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node delete test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / nodes.size())
}

// (再次插入点)
*/
// 插入边
void insertEdgeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &e : edges) {
        Transaction tx(db, Transaction::ReadWrite);

        Node &src = get_node(db, e._outV, nullptr);
        Node &dst = get_node(db, e._inV, nullptr);
        Edge &edge = db.add_edge(src, dst, "labelE");

        edge.set_property(ID_STR, e._id);
        edge.set_property(StringID("_label"), e._label);

        tx.commit();
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge insert test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / edges.size())

}

// 查询边
void getEdgeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &edge : edges) {
        Transaction tx(db, Transaction::ReadOnly);
        get_edge(db, edge._id, nullptr);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge get test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / edges.size())
}
/*
// 修改边
void updateEdgeBenchmark(euler_db &db) {
    std::string label = "newlabel";
    auto start_t = system_clock::now();
    for (auto &edge : edges) {
        db.UpdateEdge(edge._id, label);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge update test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / edges.size())
}


// 删除边
void deleteEdgeBenchmark(euler_db &db) {
    auto start_t = system_clock::now();
    for (auto &edge : edges) {
        db.DeleteEdge(edge._id);
    }
    auto end_t   = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge delete test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den, double(duration.count()) / edges.size())
}*/

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: program [ldbc_dataset_path]\n");
        exit(0);
    }
    // init log
    initLog();
    // load data
    loadLdbcDataSet(argv[1]);

    // create database
    try {
        Graph db("ldbc_benchmark", Graph::Create);

        Transaction tx(db, Transaction::ReadWrite);
        ID = StringID(ID_STR);
        db.create_index(Graph::NodeIndex, 0, ID_STR, PropertyType::Integer);
        db.create_index(Graph::EdgeIndex, 0, ID_STR, PropertyType::Integer);
        tx.commit();

        insertNodeBenchmark(db);

        getNodeBenchmark(db);

        insertEdgeBenchmark(db);

        getEdgeBenchmark(db);

//        auto start_t = system_clock::now();
//        auto end_t   = system_clock::now();
//        auto duration = duration_cast<microseconds>(end_t - start_t);
//        printf("load finished. duration is %f microseconds ≈ %f s, \n", double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den);
    }
    catch (Exception e) {
        print_exception(e);
        // std::cerr << argv[0] << ": stdin: Exception " << e.name << " " << e.msg << "\n";
        return 1;
    }
}

void node_added(Node &n)
{
    ++num_nodes;
}

void edge_added(Edge &e)
{
    ++num_edges;
}

static Node &get_node(Graph &db, long long id,
                      std::function<void(Node &)> node_func)
{
    NodeIterator nodes = db.get_nodes(0,
                                      PropertyPredicate(ID_STR, PropertyPredicate::Eq, id));
    if (nodes) return *nodes;

    std::cerr << "node not found id:" << id << "\n";

    assert(false);

    // Node not found; add it
    // Node &node = db.add_node(0);
    /*Node &node = db.add_node("labelV");
    node.set_property(ID_STR, id);
    if (node_func)
        node_func(node);
    return node;*/
}

static Edge &get_edge(Graph &db, long long id,
                      std::function<void(Edge &)> edge_func)
{
    EdgeIterator edges = db.get_edges(0,
                                      PropertyPredicate(ID_STR, PropertyPredicate::Eq, id));
    if (edges) return *edges;

    std::cerr << "edge not found id:" << id << "\n";

    assert(false);

    // Node not found; add it
    // Node &node = db.add_node(0);
    /*Node &node = db.add_node("labelV");
    node.set_property(ID_STR, id);
    if (node_func)
        node_func(node);
    return node;*/
}