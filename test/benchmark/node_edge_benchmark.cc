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
    FILE *fp = fopen(ldbcPath, "r");

    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));

    Document d;
    d.ParseStream<0, UTF8<>, FileReadStream>(is);

    long long maxE = 0, maxV = 0;

    // std::cout << d["mode"].GetString();
    for (auto &v : d["vertices"].GetArray()) {
        LDBCNode node;
        node._id = v["_id"].GetInt64();
        maxV = max(maxV, node._id);
        node.xlabel = v["xlabel"]["value"].GetString();
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

//        cout << v["_id"].GetUint64() << endl;
        // _id xlabel iid  oid
        // birthday lastName gender browserUsed length language creationDate
        // title type xname content url firstName imageFile locationIP email
        // because euler id from 1, ldbc from zero, need + 1
    }
    LOG_INFO_WRITE("console", "Load {} nodes. maxNodeId is {}", nodes.size(), maxV)

    for (auto &e : d["edges"].GetArray()) {
        LDBCEdge edge;
        edge._id = e["_id"].GetInt64();
        // assert(edge._id != 0);
        maxE = max(maxE, edge._id);
        edge._inV = e["_inV"].GetInt64();
        edge._outV = e["_outV"].GetInt64();
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
        node.set_property(ID_STR, n._id);
        // 可以省略了插入属性
        for (auto &p : n.ps) {
            node.set_property(StringID(p.first.c_str()), p.second);
        }
        tx.commit();
    }

    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node insert test (not include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / nodes.size())
}

// 查询点
void getNodeBenchmark(Graph  &db) {
    auto start_t = system_clock::now();
    for (auto &node : nodes) {
        Transaction tx(db, Transaction::ReadOnly);
        get_node(db, node._id, nullptr);
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node get test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / nodes.size())
}


// 修改点(label)
void updateNodeBenchmark(Graph &db) {
    std::string label = "newlabel";
    auto start_t = system_clock::now();

    for (auto &n : nodes) {
        Transaction tx(db, Transaction::ReadWrite);
        Node &node = get_node(db, n._id, nullptr);
        node.set_property(StringID("xlabel"), label);
        tx.commit();
    }

    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node update test (include property) => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / nodes.size())
}

// 删除点
void deleteNodeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &n : nodes) {
        Transaction tx(db, Transaction::ReadWrite);
        Node &node = get_node(db, n._id, nullptr);
        db.remove(node);
        tx.commit();
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "node delete test => number of record: {}", nodes.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / nodes.size())
}

// (再次插入点)

// 插入边
void insertEdgeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &e : edges) {
        Transaction tx(db, Transaction::ReadWrite);

        Node &src = get_node(db, e._outV, nullptr);
        Node &dst = get_node(db, e._inV, nullptr);
        Edge &edge = db.add_edge(src, dst, "labelE");

        edge.set_property(ID_STR, e._id);
        // maybe not include property
        edge.set_property(StringID("_label"), e._label);

        tx.commit();
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge insert test (not include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / edges.size())

}

// 查询边
void getEdgeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &edge : edges) {
        Transaction tx(db, Transaction::ReadOnly);
        get_edge(db, edge._id, nullptr);
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge get test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / edges.size())
}

// 修改边
void updateEdgeBenchmark(Graph &db) {
    std::string label = "newlabel";
    auto start_t = system_clock::now();
    for (auto &e : edges) {
        Transaction tx(db, Transaction::ReadWrite);
        Edge &edge = get_edge(db, e._id, nullptr);
        edge.set_property(StringID("_label"), e._label);
        tx.commit();
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge update test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / edges.size())
}

// 删除边
void deleteEdgeBenchmark(Graph &db) {
    auto start_t = system_clock::now();
    for (auto &e : edges) {
        Transaction tx(db, Transaction::ReadWrite);
        Edge &edge = get_edge(db, e._id, nullptr);
        db.remove(edge);
        tx.commit();
    }
    auto end_t = system_clock::now();
    auto duration = duration_cast<microseconds>(end_t - start_t);
    LOG_DEBUG_WRITE("console", "edge delete test (include property) => number of record: {}", edges.size())
    LOG_DEBUG_WRITE("console", "duration is {} microseconds ≈ {} s, {} microseconds each record",
                    double(duration.count()),
                    double(duration.count()) * microseconds::period::num / microseconds::period::den,
                    double(duration.count()) / edges.size())
}

void count(Graph &db) {
    // count nodes and edges
    {
        Transaction tx(db, Transaction::ReadOnly);
        // printf("## Trying plain get_nodes() and get_edges() iterator\n");
        long long nodes_added = 0, edges_added = 0;
        auto start_t = system_clock::now();
        for (NodeIterator i = db.get_nodes(); i; i.next())
            nodes_added++;
        auto end_n = system_clock::now();
        for (EdgeIterator i = db.get_edges(); i; i.next())
            edges_added++;
        auto end_e = system_clock::now();
        auto duration_n = duration_cast<microseconds>(end_n - start_t);
        auto duration_e = duration_cast<microseconds>(end_e - end_n);
        LOG_DEBUG_WRITE("console", "edge count test => number of nodes: {} number of edges: {}", nodes_added, edges_added)
        LOG_DEBUG_WRITE("console", "count node duration is {} microseconds ≈ {} s",
                        double(duration_n.count()),
                        double(duration_n.count()) * microseconds::period::num / microseconds::period::den)
        LOG_DEBUG_WRITE("console", "count edge duration is {} microseconds ≈ {} s",
                        double(duration_e.count()),
                        double(duration_e.count()) * microseconds::period::num / microseconds::period::den)
    }
}

void nodePropertySearch(Graph &db, vector<long long> &ids) {
    // 先插入10个属性
    const char* PROPERTY_NAME = "test_specific_property";
    vector<string> vals(ids.size());
    for (int i = 0;i < ids.size(); i++) {
        vals[i] = "test_value_" + std::to_string(i);
        {
            Transaction tx(db, Transaction::ReadWrite);
            Node &node = get_node(db, ids[i], nullptr);
            node.set_property(PROPERTY_NAME, vals[i].c_str());
            tx.commit();
            LOG_DEBUG_WRITE("console", "insert {}:{} to node id:{}", PROPERTY_NAME, vals[i], ids[i])
        }
    }

    // 仿照test suite中
    {
        Transaction tx(db, Transaction::ReadOnly);
        for (int i = 0;i < ids.size(); i++) {
            long long count = 0;
            auto start_t = system_clock::now();
            NodeIterator nodes = db.get_nodes(0,
                                              PropertyPredicate(PROPERTY_NAME, PropertyPredicate::Eq, vals[i]));
            while(nodes) {
                count++;
                nodes.next();
            }
            auto end_n = system_clock::now();
            auto duration_n = duration_cast<microseconds>(end_n - start_t);
            LOG_DEBUG_WRITE("console", "iterator_{} {}:{} count:{} duration is {} microseconds ≈ {} s",
                    i, PROPERTY_NAME, vals[i], count, double(duration_n.count()),
                            double(duration_n.count()) * microseconds::period::num / microseconds::period::den)
        }
        for (int i = ids.size();i >= 0;i--) {
            long long count = 0;
            auto start_t = system_clock::now();
            NodeIterator nodes = db.get_nodes(0,
                                              PropertyPredicate(PROPERTY_NAME, PropertyPredicate::Eq, vals[i]));
            while(nodes) {
                count++;
                nodes.next();
            }
            auto end_n = system_clock::now();
            auto duration_n = duration_cast<microseconds>(end_n - start_t);
            LOG_DEBUG_WRITE("console", "iterator_10_{} {}:{} count:{} duration is {} microseconds ≈ {} s",
                            i, PROPERTY_NAME, vals[i], count, double(duration_n.count()),
                            double(duration_n.count()) * microseconds::period::num / microseconds::period::den)
        }
    }
}

void edgePropertySearch(Graph &db, vector<long long> &ids) {
    // 先添加10条边
    const char* LABEL = "test_label";
    const char* PROPERTY_NAME = "test_specific_property";
    vector<string> vals(ids.size());
    for (int i = 0;i < ids.size(); i++) {
        vals[i] = "test_value_" + std::to_string(i);
        {
            Transaction tx(db, Transaction::ReadWrite);
            Node &n = get_node(db, ids[i], nullptr);
            Node &m = get_node(db, ids[(i + 1) % ids.size()], nullptr);
            Edge &edge = db.add_edge(n, m, LABEL);
            edge.set_property(PROPERTY_NAME, vals[i].c_str());
            tx.commit();
            LOG_DEBUG_WRITE("console", "insert {}:{} to edge {}->{}",
                    PROPERTY_NAME, vals[i], ids[i], ids[(i + 1) % ids.size()])
        }
    }

    // 仿照test suite中
    {
        Transaction tx(db, Transaction::ReadOnly);
        for (int i = 0;i < ids.size(); i++) {
            long long count = 0;
            auto start_t = system_clock::now();
            EdgeIterator edges = db.get_edges(0,
                                              PropertyPredicate(PROPERTY_NAME, PropertyPredicate::Eq, vals[i]));
            while(edges) {
                count++;
                edges.next();
            }
            auto end_n = system_clock::now();
            auto duration_n = duration_cast<microseconds>(end_n - start_t);
            LOG_DEBUG_WRITE("console", "iterator_{} {}:{} count:{} duration is {} microseconds ≈ {} s",
                            i, PROPERTY_NAME, vals[i], count, double(duration_n.count()),
                            double(duration_n.count()) * microseconds::period::num / microseconds::period::den)
        }
        for (int i = ids.size();i >= 0;i--) {
            long long count = 0;
            auto start_t = system_clock::now();
            EdgeIterator edges = db.get_edges(0,
                                              PropertyPredicate(PROPERTY_NAME, PropertyPredicate::Eq, vals[i]));
            while(edges) {
                count++;
                edges.next();
            }
            auto end_n = system_clock::now();
            auto duration_n = duration_cast<microseconds>(end_n - start_t);
            LOG_DEBUG_WRITE("console", "iterator_10_{} {}:{} count:{} duration is {} microseconds ≈ {} s",
                            i, PROPERTY_NAME, vals[i], count, double(duration_n.count()),
                            double(duration_n.count()) * microseconds::period::num / microseconds::period::den)
        }
    }
}

void usage() {
    printf("Usage:\n");
    printf("\tprogram create [dataset_path] [Graph_Name] [count...getnode...getedge...updatenode...updateedge...deleteedge...deletenode]\n");
    printf("\tprogram load [Graph_Name] [count]\n");
    printf("\tprogram nsearch [Graph_Name] (only for ldbc)\n");
    printf("\tprogram esearch [Graph_Name] (only for ldbc)\n");
    exit(0);
}

void doWork(int index, int argc, char* argv[], Graph &db) {
    for (int i = index;i < argc; i++) {
        if (!strcmp(argv[i], "count")) {
            count(db);
        } else if (!strcmp(argv[i], "getnode")) {
            getNodeBenchmark(db);
        } else if (!strcmp(argv[i], "getedge")) {
            getEdgeBenchmark(db);
        } else if (!strcmp(argv[i], "updatenode")) {
            updateNodeBenchmark(db);
        } else if (!strcmp(argv[i], "updateedge")) {
            updateEdgeBenchmark(db);
        } else if (!strcmp(argv[i], "deleteedge")) {
            deleteEdgeBenchmark(db);
        } else if (!strcmp(argv[i], "deletenode")) {
            deleteNodeBenchmark(db);
        } else {
            usage();
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage();
    }

    // init log
    initLog();

    // create database
    try {
        if (!strcmp(argv[1], "create")) {
            if (argc < 2) {
                usage();
            }
            // load data
            loadLdbcDataSet(argv[2]);
            // create new database
            Graph db(argv[3], Graph::Create);
            Transaction tx(db, Transaction::ReadWrite);
            ID = StringID(ID_STR);
            db.create_index(Graph::NodeIndex, 0, ID_STR, PropertyType::Integer);
            db.create_index(Graph::EdgeIndex, 0, ID_STR, PropertyType::Integer);
            tx.commit();

            insertNodeBenchmark(db);

            insertEdgeBenchmark(db);

            doWork(4, argc, argv, db);

        } else if (!strcmp(argv[1], "load")) {
            // load ReadWrite
            Graph db(argv[2], Graph::ReadWrite);
            Transaction tx(db, Transaction::ReadWrite);
            ID = StringID(ID_STR);
            tx.commit();

            count(db);

        } else if (!strcmp(argv[1], "nsearch")) {
            Graph db(argv[2], Graph::ReadWrite);
            Transaction tx(db, Transaction::ReadWrite);
            ID = StringID(ID_STR);
            tx.commit();
            // ldbc
            vector<long long> ids = {168062,126101,2900,164352,95984,115510,18809,103863,97894,32702};
            nodePropertySearch(db, ids);
        } else if (!strcmp(argv[1], "esearch")) {
            Graph db(argv[2], Graph::ReadWrite);
            Transaction tx(db, Transaction::ReadWrite);
            ID = StringID(ID_STR);
            tx.commit();
            // ldbc
            vector<long long> ids = {168062,126101,2900,164352,95984,115510,18809,103863,97894,32702};
            edgePropertySearch(db, ids);
        } else {
            usage();
        }
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