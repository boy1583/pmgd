/**
 * @file   load_gson_test.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#include <iostream>
#include <chrono>
#include "pmgd.h"
#include "util.h"

using namespace PMGD;
using namespace std;
using namespace chrono;

long long num_nodes = 0;
long long num_edges = 0;

static void node_added(Node &);
static void edge_added(Edge &);

int main(int argc, char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <input>" << std::endl;
        return -1;
    }

    try {
        Graph db("load_gson_graph", Graph::Create);

        auto start_t = system_clock::now();
        load_gson(db, argv[1], node_added, edge_added);
        auto end_t   = system_clock::now();
        auto duration = duration_cast<microseconds>(end_t - start_t);
        printf("load finished. duration is %f microseconds ≈ %f s, \n", double(duration.count()), double(duration.count()) * microseconds::period::num / microseconds::period::den);

        // Transaction tx(db);
        // dump_debug(db);
        // tx.commit();

        Transaction tx(db, Transaction::ReadOnly);
        // 检验是否读取正确
        NodeIterator nodes = db.get_nodes(0,
                                          PropertyPredicate(StringID("pmgd.loader.id"), PropertyPredicate::Eq, 1));
        Node &node = *nodes;
        auto iter = node.get_properties();
        while (iter) {
            printf("  %s: %s\n", iter->id().name().c_str(), property_text(*iter).c_str());
            iter.next();
        }

    }
    catch (Exception e) {
        print_exception(e);
        return 1;
    }

    std::cout << "\nNodes (" << num_nodes << ")\t";
    std::cout << "Edges (" << num_edges << ")\n";



    return 0;
}

static void node_added(Node &n)
{
    ++num_nodes;
}

static void edge_added(Edge &e)
{
    ++num_edges;
}
