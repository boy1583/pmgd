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

void usage() {
    printf("Usage: program [db_name] [ldbc/freebase/twitter/graph500]\n");
    exit(0);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage();
    }

    // init log
    initLog();

    char* dbName = argv[1];
    string dataset = argv[2];

    vector<long long> ids;
    if (dataset == "ldbc") {
        ids = {871871,562439,1022837,842648,399882,519531,1148153,449638,411950,1257602};
    } else if (dataset == "freebase") {
        ids = {90775171808048LL,95575470755348LL,81707782495348LL,80669087845748LL,48525481515448LL,57676777527448LL,57505683687148LL,499084754948LL,68875188845748LL,505274954948LL};
    } else if (dataset == "twitter") {

    } else if (dataset == "graph500") {

    } else {
        usage();
    }

    try {
        Graph db(dbName, Graph::ReadOnly);
        Transaction tx(db, Transaction::ReadOnly);

        for (int i = 0;i < ids.size(); i++) {
            int j = (i + 1) % ids.size();
            long long src = ids[i];
            long long dst = ids[j];
            // from src to dst






        }


    } catch (Exception e) {
        print_exception(e);
        return 1;
    }





}