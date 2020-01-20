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

    Graph db(dbName, Graph::ReadOnly);

    string dataset = argv[2];
    if (dataset == "ldbc") {

    } else if (dataset == "freebase") {

    } else if (dataset == "twitter") {

    } else if (dataset == "graph500") {

    } else {
        usage();
    }

}