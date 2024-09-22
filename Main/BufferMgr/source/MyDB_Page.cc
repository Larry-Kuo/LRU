
#ifndef MYDB_PAGE_C
#define MYDB_PAGE_C

#include "MyDB_Page.h"
#include <iostream>

using namespace std;

PageBase :: ~PageBase () {
    memory = nullptr;
}

#endif