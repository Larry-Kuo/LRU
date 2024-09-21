#ifndef MYDB_PAGE_H
#define MYDB_PAGE_H

#include "MyDB_PageHandle.h"
#include "unordered_set"
#include <string>

using namespace std;

class MyDB_PageHandleBase;
typedef std::shared_ptr<MyDB_PageHandleBase> MyDB_PageHandle;

struct Page {
    char* memory;
    std::string table;
    long pageID;
    bool pin;
    bool dirty;
    int referenceCount;


    Page(char* address, std::string tableName, long ID, bool pinValid)
        : memory(address), table(tableName), pageID(ID), pin(pinValid), dirty(false), referenceCount(0) {}
};

#endif