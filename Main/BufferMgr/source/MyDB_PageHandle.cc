
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"
#include <iostream>
using namespace std;

void *MyDB_PageHandleBase :: getBytes () {
	if (pagePtr->memory)
		return pagePtr->memory;
	else {
		return bufferManager.retrievePage(pagePtr);
	}
}

void MyDB_PageHandleBase :: wroteBytes () {
	pagePtr->dirty = true;
}

void MyDB_PageHandleBase :: unpin () {
	pagePtr->pin = false;
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
	if (pagePtr.use_count() == 3 && pagePtr->pin) {
    	pagePtr->pin = false; 
	}
	if (pagePtr.use_count() == 2) {
		bufferManager.updateBufferMap(pagePtr->table, pagePtr->pageID);
	}
}

#endif

