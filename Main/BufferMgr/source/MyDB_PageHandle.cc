
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
	--pagePtr->referenceCount;
	if (pagePtr->referenceCount == 0) {
		if (pagePtr->memory) {
			pagePtr->pin = false;
		} else {
			bufferManager.deletePage(pagePtr);
		}
	}
	pagePtr = nullptr;
}

#endif

