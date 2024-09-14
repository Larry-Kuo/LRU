
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"

void *MyDB_PageHandleBase :: getBytes () {
	return pagePtr->memory;
}

void MyDB_PageHandleBase :: wroteBytes () {
	pagePtr->dirty = true;
}

void MyDB_PageHandleBase :: unpin () {
	pagePtr->pin = false;
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
}

#endif

