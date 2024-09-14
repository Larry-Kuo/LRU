
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long id) {
	MyDB_PageHandle pageHandle = nullptr;
	// check whether table-page pair is in the buffer map or not
	if (bufferMap.find({whichTable->getName(), id}) != bufferMap.end()) {
		// the page is in the buffer 
		auto itr = bufferMap[{whichTable->getName(), id}];
		// update the order of pageContainer 
		pageContainer.splice(pageContainer.end(), pageContainer, itr);
		// add the reference count of targetPage
		Page& targetPage = *itr;
		++targetPage.referenceCount;

		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
	} else {   
		// the page is not in the buffer
		// If the file do not exist, create one
		if (access(whichTable->getStorageLoc().c_str(), F_OK) == -1) {
			tableMap[whichTable->getStorageLoc()] = open(whichTable->getStorageLoc().c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		}
		// read data from disk   
		char data[pageSize];
		lseek(tableMap[whichTable->getStorageLoc()], id*pageSize, SEEK_SET);
    	read(tableMap[whichTable->getStorageLoc()], data, sizeof(data));

		// write target page data into bufferpool
		if (currPage < numPages) {  
			// write data into bufferpool
			int offset = currPage*pageSize;
			memcpy(bufferPool+offset, data, pageSize);
			// update the pageContainer 
			pageContainer.push_back(Page(bufferPool+offset, whichTable->getName(), id, false));
			// update bufferMap
			bufferMap[{whichTable->getName(), id}] = --pageContainer.end();
			// 
			Page& targetPage = *bufferMap[{whichTable->getName(), id}];
			pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
			currPage++;
		} else {
			// the bufferpoll is full
			// find the victim page
			list<Page>::iterator evictItr;
			for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
				if (!(*it).pin) {
					evictItr = it;
					break;
				}
			}
			// write back to disk
			Page& victim = *evictItr;
			if (victim.table!="" && victim.dirty) {
				lseek(tableMap[victim.table], victim.pageID*pageSize, SEEK_SET);
				write(tableMap[victim.table], victim.memory, pageSize);
			} else if (victim.table=="") {
				lseek(tableMap[tempFile], victim.pageID*pageSize, SEEK_SET);
				write(tableMap[tempFile], victim.memory, pageSize);
			}
			// write new data into bufferpool 
			memcpy(victim.memory, data, pageSize);
			// update the pageContainer 
			pageContainer.push_back(Page(victim.memory, whichTable->getName(), id, false));
			// update bufferMap
			bufferMap[{whichTable->getName(), id}] = --pageContainer.end();
			bufferMap.erase({victim.table, victim.pageID});
			// evict victim page
			victim.memory = nullptr;
			pageContainer.erase(evictItr);
			// 
			Page& targetPage = *bufferMap[{whichTable->getName(), id}];
			pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
		}
	}
	return pageHandle;		
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	MyDB_PageHandle pageHandle = nullptr;

	if (currPage < numPages) {  
		int offset = currPage*pageSize;
		// update the pageContainer 
		pageContainer.push_back(Page(bufferPool+offset, "", anonymousID, false));
		// update bufferMap
		bufferMap[{"", anonymousID}] = --pageContainer.end();
		// 
		Page& targetPage = *bufferMap[{"", anonymousID}];
		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
		currPage++;
		anonymousID++;
	} else {
		// the bufferpoll is full
		// find the victim page
		list<Page>::iterator evictItr;
		for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
			if (!(*it).pin) {
				evictItr = it;
				break;
			}
		}
		// write back to disk
		Page& victim = *evictItr;
		if (victim.table!="" && victim.dirty) {
			lseek(tableMap[victim.table], victim.pageID*pageSize, SEEK_SET);
			write(tableMap[victim.table], victim.memory, pageSize);
		} else if (victim.table=="") {
			lseek(tableMap[tempFile], victim.pageID*pageSize, SEEK_SET);
			write(tableMap[tempFile], victim.memory, pageSize);
		}
		// update the pageContainer 
		pageContainer.push_back(Page(victim.memory, "", anonymousID, false));
		// update bufferMap
		bufferMap[{"", anonymousID}] = --pageContainer.end();
		bufferMap.erase({victim.table, victim.pageID});
		// evict victim page
		victim.memory = nullptr;
		pageContainer.erase(evictItr);
		// 
		Page& targetPage = *bufferMap[{"", anonymousID}];
		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
	}
	return pageHandle;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long id) {
	MyDB_PageHandle pageHandle = nullptr;
	// check whether table-page pair is in the buffer map or not
	if (bufferMap.find({whichTable->getName(), id}) != bufferMap.end()) {
		// the page is in the buffer 
		auto itr = bufferMap[{whichTable->getName(), id}];
		// update the order of pageContainer 
		pageContainer.splice(pageContainer.end(), pageContainer, itr);
		// add the reference count of targetPage
		Page& targetPage = *itr;
		++targetPage.referenceCount;
		targetPage.pin = true;

		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
	} else {   
		// the page is not in the buffer
		// If the file do not exist, create one
		if (access(whichTable->getStorageLoc().c_str(), F_OK) == -1) {
			tableMap[whichTable->getStorageLoc()] = open(whichTable->getStorageLoc().c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		}
		// read data from disk   
		char data[pageSize];
		lseek(tableMap[whichTable->getStorageLoc()], id*pageSize, SEEK_SET);
    	read(tableMap[whichTable->getStorageLoc()], data, sizeof(data));

		// write target page data into bufferpool
		if (currPage < numPages) {  
			// write data into bufferpool
			int offset = currPage*pageSize;
			memcpy(bufferPool+offset, data, pageSize);
			// update the pageContainer 
			pageContainer.push_back(Page(bufferPool+offset, whichTable->getName(), id, true));
			// update bufferMap
			bufferMap[{whichTable->getName(), id}] = --pageContainer.end();
			// 
			Page& targetPage = *bufferMap[{whichTable->getName(), id}];
			pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
			currPage++;
		} else {
			// the bufferpoll is full
			// find the victim page
			list<Page>::iterator evictItr;
			for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
				if (!(*it).pin) {
					evictItr = it;
					break;
				}
			}
			// write back to disk
			Page& victim = *evictItr;
			if (victim.table!="" && victim.dirty) {
				lseek(tableMap[victim.table], victim.pageID*pageSize, SEEK_SET);
				write(tableMap[victim.table], victim.memory, pageSize);
			} else if (victim.table=="") {
				lseek(tableMap[tempFile], victim.pageID*pageSize, SEEK_SET);
				write(tableMap[tempFile], victim.memory, pageSize);
			}
			// write new data into bufferpool 
			memcpy(victim.memory, data, pageSize);
			// update the pageContainer 
			pageContainer.push_back(Page(victim.memory, whichTable->getName(), id, true));
			// update bufferMap
			bufferMap[{whichTable->getName(), id}] = --pageContainer.end();
			bufferMap.erase({victim.table, victim.pageID});
			// evict victim page
			victim.memory = nullptr;
			pageContainer.erase(evictItr);
			// 
			Page& targetPage = *bufferMap[{whichTable->getName(), id}];
			pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
		}
	}
	return pageHandle;			
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	MyDB_PageHandle pageHandle = nullptr;

	if (currPage < numPages) {  
		int offset = currPage*pageSize;
		// update the pageContainer 
		pageContainer.push_back(Page(bufferPool+offset, "", anonymousID, true));
		// update bufferMap
		bufferMap[{"", anonymousID}] = --pageContainer.end();
		// 
		Page& targetPage = *bufferMap[{"", anonymousID}];
		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
		currPage++;
		anonymousID++;
	} else {
		// the bufferpoll is full
		// find the victim page
		list<Page>::iterator evictItr;
		for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
			if (!(*it).pin) {
				evictItr = it;
				break;
			}
		}
		// write back to disk
		Page& victim = *evictItr;
		if (victim.table!="" && victim.dirty) {
			lseek(tableMap[victim.table], victim.pageID*pageSize, SEEK_SET);
			write(tableMap[victim.table], victim.memory, pageSize);
		} else if (victim.table=="") {
			lseek(tableMap[tempFile], victim.pageID*pageSize, SEEK_SET);
			write(tableMap[tempFile], victim.memory, pageSize);
		}
		// update the pageContainer 
		pageContainer.push_back(Page(victim.memory, "", anonymousID, true));
		// update bufferMap
		bufferMap[{"", anonymousID}] = --pageContainer.end();
		bufferMap.erase({victim.table, victim.pageID});
		// evict victim page
		victim.memory = nullptr;
		pageContainer.erase(evictItr);
		// 
		Page& targetPage = *bufferMap[{"", anonymousID}];
		pageHandle = make_shared<MyDB_PageHandleBase>(&targetPage);
	}
	return pageHandle;	
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	unpinMe->unpin();
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pSize, size_t nPages, string tempFileName) {
	// create memory for buffer 
	pageSize = pSize;
	numPages = nPages;
	tempFile = tempFileName;
	size_t totalSize = pageSize * numPages;
	bufferPool = (char*)malloc(totalSize);
	// create temp file
	tableMap[tempFile] = open(tempFile.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
}
	
#endif


