
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <iostream>
#include <cstring>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long id) {
	MyDB_PageHandle pageHandle = nullptr;
	// check whether table-page pair is in the bufferpool or not
	auto target = bufferMap.find({whichTable->getName(), id});
	if (target != bufferMap.end() && target->second.second->memory) {
		// the page is in the bufferpool
		auto itr = target->second.first;
		// update the order of pageContainer 
		pageContainer.splice(pageContainer.end(), pageContainer, itr);
		// add the handler reference to targetPage and make the handler point to the page
		Page targetPage = *itr;
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
	} else {   
		// If the disk file does not exist, create one
		if (tableMap.find(whichTable->getName())==tableMap.end()) {
			tableMap[whichTable->getName()] = open(whichTable->getStorageLoc().c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		}
		// read data from disk   
		char data[pageSize];
		lseek(tableMap[whichTable->getName()], id*pageSize, SEEK_SET);
    	read(tableMap[whichTable->getName()], data, sizeof(data));

		// write target page data into bufferpool
		if (currPage < numPages) {  
			// write data into bufferpool
			int offset = currPage*pageSize;
			memcpy(bufferPool+offset, data, pageSize);
			// update the pageContainer 
			// check if the page is in the bufferMap 
			if (target != bufferMap.end()) {
				target->second.second->memory = bufferPool+offset;
				pageContainer.push_back(target->second.second);
				target->second.first = --pageContainer.end();
			} else {
				Page newPage = make_shared<PageBase>(bufferPool+offset, whichTable->getName(), id, false, *this);
				pageContainer.push_back(newPage);
				bufferMap[{whichTable->getName(), id}] = {--pageContainer.end(), newPage};
			}
			// 
			Page targetPage = *(bufferMap[{whichTable->getName(), id}].first);
			pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
			currPage++;
		} else {
			// the bufferpoll is full
			// find the victim page
			list<Page>::iterator evictItr;
			for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
				if (!(**it).pin) {
					evictItr = it;
					break;
				}
			}
			// write back to disk
			Page victim = *evictItr;
			if (victim->dirty) {
				off_t offset = lseek(tableMap[victim->table], victim->pageID*pageSize, SEEK_SET);
				if (offset == (off_t)-1) {
				    perror("Error seeking file");
				}
				ssize_t bytesWritten = write(tableMap[victim->table], victim->memory, pageSize);
				if (bytesWritten != pageSize) {
				    perror("Error writing to file");
				}
				victim->dirty = false;
			}
			// update associated 
			// write new data into bufferpool 
			memcpy(victim->memory, data, pageSize);
			// update the pageContainer 
			// check if the page is in the bufferMap 
			if (target != bufferMap.end()) {
				target->second.second->memory = victim->memory;
				pageContainer.push_back(target->second.second);
				target->second.first = --pageContainer.end();
			} else {
				Page newPage = make_shared<PageBase>(victim->memory, whichTable->getName(), id, false, *this);
				pageContainer.push_back(newPage);
				bufferMap[{whichTable->getName(), id}] = {--pageContainer.end(), newPage};
			}
			pageContainer.erase(evictItr);
			// if the victim page is still be referenced
			if (victim.use_count() > 1) {
				victim->memory = nullptr;
			} else {
				bufferMap.erase({victim->table, victim->pageID});
			}
			// 
			Page targetPage = *(bufferMap[{whichTable->getName(), id}].first);
			pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
		}
	}
	return pageHandle;		
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	MyDB_PageHandle pageHandle = nullptr;

	if (currPage < numPages) {  
		int offset = currPage*pageSize;
		// update the pageContainer 
		Page newPage = make_shared<PageBase>(bufferPool+offset, "", anonymousID, false, *this);
		pageContainer.push_back(newPage);
		// update bufferMap
		bufferMap[{"", anonymousID}] = {--pageContainer.end(), newPage};
		// 
		Page targetPage = *(bufferMap[{"", anonymousID}].first);
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
		currPage++;
	} else {
		// the bufferpoll is full
		// find the victim page
		list<Page>::iterator evictItr;
		for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
			if (!(**it).pin) {
				evictItr = it;
				break;
			}
		}
		// write back to disk
		Page victim = *evictItr;
		if (victim->dirty) {
			lseek(tableMap[victim->table], victim->pageID*pageSize, SEEK_SET);
			write(tableMap[victim->table], victim->memory, pageSize);
			victim->dirty = false;
		}
		// update the pageContainer 		
		Page newPage = make_shared<PageBase>(victim->memory, "", anonymousID, false, *this);
		pageContainer.push_back(newPage);
		bufferMap[{"", anonymousID}] = {--pageContainer.end(), newPage};
		pageContainer.erase(evictItr);

		// check if page should be delete (check the handler num)
		if (victim.use_count() > 1) {
			victim->memory = nullptr;
		} else {
			bufferMap.erase({victim->table, victim->pageID});
		}
		//
		Page targetPage = *(bufferMap[{"", anonymousID}].first);
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
	}
	anonymousID++;
	return pageHandle;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long id) {
	MyDB_PageHandle pageHandle = nullptr;
	// check whether table-page pair is in the bufferpool or not
	auto target = bufferMap.find({whichTable->getName(), id});
	if (target != bufferMap.end() && target->second.second->memory) {
		// the page is in the bufferpool
		auto itr = target->second.first;
		// update the order of pageContainer 
		pageContainer.splice(pageContainer.end(), pageContainer, itr);
		// add the handler reference to targetPage and make the handler point to the page
		Page targetPage = *itr;
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
		targetPage->pin = true;
	} else {   
		// If the disk file does not exist, create one
		if (tableMap.find(whichTable->getName())==tableMap.end()) {
			tableMap[whichTable->getName()] = open(whichTable->getStorageLoc().c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		}
		// read data from disk   
		char data[pageSize];
		lseek(tableMap[whichTable->getName()], id*pageSize, SEEK_SET);
    	read(tableMap[whichTable->getName()], data, sizeof(data));

		// write target page data into bufferpool
		if (currPage < numPages) {  
			// write data into bufferpool
			int offset = currPage*pageSize;
			memcpy(bufferPool+offset, data, pageSize);
			// update the pageContainer 
			// check if the page is in the bufferMap 
			if (target != bufferMap.end()) {
				target->second.second->memory = bufferPool+offset;
				pageContainer.push_back(target->second.second);
				target->second.first = --pageContainer.end();
			} else {
		        Page newPage = make_shared<PageBase>(bufferPool+offset, whichTable->getName(), id, false, *this);
				pageContainer.push_back(newPage);
				bufferMap[{whichTable->getName(), id}] = {--pageContainer.end(), newPage};
			}
			// 
			Page targetPage = *(bufferMap[{whichTable->getName(), id}].first);
			pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
			targetPage->pin = true;
			currPage++;
		} else {
			// the bufferpoll is full
			// find the victim page
			list<Page>::iterator evictItr;
			for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
				if (!(**it).pin) {
					evictItr = it;
					break;
				}
			}
			// write back to disk
			Page victim = *evictItr;
			if (victim->dirty) {
				off_t offset = lseek(tableMap[victim->table], victim->pageID*pageSize, SEEK_SET);
				if (offset == (off_t)-1) {
				    perror("Error seeking file");
				}
				ssize_t bytesWritten = write(tableMap[victim->table], victim->memory, pageSize);
				if (bytesWritten != pageSize) {
				    perror("Error writing to file");
				}
				victim->dirty = false;
			}
			// update associated 
			// write new data into bufferpool 
			memcpy(victim->memory, data, pageSize);
			// update the pageContainer 
			// check if the page is in the bufferMap 
			if (target != bufferMap.end()) {
				target->second.second->memory = victim->memory;
				pageContainer.push_back(target->second.second);
				target->second.first = --pageContainer.end();
			} else {
				Page newPage = make_shared<PageBase>(victim->memory, whichTable->getName(), id, false, *this);
				pageContainer.push_back(newPage);
				bufferMap[{whichTable->getName(), id}] = {--pageContainer.end(), newPage};
			}
			pageContainer.erase(evictItr);
			// check if page should be delete (check the handler num)
			if (victim.use_count() > 1) {
				victim->memory = nullptr;
			} else {
				bufferMap.erase({victim->table, victim->pageID});
			}
			// 
			Page targetPage = *(bufferMap[{whichTable->getName(), id}].first);
			pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
			targetPage->pin = true;
		}
	}
	return pageHandle;			
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	MyDB_PageHandle pageHandle = nullptr;

	if (currPage < numPages) {  
		int offset = currPage*pageSize;
		// update the pageContainer 
		Page newPage = make_shared<PageBase>(bufferPool+offset, "", anonymousID, false, *this);
		pageContainer.push_back(newPage);
		// update bufferMap
		bufferMap[{"", anonymousID}] = {--pageContainer.end(), newPage};
		// 
		Page targetPage = *(bufferMap[{"", anonymousID}].first);
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
		targetPage->pin = true;
		currPage++;
	} else {
		// the bufferpoll is full
		// find the victim page
		list<Page>::iterator evictItr;
		for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
			if (!(**it).pin) {
				evictItr = it;
				break;
			}
		}
		// write back to disk
		Page victim = *evictItr;
		if (victim->dirty) {
			// lseek(tableMap[victim.table], victim.pageID*pageSize, SEEK_SET);
			// write(tableMap[victim.table], victim.memory, pageSize);			
			lseek(tableMap[victim->table], victim->pageID*pageSize, SEEK_SET);
			write(tableMap[victim->table], victim->memory, pageSize);
			victim->dirty = false;
		}
		// update the pageContainer 		
		Page newPage = make_shared<PageBase>(victim->memory, "", anonymousID, false, *this);
		pageContainer.push_back(newPage);
		bufferMap[{"", anonymousID}] = {--pageContainer.end(), newPage};
		pageContainer.erase(evictItr);

		// check if page should be delete (check the handler num)
		if (victim.use_count() > 1) {
			victim->memory = nullptr;
		} else {
			bufferMap.erase({victim->table, victim->pageID});
		}
		//
		Page targetPage = *(bufferMap[{"", anonymousID}].first);
		pageHandle = make_shared<MyDB_PageHandleBase>(targetPage, *this);
		targetPage->pin = true;
	}
	anonymousID++;
	return pageHandle;		
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	unpinMe->unpin();
}

char* MyDB_BufferManager :: retrievePage(Page page) {
	// read data from disk   
	char data[pageSize];
	lseek(tableMap[page->table], page->pageID*pageSize, SEEK_SET);
    read(tableMap[page->table], data, sizeof(data));
	// find the victim page
	list<Page>::iterator evictItr;
	for (auto it = pageContainer.begin();it!=pageContainer.end();it++) {
		if (!(**it).pin) {
			evictItr = it;
			break;
		}
	}
	// write back to disk
	Page victim = *evictItr;
	if (victim->dirty) {
		lseek(tableMap[victim->table], victim->pageID*pageSize, SEEK_SET);
		write(tableMap[victim->table], victim->memory, pageSize);
		victim->dirty = false;
	}
	// write new data into bufferpool 
	memcpy(victim->memory, data, pageSize);
	// update page info
	page->memory = victim->memory;
	pageContainer.push_back(page);
	bufferMap[{page->table, page->pageID}] = {--pageContainer.end(), page};		

	// 
	pageContainer.erase(evictItr);
	if (victim.use_count() > 1) {
		victim->memory = nullptr;
	} else {
		bufferMap.erase({victim->table, victim->pageID});
	}
	return page->memory;
}

void MyDB_BufferManager :: updateBufferMap(string table, int id) {
	bufferMap.erase({table, id});
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pSize, size_t nPages, string tempFileName) {
	// create memory for buffer 
	pageSize = pSize;
	numPages = nPages;
	tempFile = tempFileName;
	size_t totalSize = pageSize * numPages;
	bufferPool = (char*)malloc(totalSize);
	// create temp file
	tableMap[""] = open(tempFile.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	// write dirty page back to disk
	for (std::list<Page>::iterator it = pageContainer.begin(); it != pageContainer.end();it++) {
        if ((*it)->dirty) {
			if ((*it)->table != "") {
				lseek(tableMap[(*it)->table], (*it)->pageID*pageSize, SEEK_SET);
				write(tableMap[(*it)->table], (*it)->memory, pageSize);
			}
		}
    }

	//  close file
	for (auto it = tableMap.begin(); it != tableMap.end();it++) {
	    close(it->second); 
	}
	// delete temp file
    if (remove(tempFile.c_str()) != 0) {
        perror("Error deleting file");
    }

	// delete buffer pool 
	free(bufferPool);
	bufferPool = nullptr;
}
	
#endif


