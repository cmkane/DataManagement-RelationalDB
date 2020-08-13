#include "ix.h"
#define DEBUG 0
#define ORDER 10

// Note:
//      Using the right child when two key equal
//
void printLeafPage(IXFileHandle &ixFileHandle, std::string tabs, PageNum pageNum, Attribute attribute);
void printNonLeafPage(IXFileHandle &ixFileHandle,const std::string tabs, PageNum pageNum, Attribute attribute);

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    int result = PagedFileManager::instance().openFile(fileName,ixFileHandle.fileHandle);
    if(result == 0){
        ixFileHandle.getCounters();
        // update rootPageNum is now handled by pfm as well
    }
    return result;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().closeFile(ixFileHandle.fileHandle);
}

int calcKeySize(const void* key, const Attribute &attribute){
    if(attribute.type == TypeInt or attribute.type == TypeReal){
        return sizeof(int);
    }else if(attribute.type == TypeVarChar){
        int strlen = 0;
        memcpy(&strlen, key, sizeof(int));
        return strlen + sizeof(int);
    }
    if(DEBUG){
        std::cerr << "[calcKeySize] ERROR: SORRY SOMETHING WENT WRONG keysize = 0\n";
    }
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(ixFileHandle.ixRootPageNum == 0){
        // initial insertion
        // page 0 would be wasted!!!
        // this following page is used for root page
        // this page is a leaf page at the beginning, when more nodes show up then split
        PageNum childPageNum;
        ixFileHandle.appendLeafPage(childPageNum); // this is a unused leaf page

        PageNum rootPageNum;
        ixFileHandle.appendLeafPage(rootPageNum);
        ixFileHandle.fileHandle.rootPageNum = rootPageNum;
        ixFileHandle.ixRootPageNum = ixFileHandle.fileHandle.rootPageNum;
        //ixFileHandle.fileHandle.updateCounters();


        if(DEBUG){
            std::cout << "[insertEntry] First Insertion, adding root page " << ixFileHandle.ixRootPageNum << " to " << ixFileHandle.fileHandle.fileName << std::endl;
        }
        //return 0;
    }

    int keySize = calcKeySize(key, attribute);
    PageNum nullNum = UINT_MAX;
    void* null = nullptr;
    insertIndexEntry(ixFileHandle,keySize, ixFileHandle.ixRootPageNum, attribute, key,null,nullNum, rid);


    //PageNum pageNum = findPage(ixFileHandle, attribute.type);
    // find page Num should be a recursive call?

    // at the end of insertEntry
    // write back counter & rootPageNum
    return 0;
}
// this findIndex rely on the Key being sorted already!
// Question: When equal return the right pointer?
//
// Assumption: once the param is bigger than the entry, return the left node ?
// Assumption 2: the returned value would always be left of the bigger record
//
PageNum findPage(IXFileHandle &ixFileHandle, const void* pageData, const void* entry, const Attribute &attribute, PageInfo pageInfo){
    //PageInfo pageInfo = ixFileHandle.getIndexPageInfo(pageData);

    PageNum pageNum = 0;
    int intVal;
    float floatVal;
    int strLen;
    int paramStrLen;
    int paramIntVal;
    float paramFloatVal;
    std::string paramStrVal;
    std::string strVal;
    if(pageInfo.numRecords < 1){
        std::cerr << "[findIndex] ERROR: findIndex wouldn't work with pages with 0 records \n";
        //;IndexManager::instance().printBtree(ixFileHandle, attribute);
        //printNonLeafPage(ixFileHandle, "", ixFileHandle.ixRootPageNum, attribute);
        std::cout << "\n Finished printing the root page\n";
        return 0;
    }
    for(int i = 1; i <= pageInfo.numRecords; ++i){
        RecordInfo recInfo = ixFileHandle.getIndexRecordInfo(i,pageData);
        // The key starts at the offset!
        if(recInfo.offset < sizeof(PageNum)){  //
            std::cerr << "[findIndex] Record offset error occured, offset should never be less than 4 !\n";
        }
        switch(attribute.type){
            case TypeInt:
                memcpy(&intVal, (char*)pageData+recInfo.offset, sizeof(int));
                memcpy(&paramIntVal,entry,sizeof(int));
                if(intVal > paramIntVal){ // entry > key
                    if(DEBUG){
                        std::cout << intVal << " < "<< paramIntVal << std::endl;
                    }
                    memcpy(&pageNum, (char*)pageData+recInfo.offset - sizeof(PageNum), sizeof(PageNum));
                    if(pageNum == 0){
                        std::cerr << "pageNum == 0 and THIS SHOULD NEVER HAPPEN\n";
                    }
                    return pageNum;
                }
                break;
            case TypeReal:
                memcpy(&floatVal, (char*)pageData+recInfo.offset, sizeof(float));
                memcpy(&paramFloatVal,entry,sizeof(float));
                if(floatVal > paramFloatVal){ // entry > key
                    memcpy(&pageNum, (char*)pageData+recInfo.offset - sizeof(PageNum), sizeof(PageNum));
                    return pageNum;
                }
                break;
            case TypeVarChar:
                memcpy(&strLen,(char*)pageData+recInfo.offset,sizeof(int));
                strVal.append((char*)pageData+recInfo.offset+sizeof(int),strLen);
                memcpy(&paramStrLen,entry,sizeof(int));
                paramStrVal.append((char*)entry+sizeof(int),paramStrLen);

                if(strVal.compare(paramStrVal) > 0){ // entry > key
                    memcpy(&pageNum, (char*)pageData+recInfo.offset - sizeof(PageNum), sizeof(PageNum));
                    return pageNum;
                }
                break;
            default:
                break;
        }
        // all the records in the current node are smaller than parameter
        // need to go right, returns the last pointer
        if(i == pageInfo.numRecords){
            memcpy (&pageNum, (char*)pageData+recInfo.offset+recInfo.length-sizeof(PageNum), sizeof(PageNum));
            if(DEBUG){
                std::cout << "[findPage] findPage reach the last record, the returned PageNum = " << pageNum << std::endl;
            }
            return pageNum;
        }

    }

    // Dropping here meaning something went wrong with logic in the function.
    if(DEBUG){
        std::cout << "[findIndex] Unable to find the desired Index in the current Page\n";
    }
    return 0;
}
RC IndexManager::insertIndexEntry(IXFileHandle &ixFileHandle,int keySize, PageNum pointer, const Attribute &attribute, const void *entry, void* &newChildEntry,PageNum &newChildPageNum, const RID &rid) {
    void *pageData = malloc(PAGE_SIZE); // recursive calls should not allocate on stack
    //''this->printBtree(ixFileHandle, attribute);
    ixFileHandle.fileHandle.readPage(pointer,pageData);
    ixFileHandle.getCounters();
    PageInfo pageInfo = ixFileHandle.getIndexPageInfo(pageData);
    // need to calculate key size
    if(DEBUG){
        std::cout << "[insertIndexEntry] Retrieved Page " << pointer << "\n";
        std::cout << "[insertIndexEntry] PageInfo: " << pageInfo << std::endl;
    }
    if( not ixFileHandle.isLeafPage(pageData)){ // Non-leaf Page
        // find index to insert to
        PageNum pageNum = findPage(ixFileHandle, pageData, entry, attribute, pageInfo);
        if(DEBUG){
            std::cout << "[insertIndexEntry] findPage returned PageNum = " << pageNum << std::endl;
        }
        if(pageNum == 0){
            std::cerr << "[insertIndexEntry] Sorry something went wrong in findPage\n";
            free(pageData);
            return -1;
        }
        if(insertIndexEntry(ixFileHandle,keySize, pageNum, attribute, entry, newChildEntry, newChildPageNum, rid)){
            // something went wrong in the recursion
            std::cerr << "[insertIndexEntry] something went wrong in recursion\n";
            return -1;
        };
        if(DEBUG){
            std::cout << "[insertIndexEntry] Finish dealing with child, newChildEntry = " << newChildEntry << std::endl;
        }
        if(newChildEntry == nullptr){
            free(pageData);
            return 0;
        }else{
            if(DEBUG){
                std::cout << pageInfo << std::endl;
            }
            int recordIndex = ixFileHandle.searchEntryInsertPoint(pageData,pageInfo, attribute,newChildEntry);
            int insertSize = 0;
            if(recordIndex < 0){ // " = "no key needs to be inserted, insert RID at recordIndex
                insertSize = sizeof(PageNum);
                recordIndex *= (-1); // flip to be positive
            }else if(recordIndex == 0){ // " < " insert at the end
                insertSize = keySize + sizeof(PageNum) + sizeof(RecordInfo);
                recordIndex = pageInfo.numRecords;

            }else if(recordIndex > 0){// " > ", insert before recordIndex
                insertSize = keySize + sizeof(PageNum) + sizeof(RecordInfo);
            }
            if(pageInfo.freeSpace > insertSize){
                if(DEBUG){
                    std::cout << "[insertIndexEntry] KIDS OF Page " << pointer << " splits inserting new key in parent\n";
                    std::cout << "[insertIndexEntry] inserting the new ChildEntry into parent\n";
                }
                if(DEBUG){
                    if(attribute.type == TypeVarChar){
                        int strLen = 0;
                        memcpy(&strLen, newChildEntry, sizeof(int));
                        std::string temp = std::string((char*)newChildEntry+sizeof(int),strLen);
                        std::cout << "key to insert into the NonLeaf is: " << temp << "\n";
                    }
                }
                if(ixFileHandle.insertPair(pageData,pointer,attribute,newChildEntry,sizeof(PageNum),(void*)&newChildPageNum)){
                    std::cout << "NOT ENOUGH SPACE\n";
                    return -1;
                }
                free(newChildEntry);
                newChildEntry = nullptr;
                newChildPageNum = UINT_MAX;
            }else{
                // leave it here now
                // YOUR CHILD SPLITS and YOU NEED TO SPLIT TOO! WHAT A MESS!
                // inserting newChildEntry
                //int tempKeySize = calcKeySize(newChildRecor);
                //ixFileHandle.insertPairIntoLeafOrNonLeaf(pointer, attribute, newChildEntry,sizeof(PageNum),(void*)&newChildPageNum);
               //free(newChildEntry);

               //std::cout << "WHAT A MESS!\n";

                bool insertion_flag = false;
                int splitRecordIndex = pageInfo.numRecords / 2 + 1;
                //bool insertLeftOrRight = false;
                //if()
                //RecordInfo splitRecordInfo = ixFileHandle.getIndexRecordInfo(splitRecordIndex, pageData);
                uint8_t newChildData[PAGE_SIZE];
                PageNum tempChildPageNum = newChildPageNum;
                void* tempChildEntry = newChildEntry;
                // DEBUG!!!
                if(*(int*)tempChildEntry != *(int*)newChildEntry){
                    std::cout << *(int*)tempChildEntry << " !=  " << *(int*)newChildEntry << std::endl;
                }
                ixFileHandle.appendNonLeafPage(newChildPageNum);
                ixFileHandle.getCounters();
                //newChildEntry = malloc()

                ixFileHandle.fileHandle.readPage(newChildPageNum,newChildData);

                //if(DEBUG){
                //   std::cout << "\n\n****************** BEFORE PARENT SPLIT ******************\n";
                //   printNonLeafPage(ixFileHandle,"",pointer, attribute);
                //   std::cout << "\n\n";
                //
                //}
                // manually copy the pointer from pushed record to the beginning of the new page
                uint16_t offset = 1 * sizeof(PageNum);
                PageInfo oldChildPageInfo = ixFileHandle.getIndexPageInfo(pageData);
                PageInfo newChildPageInfo = ixFileHandle.getIndexPageInfo(newChildData);
                int tempKeySize;
                if(DEBUG) std::cout << "recordIndex = " << recordIndex << ", splitRecordIndex = " << splitRecordIndex << std::endl;
                if(recordIndex == splitRecordIndex){
                    // newChildEntry get pushed up
                    tempKeySize = calcKeySize((char*)newChildEntry,attribute);
                    memcpy(newChildData, &tempChildPageNum, sizeof(PageNum));
                    newChildPageInfo.freeSpace -= sizeof(PageNum);
                    if(DEBUG){
                        if(newChildPageInfo.freeSpace < sizeof(PageNum))
                            std::cout << "newChildPageInfo.freeSpace < sizeof(PageNum);\n";
                    }
                }else{
                    insertion_flag = true; // important for being lazy!
                    RecordInfo pushedRecord = ixFileHandle.getIndexRecordInfo(splitRecordIndex, pageData);
                    tempKeySize = calcKeySize((char*)pageData+pushedRecord.offset, attribute);
                    newChildEntry = malloc(tempKeySize);
                    memcpy(newChildEntry,(char*)pageData+pushedRecord.offset,tempKeySize);
                    memcpy(newChildData,(char*)pageData+pushedRecord.offset+pushedRecord.length-sizeof(PageNum),sizeof(PageNum));
                    newChildPageInfo.freeSpace -= sizeof(PageNum);
                    if(DEBUG){
                        if(newChildPageInfo.freeSpace < sizeof(PageNum))
                            std::cout << "newChildPageInfo.freeSpace < sizeof(PageNum);\n";
                    }
                    splitRecordIndex++;
                }
                //std::cout << "The copied up index is = " << *(int*)newChildEntry << std::endl;
                for(int i = splitRecordIndex; i <= pageInfo.numRecords; ++i){
                    RecordInfo newChildRecord;
                    RecordInfo oldChildRecord = ixFileHandle.getIndexRecordInfo(i, pageData);
                    newChildRecord.offset = offset;
                    newChildRecord.length = oldChildRecord.length;

                    memcpy((char*)newChildData+newChildRecord.offset,(char*)pageData+oldChildRecord.offset,newChildRecord.length);

                    ixFileHandle.insertIndexRecordInfo(i+1-splitRecordIndex, newChildData, &newChildRecord);

                    newChildPageInfo.numRecords += 1;
                    newChildPageInfo.freeSpace -= (newChildRecord.length + sizeof(RecordInfo));

                    oldChildPageInfo.numRecords -= 1;
                    oldChildPageInfo.freeSpace += (newChildRecord.length + sizeof(RecordInfo));
                    offset += newChildRecord.length;
                }
                if(insertion_flag){
                    oldChildPageInfo.numRecords -= 1;
                    oldChildPageInfo.freeSpace += (tempKeySize + sizeof(PageNum) + sizeof(RecordInfo));
                }

                ixFileHandle.insertIndexPageInfo(pageData, &oldChildPageInfo);
                ixFileHandle.insertIndexPageInfo(newChildData, &newChildPageInfo);

                ixFileHandle.fileHandle.writePage(pointer,pageData);
                ixFileHandle.fileHandle.writePage(newChildPageNum,newChildData);
                // now we need to insert into the page
                if(insertion_flag){
                    //std::cout << "The Index to be inserted = " << *(int*)tempChildEntry << std::endl;
                    if(recordIndex < splitRecordIndex){
                        ixFileHandle.insertPairIntoLeafOrNonLeaf(pointer, attribute, tempChildEntry, sizeof(PageNum),&tempChildPageNum);
                    }else{
                        ixFileHandle.insertPairIntoLeafOrNonLeaf(newChildPageNum,attribute,tempChildEntry,sizeof(PageNum), &tempChildPageNum);
                    }
                    free(tempChildEntry);
                }
                if(DEBUG){
                    std::cout << "\n *************** PARENT SPLIT *****************\n";
                    std::cout << "The previous page is: "<< pointer <<"\n";
                    printNonLeafPage(ixFileHandle,"",pointer,attribute);
                    std::cout << "the new child page is: "<< newChildPageNum <<"\n";
                    printNonLeafPage(ixFileHandle,"",newChildPageNum,attribute);
                }

                //if(DEBUG){
                //}
                // IF YOU ARE ROOT and you need to split, create another root

                //printNonLeafPage(ixFileHandle,"",newChildPageNum,attribute);
                //printNonLeafPage(ixFileHandle,"",pointer,attribute);
                // IF YOU ARE ROOT and you need to split, create another root
                if(pointer == ixFileHandle.ixRootPageNum){
                    PageNum newRoot = ixFileHandle.appendNewRoot(tempKeySize, newChildEntry, pointer, newChildPageNum);
                    ixFileHandle.getCounters();
                    free(newChildEntry);
                    newChildEntry = nullptr;
                    newChildPageNum = UINT_MAX;
                    //ixFileHandle.appendNewRoot();
                    //IndexManager::instance().printBtree(ixFileHandle, attribute);


                    if(DEBUG) printNonLeafPage(ixFileHandle,"",ixFileHandle.ixRootPageNum,attribute);
                    if(DEBUG) std::cout << "[insertIndexEntry] Finished printing the new ROOT" << newRoot << "\n";

                }
            }
        }

    }else{
        // if current leaf page
        //ixFileHandle.fileHandle.readPage(pointer, pageData);
        pageInfo = ixFileHandle.getIndexPageInfo(pageData);
        int recordIndex = ixFileHandle.searchEntryInsertPoint(pageData,pageInfo, attribute,entry);
        int insertSize = 0;
        if(recordIndex < 0){ // " = "no key needs to be inserted, insert RID at recordIndex
            insertSize = sizeof(RID);
            recordIndex *= (-1); // flip to be positive
        }else if(recordIndex == 0){ // " < " insert at the end
            insertSize = keySize + sizeof(RID) + sizeof(RecordInfo);
            recordIndex = pageInfo.numRecords;

        }else if(recordIndex > 0){// " > ", insert before recordIndex
            insertSize = keySize + sizeof(RID) + sizeof(RecordInfo);
        }


            //if(DEBUG){
            //    std::cout << "[insertIndexEntry] PageInfo.freeSpace = " << pageInfo.freeSpace << ", insertSize = " << insertSize << std::endl;
            //}
        if(pageInfo.freeSpace > insertSize){
            if(DEBUG){
                if(attribute.type == TypeVarChar){
                    int strLen = 0;
                    memcpy(&strLen, entry, sizeof(int));
                    std::string temp = std::string((char*)entry+sizeof(int),strLen);
                    std::cout << "key to insert into the Leaf is: " << temp << "\n";
                }
            }
            int code = ixFileHandle.insertPair(pageData,pointer, attribute, entry, sizeof(RID),(void*)&rid);
            if(code != 0) {
                //if(DEBUG){
                    std::cout << "[insertIndexEntry] insertPair returned " << code << std::endl;
                //}
                return -1;
            }
            if(newChildEntry != nullptr){
                free(newChildEntry);
                newChildEntry = nullptr;
                newChildPageNum = 0;
            }
        }else{
            if(DEBUG){
                std::cout << "[insertIndexEntry] " << pageInfo << std::endl;
                std::cout << "[insertIndexEntry] Current LeafPage " << pointer << " filled need to split\n";

            }
            //std::cout << "[insertIndexEntry] key = " << *((int *)entry) << std::endl;

            //ixFileHandle.insertPairIntoLeafOrNonLeaf(pointer, attribute, entry, sizeof(RID),(void*)&rid);
            //ixFileHandle.fileHandle.readPage(pointer, pageData);// insert and return back
            //IndexManager::instance().printBtree(ixFileHandle, attribute);
            //ixFileHandle.fileHandle.readPage(pointer,pageData);
            //pageInfo = ixFileHandle.getIndexPageInfo(pageData);
            bool insertLeftOrRight = false; // false goes left, true goes right
            if(DEBUG){
                std::cout << "\n[insertIndexEntry] InsertEntry = " << recordIndex << std::endl;
            }
            if(recordIndex >= (pageInfo.numRecords/2 +1)){
                insertLeftOrRight = true;
            }
            int splitRecordIndex = pageInfo.numRecords / 2 + 1; // split into half
            //RecordInfo splitRecordInfo = ixFileHandle.getIndexRecordInfo(splitRecordIndex,pageData);
            ixFileHandle.appendLeafPage(newChildPageNum);
            ixFileHandle.getCounters();
            if(DEBUG){
                std::cout << "[insertIndexEntry] Splitting, appending new child page " << newChildPageNum << std::endl;
            }
            uint8_t newChildData[PAGE_SIZE];
            ixFileHandle.fileHandle.readPage(newChildPageNum,newChildData);

            // moving data from the first page to the second page
            uint16_t offset = 2 * sizeof(PageNum);
            PageInfo oldChildPageInfo = ixFileHandle.getIndexPageInfo(pageData);
            PageInfo newChildPageInfo = ixFileHandle.getIndexPageInfo(newChildData);
            for(int i = splitRecordIndex; i <= pageInfo.numRecords; ++i){
                RecordInfo newChildRecord;
                RecordInfo oldChildRecord = ixFileHandle.getIndexRecordInfo(i,pageData);
                newChildRecord.offset = offset;
                newChildRecord.length = oldChildRecord.length;

                memcpy((char*)newChildData+newChildRecord.offset, (char*)pageData+oldChildRecord.offset, newChildRecord.length);
                ixFileHandle.insertIndexRecordInfo(i+1-splitRecordIndex,newChildData,&newChildRecord);

                newChildPageInfo.numRecords += 1;
                newChildPageInfo.freeSpace -= (newChildRecord.length + sizeof(RecordInfo));
                if(newChildPageInfo.freeSpace < (newChildRecord.length + sizeof(RecordInfo))){
                    std::cout << "FREESPACE OVERFLOW: " << newChildPageInfo.freeSpace << " < " << sizeof(RecordInfo) << std::endl;
                }
                // Decrementing the number of records and increment freeSpace, mark->records as deleted basically
                oldChildPageInfo.numRecords -= 1;
                oldChildPageInfo.freeSpace += (newChildRecord.length + sizeof(RecordInfo));

                offset += newChildRecord.length;
            }
            ixFileHandle.insertIndexPageInfo(newChildData, &newChildPageInfo);
            ixFileHandle.insertIndexPageInfo(pageData, &oldChildPageInfo);

            // TODO: link two Pages together
            PageNum prevNext;
            memcpy(&prevNext,(char*)pageData+sizeof(PageNum),sizeof(PageNum));         // tempNext = prev->next;
            memcpy((char*)pageData+sizeof(PageNum),&newChildPageNum,sizeof(PageNum));  // prev->next = curr;
            memcpy(newChildData,&pointer,sizeof(PageNum));                             // curr->prev = prev;
            memcpy((char*)newChildData+sizeof(PageNum), &prevNext, sizeof(PageNum));   // curr->next = tempNext;

            // TODO: write back both pages!
            // finish modifying the prev and current page, write back to the disk
            ixFileHandle.fileHandle.writePage(pointer,pageData);
            ixFileHandle.fileHandle.writePage(newChildPageNum, newChildData);
            if(DEBUG){
                std::cout << "\n\n\n****************** SPLITTING **********************\n";
                std::cout << "The previous old page is:\n";
                printLeafPage(ixFileHandle,"",pointer,attribute);
                std::cout << "\nThe new child page is:\n";
                printLeafPage(ixFileHandle,"",newChildPageNum,attribute);

            }
            //free(newChildData);
            if(insertLeftOrRight){ // insert right
                if(DEBUG){
                    std::cout << "Before inserting into Right\n";
                    printLeafPage(ixFileHandle, "", newChildPageNum, attribute);
                }
                if(ixFileHandle.insertPairIntoLeafOrNonLeaf(newChildPageNum, attribute, entry, sizeof(RID),(void*)&rid)){
                std::cerr << "FAIL!!! Somehow enough space and need to split again\n";
                return -1;
                }
                if(DEBUG){
                    std::cout << "After inserting into Right\n";
                    printLeafPage(ixFileHandle, "", newChildPageNum, attribute);
                }
                ixFileHandle.fileHandle.readPage(newChildPageNum,newChildData);
            }else{// insert left
                if(ixFileHandle.insertPairIntoLeafOrNonLeaf(pointer,attribute, entry, sizeof(RID),(void*)&rid)){
                std::cerr << "FAIL!!! Somehow not enough space and need to split again\n";
                return -1;

                }
            }

            //ixFileHandle.fileHandle.readPage(newChildPageNum, newChildData);
            RecordInfo newChildRecordInfo = ixFileHandle.getIndexRecordInfo(1,newChildData);
            if(DEBUG){
                std::cout << "\nnewChildRecordInfo: offset = " << newChildRecordInfo.offset << ", length = " << newChildRecordInfo.length << std::endl;
            }
            int tempKeySize = calcKeySize((char*)newChildData+newChildRecordInfo.offset,attribute);
            newChildEntry = malloc(tempKeySize);
            //std:: cout << "newChildEntry = " << newChildEntry << std::endl;
            memcpy(newChildEntry,(char*)newChildData+newChildRecordInfo.offset,tempKeySize);
            if(DEBUG){
                std::string output = std::string((char*)newChildEntry+sizeof(int),tempKeySize-sizeof(int));
                std::cout << "\n The pushed up key is: " << output;
                std::cout << "\n\n";

            }


            if(prevNext != UINT_MAX){ // not NULL_PAGE_NUM
            // Re-use pageData for the newPage to reduce stack usage
                ixFileHandle.fileHandle.readPage(prevNext,pageData);
                memcpy(pageData,&newChildPageNum, sizeof(PageNum));                    // tempNext->prev = curr;
                ixFileHandle.fileHandle.writePage(prevNext,pageData);
            }



            if(pointer == ixFileHandle.ixRootPageNum){ // at top level of recursion, manually handle parent

                PageNum newRoot = ixFileHandle.appendNewRoot(tempKeySize, newChildEntry,pointer, newChildPageNum);
                ixFileHandle.getCounters();
                free(newChildEntry);
                newChildEntry = nullptr;
                newChildPageNum = UINT_MAX;
                if(DEBUG){
                    std::cout << "[insertIndexEntry] Inserting a new root " << newRoot << " to the B+ tree\n";

                }
            }
            //IndexManager::instance().printBtree(ixFileHandle, attribute);
            // update pointer and newChildEntry to points to each other

        }

    }
    if(DEBUG){
        ixFileHandle.fileHandle.readPage(pointer, pageData);
        PageInfo debugInfo = ixFileHandle.getIndexPageInfo(pageData);
        std::cout << "[insertIndexEntry] PageInfo is: " << debugInfo << std::endl;
    }
    free(pageData);
    return 0;
}

PageNum IXFileHandle::appendNewRoot(int KeySize, const void* newChildEntry, PageNum oldChildPageNum, PageNum newChildPageNum){
    PageNum newRoot;
    this->appendNonLeafPage(newRoot, KeySize, newChildEntry,oldChildPageNum, newChildPageNum);
    this->ixRootPageNum = newRoot;
    this->fileHandle.rootPageNum = newRoot;
    return newRoot;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

    if(DEBUG) std::cout << "[deleteEntry] Delete RID(" << rid.pageNum << "," << rid.slotNum << ")\n";
    void* null = nullptr;
    int keySize = calcKeySize(key, attribute);
    return deleteIndexEntry(ixFileHandle, keySize, ixFileHandle.ixRootPageNum, key, null, attribute,rid);

    //return 0;
}
int findRIDIndex(const void* pageData, RecordInfo &recInfo, const RID &rid, int keySize, int &num_rids){
    //num_rids = (recInfo.length - keySize) / sizeof(RID);
    int offset = recInfo.offset + keySize;
    RID tempRID;
    if(DEBUG) std::cout << "\n[findRIDIndex] offset = recInfo.offset + keySize = " << offset << std::endl;
    for(int i = 0; i < num_rids; i++){
        memcpy(&tempRID,(char*)pageData + offset + i*sizeof(RID), sizeof(RID));
        //if(DEBUG) std::cout << "[findRIDIndex] tempRID = (" << tempRID.pageNum << "," << tempRID.slotNum << ")\n";
        if(tempRID.pageNum == rid.pageNum and tempRID.slotNum == rid.slotNum){
            return i;
        }
    }
    return -1;

}
RC IndexManager::removeIndex(IXFileHandle &ixFileHandle, void *pageData, int recordIndex, PageInfo &pageInfo, const Attribute &attributem, int keySize, const RID &rid){
    RecordInfo recInfo = ixFileHandle.getIndexRecordInfo(recordIndex, pageData);
    int recordBase = recordIndex+1;
    int num_rids = (recInfo.length - keySize) / sizeof(RID);
    int ridIndex = findRIDIndex(pageData, recInfo, rid, keySize, num_rids);
    if(ridIndex == -1){
        if(DEBUG){
            std::cout << std::endl;
            std::cout << "[removeIndex] recInfo.offset = " << recInfo.offset << ", recInfo.length = " << recInfo.offset <<std::endl;
            std::cout << "[removeIndex] RID ("<< rid.pageNum <<","<< rid.slotNum << ") not found in the current key\n";
        }
        return -1;
    }
    if(recInfo.length == keySize + sizeof(RID)){
        if(ridIndex != 0){
            if(DEBUG) std::cerr << "[removeIndex] ERROR: ridIndex should be 1 at this point!!!!!\n";
        }

        ixFileHandle.moveRecordsByOffset(pageInfo.numRecords, recordBase, recInfo.length * (-1),pageData);
        if(recordIndex == 0){
            std::cout << "WTRWWWERWERWEIIOUYCIDSOUCSIODC\n";
        }
        for(int i = recordIndex+1; i <= pageInfo.numRecords; ++i){
            memmove((char*)pageData+PAGE_SIZE - 1 - sizeof(PageInfo) - (i-1) * sizeof(RecordInfo),
                    (char*)pageData+PAGE_SIZE - 1 - sizeof(PageInfo) - i * sizeof(RecordInfo),
                    sizeof(RecordInfo));
        }
        pageInfo.numRecords -= 1;
        pageInfo.freeSpace += recInfo.length;
    }else{
        // remove the RID
        if(ridIndex+1 != num_rids){
            memmove((char*)pageData+recInfo.offset+keySize+sizeof(RID)*(ridIndex),
                    (char*)pageData+recInfo.offset+keySize+sizeof(RID)*(ridIndex + 1),
                    sizeof(RID) * (num_rids - ridIndex + 1));

        }
        // update RecordInfo
        recInfo.length -= sizeof(RID);
        ixFileHandle.insertIndexRecordInfo(recordIndex, pageData, &recInfo);
        ixFileHandle.moveRecordsByOffset(pageInfo.numRecords, recordBase,(int16_t)sizeof(RID) * (-1), pageData);
        pageInfo.freeSpace += sizeof(RID);

    }
    ixFileHandle.insertIndexPageInfo(pageData, &pageInfo);


    return 0;
}

RC IndexManager::deleteIndexEntry(IXFileHandle &ixFileHandle, const int keySize, PageNum pointer, const void *entry, void* &oldChildEntry, const Attribute &attribute, const RID &rid){
    void *pageData = malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(pointer, pageData);
    ixFileHandle.getCounters();
    PageInfo pageInfo = ixFileHandle.getIndexPageInfo(pageData);
    if(not ixFileHandle.isLeafPage(pageData)){ // Non-leaf Page
        PageNum pageNum = findPage(ixFileHandle, pageData, entry, attribute, pageInfo);
        if(deleteIndexEntry(ixFileHandle,keySize,pageNum, entry, oldChildEntry,attribute, rid)){
            if(DEBUG){
                std::cerr << "[deleteIndexEntry] FAIL: Didn't find such entry within the page\n";
            }
            free(pageData);
            return -1;
        }
        if(oldChildEntry == nullptr){
            free(pageData);
            return 0;
        }else{
            std::cout << "[deleteIndexEntry] Shouldn't be here yet!\n";
            int recordIndex = ixFileHandle.searchEntryInsertPoint(pageData, pageInfo, attribute, oldChildEntry);
            if(recordIndex >= 0){
                if(DEBUG){
                    std::cerr << "[deleteIndexEntry] FAIL: Didn't find such entry within the page\n";
                }
                free(pageData);
                return -1;
            }else{
                if(removeIndex(ixFileHandle,pageData, recordIndex*(-1), pageInfo, attribute,keySize,rid)){

                }
                ixFileHandle.fileHandle.writePage(pointer, pageData);

            }
            //if(removeEntryFromNonLeaf(pageData, attribute, oldChildEntry));
            // remove entry from NonLeaf, don't need to merge
        }
        return -1;
    }else{ // Leaf-Page
        if(DEBUG){
            std::cout << "[deleteIndexEntry] Trying to delete from page " << pointer << std::endl;
            printLeafPage(ixFileHandle, "", pointer, attribute);
            std::cout << "\n";
        }
        int recordIndex = ixFileHandle.searchEntryInsertPoint(pageData, pageInfo, attribute, entry);
        if(recordIndex >= 0){
            //if(DEBUG){
            if(attribute.type == TypeInt){
                std::cout << "key = "<<*(int*)entry << std::endl ;
            }
            if(DEBUG){
                std::cerr << "[deleteIndexEntry] FAIL: Didn't find such entry within the page\n";
                std::cerr << pageInfo << std::endl;


                //printLeafPage(ixFileHandle, "" ,pointer,attribute);
            }
            free(pageData);
            return -1;
        }else{
            if(removeIndex(ixFileHandle, pageData, recordIndex*(-1), pageInfo, attribute,keySize,rid)){
                return -1;
            }
            ixFileHandle.fileHandle.writePage(pointer, pageData);
        }
        if(DEBUG){
            std::cout << "[deleteIndexEntry] After deleting from page " << pointer << std::endl;
            printLeafPage(ixFileHandle, "", pointer, attribute);
            std::cout << "\n";
        }
        // remove entry form Leaf
        // if(remove)
    }
    free(pageData);

    return 0;
}
RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if(ixFileHandle.fileHandle.f == NULL || !ixFileHandle.fileHandle.f->is_open()) return -1;
    ix_ScanIterator.fileHandle = &ixFileHandle;
    ix_ScanIterator.attribute = attribute;
    int str_len1, str_len2;
    //char *low, *high;
    ix_ScanIterator.lowNull = false;
    ix_ScanIterator.highNull = false;
    switch(attribute.type) {
        case TypeInt:
            if(lowKey == nullptr){
                ix_ScanIterator.lowNull = true;
            } else{
                memcpy(&ix_ScanIterator.lowInt, lowKey, sizeof(int));
            }
            if(highKey == nullptr){
                ix_ScanIterator.highNull = true;
            }else{

                memcpy(&ix_ScanIterator.highInt, highKey, sizeof(float));
            }
            break;
        case TypeReal:
            if(lowKey == nullptr){
                ix_ScanIterator.lowNull = true;
            } else{

                memcpy(&ix_ScanIterator.lowReal, lowKey, sizeof(float));
            }
            if(highKey == nullptr){
                ix_ScanIterator.highNull = true;
            }else{

                memcpy(&ix_ScanIterator.highReal, highKey, sizeof(float));
            }
            break;
        case TypeVarChar:
            if(lowKey == nullptr){
                ix_ScanIterator.lowNull = true;
            } else{

                memcpy(&str_len1, lowKey, sizeof(int));
                ix_ScanIterator.lowStr.append((char*)lowKey+sizeof(int),str_len1);

            }
            if(highKey == nullptr){
                ix_ScanIterator.highNull = true;
            }else{
                memcpy(&str_len2, highKey, sizeof(int));
                ix_ScanIterator.highStr.append((char*)highKey+sizeof(int),str_len2);

            }
            break;
        default:
            std::cout << "[scan] Error: Unknown attribute type.\n";
            return -1;
    }

    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;

    ix_ScanIterator.setcurrRID(UINT_MAX, 1);
    return 0;
}

// Question: should this use lowKey value to go to the according PageNum? yes.
RC IX_ScanIterator::getFirstLeafPage(PageNum &pageNum) {
    // Get to the first leaf node
    PageInfo pageInfo;
    void *pageData = malloc(PAGE_SIZE);
    if(DEBUG) {
        std::cout << "[getFirstLeafPage] The root page = " << fileHandle->ixRootPageNum << std::endl;
        if(!lowNull) std::cout << "[getFirstLeafPage] The low value is " << lowReal << std::endl;
    }
    fileHandle->fileHandle.readPage(fileHandle->ixRootPageNum, pageData);
    pageInfo = fileHandle->getIndexPageInfo(pageData);
    int recInt;
    float recReal;
    int str_len;
    std::string recStr;
    char *recVarChar;
    int flag = 0;
    PageNum currPageNum = fileHandle->ixRootPageNum;
    while(!fileHandle->isLeafPage(pageData)) {
        for(int i = 1; i <= pageInfo.numRecords; i++) {
            RecordInfo recordInfo = fileHandle->getIndexRecordInfo(i, pageData);
            // If the low key is null just go to the left most leaf page.
            if(lowNull) {
                memcpy(&currPageNum, (char *)pageData + recordInfo.offset - sizeof(PageNum), sizeof(PageNum));
                // If the leftmost pointer is null, go to the next left most
                if(currPageNum == UINT_MAX) {
                    // If you're at the last record, and still only find nulls, try the right-most pointer
                    if(i == pageInfo.numRecords) {
                        memcpy(&currPageNum, (char *)pageData + recordInfo.offset + recordInfo.length, sizeof(PageNum));
                        if(currPageNum == UINT_MAX) {
                            std::cout << "[getFirstLeafPage] All the pointers for this non-leaf node were null.\n";
                            return -1;
                        }
                        break;
                    }
                    continue; // keep looking for a pointer to go down.
                }
                // If the page num is not null go down to that child.
                break;
            }
            switch(attribute.type) {
                case TypeInt:
                    memcpy(&recInt, (char *)pageData + recordInfo.offset, sizeof(int));
                    if(fileHandle->fileHandle.compareIntAttribute(LT_OP, lowInt, recInt) == 0) {
                        if(DEBUG) std::cout << "getFirstLeafPage] Going left from " << recInt << ", record #" << i << std::endl;
                        memcpy(&currPageNum, (char *)pageData + recordInfo.offset - sizeof(PageNum), sizeof(PageNum));
                        flag = 1;
                    }
                    break;
                case TypeReal:
                    memcpy(&recReal, (char *)pageData + recordInfo.offset, sizeof(float));
                    if(fileHandle->fileHandle.compareRealAttribute(LT_OP, lowReal, recReal) == 0) {
                        memcpy(&currPageNum, (char *)pageData + recordInfo.offset - sizeof(PageNum), sizeof(PageNum));
                        flag = 1;
                    }
                    break;
                case TypeVarChar:
                    memcpy(&str_len, (char *)pageData + recordInfo.offset, sizeof(int));
                    recVarChar = (char *)malloc(str_len + 1);
                    memcpy(recVarChar, (char *)pageData + recordInfo.offset + sizeof(int), str_len);
                    memcpy(recVarChar+str_len,"\0",sizeof(char));
                    recStr = std::string(recVarChar, str_len);
                    if(fileHandle->fileHandle.compareVarcharAttribute(LT_OP, lowStr, recStr) == 0) {
                        memcpy(&currPageNum, (char *)pageData + recordInfo.offset - sizeof(PageNum), sizeof(PageNum));
                        flag = 1;
                    }
                    free(recVarChar);
                    break;
                default:
                    return -1;
            }
            if(flag) {
                flag = 0;
                break;
            }
            if(i == pageInfo.numRecords) {
                if(DEBUG) std::cout << "[getFirstLeafPage] Going to right from " << recInt << ", record #" << i << std::endl;
                memcpy(&currPageNum, (char *)pageData + recordInfo.offset + recordInfo.length - sizeof(PageNum), sizeof(PageNum));
                break;
            }
        }

        if(DEBUG) std::cout << "[getFirstLeafPage] The next pageNum is " << currPageNum << std::endl;
        // get the new pageData
        fileHandle->fileHandle.readPage(currPageNum, pageData);
        pageInfo = fileHandle->getIndexPageInfo(pageData);
    }

    while(pageInfo.numRecords == 0) {
        // get the next page
        memcpy(&currPageNum, (char *)pageData + sizeof(PageNum), sizeof(PageNum));
        if(currPageNum == UINT_MAX) return IX_EOF;
        fileHandle->fileHandle.readPage(currPageNum, pageData);
        pageInfo = fileHandle->getIndexPageInfo(pageData);
    }

    if(DEBUG) std::cout << "[getFirstLeafPage] The first page num is " << currPageNum << std::endl;

    pageNum = currPageNum;
    currRID.pageNum = currPageNum;
    if(lowNull) {
        currRID.slotNum = 1;
        free(pageData);
        return 0;
    }

    // Get the first RID as well.
    RecordInfo rInfo;
    bool found = false;
    for(int x = 1; x <= pageInfo.numRecords; x++) {
        rInfo = fileHandle->getIndexRecordInfo(x, pageData);
        switch(attribute.type) {
            case TypeInt:
                memcpy(&recInt, (char *)pageData + rInfo.offset, sizeof(int));
                if(recInt >= lowInt) {
                    currRID.slotNum = x;
                    found = true;
                }
                break;
            case TypeReal:
                memcpy(&recReal, (char *)pageData + rInfo.offset, sizeof(float));
                if(recReal >= lowReal) {
                    currRID.slotNum = x;
                    found = true;
                }
                break;
            case TypeVarChar:
                memcpy(&str_len, (char *)pageData + rInfo.offset, sizeof(int));
                recVarChar = (char *)malloc(str_len + 1);
                memcpy(recVarChar, (char *)pageData + rInfo.offset + sizeof(int), str_len);
                memcpy(recVarChar+str_len,"\0",sizeof(char));
                recStr = std::string(recVarChar, str_len);
                free(recVarChar);
                if(fileHandle->fileHandle.compareVarcharAttribute(GE_OP, recStr, lowStr) == 0) {
                    currRID.slotNum = x;
                    found = true;
                }
                break;
            default:
                break;
        }
        if(found) break;
    }

    free(pageData);
    if(!found) return -1;
    return 0;
}

void printNonLeafPage(IXFileHandle &ixFileHandle, const std::string tabs, PageNum pageNum, Attribute attribute) {
    std::cout << tabs << "{ \"keys\":[";
    std::vector<int> childrenPages;

    // Get the page data
    void *pageData = malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(pageNum, pageData);

    PageInfo pageInfo = ixFileHandle.getIndexPageInfo(pageData);
    if(DEBUG){
        //std::cout << std::endl << "[printNonLeafPage] PageInfo: " << pageInfo << std::endl;
    }
    int count = pageInfo.numRecords;

    // Go through the keys in Non-Leaf page
    //int offset = 0;
    int str_len = 0;
    float num = 0.0;
    char *name;
    for(int x = 0; x < count; x++) {
        // print out the keys
        if(x != 0) std::cout << ",";
        RecordInfo recordInfo = ixFileHandle.getIndexRecordInfo(x+1, pageData);
        if(DEBUG){
        //    std::cout << "[printNonLeafPage] Record Num " << x+1 << " has offset " << recordInfo.offset << " and length " << recordInfo.length << std::endl;
        }
        switch(attribute.type) {
            case TypeInt:
                memcpy(&str_len, (char *)pageData + recordInfo.offset, sizeof(int));
                std::cout << str_len;
                break;
            case TypeReal:
                memcpy(&num, (char *)pageData + recordInfo.offset, sizeof(float));
                std::cout << num;
                break;
            case TypeVarChar:
                memcpy(&str_len, (char *)pageData + recordInfo.offset, sizeof(int));
                name = (char *)malloc(str_len+1);
                memcpy(name, (char *)pageData + recordInfo.offset + sizeof(int), str_len);
                memcpy(name+str_len,"\0",sizeof(char));
                std::cout << "\"" << name << "\"";
                free(name);
                break;
            default:
                std::cout << "[printNonLeafPage] Error: Unknown attribute type.\n";
                return;
        }

        // Fill in the child page numbers
        if(x == 0) {
            memcpy(&str_len, (char *) pageData, sizeof(int));
            childrenPages.push_back(str_len);
        }
        memcpy(&str_len, (char *)pageData + recordInfo.offset + recordInfo.length - sizeof(int), sizeof(int));
        childrenPages.push_back(str_len);
    }

    std::cout << "],\n" << tabs << "\"children\": [\n";

    // For each child check if the page is a leaf or non-leaf,
    // and call necessary functions.
    bool first = true;
    for(auto childPage : childrenPages) {
        if(!first) std::cout << ",\n";
        if(ixFileHandle.isLeafPage(childPage)) {
            printLeafPage(ixFileHandle, tabs + "\t", childPage, attribute);
        } else {
            printNonLeafPage(ixFileHandle, tabs + "\t", childPage, attribute);
        }
        first = false;
    }

    std::cout << tabs << "]}\n";

    free(pageData);
}

void printLeafPage(IXFileHandle &ixFileHandle, const std::string tabs, PageNum pageNum, Attribute attribute) {
    std::cout << tabs << "{\"keys\": [";

    // Get the page data
    void *pageData = malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(pageNum, pageData);

    //std::cout << "prev = " << *(unsigned int*)pageData << ", next = " << *(unsigned int*)((char*)pageData+sizeof(PageNum)) << std::endl;
    PageInfo pageInfo = ixFileHandle.getIndexPageInfo(pageData);
    if(DEBUG){
    //    std::cout << "[printLeafPage] Leaf Page " << pageNum << " numRecords = " << pageInfo.numRecords << ", freeSpace = " << pageInfo.freeSpace << std::endl;
    }
    int count = pageInfo.numRecords;

    int keySize;
    int currInt;
    float currFloat;
    char *currVarChar;
    int firstRid = true;
    bool first = true;
    RID rid;
    std::string prevStr, currStr;
    for(int x = 0; x < count; x++) {
        if(!first) std::cout << ",";
        RecordInfo recordInfo = ixFileHandle.getIndexRecordInfo(x+1, pageData);
        if(DEBUG) {
        //    std::cout << "recordLen = " << recordInfo.length << ", offset = " << recordInfo.offset << std::endl;
        }
        switch(attribute.type) {
            case TypeInt:
                keySize = calcKeySize((char *)pageData + recordInfo.offset, attribute);
                memcpy(&currInt, (char *)pageData + recordInfo.offset, sizeof(int));
                std::cout << "\"" << currInt << ":[";
                firstRid = true;
                for(unsigned int i = 0; i < (recordInfo.length - keySize)/sizeof(RID); i++) {
                    if(!firstRid) std::cout << ",";
                    memcpy(&rid, (char *)pageData + recordInfo.offset + keySize + i*sizeof(RID), sizeof(RID));
                    std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    firstRid = false;
                }
                std::cout << "]\"";
                first = false;
                break;
            case TypeReal:
                keySize = calcKeySize((char *)pageData + recordInfo.offset, attribute);
                memcpy(&currFloat, (char *)pageData + recordInfo.offset, sizeof(float));
                std::cout << "\"" << currFloat << ":[";
                firstRid = true;
                for(unsigned int i = 0; i < (recordInfo.length - keySize)/sizeof(RID); i++) {
                    if(!firstRid) std::cout << ",";
                    memcpy(&rid, (char *)pageData + recordInfo.offset + keySize + i*sizeof(RID), sizeof(RID));
                    std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    firstRid = false;
                }
                std::cout << "]\"";
                first = false;
                break;
            case TypeVarChar:
                keySize = calcKeySize((char *)pageData + recordInfo.offset, attribute);
                currVarChar = (char *)malloc(keySize-sizeof(int)+1);
                memcpy(currVarChar, (char *)pageData + recordInfo.offset + sizeof(int), keySize-sizeof(int));
                memcpy((char*)currVarChar+keySize-sizeof(int), "\0", sizeof(char));
                currStr = std::string(currVarChar, keySize - sizeof(int));
                std::cout << "\"" << currStr << ":[";
                firstRid = true;
                for(unsigned int i = 0; i < (recordInfo.length - keySize)/sizeof(RID); i++) {
                    if(!firstRid) std::cout << ",";
                    memcpy(&rid, (char *)pageData + recordInfo.offset + keySize + i*sizeof(RID), sizeof(RID));
                    std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
                    firstRid = false;
                }
                std::cout << "]\"";
                free(currVarChar);
                first = false;
                break;
            default:
                return;
        }
    }

    std::cout << "]}";
    free(pageData);

}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    std::string tabs = "";

    ixFileHandle.getCounters();
    if(ixFileHandle.isLeafPage(ixFileHandle.ixRootPageNum)) printLeafPage(ixFileHandle, tabs, ixFileHandle.ixRootPageNum, attribute);
    else printNonLeafPage(ixFileHandle, tabs, ixFileHandle.ixRootPageNum, attribute);
}

RC IXFileHandle::getKey(const void *pageData, const Attribute &attribute, const RecordInfo recInfo, void* key, int &keySize){
    // assumption for the key, dataSize = recLength - sizeof(PageNum) for nonLeafPage
    //                         dataSize = ?????                       for LeafPage

    int str_len;
    switch(attribute.type) {
        case TypeInt:
            memcpy(key, (char *)pageData + recInfo.offset, sizeof(int));
            keySize = sizeof(int);
            break;
        case TypeReal:
            memcpy(key, (char *)pageData + recInfo.offset, sizeof(float));
            keySize = sizeof(float);
            break;
        case TypeVarChar:
            memcpy(&str_len, (char *)pageData + recInfo.offset, sizeof(int));
            memcpy(key, (char *)pageData + recInfo.offset + sizeof(int), str_len);
            memcpy((char *)key+str_len,"\0",sizeof(char));
            keySize = str_len;
            break;
        default:
            return -1;
    }

    return 0;
}

IX_ScanIterator::IX_ScanIterator() {
    currRID.pageNum = UINT_MAX;
    currRID.slotNum = 0;
    ridNum = 0;
    noMore = false;
}

IX_ScanIterator::~IX_ScanIterator() {
}


// function returns 1 on reach last
//
bool IX_ScanIterator::reachLastLeaf(const void *pageData){
    PageNum next = 0;
    memcpy(&next,(char*)pageData + sizeof(PageNum), sizeof(PageNum) );
    if(next == UINT_MAX){
        return true;
    }
    return false;
}

RC IX_ScanIterator::checkDeletion(const void* pageData) {
    if(DEBUG) std::cout << "Checking deletion: pageNum = " << currRID.pageNum << ", slotNum = " << currRID.slotNum << std::endl;

    PageInfo pageInfo = fileHandle->getIndexPageInfo(pageData);
    if(prevNumRecords > pageInfo.numRecords) {
        // A whole key with RIDS was deleted. Don't change the slotNum, but reset the ridNum.
        if(pageInfo.numRecords == 0) {
            // Set the currRID to next page
            memcpy(&currRID.pageNum, (char *)pageData + sizeof(PageNum), sizeof(PageNum));
            if(currRID.pageNum == UINT_MAX) noMore = true;
            currRID.slotNum = 1;
        }
        ridNum = 0;
        if(DEBUG) std::cout << "[checkDeletion] There was a deletion of whole key.\n";
        return 1;
    }

    // Check if the RID is as expected
    RID rid;
    RecordInfo recordInfo = fileHandle->getIndexRecordInfo(currRID.slotNum, pageData);
    int keySize = calcKeySize((char *)pageData + recordInfo.offset, attribute);

    memcpy(&rid, (char *)pageData + recordInfo.offset + keySize + ridNum * sizeof(RID), sizeof(RID));
    if(rid.pageNum != prevRID.pageNum || rid.slotNum != prevRID.slotNum) {
        if(DEBUG) {
            std::cout << "[checkDeletion] There was a deletion of an rid.\n";
            std::cout << "prevRID(" << prevRID.pageNum << "," << prevRID.slotNum << ") != currRID(" << rid.pageNum << "," << rid.slotNum << ")\n";
        }
        return 2;
    }

    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if(DEBUG) {
        std::cout << "[getNextEntry] Checking RID(" << currRID.pageNum << ", " << currRID.slotNum << ")" << std::endl;
        std::cout << "[getNextEntry] prevInt = " << prevInt << std::endl;
    }

    void *pageData = malloc(PAGE_SIZE);
    void *tempKey = malloc(PAGE_SIZE);
    int keySize;
    PageNum pageNum;
    PageInfo pageInfo;

    if(currRID.pageNum == UINT_MAX) {
        int result = getFirstLeafPage(pageNum);
        if(result == IX_EOF){
            return IX_EOF;
        }
        if(DEBUG) {
            std::cout << "[getNextEntry] getFirstLeafPage returned " << pageNum << std::endl;
        }
    } else {
        // Check for deletion
        fileHandle->fileHandle.readPage(currRID.pageNum, pageData);
        int deletion = checkDeletion(pageData);
        if(deletion == 0) {
            pageInfo = fileHandle->getIndexPageInfo(pageData);
            RecordInfo rInfo = fileHandle->getIndexRecordInfo(currRID.slotNum, pageData);
            fileHandle->getKey(pageData, attribute, rInfo, tempKey, keySize);
            // If no deletion update normally
            if(ridNum == (rInfo.length - keySize)/sizeof(RID) - 1) {
                currRID.slotNum = currRID.slotNum + 1;
                ridNum = 0;
            } else {
                ridNum++;
            }

            // If there are no more records to check in a given page
            if(currRID.slotNum > pageInfo.numRecords) {
                // Get the next page num
                memcpy(&currRID.pageNum, (char *)pageData + sizeof(PageNum), sizeof(PageNum));
                if(currRID.pageNum == UINT_MAX) {
                    noMore = true;
                }
                currRID.slotNum = 1;
                ridNum = 0;
            }
        } else {
            if(DEBUG) std::cout << "[getNextEntry] There was a deletion before.\n";
        }
    }

    if(noMore) {
        if(DEBUG) {
            std::cout << "[getNextEntry] There are no more leaf pages.\n";
        }
        free(pageData);
        free(tempKey);
        return IX_EOF;
    }

    //printLeafPage(*this->fileHandle,"",pageNum,this->attribute);

    CompOp highComp;
    if(highKeyInclusive) highComp = LE_OP;
    else highComp = LT_OP;

    fileHandle->fileHandle.readPage(currRID.pageNum, pageData);
    pageInfo = fileHandle->getIndexPageInfo(pageData);

    while(pageInfo.numRecords == 0) {
        if(DEBUG) std::cout << "[getNextEntry] This page is empty, trying next one!\n";
        //std::cout << currRID.pageNum << std::endl;
        currRID.slotNum = 1;
        ridNum = 0;
        fileHandle->fileHandle.readPage(currRID.pageNum, pageData);
        pageInfo = fileHandle->getIndexPageInfo(pageData);
    }
    prevNumRecords = pageInfo.numRecords;

    RecordInfo recordInfo = fileHandle->getIndexRecordInfo(currRID.slotNum, pageData);
    fileHandle->getKey(pageData, attribute, recordInfo, tempKey, keySize);

    if(DEBUG) std::cout << "[getNextEntry] keySize = " << keySize << std::endl;

    // compare the key to the high value

    int keyInt;
    float keyReal;
    std::string keyStr;
    switch(attribute.type) {
        case TypeInt:
            memcpy(&keyInt, tempKey, keySize);
            prevInt = keyInt;
            if(!highNull && fileHandle->fileHandle.compareIntAttribute(highComp, keyInt, highInt) != 0) {
                free(pageData);
                free(tempKey);
                return IX_EOF;
            }
            if(DEBUG) {
                std::cout << "[getNextEntry] Matched key value = " << keyInt << std::endl;
            }
            break;
        case TypeReal:
            memcpy(&keyReal, tempKey, keySize);
            prevFloat = keyReal;
            if(!highNull && fileHandle->fileHandle.compareRealAttribute(highComp, keyReal, highReal) != 0) {
                free(pageData);
                free(tempKey);
                return IX_EOF;
            }
            if(DEBUG) {
                std::cout << "[getNextEntry] Matched key value = " << keyReal << std::endl;
            }
            break;
        case TypeVarChar:
            keyStr = std::string((char *)tempKey, keySize);
            prevStr = keyStr;
            if(!highNull && fileHandle->fileHandle.compareVarcharAttribute(highComp, keyStr, highStr) != 0) {
                free(pageData);
                free(tempKey);
                return IX_EOF;
            }
            if(DEBUG) {
                std::cout << "[getNextEntry] Matched key value = " << keyStr << std::endl;
            }
            break;
        default:
            free(pageData);
            free(tempKey);
            return -1;
    }

    if(DEBUG) std::cout << "[getNextEntry] ridNum = " << ridNum << std::endl;

    if(attribute.type == TypeVarChar) keySize += sizeof(int);

    // copy over the key and rid
    memcpy(key, (char *)pageData + recordInfo.offset, keySize);
    memcpy(&rid, (char *)pageData + recordInfo.offset + keySize + ridNum*sizeof(RID), sizeof(RID));

    if(DEBUG) std::cout << "[getNextEntry] The rid retrieved is (" << rid.pageNum << "," << rid.slotNum << ")\n";

    prevRID = rid;

    free(tempKey);
    free(pageData);

    return 0;
}

RC IX_ScanIterator::close() {
    currRID.pageNum = UINT_MAX;
    currRID.slotNum = 1;
    noMore = false;
    return 0;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    ixRootPageNum = 0;
    ixCurrLeafPageNum = 0;

}

IXFileHandle::~IXFileHandle() {
}


RC IXFileHandle::appendLeafPage(PageNum &pageNum) {
    uint8_t tempPageArray[PAGE_SIZE];
    memset(tempPageArray,0,PAGE_SIZE);
    tempPageArray[PAGE_SIZE - 1] = 1; // Set flag as 1 for Leaf Page

    PageInfo pInfo;
    pInfo.numRecords = 0;
    pInfo.freeSpace = PAGE_SIZE - 1 - sizeof(PageInfo) - 2 * sizeof(PageNum);
    PageNum nullPageNum = UINT_MAX;
    //memcpy((char *)tempPageArray + PAGE_SIZE - 1 - sizeof(PageInfo), &pInfo, sizeof(PageInfo));
    insertIndexPageInfo(tempPageArray, &pInfo);
    memcpy((char*)tempPageArray,&nullPageNum, sizeof(PageNum));
    memcpy((char*)tempPageArray+sizeof(PageNum),&nullPageNum,sizeof(PageNum));

    // setting the prev page's pointer to current Leaf page

    void* tempdata = &tempPageArray[0];
    int code = fileHandle.appendPage(tempdata);
    if(code){
        std::cerr << "[appendLeafPage] Insertion failed probably due to not opening the file.\n";
        return code;
    }

    getCounters();
    pageNum = ixAppendPageCounter - 1;
    //if(ixCurrLeafPageNum != UINT_MAX){
    //    PageNum prevNextPage;
    //    fileHandle.readPage(ixCurrLeafPageNum,tempdata);
    //
    //    memcpy(&prevNextPage, (char*)tempdata+sizeof(PageNum),sizeof(PageNum));

    //    // writing the new pageNum into prev->next
    //    memcpy((char*)tempdata+sizeof(PageNum),&pageNum,sizeof(PageNum));

    //    fileHandle.writePage(ixCurrLeafPageNum,tempdata);
    //}
    return code;
}

RC IXFileHandle::appendNonLeafPage(PageNum &pageNum){
    uint8_t tempPageArray[PAGE_SIZE];
    memset(tempPageArray,0,PAGE_SIZE);

    PageInfo pInfo;
    pInfo.freeSpace = PAGE_SIZE - 1 - sizeof(PageInfo);
    pInfo.numRecords = 0;
    insertIndexPageInfo(tempPageArray,&pInfo);;
    int code = fileHandle.appendPage(tempPageArray);
    if(code){
        std::cerr << "[appendNonLeafPage] Insertion failed probably due to not opening the file.\n";
        return code;
    }
    getCounters();
    pageNum = ixAppendPageCounter - 1;
    return code;

}
RC IXFileHandle::appendNonLeafPage(PageNum &pageNum, const int keySize, const void* key, const PageNum left, const PageNum right) {
    if(DEBUG){
        std::cout << "[appendNonLeafPage] keySize = " << keySize << std::endl;
        std::cout << "[appendNonLeafPage] leftPageNum = " << left << std::endl;
        std::cout << "[appendNonLeafPage] rightPageNum = " << right << std::endl;
    }
    uint8_t tempPageArray[PAGE_SIZE];
    memset(tempPageArray,0,PAGE_SIZE); // Leave flag as 0 for Non-Leaf Page

    memcpy(tempPageArray, &left, sizeof(PageNum));
    memcpy((char*)tempPageArray+sizeof(PageNum),key,keySize);

    memcpy((char*)tempPageArray+sizeof(PageNum)+keySize,&right,sizeof(PageNum));
    PageInfo pInfo; // set the correct PageInfo
    pInfo.freeSpace = PAGE_SIZE - 1 - sizeof(PageInfo) - keySize - sizeof(RecordInfo) - 1 * sizeof(PageNum);
    pInfo.numRecords = 1;

    memcpy((char *)tempPageArray + PAGE_SIZE - 1 - sizeof(PageInfo), &pInfo, sizeof(PageInfo));

    RecordInfo recInfo;
    recInfo.offset = sizeof(PageNum);
    recInfo.length = keySize + sizeof(PageNum);
    void* tempdata = &tempPageArray[0];
    insertIndexRecordInfo(1, tempdata, &recInfo);
    int code = fileHandle.appendPage(tempdata);
    if(code){
        std::cerr << "[appendNonLeafPage] Insertion failed probably due to not opening the file.\n";
        return code;
    }

    getCounters();
    pageNum = ixAppendPageCounter - 1;
    return code;
}

int IXFileHandle::isLeafPage(const void *pageData) {
    uint8_t flag;
    memcpy(&flag,(char*)pageData + PAGE_SIZE - sizeof(uint8_t), sizeof(uint8_t));
    return flag;
}

int IXFileHandle::isLeafPage(int pageNum) {
    void *data = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, data);
    uint8_t flag;
    memcpy(&flag,(char*)data + PAGE_SIZE - sizeof(uint8_t), sizeof(uint8_t));
    free(data);
    return flag;
}

PageInfo IXFileHandle::getIndexPageInfo(const void *pageData) {
    PageInfo pInfo;
    // There is an extra byte at the end of a page for determining if it's a Non-Leaf or a Leaf page
    memcpy(&pInfo,(char*)pageData + PAGE_SIZE - sizeof(PageInfo) - 1, sizeof(PageInfo));
    return pInfo;
}

RecordInfo IXFileHandle::getIndexRecordInfo(unsigned slotNum,const void* pageData){
    RecordInfo recInfo;
    memcpy(&recInfo,(char*)pageData + PAGE_SIZE - sizeof(PageInfo) - 1 - sizeof(RecordInfo)*slotNum, sizeof(recInfo));
    return recInfo;
}

void IXFileHandle::insertIndexRecordInfo(unsigned slotNum, void* pageData, RecordInfo *recInfo){
    memcpy((char*)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - sizeof(RecordInfo)*slotNum, recInfo, sizeof(RecordInfo));
}
void IXFileHandle::insertIndexPageInfo(void *pageData, PageInfo *pageInfo){
    memcpy((char*)pageData + PAGE_SIZE - 1 - sizeof(PageInfo), pageInfo, sizeof(PageInfo));
}

// get the counter values from the fileHandle
void IXFileHandle::getCounters(){
    ixReadPageCounter = fileHandle.readPageCounter;
    ixWritePageCounter = fileHandle.writePageCounter;
    ixAppendPageCounter = fileHandle.appendPageCounter;
    ixRootPageNum = fileHandle.rootPageNum;
    ixCurrLeafPageNum = fileHandle.currLeafPageNum;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    int code = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    return code;
}

RC IXFileHandle::insertPair(void *pageData,PageNum pageNum, const Attribute &attribute, const void* entry,
                                             int size, const void *data) {
    // get page data
    // get page info
    PageInfo pageInfo = getIndexPageInfo(pageData);

    // sequentially go through records, find where to put entry.
    // Get the RID of base, also note the offset for where to put new Record
    int recordPlace = searchEntryInsertPoint(pageData, pageInfo, attribute, entry);
    if(recordPlace == INT_MAX) {
        std::cout << "[insertPair] Error in searchEntryInsertPoint.\n";
        return -1;
    }

    uint16_t offset = 0;
    int str_len;
    switch(attribute.type) {
        case TypeInt:
            if(recordPlace >= 0)offset = sizeof(int) + size;
            else offset = size;
            break;
        case TypeReal:
            if(recordPlace >= 0)offset = sizeof(float) + size;
            else offset = size;
            break;
        case TypeVarChar:
            if(recordPlace >= 0){
                memcpy(&str_len, (char *)entry, sizeof(int));
                offset = sizeof(int) + str_len + size;
            }else{
                offset = size;
            }
            break;
    }
    if(pageInfo.freeSpace - offset < 0){
        std::cout << "[insertPair] Not enough room to insert into page " << pageNum << std::endl << pageInfo << std::endl;
        return -1;
    }

    RecordInfo toShift;
    RecordInfo newRecordInfo;

    bool foundEqual = false;
    if(recordPlace == 0) {
        RecordInfo lastRecordInfo;
        if(pageInfo.numRecords == 0) {
            if(isLeafPage(pageData)) lastRecordInfo.offset = 2 * sizeof(PageNum);
            else lastRecordInfo.offset = sizeof(PageNum);
            lastRecordInfo.length = 0;
        } else {
            lastRecordInfo = getIndexRecordInfo(pageInfo.numRecords, pageData);
        }
        if(DEBUG){
            std::cout << "[insertPair] lastRecordInfo.offset = " << lastRecordInfo.offset  << " and length = " << lastRecordInfo.length << std::endl;
        }
        int position = lastRecordInfo.offset + lastRecordInfo.length;
        memcpy((char *)pageData + position,entry,offset - size);
        memcpy((char *)pageData + position + offset - size,data,size);

        newRecordInfo.length = offset; // this is the offset used to move records by offset from before
        newRecordInfo.offset = position;

        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (pageInfo.numRecords+1) * sizeof(RecordInfo),
               &newRecordInfo,
               sizeof(RecordInfo));
    } else if(recordPlace > 0) {
        toShift = getIndexRecordInfo(recordPlace, pageData);

        // shift data using moveRecordByOffset apart of RecordBasedFileManager
        // enter the new data into the emptied space
        moveRecordsByOffset(pageInfo.numRecords, recordPlace, offset, pageData);
        memcpy((char *)pageData + toShift.offset, entry, offset - size);
        memcpy((char *)pageData + toShift.offset + offset - size, data, size);

        newRecordInfo.length = offset; // this is the size of the key + size of data (either rid or pointer)
        newRecordInfo.offset = toShift.offset; // offset is the old offset of the shifted record

        // Implement shifting the Record infos, and entering the new one
        memmove((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (pageInfo.numRecords+1)*sizeof(RecordInfo),
                (char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - pageInfo.numRecords*sizeof(RecordInfo),
                (pageInfo.numRecords - recordPlace + 1)*sizeof(RecordInfo));
        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (recordPlace)*sizeof(RecordInfo),
               &newRecordInfo,
               sizeof(RecordInfo));
    } else {
        if(DEBUG) {
            std::cout << "[insertPair] Found a key that is equal inside leaf page.\n";
        }
        foundEqual = true;
        recordPlace = recordPlace * -1;
        RecordInfo keyInfo = getIndexRecordInfo(recordPlace, pageData);

        if(recordPlace == pageInfo.numRecords) { // is the equal key the last record?
            if(DEBUG) std::cout << "[insertPair] the record found is the last record.\n";
            // add the rid to the end
            memcpy((char *)pageData + keyInfo.offset + keyInfo.length, data, size);

        } else { // need to shift records that are beyond the equivalent one
            toShift = getIndexRecordInfo(recordPlace + 1, pageData); // get key info of the later key to start shift

            // copy over the data into the space freed by the shift
            recordPlace++;
            moveRecordsByOffset(pageInfo.numRecords, recordPlace, size, pageData);
            recordPlace--;
            memcpy((char *)pageData + toShift.offset, data, size);
        }

        keyInfo.length += size;
        // update the index key info
        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - recordPlace*sizeof(RecordInfo),
               &keyInfo,
               sizeof(RecordInfo));
    }

    // update the pageInfo
    if(!foundEqual) {
        pageInfo.numRecords++;
        pageInfo.freeSpace = pageInfo.freeSpace - offset -sizeof(RecordInfo);

        if(DEBUG and pageInfo.freeSpace < offset){
            std::cout << "ERROR: freeSpace underflow\n";
        }
    } else {
        pageInfo.freeSpace = pageInfo.freeSpace - size;
        if(DEBUG and pageInfo.freeSpace < size){
            std::cout << "ERROR: freeSpace underflow\n";
        }
    }
    memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo), &pageInfo, sizeof(PageInfo));

    // write back the pageData
    fileHandle.writePage(pageNum, pageData);
    // Successful termination
    return 0;
}

RC IXFileHandle::insertPairIntoLeafOrNonLeaf(PageNum pageNum, const Attribute &attribute, const void* entry,
                                        int size, const void *data) {
    // get page data
    // get page info
    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, pageData);
    PageInfo pageInfo = getIndexPageInfo(pageData);

    // sequentially go through records, find where to put entry.
    // Get the RID of base, also note the offset for where to put new Record
    int recordPlace = searchEntryInsertPoint(pageData, pageInfo, attribute, entry);

    if(DEBUG){
        std::cout << "recordPlace = " << recordPlace << std::endl;
    }
    if(recordPlace == INT_MAX) {
        std::cout << "recordPlace =" << recordPlace << "\n";
        std::cout << "[insertPairIntoLeaf] Error in searchEntryInsertPoint.\n";
        return -1;
    }

    uint16_t offset = 0;
    int str_len;
    switch(attribute.type) {
        case TypeInt:
            if(recordPlace >= 0) offset = sizeof(int) + size;
            else offset = size;
            break;
        case TypeReal:
            if(recordPlace >= 0) offset = sizeof(float) + size;
            else offset = size;
            break;
        case TypeVarChar:
            memcpy(&str_len, (char *)entry, sizeof(int));
            if(recordPlace >= 0) offset = sizeof(int) + str_len + size;
            else offset = size;
            break;
    }

    if(pageInfo.freeSpace - offset < 0) {
        std::cout << "[insertPairIntoLeafOrNonLeaf] Not enough room to insert into page " << pageNum << std::endl;

        std::cout << pageInfo.freeSpace << " < " << offset << std::endl;
        free(pageData);
        return -1;
    }

    RecordInfo toShift;
    RecordInfo newRecordInfo;

    bool foundEqual = false;
    if(recordPlace == 0) {
        RecordInfo lastRecordInfo;
        if(pageInfo.numRecords == 0) {
            if(isLeafPage(pageData)) lastRecordInfo.offset = 2 * sizeof(PageNum);
            else lastRecordInfo.offset = sizeof(PageNum);
            lastRecordInfo.length = 0;
        } else {
            lastRecordInfo = getIndexRecordInfo(pageInfo.numRecords, pageData);
        }
        if(DEBUG){
            std::cout << "[insertPairIntoLeafOrNonLeaf] lastRecordInfo.offset = " << lastRecordInfo.offset  << " and length = " << lastRecordInfo.length << std::endl;
        }
        memcpy((char *)pageData + lastRecordInfo.offset + lastRecordInfo.length,
                entry,
                offset - size);
        memcpy((char *)pageData + lastRecordInfo.offset + lastRecordInfo.length + offset - size,
                data,
                size);

        newRecordInfo.length = offset; // this is the offset used to move records by offset from before
        newRecordInfo.offset = lastRecordInfo.offset + lastRecordInfo.length;

        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (pageInfo.numRecords+1) * sizeof(RecordInfo),
                &newRecordInfo,
                sizeof(RecordInfo));
    } else if(recordPlace > 0) {
        toShift = getIndexRecordInfo(recordPlace, pageData);

        // shift data using moveRecordByOffset apart of RecordBasedFileManager
        // enter the new data into the emptied space
        moveRecordsByOffset(pageInfo.numRecords, recordPlace, offset, pageData);
        if(DEBUG){
            if(size == sizeof(PageNum)){
                 int strLen = 0;
                 memcpy(&strLen, entry, sizeof(int));
                 std::string temp = std::string((char*)entry+sizeof(int),strLen);
                 std::cout << "entry to insert is: " << temp << "\n";
            }

        }
        memcpy((char *)pageData + toShift.offset, entry, offset - size);
        memcpy((char *)pageData + toShift.offset + offset - size, data, size);

        newRecordInfo.length = offset; // this is the size of the key + size of data (either rid or pointer)
        newRecordInfo.offset = toShift.offset; // offset is the old offset of the shifted record

        // Implement shifting the Record infos, and entering the new one
        memmove((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (pageInfo.numRecords+1)*sizeof(RecordInfo),
                (char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - pageInfo.numRecords*sizeof(RecordInfo),
                (pageInfo.numRecords - recordPlace + 1)*sizeof(RecordInfo));
        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - (recordPlace)*sizeof(RecordInfo),
               &newRecordInfo,
               sizeof(RecordInfo));
    } else {
        if(DEBUG) {
            std::cout << "[insertPairIntoLeafOrNonLeaf] Found a key that is equal inside leaf page.\n";
        }
        foundEqual = true;
        recordPlace = recordPlace * -1;
        RecordInfo keyInfo = getIndexRecordInfo(recordPlace, pageData);

        if(recordPlace == pageInfo.numRecords) { // is the equal key the last record?
            if(DEBUG) std::cout << "[insertPairIntoLeafOrNonLeaf] the record found is the last record.\n";
            // add the rid to the end
            memcpy((char *)pageData + keyInfo.offset + keyInfo.length, data, size);

        } else { // need to shift records that are beyond the equivalent one
            toShift = getIndexRecordInfo(recordPlace + 1, pageData); // get key info of the later key to start shift

            // copy over the data into the space freed by the shift
            recordPlace++;
            moveRecordsByOffset(pageInfo.numRecords, recordPlace, size, pageData);
            recordPlace--;
            memcpy((char *)pageData + toShift.offset, data, size);
        }

        keyInfo.length += size;
        // update the index key info
        memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo) - recordPlace*sizeof(RecordInfo),
               &keyInfo,
               sizeof(RecordInfo));
    }

    // update the pageInfo
    if(!foundEqual) {
        pageInfo.numRecords++;
        pageInfo.freeSpace = pageInfo.freeSpace - offset - sizeof(RecordInfo);
    } else {
        pageInfo.freeSpace = pageInfo.freeSpace - size;
    }
    memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(PageInfo), &pageInfo, sizeof(PageInfo));

    // write back the pageData
    fileHandle.writePage(pageNum, pageData);
    getCounters();
    free(pageData);
    // Done!!
    return 0;
}

int IXFileHandle::searchEntryInsertPoint(const void* pageData, PageInfo &pageInfo, const Attribute &attribute, const void* entry) {
    int str_len;
    int entryInt, recInt;
    float entryFloat, recFloat;
    char *entryVarChar, *recVarChar;
    std::string entryStr, recStr;
    for(int i = 1; i <= pageInfo.numRecords; i++) {
        RecordInfo recordInfo = getIndexRecordInfo(i, pageData);
        switch(attribute.type) {
            case TypeInt:
                memcpy(&entryInt, (char *)entry, sizeof(int));
                memcpy(&recInt, (char *)pageData + recordInfo.offset, sizeof(int));
                //std::cout << "i = " << i << ", Comparing " << entryInt << " < " << recInt << "\n";
                if(fileHandle.compareIntAttribute(EQ_OP, entryInt, recInt) == 0) {
                    return -1 * i;
                }
                if(fileHandle.compareIntAttribute(LT_OP, entryInt, recInt) == 0) {
                    return i;
                }
                break;
            case TypeReal:
                memcpy(&entryFloat, (char *)entry, sizeof(float));
                memcpy(&recFloat, (char *)pageData + recordInfo.offset, sizeof(float));
                if(fileHandle.compareRealAttribute(EQ_OP, entryFloat, recFloat) == 0) {
                    return -1 * i;
                }
                if(fileHandle.compareRealAttribute(LT_OP, entryFloat, recFloat) == 0) {
                    return i;
                }
                break;
            case TypeVarChar:
                memcpy(&str_len, (char *)entry, sizeof(int));
                if(str_len > 2000){
                    std::cout << "Suspicously large string!!!\n";
                }
                entryVarChar = (char *)malloc(str_len+1);
                memcpy(entryVarChar, (char *)entry + sizeof(int), str_len);
                memcpy(entryVarChar+str_len,"\0",sizeof(char));
                entryStr = std::string(entryVarChar, str_len);

                memcpy(&str_len, (char *)pageData + recordInfo.offset, sizeof(int));
                recVarChar = (char *)malloc(str_len+1);
                memcpy(recVarChar, (char *)pageData + recordInfo.offset + sizeof(int), str_len);
                memcpy(recVarChar+str_len,"\0",sizeof(char));
                recStr = std::string(recVarChar, str_len);

                free(entryVarChar);
                free(recVarChar);
                if(fileHandle.compareVarcharAttribute(EQ_OP, entryStr, recStr) == 0) {
                    return -1 * i;
                }
                if(fileHandle.compareVarcharAttribute(LT_OP, entryStr, recStr) == 0) {
                    return i;
                }
                break;
            default:
                return INT_MAX;
        }
    }
    if(DEBUG){
        std::cout << "[searchEntryInsertPoint] All Entries in current node are smaller than insertion\n";
    }
    return 0; // couldn't find a match
}

bool recordOffsetComparator2(std::pair<uint16_t,RecordInfo> &p1,std::pair<uint16_t,RecordInfo> &p2){
    return p1.second.offset < p2.second.offset;
};

int IXFileHandle::moveRecordsByOffset(uint16_t totalNumRecords, int &recordBase, int16_t offset, void* pageData){

    // sanity check for the offset value
    if(offset >= PAGE_SIZE or offset <= -PAGE_SIZE){ // the min - max bound can be reduced to a smaller range if desired
        std::cerr << "[moveRecordsByOffset] Invalid offset: " << offset << std::endl;
        return -1;
    }
    std::vector<std::pair<uint16_t,RecordInfo>> RecordsToMove; // first is slotNum and second is RecordInfo
    //RecordInfo recordBase = getRecordInfo(slotNum, pageData);

    for(uint16_t i = 1; i <= totalNumRecords; ++i){
        RecordInfo currRecInfo = getIndexRecordInfo(i,pageData);
        if(i >= recordBase and (currRecInfo.length != RecordDeleted)){
            if(DEBUG){
                std::cout << "[moveRecordsByOffset] currRecInfo.offset = " << currRecInfo.offset <<
                          " , recordBase = " << recordBase << std:: endl;
                std::cout << "currRecrInfo has length: " << currRecInfo.length << std::endl;
            }
            RecordsToMove.push_back(std::make_pair(i,currRecInfo));
        }
    }
    std::sort(RecordsToMove.begin(),RecordsToMove.end(),recordOffsetComparator2);
    if(DEBUG){
        for(auto iter:RecordsToMove){
            std::cout << "The slot " << iter.first << " has RecordInfo length: " << iter.second.length << " offset: " << iter.second.offset << std:: endl;
        }
    }

    if(offset == 0){
        // no need to move data if offset = 0
        return -1;
    }
    else if(offset < 0){
        if(DEBUG) std::cout << "HERE\n";
        for(auto iter = RecordsToMove.begin(); iter != RecordsToMove.end();++iter){
            RecordInfo currRecordInfo = iter->second;
            uint16_t moveSize = 0;
            moveSize = currRecordInfo.length;
            memmove((char*)pageData+currRecordInfo.offset+offset,(char*)pageData+currRecordInfo.offset,moveSize);
            // update the recordInfo after copying data
            currRecordInfo.offset += offset;
            insertIndexRecordInfo(iter->first,pageData,&currRecordInfo);
        }
    }else if(offset > 0){
        for(auto iter = RecordsToMove.rbegin(); iter != RecordsToMove.rend(); ++iter){
            RecordInfo currRecordInfo = iter->second;
            uint16_t moveSize = 0;
            if(currRecordInfo.length == RecordMoved){
                moveSize = MIN_RECORD_SIZE;
            }else{
                moveSize = currRecordInfo.length;
            }
            memmove((char*)pageData+currRecordInfo.offset+offset,(char*)pageData+currRecordInfo.offset,moveSize);
            // update the recordInfo after copyting data
            currRecordInfo.offset += offset;
            insertIndexRecordInfo(iter->first,pageData,&currRecordInfo);

        }
    }
    return 0;
}
