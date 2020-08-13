#include "pfm.h"
#define DEBUG 0
#define NUM_INT_BEFORE_PRIORITY_QUEUE 5
// TODO:
//      1. Implement dumping the priority Queue into the hidden pages
//      2. Implement a function to read the priority Queue back from hidden pages


// My Own helper funcs goes here
bool fileExists(const std::string &fName){
    struct stat buff;
    return (stat (fName.c_str(),&buff) == 0);
}

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}
PagedFileManager::PagedFileManager(){
    // Need to intialize the what?
};

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

uint16_t PagedFileManager::getFreeSpace(FileHandle &fileHandle, PageNum pageNum){
    return fileHandle.getFreeSpace(pageNum);
}

void PagedFileManager::setSysFlag(FileHandle &fileHandle,bool isSystem){
    if(!isSystem) return;
    fileHandle.f->seekp(std::ios::beg + PAGE_SIZE - 1);
    fileHandle.f->write((char*)&isSystem,sizeof(bool));
}
bool PagedFileManager::isSystem(FileHandle &fileHandle){
    bool isSys = false;
    fileHandle.f->seekg(std::ios::beg + PAGE_SIZE - 1);
    fileHandle.f->read((char*)&isSys,sizeof(bool));
    return isSys;
}
// Return true for success
bool PagedFileManager::removeFromPriorityQueue(FileHandle &fileHandle, PageNum pageNum){
    std::stack<PageFreeInfo, std::vector<PageFreeInfo>> tempStack;
    if(DEBUG){
        std::cout << "[removeFromPriorityQueue] Removing Page: " << pageNum << std::endl;
    }
    while(fileHandle.freePageQueue.size() != 0){
        PageFreeInfo tempInfo = fileHandle.freePageQueue.top();
        if(tempInfo.pageNum == pageNum){
            fileHandle.freePageQueue.pop();
            while(tempStack.size() != 0){
                fileHandle.freePageQueue.push(tempStack.top());
                tempStack.pop();
            }
            return true;
        }
        tempStack.push(fileHandle.freePageQueue.top());
        fileHandle.freePageQueue.pop();
    }
    if(DEBUG){
        std::cerr << "[removeFromPriorityQueue] fail to find page: " << pageNum << std::endl;
    }
    // pushing the items inside the tempStack back to the priority Queue
    while(tempStack.size() != 0){
        fileHandle.freePageQueue.push(tempStack.top());
        tempStack.pop();
    }

    return false;
}

PageNum PagedFileManager::findPageWithFreeSpace(FileHandle &fileHandle, unsigned int size,PageFreeInfo &pfInfo){
    std::stack <PageFreeInfo,std::vector<PageFreeInfo>> tempStack;
    if(DEBUG){
        std::cout << "[findPageWithFreeSpace] Queue size = " << fileHandle.freePageQueue.size() << std::endl;
    }
    while(fileHandle.freePageQueue.size() != 0){

        PageFreeInfo tempInfo = fileHandle.freePageQueue.top();
        if(DEBUG){
            std::cout << "[findPageWithFreeSpace] Looking for page with at least " << size << std::endl;
            std::cout << "[findPageWithFreeSpace] tempInfo = PageNum is: " << tempInfo.pageNum << ", freeSpace is: " << tempInfo.freeSpace << std::endl;
        }
        if(tempInfo.freeSpace > size){
            // if found, pop tempStack and pushes all the value back to the original priority queue
            fileHandle.freePageQueue.pop();
            while(!tempStack.empty()){
                fileHandle.freePageQueue.push(tempStack.top());
                tempStack.pop();    // Pop after pushed into the original stack
            }
            if(DEBUG){
                std::cout << "[findPageWithFreeSpace] Page Found! PageNum is: " << tempInfo.pageNum << " with " << tempInfo.freeSpace << " bytes of freeSpace" << std::endl;
            }
            return tempInfo.pageNum;
        }else{
            // push the top item so that the next element in the priority queue can be examined
            tempStack.push(tempInfo);
            fileHandle.freePageQueue.pop();
        }
    }

    // if not found still need to push all the elements in the tempStack back into freePageQueue
    while(!tempStack.empty()){
        fileHandle.freePageQueue.push(tempStack.top());
        tempStack.pop();    // Pop after pushed into the original stack
    }
    if(DEBUG){
        std::cout << "[findPageWithFreeSpace] Page NOT Found!\n";
    }
    return UINT_MAX;
}


RC PagedFileManager::createFile(const std::string &fileName) {
    // check if exists
    if(fileExists(fileName)) {return -1;}

    // if file not exists create
    std::fstream f(fileName, std::ios::out | std::ios::binary);
    if(DEBUG){
        std::cout << "fileName is " << fileName << std::endl;
    }

    if(not fileExists(fileName)){
        if(DEBUG){
            std::cerr << "Error creating " << fileName << std::endl;
        }
        return -1;
    }

    unsigned int zero = 0; // this here is the initial value for all the three counters

    // use a for loop in case we need more 0s in the future
    PageNum nullPageNum = UINT_MAX;
    for(int i = 0; i < 4; ++i){
        f.write((char*)&zero,sizeof(unsigned int));
    }
    f.write((char*)&nullPageNum, sizeof(PageNum));

    // before closing the file, we need to append the rest of the page
    // with zeros into the file
    unsigned int null_len = PAGE_SIZE - sizeof(unsigned int) * 5;
    char null_str[null_len];
    std::memset(null_str, '\0',null_len);
    
    f.write(null_str,null_len);
    f.close();
    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {

    // if file not exist, fail
    if(!fileExists(fileName)) {return -1;}

    // if exists
    if(remove(fileName.c_str())!= 0){
        if(DEBUG){
            std::cerr << "Error deleting " << fileName << std::endl;
        }
        return -1;
    }else{
        return 0;
    }
    return -1;
}

RC PagedFileManager::buildPriorityQueue(FileHandle &fileHandle) {
    if(DEBUG){
        std::cout << "[buildPriorityQueue] Begin building priority queue." << std::endl;
    }
    int16_t currFreeSpace = 0;
    PageFreeInfo pfInfo;
    for(int i = 0; i < fileHandle.appendPageCounter; i++) {
        currFreeSpace = fileHandle.getFreeSpace(i);
        if(DEBUG){
            std::cout << "[buildPriorityQueue] Page:" << i << ", freeSpace:" << currFreeSpace << std::endl;
        }
        pfInfo.pageNum = i;
        pfInfo.freeSpace = currFreeSpace;
        if(pfInfo.freeSpace != 0){
            fileHandle.freePageQueue.push(pfInfo);
        }
    }
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if(!fileExists(fileName)) return -1; // fail if not exist

    //fail if the fstream of the FileHandle has opened another file
    if(fileHandle.f != nullptr and fileHandle.f->is_open()){
        if(DEBUG) std::cout << "[openfile] FAIL: The fstream of this Handle has open another file" << std::endl;
        return -1;
    }  

    fileHandle.f->open(fileName, std::ios::in | std::ios::out 
            | std::ios::binary); //removed the append flag

    if(!fileHandle.f->is_open()){
        if(DEBUG) std::cout <<"[openFile] Sorry, something went wrong when opening:" << fileName <<std::endl;
        return -1; // fail to open the file
    } 
    fileHandle.fileName = fileName;

    // Should also read the current counts from the file
    // call function collectCounterValues
    // The first page should've already intialized by creating 

    fileHandle.f->seekg(std::ios::beg);
    // Don't need to find if file's empty since intialized in the createFile function
    fileHandle.f->read((char*)&fileHandle.readPageCounter,sizeof(unsigned int));
    fileHandle.f->read((char*)&fileHandle.writePageCounter,sizeof(unsigned int));
    fileHandle.f->read((char*)&fileHandle.appendPageCounter,sizeof(unsigned int));
    fileHandle.f->read((char*)&fileHandle.rootPageNum,sizeof(unsigned int));
    fileHandle.f->read((char*)&fileHandle.currLeafPageNum,sizeof(unsigned int));

    // Load the priority Queue into the memory on opening the file
    //buildPriorityQueue(fileHandle);
    fileHandle.read_priority_queue();

    //if(DEBUG){
    //    std::cout << "[openFile] Counter Values when opening "<<fileName<<" are: \"" << fileHandle.readPageCounter << " "
    //        << fileHandle.writePageCounter << " " << fileHandle.appendPageCounter << "\"" << std::endl;
    //}
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if(fileHandle.f != nullptr and !fileHandle.f->is_open()) return -1;

    // need to flush the page counts into the file before closing
    fileHandle.f->seekp(std::ios::beg); // seek to the beginning

    fileHandle.f->write((char*)&fileHandle.readPageCounter,sizeof(unsigned int));
    fileHandle.f->write((char*)&fileHandle.writePageCounter,sizeof(unsigned int));
    fileHandle.f->write((char*)&fileHandle.appendPageCounter,sizeof(unsigned int));
    fileHandle.f->write((char*)&fileHandle.rootPageNum,sizeof(unsigned int));
    fileHandle.f->write((char*)&fileHandle.currLeafPageNum,sizeof(unsigned int));


    // EDIT: No longer dumping the priority queue into the file, instead
    //       we scan the entire file to initialize the priority queue inside the memory on fileOpen
    if(DEBUG){
        std::cout << "[closeFile] Before closing: the pq size = " << fileHandle.freePageQueue.size();
    }
    
    fileHandle.write_priority_queue(fileHandle.queuePageNum);
    // Removing the filename and close the fstream
    fileHandle.fileName.clear();
    fileHandle.f->close();
    //delete(fileHandle.f);
    //fileHandle.f = new std::fstream();
    //clear the priority queue
    while(not fileHandle.freePageQueue.empty()){
        fileHandle.freePageQueue.pop();
    }
    return 0; // successful termination
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    rootPageNum = 0;
    //recordPageCounter = 0;
    queueSize = 0;
    queuePageNum = 0;
    fileName = "";
    f = new std::fstream();
}
// need to deallocate member f
FileHandle::~FileHandle(){
    delete f;
} 

// writing_priority_queue function should be done
// however, before calling the function for the first time, make sure
// append one new Page and assign that number to the queuePageNum;
int FileHandle::write_priority_queue(PageNum pageNum){
    // | counter | counter | counter | rootPageNum | currLeafPageNum | queueSize | pq_pageNum | tbl_id | ....

    // Write the queue size and starting pageNum to the file
    f->seekp(std::ios::beg + NUM_INT_BEFORE_PRIORITY_QUEUE * sizeof(unsigned));
    unsigned size = freePageQueue.size();
    f->write((char*)&size, sizeof(unsigned));
    f->write((char*)&pageNum, sizeof(PageNum));

    std::stack<PageFreeInfo,std::vector<PageFreeInfo>> tempStack;
    uint8_t pageDataArray[PAGE_SIZE] = {0};
    void* pageData = &pageDataArray;
    //bool appendPage = false;
    while(freePageQueue.size() != 0){
        for(uint16_t curr_offset = 0; curr_offset < PAGE_SIZE; curr_offset += sizeof(PageFreeInfo)){
            PageFreeInfo pfInfo = freePageQueue.top();
            tempStack.push(pfInfo);
            freePageQueue.pop();
            memcpy((char*)pageData+curr_offset,&pfInfo,sizeof(PageFreeInfo));
            if(freePageQueue.empty()){
                // don't need a zero since we have queueSize
                writePage(pageNum,pageData);
                //free(pageData);
                break;
            }
            if(curr_offset == PAGE_SIZE - sizeof(PageNum) - sizeof(PageFreeInfo)){
                // if reach the end of the hidden page
                // append a Page and retrieve the pageNum as the new hidden page
                uint8_t init_hidden_page_data_array[PAGE_SIZE];
                void* init_hidden_page_data = &init_hidden_page_data_array;
                
                if(appendPage(init_hidden_page_data) != 0){
                    std::cerr << "[write_priority_queue] Error append a new page\n";
                    return -1; //
                }
                // Now the new added pageNum = appendCounter - 1
                PageNum newPageNum = appendPageCounter - 1;
                memcpy((char*)pageData+curr_offset, &newPageNum,sizeof(PageNum));
                curr_offset += 6; // this should rest in offset == 4096 and break the loop;
                writePage(pageNum, pageData);
                pageNum = newPageNum;
            }
        }
        memset(pageDataArray,0,PAGE_SIZE); // reset the pageData for next iteration if needed
    }
    // need to dump the tempStack back into the freePageQueue
    while(tempStack.size() != 0){
        freePageQueue.push(tempStack.top());
        tempStack.pop();
    }
    
    return 0;
}

// This function serves the purpose of reading the priority queue
// from the disk into the RAM, theoretically, this function should
// be only called on 
int FileHandle::read_priority_queue(){
    f->seekg(std::ios::beg + NUM_INT_BEFORE_PRIORITY_QUEUE * sizeof(unsigned));// move the cursor
    f->read((char*)&queueSize,sizeof(unsigned));
    f->read((char*)&queuePageNum,sizeof(unsigned));

    if(DEBUG){
        std::cout << "[read_priority_queue]queueSize = " << queueSize <<
        ", queuePageNum = " << queuePageNum << std::endl;
        std::cout << "[read_priority_queue] The size of pq is: " << queueSize << std::endl;
    }

    if(queuePageNum == 0){
        // The Priority_Queue's empty
        return 0; // successful
    }
    uint8_t pageDataArray[PAGE_SIZE] = {0};
    void* pageData = &pageDataArray;
    PageNum pageToRead = queuePageNum;
    readPage(queuePageNum,pageData);

    unsigned readCount = 0;
    unsigned offset = 0;
    while(readCount++ < queueSize){ // should this be "<=" or "<"
        if(offset == PAGE_SIZE - sizeof(PageNum)-sizeof(PageFreeInfo)){
            memcpy((char*)pageData+offset,&pageToRead,sizeof(PageNum));
            offset = 0; // reset the offset to 0 for a brand new page
            readPage(pageToRead,pageData);
        }
        PageFreeInfo pfInfo;
        memcpy(&pfInfo,(char*)pageData+offset,sizeof(PageFreeInfo));
        if(pfInfo.freeSpace != 0){
            if(DEBUG){
                std::cout << "[read_priority_queue] Page:" << pfInfo.pageNum << ", freeSpace:" << pfInfo.freeSpace << std::endl;
            }
            freePageQueue.push(pfInfo);
        }
        offset+=sizeof(PageFreeInfo);
    }
    //free(pageData);
    return 0;   
}

// After calling this function, we should examine the return status code
// status code UCINT16_MAX is for failing
uint16_t FileHandle::getFreeSpace(PageNum pageNum){
    uint8_t dataArray[PAGE_SIZE];
    void* data = &dataArray;
    if(readPage(pageNum,data) == 0){
        //uint16_t size = (uint16_t)*((char*)data+PAGE_SIZE-sizeof(uint16_t));
        uint16_t size = 0;
        memcpy(&size, (char *)data + PAGE_SIZE - sizeof(uint16_t), sizeof(uint16_t));
        if(DEBUG){
            std::cout << "[getFreeSpace] The free space size is: " << size << " for page " << pageNum << std::endl;
        }
        return size; // think the max value for the free space in a page is 4096 < 32676 INT_MAX
    }else{
        return UINT16_MAX; // fail to read the amount of freeSpace
        // usually because pageNum is not valid
    }
}

PageInfo FileHandle::getPageInfo(const void* data){
    PageInfo pInfo;
    memcpy(&pInfo,(char*)data+PAGE_SIZE-sizeof(PageInfo),sizeof(PageInfo));
    return pInfo;
}

void FileHandle::putPageInfo(PageInfo *pageInfo, void* data){
    memcpy((char*)data+PAGE_SIZE-sizeof(PageInfo), pageInfo,sizeof(PageInfo));

}

// The offset of the previous record start from PAGE_SIZE - numRecord * 2 - 2 * 2
uint16_t FileHandle::calcOffset(uint16_t recordOffset, const void* data){
    uint16_t curr_numRecords = recordOffset; // extra variable for being lazy
    /* Calculation for the previous record descriptor */
    if(curr_numRecords == 0){
        return 0; // the offset should be 0 when no record in the data
    }
    uint16_t curr_info_offset = PAGE_SIZE - curr_numRecords * sizeof(RecordInfo) - sizeof(PageInfo);

    RecordInfo *curr_info = (RecordInfo*)((char*)data+curr_info_offset);
    if(DEBUG){
        std::cout << "numRecords: " <<curr_numRecords << ",curr_info.offset = "
        << curr_info->offset << " , curr_info,.length = " << curr_info->length << std::endl;
    }

    return curr_info->offset+curr_info->length;
}

RC FileHandle::readPage(PageNum pageNum, void *data){
    if(!f->is_open()){
        std::cerr << "[readPage] SOMETHING WENT TREMENDOUSLY WRONG!!!!!\n";
    }

    // RETARDED! Somehow the other style of writing the code break test07
    
    // unsigned cannot be neg so don't need to check
    ++pageNum;
    if(pageNum > appendPageCounter) return -1;

    // Should this seek to the beginning and then start seeking again?
    f->seekg(pageNum*PAGE_SIZE);
    f->read(reinterpret_cast<char*>(data),PAGE_SIZE);

    f->seekp(std::ios::beg);
    readPageCounter++;

    // In our implementation we probably don't need to write here
    f->write((char*)&readPageCounter, sizeof(unsigned int));

    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {

    // RETARDED! Somehow the other style of writing the code break test07
    ++pageNum;
    if(pageNum > appendPageCounter) return -1;

    // Same question as readPage
    f->seekp(std::ios::beg);
    f->seekp(pageNum * PAGE_SIZE);
    f->write((char*)data, PAGE_SIZE);

    f->seekp(std::ios::beg+sizeof(unsigned int));
    writePageCounter++;
    f->write((char*)&writePageCounter,sizeof(unsigned int));

    return 0;
}

RC FileHandle::appendPage(const void *data) {

    // Don't know if its a good design like this XD
    std::fstream temp;
    temp.open(fileName, std::ios::out | std::ios::app);
    temp.write((char*)data,PAGE_SIZE);
    temp.close();
    //f->seekp(PAGE_SIZE * appendPageCounter );
    //f->write((char*)data,PAGE_SIZE);
    //// Insert the new page info into the priority queue for keeping track of empty pages
    //PageFreeInfo *initInfo = (PageFreeInfo*)malloc(sizeof(PageFreeInfo));
    //initInfo->pageNum = appendPageCounter;
    //// 2 bytes reserved for number of records and 2 bytes reserved for number of free space
    //// Therefore, the init_free_size = PAGESIZE - 2 * 2 
    //initInfo->freeSpace = PAGE_SIZE - 2 * sizeof(int16_t);
    ////freePageQueue.push(*initInfo);
    //free(initInfo);     // Can I free this yet?
    
    
    if(DEBUG){
        if(freePageQueue.size() != 0){
            PageFreeInfo tempInfo = freePageQueue.top();
            std::cout << "[appendPage] Priority Queue: PageNum is: " << tempInfo.pageNum << "\t freeSpace: " << tempInfo.freeSpace << std::endl;
        }
    }
    
    // adding Counter to the beginning of the file
    
    f->seekp(std::ios::beg + sizeof(unsigned int) * 2);
    appendPageCounter++;
    f->write((char*)&appendPageCounter,sizeof(unsigned int));
    if(f->fail()){
        std::cerr << "fstream has raised the fail flag\n";
        return -1;
    }

    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    //f->seekp(std::ios::beg + sizeof(unsigned int) * 3);
    //f->read((char*)&recordPageCounter,sizeof(unsigned int));
    f->seekp(std::ios::beg + sizeof(unsigned int) * 2);
    f->read((char*)&appendPageCounter,sizeof(unsigned int));
    return appendPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    
    //debug message
    if(DEBUG){
        std::cout << readPageCount << " " << writePageCounter << " " << appendPageCount << std::endl;
    }

    return 0;
}

void FileHandle::updateCounters() {
    f->seekg(std::ios::beg);
    f->write((char*)&readPageCounter,sizeof(unsigned int));
    f->write((char*)&writePageCounter,sizeof(unsigned int));
    f->write((char*)&appendPageCounter,sizeof(unsigned int));
    f->write((char*)&rootPageNum,sizeof(unsigned int));
}


// function scans the hidden pages and return a vector of the hidden page numbers in ascending order
//
// NOTE: This function is deprecated, no use of this function in anywhere yet. 
// 
std::vector<unsigned> FileHandle::getHiddenPages(){
    if(queuePageNum == 0 or queueSize == 0){
        std::cerr << "[getHiddenPages] ERROR: This function needs to be called after insertRecord!\n";
    }
    std::vector<unsigned> hidden_pages;
    unsigned page_count = std::floor((double)queueSize / MAX_pfInfo_PER_PAGE );
    if(page_count == 0){
        std::cerr << "[getHiddenPages] ERROR: Sorry, something went wrong XD.\n";
    }
    
    /* First Page is always the QueuePageNum */
    PageNum curr = queuePageNum;
    hidden_pages.push_back(curr);
    page_count--;
    uint8_t pageDataArray[PAGE_SIZE];
    void *pageData = &pageDataArray;
    while(page_count-- > 0){
        readPage(curr,pageData);
        memcpy(&curr, (char*)pageData+PAGE_SIZE-sizeof(PageNum),sizeof(PageNum));
        hidden_pages.push_back(curr);
    }
    free(pageData);
    return hidden_pages;
}
