#include "rbfm.h"
#include "string.h"
#define DEBUG 0
#define BYTE_SIZE 8
#define PRIORITY_QUEUE_MAX_SIZE 500


// returns the number of bits set to 1 in a uint8_t array
int getNullCounts(uint8_t *nullIndicator, uint16_t null_size){
    int count = 0;
    int loop_count = 0;
    while(loop_count < null_size){
        uint8_t temp = *(nullIndicator+loop_count);
        for(;temp;++count){
            temp &= temp - 1;
        }
        ++loop_count;
    }
    if(DEBUG){
        std::cout << "[getNullCounts] The number of nulls = " << count << std::endl;
    }
    return count;
};

bool recordOffsetComparator(std::pair<uint16_t,RecordInfo> &p1,std::pair<uint16_t,RecordInfo> &p2){
            return p1.second.offset < p2.second.offset;
};

int RecordBasedFileManager::moveRecordsByOffset(uint16_t totalNumRecords, RecordInfo &recordBase, int16_t offset, void* pageData){

    // sanity check for the offset value
    if(offset >= PAGE_SIZE or offset <= -PAGE_SIZE){ // the min - max bound can be reduced to a smaller range if desired
        std::cerr << "[moveRecordsByOffset] Invalid offset: " << offset << std::endl;
        return -1;
    }
    std::vector<std::pair<uint16_t,RecordInfo>> RecordsToMove; // first is slotNum and second is RecordInfo
    //RecordInfo recordBase = getRecordInfo(slotNum, pageData);
    
    for(uint16_t i = 1; i <= totalNumRecords; ++i){
        RecordInfo currRecInfo = getRecordInfo(i,pageData);
        if(currRecInfo.offset > recordBase.offset and (currRecInfo.length != RecordDeleted)){
            if(DEBUG){
                std::cout << "[moveRecordsByOffset] currRecInfo.offset = " << currRecInfo.offset <<
                " , recordBase.offset = " << recordBase.offset << std:: endl;
                std::cout << "currRecrInfo has length: " << currRecInfo.length << std::endl;
            }
            RecordsToMove.push_back(std::make_pair(i,currRecInfo));
        }
    }
    std::sort(RecordsToMove.begin(),RecordsToMove.end(),recordOffsetComparator);
    if(DEBUG){
        for(auto iter:RecordsToMove){
            std::cout << "The slot " << iter.first << " has RecordInfo length: " << iter.second.length << " offset: " << iter.second.offset << std:: endl;
        }
    }
    
    if(offset == 0){
        // no need to move data if offset = 0
    }
    else if(offset < 0){
        for(auto iter = RecordsToMove.begin(); iter != RecordsToMove.end();++iter){
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
            insertRecordInfo(iter->first,pageData,&currRecordInfo);
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
            insertRecordInfo(iter->first,pageData,&currRecordInfo);

        }
    }
    return 0;
}

// This function for now calculates the size of a record correctly and reform the data into the prepared format
// IMPORTANT NOTE: The offset of each field is calculated from the starting
//                 address of the nullIndicator
uint16_t RecordBasedFileManager::calcRecSize_and_prepareRec(const std::vector<Attribute> &rd,const void *data, void* preparedData){

    uint16_t null_size = std::ceil((float)rd.size()/BYTE_SIZE);
    uint16_t raw_record_offset = null_size; // use to calculate the original size

    unsigned str_len = 0;
    if(DEBUG){
        std::cout << "null size is " << null_size << std::endl;
    }
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, data, null_size); // copy over
    // *nullIndicator = &nullIndicatorArray;
    memcpy(preparedData, data, null_size); // copy nullIndicator into prepared Data

    // Can also use the total size to determine every field's offset
    int null_counts = getNullCounts(&nullIndicator[0],null_size);
    int non_null_counts = rd.size() - null_counts;

    uint16_t recordOffset = null_size + sizeof(uint16_t)*non_null_counts;  // recordOffset should points to the beginning of data fields
    if(DEBUG){
        std::cout << "[prepareRec] recordOffset = " << recordOffset << std::endl;
    }

    uint16_t attrCounter = 0; // Loop Counter
    int nonNullFieldCount = 0;
    for(auto attr: rd){
        unsigned int indexByte = floor((float)attrCounter* (1/BYTE_SIZE));
        unsigned int shift = 7-(attrCounter%BYTE_SIZE);

        attrCounter++; // inc count after use

        // Checking to see if the NULL bit is set, if set skip 
        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)){
            continue;  
        }else{

            // also need to copy the corresponding data into preparedData
            switch(attr.type) {
                case TypeVarChar:
                    //str_len = (unsigned)*((char*)data+size);
                    memcpy(&str_len,(char*)data+raw_record_offset,sizeof(int));

                    // copy in length of varchar right before the string data for retrieving easier
                    memcpy((char *)preparedData+recordOffset, (char *)data + raw_record_offset, sizeof(unsigned));
                    recordOffset += sizeof(int);

                    raw_record_offset += sizeof(int);
                    memcpy((char*)preparedData+recordOffset,(char*)data+raw_record_offset,str_len);
                    recordOffset += str_len;
                    raw_record_offset+=str_len;
                    if(DEBUG){
                        std::cout << "[cal&prepRec]: str_len = " << str_len << std::endl;
                    }
                    break;
                case TypeInt:
                    memcpy((char*)preparedData+recordOffset,(char*)data+raw_record_offset,sizeof(int));
                    recordOffset+=sizeof(int);
                    raw_record_offset += sizeof(int); // should be same as offset+= attr.length?
                    break;
                case TypeReal:
                    memcpy((char*)preparedData+recordOffset,(char*)data+raw_record_offset,sizeof(float));
                    recordOffset+=sizeof(float);
                    raw_record_offset += sizeof(float);
                    break;
                default:
                    // Theoretically, Should never be here
                    if(DEBUG){
                        std::cerr << "[calcRecSize_and_prepareRec] Unsupported attribute type.\n";
                    }
                    return UINT16_MAX;
            }
            // Need to copy the offset to the corresponding byte of the record
            memcpy((char*)preparedData+null_size+nonNullFieldCount*sizeof(uint16_t),&recordOffset,sizeof(uint16_t));
            nonNullFieldCount++;

        }
    }
    // Calculates the difference and memset 0s into the address
    if(recordOffset < MIN_RECORD_SIZE){
        int diff = MIN_RECORD_SIZE - recordOffset;
        memset((char*)preparedData+recordOffset,0,diff);
    }
    return recordOffset;
}

uint16_t RecordBasedFileManager::calcRecSize_and_reformRec(const std::vector<Attribute> &rd, const void *data,void *preparedData) {
    uint16_t offset = 0;
    uint16_t dataSize = 0;

    // Offset for null indicators added
    uint16_t null_size = std::ceil((float)rd.size()/BYTE_SIZE);
    offset += null_size;
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, data, null_size);

    int num_ones = getNullCounts(&nullIndicator[0],null_size);
    // Offset for field offsets added
    offset += (rd.size()-num_ones) * sizeof(uint16_t);

    uint16_t attrCounter = 0; // Loop Counter
    unsigned str_len = 0; // For TypeVarChar str length
    for(auto attr: rd){
        unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
        unsigned int shift = 7-(attrCounter%BYTE_SIZE);
        attrCounter++; // inc count after use

        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)){
            //num_ones++;
            continue;
        } else {
            switch (attr.type) {
                case TypeVarChar:
                    memcpy(&str_len, (char *)data + offset, sizeof(int));
                    if(DEBUG){
                        std::cout << "[reformRec] str_len = " << str_len << std::endl;
                    }
                    offset += (sizeof(int) + str_len);
                    dataSize += (sizeof(int)+ str_len);
                    break;
                case TypeInt:
                    offset += sizeof(int);
                    dataSize += sizeof(int);
                    break;
                case TypeReal:
                    offset += sizeof(float);
                    dataSize += sizeof(float);
                    break;
                default:
                    // Shouldn't be here
                    std::cerr << "[calcRecSize_and_reformRec] Unsupported attribute type.\n";
                    return UINT16_MAX;
            }
        }
    }

    // Prepare the data without field offsets
    memcpy(preparedData, (char *)data, null_size);
    uint16_t dataOffset = null_size + (rd.size()-num_ones) * sizeof(uint16_t); // need to rethink this!
    memcpy((char *)preparedData+null_size, (char *)data + dataOffset, dataSize);

    if(DEBUG){
        std::cout << "[reformRec] The calculated size of the reformed record = "<< (null_size+dataSize) << std::endl;
    }
    return null_size+dataSize; //return the original size of the data
}


// Read the Number of Records based on provided data
// max number of records is less than 2^16

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance().closeFile(fileHandle);
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
        const void *data, RID &rid) {
    if(fileHandle.queuePageNum == 0){
        uint8_t tempDataArray[PAGE_SIZE];
        memset(tempDataArray,0,PAGE_SIZE);
        void* tempData = &tempDataArray[0];
        if(fileHandle.appendPage(tempData)){
            std::cerr << "[insertRecord] appendPage fail! probably because not opening file properly\n";
            return -1;
        }
        fileHandle.queuePageNum = fileHandle.appendPageCounter - 1;
        // need to write back the queuePageNum to the file
        fileHandle.f->seekp(std::ios::beg + sizeof(unsigned) * 4);
        fileHandle.f->write((char*)&fileHandle.queuePageNum, sizeof(unsigned));
        if(DEBUG){
            std::cout << "[insertRecord] New file, appending priority queue\n";
            std::cout << "[insertRecord] Priority Queue is starting from Page " << fileHandle.queuePageNum << std::endl;
        }
    }

    if(DEBUG){
        std::cout << "Trying to insert record" << std::endl;
        std::cout << "The record trying to insert is as follows \n";
        printRecord(recordDescriptor, data);
    }
    unsigned int dataSize = 0;

    // Need to get the size of the Record data = (size_of_null_indicator + length_of_each_field)
    uint8_t preparedDataArray[PAGE_SIZE];
    memset(preparedDataArray,0,PAGE_SIZE);
    void * preparedData = &preparedDataArray[0]; //allocate MAX_SIZE for prepareing data
    dataSize = calcRecSize_and_prepareRec(recordDescriptor, data, preparedData);
    if(dataSize > MAX_RECORD_SIZE){
        std::cerr << "[insertRecord] dataSize exceeds maximum size\n";
        return -1;
    }
    if(DEBUG){
        std::cout << "[insertRecord] The calculated Record size is: " << dataSize << std::endl;
    }


    PageFreeInfo pfInfo;
    PageNum pageNum = PagedFileManager::instance().findPageWithFreeSpace(fileHandle,dataSize+sizeof(RecordInfo),pfInfo);

    //pageNum++;
    if(pageNum == UINT_MAX){
        // New Page information is managed by the upper level instead of appendPage
        uint8_t appendPageArray[PAGE_SIZE];
        memset(appendPageArray,0,PAGE_SIZE);
        void* appendData = &appendPageArray[0];
        PageInfo tempInfo;
        tempInfo.freeSpace = PAGE_SIZE - sizeof(PageInfo);
        tempInfo.numRecords = 0;
        fileHandle.putPageInfo(&tempInfo,appendData);
        //memcpy((char*)appendData+PAGE_SIZE-sizeof(PageInfo),&tempInfo,sizeof(PageInfo));
        fileHandle.appendPage(appendData);
        pageNum = fileHandle.appendPageCounter -1 ;

        //fileHandle.recordPageCounter++;
        //fileHandle.updateCounters();
        if(DEBUG){
            std::cout << "[insertRecord] Can't find a page with free space, appending page " << pageNum << " with free space " << tempInfo.freeSpace  << "\n";
        }
    }
    if(DEBUG){
        std::cout << "The page to insert record is: " << pageNum << std::endl;
    }


    // reading the data of this page into here
    uint8_t pageDataArray[PAGE_SIZE];
    void* pageData = &pageDataArray[0];
    //PageInfo *pageInfo = (PageInfo*)malloc(sizeof(PageInfo));

    // Logic to get PageInfo and the offset
    fileHandle.readPage(pageNum,pageData);
    //PageInfo pageInfo = fileHandle.getPageInfo(pageData);
    PageInfo pageInfo;
    memcpy(&pageInfo,(char*)pageData+PAGE_SIZE-sizeof(PageInfo),sizeof(PageInfo));
    if(pageInfo.freeSpace < (dataSize + sizeof(RecordInfo))){
        std::cerr << "[insertRecord] FATAL: uint16_t OVERFLOW\n";
        std::cerr << "PageNum = " << pageNum << ", freeSpace = " << pageInfo.freeSpace << std::endl;
        return -1;
    }
    if(DEBUG){
        std::cout << "[insertRecord] inserting, Page " << pageNum << " has " << pageInfo.freeSpace << " bytes of free space and " << pageInfo.numRecords  << " numbers of records\n";
    }

    // TODO: need a new way to calculate the offset
    //
    // Equation: offset = PAGE_SIZE - sizeof(PageInfo) - numRecords * sizeof(recordInfo) - freeSpace
    // This offset calculation's still working!!!!
    
    //uint16_t offset = fileHandle.calcOffset(pageInfo.numRecords, pageData);
    uint16_t offset = PAGE_SIZE - sizeof(PageInfo) - pageInfo.numRecords * sizeof(RecordInfo) - pageInfo.freeSpace;
    if(DEBUG){
        std::cout << "[insertRecord] The number of records within the page is " << pageInfo.numRecords << std::endl;
        std::cout << "[insertRecord] The amount of freeSpace is " << pageInfo.freeSpace << std::endl;
        std::cout << "[insertRecord] The calculated offset for the new Record is: " << offset << std::endl;
    }

    //memcpy(pageData+offset,data,dataSize);
    // changed into writing preparedData into the file
    memcpy((char*)pageData+offset,preparedData,dataSize);

    // also need to write descriptor to file
    // -2 for the size of the descriptor itself
    std::shared_ptr<RecordInfo> recInfo(new RecordInfo);
    recInfo->offset = offset;
    recInfo->length = dataSize;
    if(DEBUG){
        std::cout << "The record has length: " << recInfo->length << " and offset:" << recInfo->offset << std::endl;
        //std::cout << "Writing the record to the " << (int)PAGE_SIZE-2*pageInfo.numRecords-sizeof(PageInfo)-2 << "th byte \n";
    }
    
    // The initial RecordInfo should starts at 1 instead of 0!
    // Why would it work earlier????
    int i = 1;
    for(;i <= pageInfo.numRecords; ++i){
        RecordInfo tempInfo = getRecordInfo(i,pageData);
        if(tempInfo.length == RecordDeleted){
            break;
        }
    }
    insertRecordInfo(i,pageData,recInfo.get());
    //std::cout << "i = " << i << ", numRecords = " << pageInfo.numRecords << std::endl;
    if(i > pageInfo.numRecords){
        pageInfo.numRecords++;
        pageInfo.freeSpace-=(dataSize+sizeof(RecordInfo));    // extra 2 bytes for the length and offset
    }else{
        pageInfo.freeSpace-=(dataSize);    // extra 2 bytes for the length and offset

    }
    if(DEBUG){
        std::cout << "[insertRecord] After insertion, the Page " << pageNum << " has now " << pageInfo.freeSpace << " bytes free space with " << pageInfo.numRecords << " number of records\n";
    }
    memcpy((char*)pageData+PAGE_SIZE-sizeof(PageInfo),&pageInfo,sizeof(PageInfo)); // for updating the pageInfo data in the page


    // After finishing inserting the record and descriptor into the pageData, write into file
    fileHandle.writePage(pageNum,pageData);

    // Here first read N = total number of records in page
    // Then use N to find the corresponding offset for record(offset, length)
    rid.pageNum = pageNum;
    rid.slotNum = i; // the new data should be one more record in the page

    /* Updating PageFreeInfo and insert it back into the Queue */
    pfInfo.freeSpace = pageInfo.freeSpace;
    pfInfo.pageNum = pageNum;
    //if(pfInfo.freeSpace != 0){
    //    fileHandle.freePageQueue.push(pfInfo);
    //}
    if(pfInfo.freeSpace != 0){
        if(fileHandle.freePageQueue.size() > PRIORITY_QUEUE_MAX_SIZE){
            if(fileHandle.freePageQueue.top().freeSpace < pfInfo.freeSpace){
                fileHandle.freePageQueue.pop();
                fileHandle.freePageQueue.push(pfInfo);
            }
        }else{
            fileHandle.freePageQueue.push(pfInfo);
        }
    }
    if(DEBUG){
        PageFreeInfo myInfo = fileHandle.freePageQueue.top();
        std::cout << "[pfInfo] page: " << pfInfo.pageNum << " now has " <<pfInfo.freeSpace << " bytes left \n";
        std::cout << "[insertRecord] Top of the queue = page# : " << myInfo.pageNum << ", freeSpace : " << myInfo.freeSpace << "\n";
    }
    if(DEBUG){
        std::cout << "The Data is put in page " << rid.pageNum << " slot: " << rid.slotNum << std::endl;
    }
    fileHandle.write_priority_queue(fileHandle.queuePageNum);

    // free the 2 mallocs in the function
    //free(pageData);
    //free(recInfo);
    //free(preparedData);
    //free(pageInfo);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
        const RID &rid, void *data) { 
    uint8_t pageDataArray[PAGE_SIZE];
    void* pageData = &pageDataArray[0];
    fileHandle.readPage(rid.pageNum,pageData);
    PageInfo pageInfo = fileHandle.getPageInfo(pageData);
    RecordInfo info;

    if(DEBUG){
        std::cout << "Reading slot " << rid.slotNum << " and sizeof recordInfo is " << sizeof(RecordInfo) << std::endl;
        std::cout << "Trying to read the record from " << (int)PAGE_SIZE-sizeof(PageInfo)-rid.slotNum*sizeof(RecordInfo) << "th byte\n";
    }
    info = getRecordInfo(rid.slotNum, pageData);
    RID currRID;
    //std::cout << "[readRecord] The record length is " << info.length << "\n";
    while(info.length == RecordDeleted || info.length == RecordMoved) {
        if(info.length == RecordDeleted) {
            if(DEBUG){
                std::cout << "[readRecord] The record with RID(" << rid.slotNum << ", " << rid.pageNum << ") has been deleted.\n";
            }
            //free(pageData);
            return -1;
        }
        //data in offset is new rid
        // Gabe: Data in the pageData with info.offset is the new RID
        memcpy(&currRID, (char*)pageData+info.offset, sizeof(RID));
        fileHandle.readPage(currRID.pageNum, pageData);
        info = getRecordInfo(currRID.slotNum, pageData);
    }

    if(DEBUG){
        std::cout << "[readRecord] The amount of the free space in page " << rid.pageNum << " is : " << pageInfo.freeSpace << std::endl;
        std::cout << "[readRecord] The amount of the records in page " << rid.pageNum << " is    : " << pageInfo.numRecords << std::endl;
        std::cout << "[readRecord] The record has offset: " << info.offset << " with length: " << info.length << std::endl;
    }

    uint8_t preparedDataArray[info.length];
    memset(preparedDataArray,0,info.length);
    void *preparedData = &preparedDataArray[0];
    //int original_len = calcRecSize_and_reformRec(recordDescriptor,(char *) pageData+fileHandle.calcOffset(rid.slotNum-1, pageData),preparedData);
    int original_len = calcRecSize_and_reformRec(recordDescriptor,(char *) pageData+info.offset,preparedData);
    memcpy(data, (char*)preparedData, original_len);

    //free(pageData);
    //free(preparedData);

    return 0;
}



RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    //std::cout << "[deleteRecord] Deleting RID(pageNum = " << rid.pageNum << ", slotNum = " << rid.slotNum << ")\n";
    // get page data
    uint8_t pageDataArray[PAGE_SIZE];
    memset(pageDataArray,0,PAGE_SIZE);
    void* pageData = &pageDataArray[0];
    fileHandle.readPage(rid.pageNum,pageData);
    PageInfo pageInfo;
    pageInfo = fileHandle.getPageInfo(pageData);
    
    RecordInfo info;
    info = getRecordInfo(rid.slotNum, pageData);
    //std::cout << "The record has offset " << info.offset << " and length " << info.length << std::endl;
    if(info.length == RecordDeleted){
        return -1;
    }
    if(info.length == RecordMoved){
        RID newRid;
        memcpy(&newRid,(char*)pageData+info.offset,sizeof(RID));
        if(deleteRecord(fileHandle,recordDescriptor,newRid) != 0){
            return -1;
        }
    }
    int16_t moveOffset = 0;
    if(info.length == RecordMoved){
        moveOffset = MIN_RECORD_SIZE;
    }else{
        moveOffset = info.length;
    }
    if(moveRecordsByOffset(pageInfo.numRecords, info,moveOffset*(-1),pageData) != 0){ // forgot to +1 for slotNum
        return -1;
    }
    info.length = RecordDeleted;
    //std::cout << "[deleteRecord] Writing " <<  info.length << " to the Length field\n";
    insertRecordInfo(rid.slotNum,pageData,&info);
    // Try to remove from priority Queue if possible
    PagedFileManager::instance().removeFromPriorityQueue(fileHandle, rid.pageNum); // this is to make data consistent

    //if(!PagedFileManager::instance().removeFromPriorityQueue(fileHandle, rid.pageNum)){
    //    std::cerr << "[deleteRecord] Error removing pageNum entry from priority queue.\n";
    //    return -1;
    //}
    uint16_t newFreeSpace = pageInfo.freeSpace + moveOffset;

    PageFreeInfo pfInfo;
    pfInfo.pageNum = rid.pageNum;
    pfInfo.freeSpace = newFreeSpace; // Have more free space from deleted record
    //if(pfInfo.freeSpace != 0){
    //    fileHandle.freePageQueue.push(pfInfo);
    //}
    if(pfInfo.freeSpace != 0){
        if(fileHandle.freePageQueue.size() > PRIORITY_QUEUE_MAX_SIZE){
            if(fileHandle.freePageQueue.top().freeSpace < pfInfo.freeSpace){
                fileHandle.freePageQueue.pop();
                fileHandle.freePageQueue.push(pfInfo);
            }
        }else{
            fileHandle.freePageQueue.push(pfInfo);
        }
    }

    pageInfo.freeSpace = newFreeSpace;
    fileHandle.writePage(rid.pageNum, pageData);
    fileHandle.write_priority_queue(fileHandle.queuePageNum);
    //free(pageData);

    return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned offset = 0;
    size_t null_size = ceil((float)recordDescriptor.size()/BYTE_SIZE);
    if(DEBUG){
        std::cout << "Number of attributes in record: " << recordDescriptor.size() << std::endl;
        std::cout << "The number of bytes containing null info = " << null_size << std::endl;
    }

    // If an attribute value is null, we don't need to skip offset
    // if 2nd is NULL, 3rd comes right after 1st

    //uint8_t nullIndicatorArray[null_size];
    //memset(nullIndicatorArray,0,null_size);
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, data, null_size);

    if(DEBUG){
        std::bitset<8> bits((unsigned int)*nullIndicator);
        std::cout << "The NULL bits are: "<< bits << '\n';
    }

    offset+=null_size;      // move the offset by the size of nullIndicator
    unsigned len = 0;
    char *name;             // name is used for char_str
    int intNum = 0;         // int num used for displaying integer
    float floatNum = 0.0;   // float num used for displaying float
    int attrCounter = 0;    // Counter for keeping track of the col_num
    for(auto attr:recordDescriptor){
        unsigned int indexByte = floor((float)attrCounter/8);
        unsigned int shift = 7-(attrCounter%8);

        // Checking to see if the NULL bit is set, if set then raise the flag
        // or skipping directly depending on the implementation
        if(DEBUG){
            // std::cout << std::endl;
            // std::cout << "indexByte is: " << indexByte << std::endl;
            // std::cout << "shift     is: " << shift << std::endl;
        }
        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)){
            std::cout <<  attr.name << std::setw(8) << ": NULL ";
            attrCounter++;
            continue;
        }

        // There's probably a better way to write the skip logic below
        switch(attr.type) {
            case TypeVarChar:
                if(DEBUG){
                    //std::cout << "length of the attribute string is : " << attr.length <<std::endl; 
                }
                memcpy(&len, (char*)data + offset, sizeof(int));
                offset += sizeof(int);
                //char nameArray[len+1];
                name = (char*)malloc(len+1);
                // Replacing strncpy with memcpy along with additional '\0' at the end;
                //strncpy(name, (char *)data + offset, len);
                memcpy(name,(char*)data+offset,len);
                memcpy(name+len,"\0",sizeof(char));
                offset += len;
                std::cout << attr.name << ": " << name << " ";
                free(name);

                break;
            case TypeInt:
                // Copy the Int data into the integer
                memcpy(&intNum, (char*)data + offset, sizeof(int));

                std::cout << attr.name << ": " << intNum  << " ";
                offset += sizeof(int); // should be same as offset+= attr.length?
                break;
            case TypeReal:
                // Copy float into floatNum
                memcpy(&floatNum, (char*)data + offset, sizeof(float));
                std::cout << attr.name << ": " << floatNum << " ";
                offset += sizeof(float);
                break;
            default:
                // Should never be here
                if(DEBUG){
                    std::cerr << "[printRecord] Unsupported attribute type.\n";
                }
                return -1;
        }
        attrCounter++; // increment the counter
    }
    std::cout << std::endl;
    return 0;
}

RecordInfo RecordBasedFileManager::getRecordInfo(unsigned slotNum,const void* pageData){
    RecordInfo recInfo;
    memcpy(&recInfo,(char*)pageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordInfo)*slotNum,sizeof(recInfo));
    return recInfo;
}

void RecordBasedFileManager::insertRecordInfo(unsigned slotNum, void* pageData, RecordInfo *recInfo){
    memcpy((char*)pageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordInfo)*slotNum,recInfo,sizeof(RecordInfo) );
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
        const void *data, const RID &rid) {
    // Before updating and reading, need to write a function to find the correct RID
    // 
    //printRecord(recordDescriptor,data);

    // RID point to another RID etc
    uint8_t preparedDataArray[PAGE_SIZE];
    uint8_t prevPageDataArray[PAGE_SIZE];
    void* preparedData = &preparedDataArray[0];
    void* prevPageData = &prevPageDataArray[0];
    fileHandle.readPage(rid.pageNum,prevPageData);
    PageInfo prevPageInfo = fileHandle.getPageInfo(prevPageData);
    RecordInfo prevRecInfo = getRecordInfo(rid.slotNum,prevPageData);
    if(DEBUG){
    std::cout << "The retrived slot " << rid.slotNum <<" recordInfo is: length = " << prevRecInfo.length <<
        ", offset = " << prevRecInfo.offset << std::endl;
    }
    RID realRID  = rid;
    while(prevRecInfo.length == RecordMoved){
        memcpy(&realRID,(char*)prevPageData+prevRecInfo.offset,sizeof(RID));
        fileHandle.readPage(realRID.pageNum,prevPageData);
        prevRecInfo = getRecordInfo(realRID.slotNum, prevPageData);
    }
    uint16_t prevSize = prevRecInfo.length; // store the previous length before it gets modified

    // Before updating the page pop the PageFreeInfo from the priority Queue
    PagedFileManager::instance().removeFromPriorityQueue(fileHandle, rid.pageNum); // this is to make data consistent
    //if((!PagedFileManager::instance().removeFromPriorityQueue(fileHandle, realRID.pageNum)) and (prevPageInfo.freeSpace > 50 and prevPageInfo.freeSpace != UINT16_MAX)){
    //    std::cerr << "[updateRecord] Info: pageInfo.freeSpace = " << prevPageInfo.freeSpace <<std::endl;
    //    std::cerr << "[updateRecord] Error: freeSpace is not zero and can't find the pageNum in priority Queue \n";
    //    //return -1;
    //}

    // Try to remove from priority Queue if possible
    PagedFileManager::instance().removeFromPriorityQueue(fileHandle, realRID.pageNum);

    uint16_t dataSize = calcRecSize_and_prepareRec(recordDescriptor,data,preparedData);
    if(dataSize == prevSize){
        //if(DEBUG){
        std::cout << "Size not being modified" << std::endl;
        //}
        memcpy((char*)prevPageData+prevRecInfo.offset,preparedData,dataSize);
        fileHandle.writePage(realRID.pageNum,prevPageData);

    }else if(dataSize > prevSize){
        // (dataSize - prevSize) is the real size increment
        if(dataSize - prevSize > prevPageInfo.freeSpace){
            // if dataSize is bigger than the freeSpace
            // then we need insert the data into a brand new page
            // first update previsou page with the following steps:
            //      1. update recordInfo.length = RecordMoved
            //      2. find a new page to insert this record
            //      3. Put the new RID found by the insert into the slot as a pointer
            //      4. Move the following Records to the front and modify the recInfo
            //      5. Write back

            // In this case, also need to update readRecord so that when reading recordMoved
            // we still read the offset and then find the newRid to read


            // What about updating a already moved Record????? shit!!
            //void
            //recordInfo prevRecInfo = getRecordInfo(rid.slotNum, prevPageData);
            prevRecInfo.length = RecordMoved;
            // prevRecInfo actually have updated info
            // prevRecInfo->offset should not be modified ?
            insertRecordInfo(realRID.slotNum,prevPageData,&prevRecInfo);
            RID newRid;
            insertRecord(fileHandle,recordDescriptor,data,newRid);
            
            // now the newRid would be where the actual data live
            // copy the new Rid into the record
            memcpy((char*)prevPageData+prevRecInfo.offset,&newRid,sizeof(RID)); // supposedly enough more than 6 bytes of previosu data

            // move the data to the left and move the offset in the record accordingly
            int16_t additionalOffset = sizeof(RID) - prevSize; // the amount to move left
            //uint16_t tempCount = rid.slotNum+1;
            if(moveRecordsByOffset(prevPageInfo.numRecords,prevRecInfo,additionalOffset,prevPageData) == -1){
                // if the offset is wrong then should fail to update
                return -1; 
            }
            
            // The difference between the prevSize and the sizeof(RID) is the amount of freeSpace incremented
            prevPageInfo.freeSpace = prevPageInfo.freeSpace + (prevSize - sizeof(RID));
            
            // write back
            memcpy((char*)prevPageData+PAGE_SIZE-sizeof(PageInfo),&prevPageInfo,sizeof(PageInfo));
            fileHandle.writePage(realRID.pageNum,prevPageData);


        }else if(dataSize - prevSize <= prevPageInfo.freeSpace){
            // if the freeSpace is big enough for the increment size
            // offset+
            // move the data in the following Records back wards
            //      2. put the new data into the page and update the prevRecordInfo.length
            //      3. update the pageInfo.freeSpace
            //      4. writeback the page
            
            int16_t moveOffset = dataSize - prevSize;
            if(moveRecordsByOffset(prevPageInfo.numRecords,prevRecInfo,moveOffset, prevPageData) == -1){
                return -1; 
            }
            memcpy((char*)prevPageData+prevRecInfo.offset,preparedData,dataSize);
            prevRecInfo.length = dataSize;
            insertRecordInfo(realRID.slotNum,prevPageData,&prevRecInfo);
            prevPageInfo.freeSpace -= (dataSize - prevSize);
            fileHandle.putPageInfo(&prevPageInfo,prevPageData);
            fileHandle.writePage(realRID.pageNum,prevPageData);
        }

    }else if(dataSize < prevSize){
        // if new size is smaller then, simply insert the preparedData into the record and then
        // move the latter records to the front by (prevSize - dataSize)
        //
        int16_t moveOffset = dataSize - prevSize;
        if(moveRecordsByOffset(prevPageInfo.numRecords,prevRecInfo,moveOffset, prevPageData) == -1){
            return -1; 
        }
        memcpy((char*)prevPageData+prevRecInfo.offset,preparedData,dataSize);
        prevRecInfo.length = dataSize;
        insertRecordInfo(realRID.slotNum,prevPageData,&prevRecInfo);
        prevPageInfo.freeSpace -= (dataSize - prevSize);
        fileHandle.putPageInfo(&prevPageInfo,prevPageData);
        fileHandle.writePage(realRID.pageNum,prevPageData);

    }
    // need to insert the new pageFreeInfo back into the priority queue
    PageFreeInfo pfInfo;
    pfInfo.pageNum = realRID.pageNum;
    pfInfo.freeSpace = prevPageInfo.freeSpace;
    //if(pfInfo.freeSpace != 0){
    //    fileHandle.freePageQueue.push(pfInfo);
    //}
    if(pfInfo.freeSpace != 0){
        if(fileHandle.freePageQueue.size() > PRIORITY_QUEUE_MAX_SIZE){
            if(fileHandle.freePageQueue.top().freeSpace < pfInfo.freeSpace){
                fileHandle.freePageQueue.pop();
                fileHandle.freePageQueue.push(pfInfo);
            }
        }else{
            fileHandle.freePageQueue.push(pfInfo);
        }
    }
    fileHandle.write_priority_queue(fileHandle.queuePageNum);
    //free(prevPageData);
    //free(preparedData);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
        const RID &rid, const std::string &attributeName, void *data) {
    //unsigned offset = 0;
    size_t null_size = ceil((float)recordDescriptor.size()/8);
    //if(DEBUG){
    //    std::cout << "Number of attributes in record: " << recordDescriptor.size() << std::endl;
    //    std::cout << "The number of bytes containing null info = " << null_size << std::endl;
    //}

    // If an attribute value is null, we don't need to skip offset
    // if 2nd is NULL, 3rd comes right after 1st

    uint8_t nullIndicator[null_size];
    uint8_t pageDataArray[PAGE_SIZE];
    void *pageData = &pageDataArray[0];
    fileHandle.readPage(rid.pageNum,pageData);
    RecordInfo recInfo = getRecordInfo(rid.slotNum, pageData);
    if(recInfo.length == RecordDeleted or recInfo.length == RecordMoved){
        //std::cerr << "[readAttribute] FATAL: Trying to read a deleted Record with RID(" << rid.pageNum << "," << rid.slotNum << ")\n";
        return -1;
    }
    int counter = 0;
    uint8_t recDataArray[recInfo.length];
    void *recData = &recDataArray[0];
    memcpy(recData,(char*)pageData+recInfo.offset,recInfo.length);
    memcpy(nullIndicator, recData, null_size);
    uint16_t offset = null_size;
    uint16_t real_offset = 0;
    uint16_t dataEnd = 0;
    //offset index = null_size + size(uint16_t) *( rd.size() - num_of_1s )
    int num_ones = 0;
    while(counter < recordDescriptor.size()){
        unsigned int indexByte = floor((float)counter/BYTE_SIZE);
        unsigned int shift = 7 - (counter % BYTE_SIZE);
        if(recordDescriptor.at(counter).name.compare(attributeName) == 0){ // names match
            //std::cout << "NAME MATCH" << std::endl;
            if(nullIndicator[indexByte] & (unsigned)1 << (unsigned)shift){
                // if the attribute is null immediate return;
                //free(recData);
                //free(pageData);
                return -1;
            }
            real_offset = offset;
            //break; // now the offset would be the the beginning of the real offset

        }
        if(nullIndicator[indexByte] & (unsigned)1 << (unsigned)shift){
            ++num_ones;
        }else{
            offset+=sizeof(uint16_t);
        }
        counter++;
    }
    if(DEBUG){

    }
    memcpy(&dataEnd,(char*)recData+real_offset,sizeof(uint16_t));

    uint16_t dataBegin = 0;

    if(real_offset == null_size){
        dataBegin = offset;
    }else{
        memcpy(&dataBegin,(char*)recData+real_offset-sizeof(uint16_t),sizeof(uint16_t));
    }
    memcpy(data,(char*)recData+dataBegin,dataEnd-dataBegin);
    //if(DEBUG){
    //    std::cout << "[readAttribute] The calculated offsets are as following: \n";
    //    
    //    std::cout << "\t real_offset = " << real_offset <<std::endl;
    //    std::cout << "\t dataBegin   = " << dataBegin << std::endl;
    //    std::cout << "\t dataEnd     = " << dataEnd << std::endl;

    //}
    return 0;
}

// IMPORTANT NOTE:
//      During a scan operation and while(getNextRecord()), no records should be updated, deleted
//      or inserted.
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
        const std::string &conditionAttribute, const CompOp compOp, const void *value,
        const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    if((recordDescriptor.empty() or conditionAttribute.empty() or attributeNames.empty()) and DEBUG){
        std::cout << "[scan] FATAL: One for the following is empty.\n";
        std::cout << "\t recordDescriptor, conditionAttribute, attributeNames\n";
        for(auto attr:recordDescriptor){
            std::cout << "recordDescriptor:"<<attr << std::endl;
        }
        std::cout << "conditionAttribute:"<<conditionAttribute << std::endl;
        for(auto attr:attributeNames){
            std::cout << "attributesNames:"<< attr << std::endl;
        }
    }
    if(fileHandle.f == nullptr){
        std::cout << "[scan] ERROR: fileHanlde wasn't intialized correctly\n";
    }
    rbfm_ScanIterator.fileHandle = &fileHandle;
    //copyFileHandle(rbfm_ScanIterator.fileHandle,&fileHandle);
    rbfm_ScanIterator.rd = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.op = compOp;
    rbfm_ScanIterator.param = value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    if(DEBUG){
        std::cout << "[scan] Finish initializing the RBFM_ScanIterator as following:\n";
        std::cout << "\tfileHandle                    : " << rbfm_ScanIterator.fileHandle->fileName << " at address=" << &fileHandle.f<< std::endl;
        std::cout << "\trecordDescriptor size         : " << rbfm_ScanIterator.rd.size() << std::endl;
        std::cout << "\tconditionAttribute            : " << rbfm_ScanIterator.conditionAttribute << std::endl;
        std::cout << "\tComparator                    : " << rbfm_ScanIterator.op << std::endl;
        //std::cout << "\tValue is             :" << std::endl; // value is left blank;
        std::cout << "\tNames of attributes to compare:\n";
        for(auto attr:rbfm_ScanIterator.attributeNames){
            std::cout <<"\t\t" << attr << std::endl;
        }
        std::cout << "\n";
    }
    // also should initialize the currRID to Page 0 and Slot 0
    rbfm_ScanIterator.setcurrRID(0,0);
    if(DEBUG){
        std::cout << "[scan] Now the currRID is points to Page " << rbfm_ScanIterator.getcurrRID().pageNum 
            << " and slot " << rbfm_ScanIterator.getcurrRID().slotNum << std::endl;
    }

    // initialize the attrType of the ScanIterator
    for(auto attr:recordDescriptor){
        if(attr.name.compare(conditionAttribute) == 0){
            rbfm_ScanIterator.attrType = attr.type;
        }
    }
    return 0;
}


RBFM_ScanIterator::RBFM_ScanIterator() {
    currRID.pageNum = UINT_MAX; 
    currRID.slotNum = UINT_MAX;
    //pageData = malloc(PAGE_SIZE);
    //fileHandle = (FileHandle*)malloc(sizeof(FileHandle));
    //pageData = calloc(1,PAGE_SIZE); // allocate the size for pageData
    //currPageInfo = (PageInfo*)malloc(sizeof(PageInfo));
    //currPageInfo->freeSpace = 0;
    //currPageInfo->numRecords = 0;
    //memcpy((char*)pageData+PAGE_SIZE-sizeof(PageInfo),currPageInfo,sizeof(PageInfo));
}
RBFM_ScanIterator::~RBFM_ScanIterator(){
    //free(fileHandle);
    //free(pageData);
}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    // need to use reform to construct the data form
    RecordBasedFileManager *rbfm = &RecordBasedFileManager::instance();
    if( this->fileHandle == nullptr){
        std::cerr << "[getNextRecord] FATAL: need to call scan() before getNextRecord()\n";
        return -2;
    }
    if(this->fileHandle->appendPageCounter == 0){
        if(DEBUG){
            std::cout << "[getNextRecord] Empty file\n";
        }
        return RBFM_EOF;
    }
    uint8_t pageDataArray[PAGE_SIZE];
    void *pageData = &pageDataArray[0];
    //memset(pageDataArray,0,PAGE_SIZE);
    this->fileHandle->readPage(currRID.pageNum, pageData);
    PageInfo currPageInfo = this->fileHandle->getPageInfo(pageData);

    if(currPageInfo.freeSpace == 0 and currPageInfo.numRecords == 0){
        currRID.pageNum+=1;
        currRID.slotNum=0;
        this->fileHandle->readPage(currRID.pageNum,pageData);
        currPageInfo = this->fileHandle->getPageInfo(pageData);
        if(reachEOF(currPageInfo)){
            if(DEBUG){
                std::cout << "[getNextRecord] Reach EOF at (" << currRID.pageNum << "," << currRID.slotNum << ")\n";
            }
            return RBFM_EOF;
        }
        //if(DEBUG){
        //    std::cout << "[getNextRecord] Retrieved Page " << currRID.pageNum << " with freeSpace=" << 
        //        currPageInfo.freeSpace<< ", numRecords=" << currPageInfo.numRecords <<std::endl;
        //}
    }
    if(DEBUG){
        std::cout << "[getNextRecord] Retrieved Page " << currRID.pageNum << " with freeSpace=" << 
            currPageInfo.freeSpace<< ", numRecords=" << currPageInfo.numRecords <<std::endl;
    }
    //bool found = false;
    while(1){
        // Logic to increment RID
        if(currRID.slotNum == currPageInfo.numRecords){
            // at the end of page increase pageNum, reset numRecords
            currRID.pageNum++;
            currRID.slotNum = 1;
            // also need to re-read the New Page and update currPageInfo
            this->fileHandle->readPage(currRID.pageNum,pageData);
            currPageInfo = this->fileHandle->getPageInfo(pageData);
            if(currPageInfo.freeSpace == 0 and currPageInfo.numRecords == 0){
                currRID.pageNum+=1;
                currRID.slotNum=1;
                this->fileHandle->readPage(currRID.pageNum,pageData);
                currPageInfo = this->fileHandle->getPageInfo(pageData);
                //if(DEBUG){
                //    std::cout << "[getNextRecord] Retrieved Page " << currRID.pageNum << " with freeSpace=" << 
                //        currPageInfo.freeSpace<< ", numRecords=" << currPageInfo.numRecords <<std::endl;
                //}
            }
        
        }else{
            currRID.slotNum++;
        }
        // Two cases:
        //      1. increment pageNum and numRecords == 0
        //      2. increment numRecords and now we have it points to the last record
        if(reachEOF(currPageInfo)){
            if(DEBUG){
                std::cout << "[getNextRecord] Reach EOF at (" << currRID.pageNum << "," << currRID.slotNum << ")\n";
            }
            return RBFM_EOF;
        }
        if(op == NO_OP){
            rid.slotNum = currRID.slotNum;
            rid.pageNum = currRID.pageNum;
            
            if(rbfm->readRecord(*this->fileHandle,this->rd,rid,data)){
                continue; // continue on deleted record!
            }
            if(DEBUG){
                std::cout << "[getNextRecord] The name of the condAttrbute to find is: " << this->conditionAttribute <<std::endl;
                std::cout << "[getNextRecord] The found RID is: (" << rid.pageNum << "," << rid.slotNum << ")\n";
            }
            return 0; // found!!!!!!!
        }
        uint8_t condAttrDataArray[PAGE_SIZE];
        void* condAttrData = &condAttrDataArray[0];
        memset(condAttrDataArray,0,PAGE_SIZE);
        //std::cout << "Reading RID (" << currRID.pageNum << "," << currRID.slotNum << ")\n";
        if(rbfm->readAttribute(*this->fileHandle,this->rd,this->currRID,conditionAttribute,condAttrData)){
            //free(condAttrData);
            continue;
        }
        
        // check if the attribtue data matches the condition
        int condAttrInt = 0;
        int paramInt = 0;
        float condAttrFloat = 0;
        float paramFloat = 0;
        std::string condAttrStr = "";
        std::string paramStr = "";
        int condAttrDataLen = 0;
        int paramStrLen = 0;
        //int dataOffset = 0;
        //int varCharLen = 0;
        switch(this->attrType){
            case TypeInt:
                memcpy(&condAttrInt,condAttrData,sizeof(int));
                memcpy(&paramInt,param,sizeof(int));
                if(DEBUG){
                    std::cout << condAttrInt << " " << op << " " << paramInt << std::endl;
                }
                if(this->fileHandle->compareIntAttribute(this->op,condAttrInt,paramInt)==0){
                    rid.slotNum = currRID.slotNum;
                    rid.pageNum = currRID.pageNum;
                    rbfm->readRecord(*this->fileHandle,this->rd,rid,data);
                    if(DEBUG){
                        std::cout << "[getNextRecord] The name of the condAttrbute to find is: " << this->conditionAttribute <<std::endl;
                        std::cout << "[getNextRecord] The found RID is: (" << rid.pageNum << "," << rid.slotNum << ")\n";
                    }
                    return 0; // found!!!!!!!
                }
                break;
            case TypeReal:
                memcpy(&condAttrFloat,condAttrData,sizeof(float));
                memcpy(&paramFloat, param, sizeof(float));
                if(DEBUG){
                    std::cout << condAttrInt << " " << op << " " << paramInt << std::endl;
                }
                if(this->fileHandle->compareRealAttribute(this->op,condAttrFloat,paramFloat)==0){
                    rid.slotNum = currRID.slotNum;
                    rid.pageNum = currRID.pageNum;
                    rbfm->readRecord(*this->fileHandle,this->rd,rid,data);
                    if(DEBUG){
                        std::cout << "[getNextRecord] The name of the condAttrbute to find is: " << this->conditionAttribute <<std::endl;
                        std::cout << "[getNextRecord] The found RID is: (" << rid.pageNum << "," << rid.slotNum << ")\n";
                    }
                    return 0;
                }
                break;
            case TypeVarChar:
                //strncpy(condAttrStr.c_str(),condAttrData,sizeof(float));
                
                // Preparing the string objects for comparison
                memcpy(&condAttrDataLen,condAttrData,sizeof(int));
                condAttrStr.append((char*)condAttrData+sizeof(int),condAttrDataLen);
                //memcpy(&paramLen, param, sizeof(int));
                //paramStr.append((char*)param+sizeof(int),paramLen);
                memcpy(&paramStrLen,param,sizeof(int));

                paramStr.append((char*)param+sizeof(int),paramStrLen);
                
                
                if(this->fileHandle->compareVarcharAttribute(this->op,condAttrStr,paramStr)==0){
                    rid.slotNum = currRID.slotNum;
                    rid.pageNum = currRID.pageNum;
                    rbfm->readRecord(*this->fileHandle,this->rd,rid,data);
                    if(DEBUG){
                        std::cout << "[getNextRecord] The name of the condAttrbute to find is: " << this->conditionAttribute <<std::endl;
                        std::cout << "[getNextRecord] The found RID is: (" << rid.pageNum << "," << rid.slotNum << ")\n";
                    }
                    return 0;
                }
                break;
            default:
                std::cerr << "[getNextRecord] FATAL: SORRY, SOMETHING WENT WRONG\n";
                break;
        }
        
        //for(auto attrName : attributeNames){
        //    void* recData = malloc(PAGE_SIZE);
        //    //rbfm->readAttribute(this->fileHandle,this->rd,this->getcurrRID(),

        //}
        // Should do some prepcessing for the record Data
        
        // The following compares the record, if meet the condition, return 0
        
        
        
        // If condition is m
        
    }

    return RBFM_EOF;
}
void RBFM_ScanIterator::CopyRequiredAttributes(){};

int FileHandle::compareIntAttribute(CompOp op, const int &left, const int &right){
    switch(op){
        case EQ_OP:
            if(left == right){
                return 0;
            }else{
                return 1;
            }
            break;
        case LT_OP:
            if(left < right){
                return 0;
            }else{
                return 1;
            }
            break;
        case LE_OP:
            if(left <= right){
                return 0;
            }else{
                return 1;
            }
            break;
        case GT_OP:
            if(left > right){
                return 0;
            }else{
                return 1;
            }
            break;
        case GE_OP:
            if(left >= right){
                return 0;
            }else{
                return 1;
            }
            break;
        case NE_OP:
            if(left != right){
                return 0;
            }else{
                return 1;
            }
            break;
        case NO_OP:
            // if no condition always return 1?
            return 1;
            break;
        default:
            break;
    }

}
int FileHandle::compareRealAttribute(CompOp op, const float &left, const float &right){
    switch(op){
        case EQ_OP:
            if(left == right){
                return 0;
            }else{
                return 1;
            }
            break;
        case LT_OP:
            if(left < right){
                return 0;
            }else{
                return 1;
            }
            break;
        case LE_OP:
            if(left <= right){
                return 0;
            }else{
                return 1;
            }
            break;
        case GT_OP:
            if(left > right){
                return 0;
            }else{
                return 1;
            }
            break;
        case GE_OP:
            if(left >= right){
                return 0;
            }else{
                return 1;
            }
            break;
        case NE_OP:
            if(left != right){
                return 0;
            }else{
                return 1;
            }
            break;
        case NO_OP:
            // if no condition always return 1?
            return 1;
            break;
        default:
            break;
    }
}
int FileHandle::compareVarcharAttribute(CompOp op, const std::string &left, const std::string &right){
    int result = 0;
    switch(op){
        case EQ_OP:
            if(left.compare(right) == 0){
                return 0;
            }else{
                return 1;
            }
            break;
        case LT_OP:
            if(left.compare(right) < 0){
                return 0;
            }else{
                return 1;
            }
            break;
        case LE_OP:
            result = left.compare(right);
            if(result < 0 or result == 0){
                return 0;
            }else{
                return 1;
            }
            break;
        case GT_OP:
            result = left.compare(right);
            if(result > 0){
                return 0;
            }else{
                return 1;
            }
            break;
        case GE_OP:
            result = left.compare(right);
            if(result > 0 or result == 0 ){
                return 0;
            }else{
                return 1;
            }
            break;
        case NE_OP:
            if(left.compare(right) != 0){
                return 0;
            }else{
                return 1;
            }
            break;
        case NO_OP:
            // if no condition always return 1?
            return 1;
            break;
        default:
            break;
    }
}
