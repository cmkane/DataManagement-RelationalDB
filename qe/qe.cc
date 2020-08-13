#include "qe.h"
#include <float.h>
#define DEBUG 0
#define BYTE_SIZE 8

/* Function calculates record size based on recordDescriptor */
uint16_t calcRecSize(const std::vector<Attribute> &attrs, const void* data){

    uint16_t null_size = std::ceil((float)attrs.size()/BYTE_SIZE);
    uint8_t nullIndicator[null_size];
    uint16_t dataSize = null_size;
    memcpy(nullIndicator, data, null_size);
    uint16_t attrCounter = 0;
    unsigned str_len = 0;
    for(auto attr:attrs){
        unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
        unsigned int shift = 7 - (attrCounter % BYTE_SIZE);
        attrCounter++;
        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned) shift)){
            continue;
        }else{
            switch(attr.type){
                case TypeInt:
                    dataSize += sizeof(int);
                    break;
                case TypeReal:
                    dataSize += sizeof(float);
                    break;
                case TypeVarChar:
                    memcpy(&str_len, (char*)data+dataSize, sizeof(int));
                    dataSize += (sizeof(int) + str_len);
                    break;
                default:
                    break;
            }
        }
    }
    return dataSize;
}
/* Function set the key Size in the last parameter and returns the offset */
uint16_t getAttrOffsetAndSize(const std::vector<Attribute> &attrs, const void* data, int &keySize, Attribute *attribute, const std::string &attributeName ){

    uint16_t null_size = std::ceil((float)attrs.size()/BYTE_SIZE);
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, data, null_size);
    uint16_t dataSize = null_size;
    uint16_t attrCounter = 0;
    unsigned str_len = 0;
    for(auto attr:attrs){
        unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
        unsigned int shift = 7 - (attrCounter % BYTE_SIZE);
        attrCounter++;
        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned) shift)){
            if(attr.name.compare(attributeName) == 0){
                keySize = 0;
                return dataSize;
            }
            continue;
        }else{
            if(attr.name.compare(attributeName) == 0){
                if(attribute != nullptr){
                    *attribute = attr;
                }
                switch(attr.type){
                    case TypeInt:
                        keySize = sizeof(int);
                        break;
                    case TypeReal:
                        keySize = sizeof(float);
                        break;
                    case TypeVarChar:
                        int str_len;
                        memcpy(&str_len, (char*)data+dataSize,sizeof(int));
                        keySize = (str_len + sizeof(int));
                        break;
                }
                return dataSize; // dataSize should points to the beginning of the current field
            }
            switch(attr.type){
                case TypeInt:
                    dataSize += sizeof(int);
                    break;
                case TypeReal:
                    dataSize += sizeof(float);
                    break;
                case TypeVarChar:
                    memcpy(&str_len, (char*)data+dataSize, sizeof(int));
                    dataSize += (sizeof(int) + str_len);
                    break;
                default:
                    break;
            }
        }
    }
    if(DEBUG){
        std::cout << "[getAttrOffset] Unable to find attribute " << attributeName << " within the record";
    }
    return 0;
}

int compareIntAttributes(CompOp op, const int &left, const int &right){
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
    return -1;

}
int compareRealAttributes(CompOp op, const float &left, const float &right){
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
    return -1;
}
int compareVarcharAttributes(CompOp op, const std::string &left, const std::string &right){
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
    return -1;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int getNumBytesForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}

Filter::Filter(Iterator *input, const Condition &condition) {
    iterator = input;
    this->condition = condition;
}

RC Filter::getNextTuple(void *data) {
    if(iterator == nullptr) {
        return -1;
    }

    // Get the attributes from the iterator
    std::vector<Attribute> attrs;
    iterator->getAttributes(attrs);

    void *tempData = malloc(PAGE_SIZE);
    int lhsInt, rhsInt;
    float lhsReal, rhsReal;
    std::string lhsStr, rhsStr;
    AttrType type;
    bool match = false;
    int nullIndicatorSize = getNumBytesForNullsIndicator(attrs.size());
    uint8_t nullIndicator[nullIndicatorSize];
    int offset;

    while(!match) {
        bool compare = true;

        offset = nullIndicatorSize;
        int code = iterator->getNextTuple(tempData);
        if(code == QE_EOF) {
            free(tempData);
            return code;
        }

        if(DEBUG){
            std::cout << "[Filter getNextTuple] Print Record :";
            RecordBasedFileManager::instance().printRecord(attrs,tempData);
        }
        memcpy(nullIndicator, tempData, nullIndicatorSize);
        uint16_t attrCounter = 0; // Loop Counter

        for(Attribute &attribute : attrs) {
            unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
            unsigned int shift = 7-(attrCounter%BYTE_SIZE);
            attrCounter++;

            std::string attrName = attribute.name;

            bool collectRight = condition.bRhsIsAttr && attrName.compare(condition.rhsAttr) == 0;
            bool collectLeft = attrName.compare(condition.lhsAttr) == 0;

            if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)) {
                if(DEBUG) std::cout << "[Filter getNextTuple] The " << attrName << " attribute was null.\n";
                if(collectLeft || collectRight) {
                    compare = false; // If the null attribute is necessary don't make comparison.
                    if(DEBUG) std::cout << "[Filter getNextTuple] This was a necessary attribute! Skipping comparison and trying next tuple.\n;";
                }
                continue; // skip the null attribute.
            }

            int str_len;
            char *tempVarChar;
            switch(attribute.type) {
                case TypeInt:
                    if(collectRight || collectLeft) type = attribute.type;
                    if(collectRight) memcpy(&rhsInt, (char *)tempData + offset, sizeof(int));
                    if(collectLeft) memcpy(&lhsInt, (char *)tempData + offset, sizeof(int));
                    offset += sizeof(int);
                    break;
                case TypeReal:
                    if(collectRight || collectLeft) type = attribute.type;
                    if(collectRight) memcpy(&rhsReal, (char *)tempData + offset, sizeof(float));
                    if(collectLeft) memcpy(&lhsReal, (char *)tempData + offset, sizeof(float));
                    offset += sizeof(int);
                    break;
                case TypeVarChar:
                    if(collectRight || collectLeft) type = attribute.type;
                    memcpy(&str_len, (char *)tempData + offset, sizeof(int));
                    if(collectRight || collectLeft) {
                        tempVarChar = (char *)malloc(str_len + 1);
                        memcpy(tempVarChar, (char *)tempData + offset + sizeof(int), str_len);
                        memcpy(tempVarChar + str_len, "\0", sizeof(char));
                        if(collectRight) rhsStr = std::string(tempVarChar, str_len);
                        if(collectLeft) lhsStr = std::string(tempVarChar, str_len);
                        free(tempVarChar);
                    }
                    offset += sizeof(int) + str_len;
                    break;
                default:
                    free(tempData);
                    return -1;
            }
        }

        if(!compare) continue; // If one of the necessary attributes was null, try next entry.

        //Do the necessary comparison
        int str_len;
        switch(type) {
            case TypeInt:
                if(condition.bRhsIsAttr) match = compareIntAttributes(condition.op, lhsInt, rhsInt) == 0;
                else {
                    if(DEBUG) std::cout << "Is " << lhsInt << " " << condition.op << " " << *(int *)condition.rhsValue.data << " ?";
                    match = compareIntAttributes(condition.op, lhsInt, *(int *)condition.rhsValue.data) == 0;
                    if(DEBUG) {
                        if(match) std::cout << " YES\n"; else std::cout << " NO\n";
                    }
                }
                break;
            case TypeReal:
                if(condition.bRhsIsAttr) match = compareIntAttributes(condition.op, lhsReal, rhsReal) == 0;
                else match = compareRealAttributes(condition.op, lhsReal, *(float *)condition.rhsValue.data) == 0;
                break;
            case TypeVarChar:
                if(!condition.bRhsIsAttr) {
                    memcpy(&str_len, (char *)condition.rhsValue.data, sizeof(int));
                    rhsStr = std::string();
                    rhsStr.append((char*)condition.rhsValue.data+sizeof(int),str_len);
                }
                match = compareVarcharAttributes(condition.op, lhsStr, rhsStr) == 0;
                break;
            default:
                break;
        }
    }

    // After a match is found, copy over tempData into data
    memcpy(data, tempData, offset);
    free(tempData);
    return 0;
}

void Project::convertStringAttr() {
    std::vector<Attribute> filterAttrs;
    getFilterAttributes(filterAttrs);
    projAttrs.clear();
    for(std::string curr : attrs) {
        for(Attribute filtAttr : filterAttrs) {
            // If there's a match of attribute names push the filtAttr to back of projAttrs
            if(curr.compare(filtAttr.name) == 0) {
                projAttrs.push_back(filtAttr);
            }
        }
    }
    if(DEBUG) {
        if(projAttrs.size() != attrs.size()) {
            std::cout << "[Project convertStringAttr] WARNING! The sizes of project attribute vector and string vector don't match.\n";
        }
    }

}

RC Project::getNextTuple(void* data) {
    void *filterOutput = malloc(PAGE_SIZE);
    if(filter->getNextTuple(filterOutput)) {
        return QE_EOF;
    }

    std::vector<Attribute> filterAttrs;
    getFilterAttributes(filterAttrs);

    int projectNullIndicatorSize = getNumBytesForNullsIndicator(projAttrs.size()); // offset in the void* data
    int filterNullIndicatorSize = getNumBytesForNullsIndicator(filterAttrs.size()); // offset in the void* filterOutput

    uint8_t nullIndicator[filterNullIndicatorSize];
    memcpy(nullIndicator, (char *) filterOutput, filterNullIndicatorSize);

    uint8_t projNullIndicator[projectNullIndicatorSize];
    memset(projNullIndicator,0,projectNullIndicatorSize);

    int offset = projectNullIndicatorSize;
    uint16_t projAttrCounter = 0; // Loop Counter

    for(std::string curr : attrs) {
        int str_len;
        int filterOffset = filterNullIndicatorSize;
        unsigned int projIndexByte = floor((float)projAttrCounter/BYTE_SIZE);
        unsigned int projShift = 7-(projAttrCounter%BYTE_SIZE);
        projAttrCounter++; // inc count after use

        uint16_t filterAttrCounter = 0;
        for(Attribute &filtAttr : filterAttrs) {
            unsigned int filterIndexByte = floor((float)filterAttrCounter/BYTE_SIZE);
            unsigned int filterShift = 7-(filterAttrCounter%BYTE_SIZE);
            filterAttrCounter++; // inc count after use
            switch(filtAttr.type) {
                case TypeInt:
                    if(curr.compare(filtAttr.name) == 0) {
                        if(nullIndicator[filterIndexByte] & ((unsigned) 1 << (unsigned)filterShift)) {
                            // Set the new null indicator bit to 1
                            if(DEBUG) std::cout << "[Project getNextTuple] The " << filtAttr.name << " was null\n";
                            projNullIndicator[projIndexByte] = projNullIndicator[projIndexByte] + ((unsigned) 1 << (unsigned)projShift);
                        } else {
                            memcpy((char *)data + offset, (char *)filterOutput + filterOffset, sizeof(int));
                            offset += sizeof(int);
                        }
                    }
                    filterOffset += sizeof(int);
                    break;
                case TypeReal:
                    if(curr.compare(filtAttr.name) == 0) {
                        if(nullIndicator[filterIndexByte] & ((unsigned) 1 << (unsigned)filterShift)) {
                            projNullIndicator[projIndexByte] = projNullIndicator[projIndexByte] + ((unsigned) 1 << (unsigned)projShift);
                        } else {
                            memcpy((char *)data + offset, (char *)filterOutput + filterOffset, sizeof(float));
                            offset += sizeof(float);
                        }
                    }
                    filterOffset += sizeof(float);
                    break;
                case TypeVarChar:
                    memcpy(&str_len, (char *)filterOutput + filterOffset, sizeof(int));
                    if(curr.compare(filtAttr.name) == 0) {
                        if(nullIndicator[filterIndexByte] & ((unsigned) 1 << (unsigned)filterShift)) {
                            projNullIndicator[projIndexByte] = projNullIndicator[projIndexByte] + ((unsigned) 1 << (unsigned)projShift);
                        } else {
                            memcpy((char *)data + offset, &str_len, sizeof(int));
                            memcpy((char *)data + offset + sizeof(int), (char *) filterOutput + filterOffset + sizeof(int), str_len);
                            offset += (sizeof(int) + str_len);
                        }
                    }
                    filterOffset += (sizeof(int) + str_len);
                    break;
                default:
                    return -1;
            }
        }
    }

    // copy over the new null indicator
    memcpy((char *)data, projNullIndicator, projectNullIndicatorSize);
    free(filterOutput);
    return 0;
}
void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    this->leftIn->getAttributes(attrs);
    std::vector<Attribute> rightAttrs;
    this->rightIn->getAttributes(rightAttrs);
    for(auto rightAttr : rightAttrs){
        attrs.push_back(rightAttr);
    }
}

bool leftMatchRight(const void* leftData, uint16_t leftAttrSize, void* rightData, const std::vector<Attribute> &rightRd, const Condition &cond){
    if(cond.bRhsIsAttr){
        int rightKeySize;
        uint16_t offset = getAttrOffsetAndSize(rightRd, rightData, rightKeySize, nullptr, cond.rhsAttr);
        if(leftAttrSize != rightKeySize){
            return false;
        }
        if(memcmp((void*)((char*)rightData+offset), leftData ,rightKeySize) == 0){ // key match
            return true;
        }
    }else{  // right hand side is a value
        if(memcmp(leftData,cond.rhsValue.data,leftAttrSize) == 0){
            return true;
        }
    }
    return false;

}
bool leftMatchRight(RecordAttribute &leftAttr, void* rightData, const std::vector<Attribute> &rightRd, const Condition &cond){
    if(cond.bRhsIsAttr){
        int rightKeySize;
        uint16_t offset = getAttrOffsetAndSize(rightRd, rightData, rightKeySize, nullptr, cond.rhsAttr);
        if(leftAttr.getSize() != rightKeySize){
            return false;
        }
        if(memcmp((void*)((char*)rightData+offset), leftAttr.data ,rightKeySize) == 0){ // key match
            return true;
        }
    }else{  // right hand side is a value
        if(memcmp(leftAttr.data,cond.rhsValue.data,leftAttr.getSize()) == 0){
            return true;
        }
    }
    return false;

}
void concatRightAfterLeft(const void *leftData, const std::vector<Attribute> &leftRd, const void *rightData, const std::vector<Attribute> &rightRd, void* data, Iterator *iterator){
    std::vector<Attribute> joinAttributes;
    iterator->getAttributes(joinAttributes);
    auto joinAttr_iter = joinAttributes.begin();
    //auto left_iter = leftRd.begin();
    //auto right_iter = rightRd.begin();
    auto curr_iter = leftRd.begin(); // points to the left
    const void *ToRead = leftData;
    const std::vector<Attribute> *currRd = &leftRd;
    uint8_t newNullIndex = 0;
    uint8_t newNullSize = std::ceil((float)joinAttributes.size()/BYTE_SIZE);
    uint8_t newNullIndicator[newNullSize];
    memset(newNullIndicator,0,newNullSize);

    int writeOffset = newNullSize;
    while(joinAttr_iter != joinAttributes.end()){
        while(curr_iter->name.compare(joinAttr_iter->name) != 0){
            curr_iter++;
            if(curr_iter == leftRd.end()){
                curr_iter = rightRd.begin();
                ToRead = rightData;
                currRd = &rightRd;
            }
            if(curr_iter == rightRd.end() and joinAttr_iter != joinAttributes.end()){
                std::cerr << "[concatRightAfterLeft] Something weird happens, reaching the end of rightRd, however there's more wanted Attriubtes\n";
            }
        }
        int attrSize;
        uint16_t offset = getAttrOffsetAndSize(*currRd,ToRead,attrSize,nullptr, curr_iter->name);
        if(attrSize != 0){
            memcpy((char*)data+writeOffset, (char*)ToRead + offset, attrSize);
            writeOffset += attrSize;
        }else{
            // write a 1 to the nullIndicator
            unsigned int indexByte = std::floor((float)newNullIndex / BYTE_SIZE);
            unsigned int shift = 7 - (newNullIndex % BYTE_SIZE);
            newNullIndicator[indexByte] |= ((unsigned) 1 << (unsigned) shift);
        }
        newNullIndex++;
        joinAttr_iter++;
    }
    memcpy(data,newNullIndicator,newNullSize); // write the nullIndicator into the data
}

RC BNLJoin::getNextTuple(void *data){
    // iter->first contains the key
    // iter->second contains the record
    //void *rightData = malloc(PAGE_SIZE);
    if(this->first_iter){
        populateBlock();
        this->first_iter = false;
    }
    while(not leftEOFreached or not dataBlockExhausted){
        std::vector<Attribute> rightRd;
        rightIn->getAttributes(rightRd); // set the rd for right scan
        while(rightIn->getNextTuple(rightData) == 0){
            for(auto tuple: dataBlock){
                if(leftMatchRight(*tuple.first, rightData, rightRd, this->condition)){
                    // need to prepare the data if equal
                    std::vector<Attribute> leftRd;
                    this->leftIn->getAttributes(leftRd);
                    if(DEBUG){
                        std::cout << "\nRight Record is: ";
                        RecordBasedFileManager::instance().printRecord(rightRd,rightData);
                        std::cout << "Left Record is: ";
                        RecordBasedFileManager::instance().printRecord(leftRd, tuple.second->data);
                    }
                    concatRightAfterLeft(tuple.second->data, leftRd,rightData,rightRd,data, this);
                    return 0;
                }
            }
        }
        if(DEBUG){
            std::cout << "[BNLJoin->getNextTuple] dataBlockExhausted\n";
        }
        this->dataBlockExhausted = true;
        populateBlock();
        this->rightIn->setIterator();
    }

    return -1; // EOF reached
}
void BNLJoin::freeCurrentBlock(){
    for(auto iter :this->dataBlock){
        delete(iter.first);
        delete(iter.second);
    }
    this->dataBlock.clear();
}
void BNLJoin::populateBlock(){
    if(!first_iter){
        freeCurrentBlock();
    }
    unsigned int readSize = 0;
    void *tempData = malloc(PAGE_SIZE); // used to cache readTuple
    std::vector<Attribute> leftAttrs;
    leftIn->getAttributes(leftAttrs);
    // assume numPage * PAGE_SIZE wouldn't cause overflow
    int keySize;

    while(readSize < ((this->numPages-3) * PAGE_SIZE) and not leftEOFreached){    //readSize < this->numPages * PAGE_SIZE
        if(this->leftIn->getNextTuple(tempData)){
            // condition met on reaching EOF
            if(DEBUG){
                std::cout << "[populateBlock] Left Iterator EOF reached\n";
            }
            leftEOFreached = true;
        }
        uint16_t recSize = calcRecSize(leftAttrs,tempData);
        uint16_t offset = getAttrOffsetAndSize(leftAttrs, tempData, keySize, nullptr, condition.lhsAttr);
        int insertSize = keySize + recSize + sizeof(Record) + sizeof(RecordAttribute);
        //void *key = malloc(keySize);
        //memcpy(key, (char*)tempData + offset, keySize);
        //void *record = malloc(recSize);
        //memcpy(record, tempData, recSize);
        Record *record = new Record(tempData,recSize);
        RecordAttribute *key = new RecordAttribute((char*)tempData+offset, keySize);
        dataBlock.insert(std::make_pair(key, record));
        readSize += (insertSize);
        //for(auto attr: right)
    }
    if(DEBUG){
        std::cout << "[populateBlock] After populating the Block, it contains records:\n";
        for(auto iter:dataBlock){
            RecordBasedFileManager::instance().printRecord(leftAttrs,iter.second->data);
        }
    }
    if(dataBlock.size() != 0){
        this->dataBlockExhausted = false;
    }
    free(tempData);
}

void INLJoin::createAttributes() {
    left->getAttributes(joinAttrs);
    std::vector<Attribute> rightSide;
    right->getAttributes(rightSide);
    for(Attribute curr : rightSide) {
        joinAttrs.push_back(curr);
    }
}

RC INLJoin::getNextTuple(void *data) {

    // Use the iterator to get next Tuple to check if it matches condition
    bool match = false;
    std::vector<Attribute> leftAttrs;
    left->getAttributes(leftAttrs);
    std::vector<Attribute> rightAttrs;
    right->getAttributes(rightAttrs);

    void *rightData = malloc(PAGE_SIZE);

    while(!match) {
        if(getNext) {
            // read in the next tuple from the left relation
            int code = left->getNextTuple(leftData);
            // if last return EOF
            if(code == QE_EOF) {
                free(rightData);
                return code;
            }

            // reset the index scan iterator based on the condition
            // find the attribute that you want to compare from the left.

            leftOffset = getAttrOffsetAndSize(leftAttrs, leftData, leftSize, nullptr, condition.lhsAttr);
            void *leftStuff = (void*)((char *)leftData + leftOffset);
            switch(condition.op) {
                case EQ_OP:
                    right->setIterator(leftStuff, leftStuff, true, true);
                    break;
                case GT_OP:
                    right->setIterator(NULL, leftStuff, false, false);
                    break;
                case GE_OP:
                    right->setIterator(NULL, leftStuff, false, true);
                    break;
                case LT_OP:
                    right->setIterator(leftStuff, NULL, false, false);
                    break;
                case LE_OP:
                    right->setIterator(leftStuff, NULL, true, false);
                    break;
                default:
                    return -1;
            }

            getNext = false;
        }

        // get the next tuple from right
        int code = right->getNextTuple(rightData);
        // check if we need to get the next tuple from the left relation
        if(code == QE_EOF) {
            getNext = true;
            continue;
        }

        // TODO : leftMatchRight doesn't support other operators yet. May have to change.
        match = leftMatchRight((void*)((char *)leftData + leftOffset), leftSize, rightData, rightAttrs, condition);
    }

    // Put together the data to return for the join
    concatRightAfterLeft(leftData, leftAttrs, rightData, rightAttrs, data, this);

    free(rightData);
    return 0;
}
Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op):input(input),aggAttr(aggAttr),op(op){
    
    basicAggr = true;
    finished = false;
    switch(op){
        case AVG:
            result = 0;
            break;
        case MIN:
            result = FLT_MAX;
            break;
        case MAX:
            result = -FLT_MAX;
            break;
        case SUM:
            result = 0;
            break;
        case COUNT:
            result = 0;
            break;
    }
}
Aggregate::~Aggregate(){

}
RC Aggregate::getNextTuple(void *data){
    void *tupleData = malloc(PAGE_SIZE);
    //std::vector<Attribute> aggrAttributes; // this should only contain one Attribute
    //this->getAttributes(aggrAttributes);
    //Attribute aggrAttribute = aggrAttributes[0];
    int count = 0;
    int attrData;
    if(finished){
        return QE_EOF;
    }
    std::vector<Attribute> inputAttrs;
    this->input->getAttributes(inputAttrs);
    while(input->getNextTuple(tupleData) == 0){
        int attrSize;
        uint16_t attrOffset = getAttrOffsetAndSize(inputAttrs,tupleData,attrSize, nullptr, this->aggAttr.name);
        if(attrSize != 4){
            std::cerr << "[Aggregate getNextTuple] Aggregate should not be used on string\n";
        }
        memcpy(&attrData,(char*)tupleData + attrOffset,attrSize);
        switch(this->op){
            case MAX:
                if(attrData > result){
                    result = attrData;
                }
                break;
            case MIN:
                if(attrData < result){
                    result = attrData;
                }
                break;
            case SUM:
                result += attrData;
                break;
            case AVG:
                result += attrData;
                count++;
                break;
            case COUNT:
                result++;
                break;
        }
        
    }
    this->finished = true;
    if(this->op == AVG){
        float temp = result / count;
        memcpy((char*)data + 1, &temp, sizeof(float));
    }else{
        memcpy((char*)data + 1, &result, sizeof(float));
    }
    return 0;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions):leftIn(leftIn),rightIn(rightIn),condition(&condition), numPartitions(numPartitions){
    first_iter = true;

}
GHJoin::~GHJoin(){

};
RC GHJoin::getNextTuple(void *data){
    if(first_iter){
        createLeftPartition();
        createRightPartition();
        first_iter = false;
    }
    return 0;
};
void GHJoin::getAttributes(std::vector<Attribute> &attrs) const{

};
void GHJoin::createLeftPartition(){

}
void GHJoin::createRightPartition(){

}
