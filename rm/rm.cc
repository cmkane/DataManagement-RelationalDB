#include "rm.h"
#define DEBUG 0
#define BYTE_SIZE 8
#define NUM_INT_BEFORE_TABLEID 7

#define SYS_CATALOG_TABLE_FILENAME "Tables"
#define SYS_CATALOG_COL_FILENAME "Columns"
#define SYS_CATALOG_INDEX_FILENAME "Index"

// This is a temporary format used in the Index file as I haven't figure out  what else is needed
// index_filename = "tablename" + "_ "+"attribute" + "_" + "idx"   // this a temporary name format
/*
 * Here's the record Format for the Index file
 *
 *      (tbl-id,"index_filename","attribute-name")
 *
 */



RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() {
    recordFileManager = &RecordBasedFileManager::instance();
    indexManager = &IndexManager::instance();
    if(fileExists(SYS_CATALOG_TABLE_FILENAME)) {
        recordFileManager->openFile(SYS_CATALOG_TABLE_FILENAME, tableFileHandle);
    }
    if(fileExists(SYS_CATALOG_COL_FILENAME)) {
        recordFileManager->openFile(SYS_CATALOG_COL_FILENAME, colsFileHandle);
    }
    if(fileExists(SYS_CATALOG_INDEX_FILENAME)){
        recordFileManager->openFile(SYS_CATALOG_INDEX_FILENAME,indexFileHandle);
    }
}

RelationManager::~RelationManager() { 
    delete _relation_manager; 
}

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

// Calculate actual bytes for nulls-indicator for the given field counts
int getBytesForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}

// the prepared record is stored within buffer
void prepareIndexRecord(int table_id, std::string tablename,std::string attributename, void *buffer,int *recordSize){ 
    // There shouldn't be null fields inside the Index file
    int offset = 0;
    int nullFieldSize = 1;
    // After init
    uint8_t nullIndicator[nullFieldSize];
    memset(nullIndicator,0,nullFieldSize);
    memcpy(buffer,&nullIndicator[0], nullFieldSize);
    offset += nullFieldSize;
    
    memcpy((char*)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    std::string index_filename = tablename + "_" + attributename + "_idx";
    int index_filename_len = index_filename.length();
    memcpy((char*)buffer + offset, &index_filename_len,sizeof(int));
    offset += sizeof(int);
    memcpy((char*)buffer + offset, index_filename.c_str(), index_filename_len);
    offset += index_filename_len;

    int attributenameLen = attributename.length();
    memcpy((char*)buffer + offset, &attributenameLen,sizeof(int));
    offset += sizeof(int);
    memcpy((char*)buffer + offset, attributename.c_str(), attributenameLen);
    offset += attributenameLen;

    *recordSize = offset;
}

void prepareTablesRecord(int nullFieldSize, unsigned char *nullsFieldsIndicator, const int id,
        const int nameLen, const std::string &name, const int filenameLen, const std::string &filename,
        void *buffer, int *recordSize) {
    int offset = 0;

    bool nullBit = false;

    memcpy((char *)buffer + offset, nullsFieldsIndicator, nullFieldSize);
    offset += nullFieldSize;

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 7;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 6;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &nameLen, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, name.c_str(), nameLen);
        offset += nameLen;
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 5;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &filenameLen, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, filename.c_str(), filenameLen);
        offset += filenameLen;
    }

    *recordSize = offset;
}

void prepareColumnsRecord(int nullFieldSize, unsigned char *nullsFieldsIndicator, const int id,
        const int colNameLen, const std::string &colName, const int type, const int len, const int pos,
        void *buffer, int *recordSize) {
    int offset = 0;

    bool nullBit = false;

    memcpy((char *)buffer + offset, nullsFieldsIndicator, nullFieldSize);
    offset += nullFieldSize;

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 7;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 6;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &colNameLen, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, colName.c_str(), colNameLen);
        offset += colNameLen;
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 5;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &type, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 4;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &len, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullsFieldsIndicator[0] & (unsigned) 1 << (unsigned) 3;
    if (!nullBit) {
        memcpy((char *) buffer + offset, &pos, sizeof(int));
        offset += sizeof(int);
    }

    *recordSize = offset;
}

void buildTablesAttributesDescriptor(std::vector<Attribute> &recordDescriptor) {
    // Prepare record descriptor for Tables Table
    Attribute attr;
    recordDescriptor.clear();

    attr.setAttribute("table-id",TypeInt,(AttrLength) 4) ;
    recordDescriptor.push_back(attr);

    attr.setAttribute("table-name",TypeVarChar,50);
    recordDescriptor.push_back(attr);

    attr.setAttribute("file-name",TypeVarChar,(AttrLength)50);
    recordDescriptor.push_back(attr);
}

void buildColumnsAttributesDescriptor(std::vector<Attribute> &recordDescriptor) {
    // Prepare record descriptor for Columns Table
    Attribute attr;
    recordDescriptor.clear();

    attr.setAttribute("table-id", TypeInt, (AttrLength) 4);
    recordDescriptor.push_back(attr);

    attr.setAttribute("column-name", TypeVarChar, (AttrLength) 50);
    recordDescriptor.push_back(attr);

    attr.setAttribute("column-type", TypeInt, (AttrLength) 4);
    recordDescriptor.push_back(attr);

    attr.setAttribute("column-length", TypeInt, (AttrLength) 4);
    recordDescriptor.push_back(attr);

    attr.setAttribute("column-position", TypeInt, (AttrLength) 4);
    recordDescriptor.push_back(attr);
}
void buildIndexAttributesDescriptor(std::vector<Attribute> &recordDescriptor){
    Attribute attr;
    recordDescriptor.clear();
    attr.setAttribute("table-id", TypeInt,(AttrLength)4);
    recordDescriptor.push_back(attr);
    attr.setAttribute("index-filename", TypeVarChar,(AttrLength)50); 
    recordDescriptor.push_back(attr);
    attr.setAttribute("attribute-name", TypeVarChar,(AttrLength)50);
    recordDescriptor.push_back(attr);
    
}

void getAndUpdateTableId(FileHandle &fileHandle, unsigned &id) {
    // Get the highest tables id
    fileHandle.f->seekg(std::ios::beg + NUM_INT_BEFORE_TABLEID * sizeof(unsigned));
    fileHandle.f->read((char *)&id, sizeof(unsigned));
    id++;
    // Write the new highest tables id
    fileHandle.f->seekp(std::ios::beg + NUM_INT_BEFORE_TABLEID * sizeof(unsigned));
    fileHandle.f->write((char *)&id, sizeof(unsigned));
}

RC RelationManager::getTableIdAndFilename(const std::string &tableName, int &id, std::string &filename) {
    std::string tablesFilename = SYS_CATALOG_TABLE_FILENAME;
//    FileHandle tablesFileHandle;
//    recordFileManager->openFile(tablesFilename, tablesFileHandle);

    RBFM_ScanIterator tableScanner;
    std::vector<Attribute> tableAttributes;
    buildTablesAttributesDescriptor(tableAttributes);

    std::vector<std::string> attributeNames;
    attributeNames.push_back("table-id");
    attributeNames.push_back("file-name");
    
    void *tablename = malloc(tableName.length()+sizeof(int));
    int length = tableName.length();
    memcpy(tablename,&length,sizeof(int));
    memcpy((char*)tablename+sizeof(int),tableName.c_str(),length);
    if(recordFileManager->scan(tableFileHandle, tableAttributes, "table-name",
                                    EQ_OP, tablename, attributeNames, tableScanner)) {
        std::cout << "[getTableIdAndFilename] There was an error scanning the Tables table\n";
        return -1;
    }

    RID tableRID;
    uint8_t tableBufferArray[PAGE_SIZE];
    void *tableBuffer = &tableBufferArray[0];
    if(tableScanner.getNextRecord(tableRID, tableBuffer)) {
        if(DEBUG){
            std::cout << "[getTableIdAndFilename] There was no Tables entry for " << tableName << std::endl;
        }
        return -1;
    }

    //get the table id and filename
    int offset = 1;
    memcpy((char *)&id, (char *)tableBuffer+offset, sizeof(int));
    offset += sizeof(int);
    int fileLen;
    memcpy((char *)&fileLen, (char *)tableBuffer + offset, sizeof(int));
    char *fileName = (char*)calloc(1,fileLen+1); // LOl this +1 takes me forever to find
    offset += sizeof(int);
    memcpy(fileName, (char *)tableBuffer + offset, fileLen);
    //filename = (char*)tableBuffer + offset;
    filename = fileName;
    if(DEBUG){
        std::cout << "[getTableIdAndFilename] The table-id for " << tableName << " is " << id << std::endl;
    }

    free(fileName);
    return 0;
}

void Attribute::setAttribute(std::string name, AttrType type,
        AttrLength len){
    this->name = name;
    this->type = type;
    this->length = len;
}

RC RelationManager::createCatalog() {
    std::string tablesFilename = SYS_CATALOG_TABLE_FILENAME;
    std::string colsFilename = SYS_CATALOG_COL_FILENAME;
    std::string indexFilename = SYS_CATALOG_INDEX_FILENAME; 
    if(fileExists(tablesFilename) || fileExists(colsFilename) || fileExists(indexFilename)) {
        if(DEBUG){
            std::cout << "[createCatalog] ERROR: Catalog files exists\n";
        }
        return -1;
    }
    // Create and Open the Index Catalog
    recordFileManager->createFile(indexFilename);
    recordFileManager->openFile(indexFilename,indexFileHandle);

    recordFileManager->createFile(tablesFilename);
    recordFileManager->openFile(tablesFilename, tableFileHandle);
    // Write the tableIdCounter
    tableFileHandle.f->seekp(std::ios::beg + NUM_INT_BEFORE_TABLEID * sizeof(unsigned));
    unsigned count = 2;
    tableFileHandle.f->write((char *)&count, sizeof(unsigned));
    PagedFileManager::instance().setSysFlag(tableFileHandle, 1);
    if(!PagedFileManager::instance().isSystem(tableFileHandle)) {
        std::cout << "[createCatalog] Error : system flag not set correctly.\n";
    }

    std::vector<Attribute> recordDescriptor;
    buildTablesAttributesDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getBytesForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert Records for the Tables Table
    uint8_t recordArray[PAGE_SIZE];
    void *record = &recordArray[0];
    RID rid;
    //memset(record, 0, PAGE_SIZE);
    int recordLen;
    prepareTablesRecord(nullFieldsIndicatorActualSize, nullsIndicator, 1, 6, "Tables",
            6, "Tables", record, &recordLen);
    recordFileManager->insertRecord(tableFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserting record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen <<"\n";

    prepareTablesRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 7, "Columns",
            7, "Columns", record, &recordLen);
    recordFileManager->insertRecord(tableFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";

    if(DEBUG) {
        std::cout << "[createCatalog] Finished the creating table file\n";
    }

    recordFileManager->createFile(colsFilename);
    recordFileManager->openFile(colsFilename, colsFileHandle);
    // Write the tableIdCounter
    //fileHandle.f->seekp(std::ios::beg + 5 * sizeof(unsigned));
    //unsigned count = 8;
    //fileHandle.f->write((char *)&count, sizeof(unsigned));
    buildColumnsAttributesDescriptor(recordDescriptor);

    // Insert records for the Columns Table
    memset(record, '\0', PAGE_SIZE);
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 1, 8, "table-id", TypeInt, 4, 1, record, &recordLen);
    //std::cout << "[createCatalog] Record Length: " << recordLen << std::endl;
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 1, 10, "table-name", TypeVarChar, 50, 2, record, &recordLen);
    //std::cout << "[createCatalog] Record Length: " << recordLen << std::endl;
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 1, 9, "file-name", TypeVarChar, 50, 3, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);

    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 8, "table-id", TypeInt, 4, 1, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 11, "column-name", TypeVarChar, 50, 2, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 11, "column-type", TypeInt, 4, 3, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 13, "column-length", TypeInt, 4, 4, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";
    prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, 2, 15, "column-position", TypeInt, 4, 5, record, &recordLen);
    recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
    if(DEBUG) std::cout << "[createCatalog] inserted record RID(" << rid.pageNum << ", " << rid.slotNum << ") with record length = " << recordLen << "\n";


    free(nullsIndicator);

    return 0;
}

RC RelationManager::deleteCatalog() {
    std::string tablesFilename = SYS_CATALOG_TABLE_FILENAME;
    std::string colsFilename = SYS_CATALOG_COL_FILENAME;
    std::string indexFilename = SYS_CATALOG_INDEX_FILENAME;
     if(!fileExists(tablesFilename) || !fileExists(colsFilename) || !fileExists(indexFilename)) {
         if(DEBUG){
            std::cerr << "[deleteCatalog] Tables does not exists? -> " << fileExists(tablesFilename) << std::endl;
            std::cerr << "[deleteCatalog] Tables does not exists? -> " << fileExists(colsFilename) << std::endl;
            std::cerr << "[deleteCatalog] Tables does not exists? -> " << fileExists(indexFilename) << std::endl;
         }
         return -1;
     }
    recordFileManager->closeFile(tableFileHandle);
    recordFileManager->closeFile(colsFileHandle);
    recordFileManager->closeFile(indexFileHandle);

     int table_result = remove(tablesFilename.c_str());
     int col_result = remove(colsFilename.c_str());
     int index_result = remove(indexFilename.c_str());
     if(DEBUG){
         std::cout << "[deleteCatalog] Tables remove status: " << table_result;
         std::cout << "[deleteCatalog] Columns remove status: " << col_result;
         std::cout << "[deleteCatalog] Index remove status: " << index_result;
     }
     if( table_result != 0 or col_result != 0 or index_result != 0){
         if(DEBUG){
             std::cerr << "Error deleting " << tablesFilename << " or " << colsFilename << " or " << indexFilename << std::endl;
         }
         return -1;
     }else{
         return 0;
     }
    //return recordFileManager->destroyFile(tablesFilename) and recordFileManager->destroyFile(colsFilename);

}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    // add to the tables table and columns table
    // create the new file
    if(tableName == SYS_CATALOG_TABLE_FILENAME or tableName == SYS_CATALOG_COL_FILENAME or tableName == SYS_CATALOG_INDEX_FILENAME){
        std::cerr << "Error creating table with name " << tableName << std::endl;
        std::cerr << SYS_CATALOG_TABLE_FILENAME << " and " << SYS_CATALOG_COL_FILENAME << " and " << SYS_CATALOG_INDEX_FILENAME << " are reserved for System Catalog\n";
        return -1;
    }

    std::string tablesFilename = "Tables";
    std::string colsFilename = "Columns";

    std::vector<Attribute> recordDescriptor;
    buildTablesAttributesDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getBytesForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) calloc(1,nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    unsigned id = 0;
    getAndUpdateTableId(tableFileHandle, id);
    if(DEBUG){
        std::cout << "[createTable] table id counter updated to " << id << std::endl;
    }

    // Insert the new Table entry into Tables table
    void *record = calloc(1,PAGE_SIZE);
    RID rid;
    //memset(record, 0, PAGE_SIZE);
    int recordLen = 0;

    const std::string tableFileName = tableName;
    prepareTablesRecord(nullFieldsIndicatorActualSize, nullsIndicator, id, tableName.length(), tableName,
            tableName.length(), tableFileName, record, &recordLen);
    recordFileManager->insertRecord(tableFileHandle, recordDescriptor, record, rid);


    // Put entry for each column in the Columns Table
    //recordFileManager->openFile(colsFilename, colsFileHandle);
    buildColumnsAttributesDescriptor(recordDescriptor);
    int pos = 1;
    for(auto attr: attrs) {
        memset(record, 0, PAGE_SIZE);
        prepareColumnsRecord(nullFieldsIndicatorActualSize, nullsIndicator, id,
                attr.name.length(), attr.name,
                attr.type, attr.length, pos,
                record, &recordLen);
        recordFileManager->insertRecord(colsFileHandle, recordDescriptor, record, rid);
        pos++;
    }

    recordFileManager->createFile(tableFileName);
    free(nullsIndicator);
    free(record);

    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    if(DEBUG) {
        std::cout << "[deleteTable] Deleting table " << tableName << std::endl;
    }
    if(tableName.compare("Tables")==0 or tableName.compare("Columns")==0){
        if(DEBUG) {
            std::cout << "[deleteTable] ERROR: User trying to delete System Catalog\n";
        }
        return -1;
    }
    // get the id for table with tableName
    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        std::cout << "[deleteTable] Couldn't find table with table-name " << tableName << std::endl;
        return -1;
    }

    // destroy the table's file

    // remove record in Tables table with the given id
    RBFM_ScanIterator tableScanner;
    std::vector<Attribute> tableAttrs;
    buildTablesAttributesDescriptor(tableAttrs);
    std::vector<std::string> tableAttributeNames;
    tableAttributeNames.push_back("table-id");
    if(recordFileManager->scan(tableFileHandle, tableAttrs,
            "table-id", EQ_OP, &id,
            tableAttributeNames, tableScanner)) {
        if(DEBUG){
            std::cout << "[deleteTable] Error scanning Tables table.\n";
        }
        return -1;
    }
    RID rid;
    uint8_t dataArray[PAGE_SIZE];
    void *data = &dataArray[0];
    while(tableScanner.getNextRecord(rid, data) == 0) {
        if(DEBUG){
            std::cout << "[deleteTable] Deleting a Table record printed below...\n";
            recordFileManager->printRecord(tableAttrs, data);

        }
        recordFileManager->deleteRecord(tableFileHandle, tableAttrs, rid);
    }

    // remove records in Columns table with given id
    RBFM_ScanIterator colScanner;
    std::vector<Attribute> colsAttrs;
    buildColumnsAttributesDescriptor(colsAttrs);
    std::vector<std::string> colAttributeNames;
    colAttributeNames.push_back("table-id");
    if(recordFileManager->scan(colsFileHandle, colsAttrs,
                               "table-id", EQ_OP, &id,
                               colAttributeNames, colScanner)) {
        if(DEBUG){
            std::cout << "[deleteTable] Error scanning Columns table.\n";
        }
        return -1;
    }
    while(colScanner.getNextRecord(rid, data) == 0) {
        if(DEBUG){
            std::cout << "[deleteTable] Deleting a Columns record printed below...\n";
            recordFileManager->printRecord(colsAttrs, data);
        }
        recordFileManager->deleteRecord(colsFileHandle, colsAttrs, rid);
    }

    recordFileManager->destroyFile(tableFilename);

    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        if(DEBUG){
            std::cout << "[getAttributes] Couldn't find table with table-name " << tableName << std::endl;
        }
        return -1;
    }

    std::vector<Attribute> recordDescriptor;
    buildColumnsAttributesDescriptor(recordDescriptor);

    //requires scan of the columns table for the attributes
    RBFM_ScanIterator scanIterator;
    std::vector<std::string> attributeNames;
    attributeNames.push_back("column-name"); // varchar
    attributeNames.push_back("column-type"); // int
    attributeNames.push_back("column-length"); // int
    attributeNames.push_back("column-position"); // int
    if(DEBUG){
        std::cout << "[getAttributes] Scanning in Columns Catalog for table-id = " << id << std::endl;
    }

    if(recordFileManager->scan(colsFileHandle, recordDescriptor,
            "table-id", EQ_OP, &id,
            attributeNames, scanIterator)) {
        if(DEBUG){
            std::cout << "[getAttributes] Error getting scan iterator for Columns table.\n";
        }
        return -1;
    }

    RID rid;
    Attribute attr;
    //attr.setAttribute("",TypeInt,0); // initialize attr
    int nameLen = 0;
    //std::string name = "";
    int type = 0;
    int length = 0;
    uint8_t bufferArray[PAGE_SIZE];
    void *buffer = &bufferArray[0];
    int offset;
    // assumption nullsize is always 1
    while(scanIterator.getNextRecord(rid, buffer) != RBFM_EOF) {
        if(DEBUG){
            std::cout << "[getAttributes] Retrieved the following record\n";
            recordFileManager->printRecord(recordDescriptor,buffer);
        }
        offset = 1 + sizeof(int); //skip the first byte bc it's the null indicator ... AND also the table ID!
        //string name = "";
        memcpy(&nameLen, (char *)buffer + offset, sizeof(unsigned));
        offset += sizeof(unsigned);

        //std::cout << "nameLen = " << nameLen << std::endl;
        char *nameArray=(char*)calloc(1,nameLen+1); // has to allocate one more bye for char* !
        memcpy(nameArray, (char *)buffer + offset, nameLen);
        offset += nameLen;
        std::string name = nameArray;

        memcpy(&type, (char *)buffer + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(&length, (char *)buffer + offset, sizeof(int));
        offset += sizeof(int);

        //std::cout << "[getAttributes] Got attribute: name=" << name << ", type=" << type << ", length=" << length << std::endl;
        attr.setAttribute(name, (AttrType) type, (AttrLength) length);
        attrs.push_back(attr);
        free(nameArray);
    }

    if(DEBUG){
        std::cout << "[getAttributes] The retrieved attributes are: \n";
        for(auto Attr:attrs){
            std::cout << "attr.name = " << Attr.name << ", length = " <<Attr.length << ", attrType=" << Attr.type <<std::endl;
        }
    }
    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    if(tableName.compare("Tables")==0 or tableName.compare("Columns")==0){
        if(DEBUG) {
            std::cout << "[deleteTable] ERROR: User trying to insert into System Catalog\n";
        }
        return -1;
    }
    int id;
    std::string tableFilename;

    //std::cout << "[insertTuple] Before getTableIdAndFilename tableName to insert is: " << tableName << std::endl;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        if(DEBUG) {
            std::cout << "[RM scan] Couldn't find table with table-name " << tableName << std::endl;
        }
        return -1;
    }
    //std::cout << "[insertTuple] After tableName to insert is: " << tableName << std::endl;

    // get record descriptor for table
    std::vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor)) {
        std::cout << "[insertTuple] Error in getAttributes.\n";
        return -1;
    }
    //std::cout << "[insertTuple] After getAttributes TableName to insert is: " << tableName << std::endl;

    //std::cout << "[insertTuple] print record.\n";
    //recordFileManager->printRecord(recordDescriptor, data);

    FileHandle fileHandle;
    if(DEBUG){
        std::cout << "[insertTuple] Trying to open file: " << tableFilename << std::endl;
    }
    int openResult = recordFileManager->openFile(tableFilename, fileHandle);
    if(openResult != 0){
        std::cerr << "[insertTuple] FATAL: unable to open file:" << tableFilename << std::endl;
        return -1;
    }

    if(DEBUG){
        std::cout << "[insertTuple] Opened File: " << tableFilename << std::endl;
    }
    int insertResult = recordFileManager->insertRecord(fileHandle,recordDescriptor,data,rid);
    //recordFileManager->printRecord(recordDescriptor,data);

    recordFileManager->closeFile(fileHandle);
    if(insertResult != 0){
        if(DEBUG){
            std::cout << "[insertTuple] Fail to insert, don't update the B+ tree\n";
        }
        return insertResult;
    }

    /* Update the B+ trees to synchronize the rids */
    std::vector<Attribute> indexRd;
    buildIndexAttributesDescriptor(indexRd);
    std::vector<std::string> CondAttrs;
    CondAttrs.push_back("index-filename");
    CondAttrs.push_back("attribute-name");
    RBFM_ScanIterator indexCatalogScanner;
    RM_ScanIterator rm_iter;
    if(recordFileManager->scan(this->indexFileHandle,indexRd,"table-id",EQ_OP,&id, CondAttrs, indexCatalogScanner)){
        if(DEBUG){
            std::cout << "[insertTuple] Fail when setting up scan in index Catalog\n";
        }
    }
    RID tempRID;
    void *record = malloc(PAGE_SIZE);
    void *tempData = malloc(PAGE_SIZE);
    rm_iter.setScanIterator(indexCatalogScanner);
    rm_iter.setRecordDescriptor(indexRd);
    //rm_iter.setTableFileName("Index");
    uint16_t null_size = std::ceil((float)indexRd.size()/ BYTE_SIZE);
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator,data,null_size);
    // this is actually an expensive cost
    while(indexCatalogScanner.getNextRecord(tempRID, record) == 0){
        if(DEBUG){
            recordFileManager->printRecord(indexRd, record);
        }
        // for every index that's created in the index catalog file, insert
        rm_iter.prepareData(record, tempData);
        // tempData contains the indexFileName
        int indexFilename_len;
        int attributeName_len;
        memcpy(&indexFilename_len,(char*)tempData+null_size, sizeof(int));
        std::string indexFilename((char*)tempData+sizeof(int)+null_size,indexFilename_len);
        int attrName_offset = null_size + sizeof(int)+indexFilename_len;
        memcpy(&attributeName_len, (char*)tempData+attrName_offset, sizeof(int));
        std::string attributeName((char*)tempData+attrName_offset+sizeof(int), attributeName_len);
        if(DEBUG){
            std::cout << "[insertTuple] indexFilename_len = " << indexFilename_len << ", attributeName_len = " << attributeName_len << std::endl;
            std::cout << "[insertTuple] Inserting attribute:" << attributeName << " into file " << indexFilename << std::endl;
        }
        IXFileHandle indexAttrFileHandle;
        if(DEBUG){
            std::cout << "indexFilename = " << indexFilename;
        }
        indexManager->openFile(indexFilename, indexAttrFileHandle);
        int key_offset = null_size;
        uint16_t attrCounter = 0;
        for(auto attr:recordDescriptor){
            unsigned int IndexByte = floor((float)attrCounter/BYTE_SIZE);
            unsigned int shift = 7 - (attrCounter % BYTE_SIZE);
            attrCounter++;
            if(nullIndicator[IndexByte] & ((unsigned) 1 << (unsigned)shift)){
                continue;
            }
            if(attributeName.compare(attr.name) == 0){
                indexManager->insertEntry(indexAttrFileHandle, attr,(char*)data+key_offset, rid);
                if(DEBUG){
                    std::cout << "Printing index tree for " << attr.name << std::endl;
                    indexManager->printBtree(indexAttrFileHandle, attr);
                    std::cout << "\n\n";
                }
            }
            // This switch is ugly......
            switch(attr.type){
                case TypeInt:
                    key_offset += sizeof(int);
                    break;
                case TypeReal:
                    key_offset += sizeof(float);
                    break;
                case TypeVarChar:
                    int strLen;
                    memcpy(&strLen, (char*)data+key_offset, sizeof(int));
                    key_offset += (sizeof(int) + strLen);
                    break;
                default:
                    break;

            }
        }
        indexManager->closeFile(indexAttrFileHandle);
    }
    free(record);
    free(tempData);
    if(DEBUG){
        std::cout << "[insertTuple] Finished\n";
    }

    return insertResult;

}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    if(tableName.compare("Tables")==0 or tableName.compare("Columns")==0){
        if(DEBUG) {
            std::cout << "[deleteTable] ERROR: User trying to delete tuple in System Catalog\n";
        }
        return -1;
    }
    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        if(DEBUG){
            std::cout << "[deleteTuple] Couldn't find table with table-name " << tableName << std::endl;
        }
        return -1;
    }

    std::vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    FileHandle fileHandle;
    recordFileManager->openFile(tableFilename, fileHandle);
    // Delete the corresponding entry in created B+ trees 
    std::vector<Attribute> indexRd;
    buildIndexAttributesDescriptor(indexRd);
    std::vector<std::string> CondAttrs;
    CondAttrs.push_back("index-filename");
    CondAttrs.push_back("attribute-name");
    RBFM_ScanIterator indexCatalogScanner;
    RM_ScanIterator rm_iter;
    if(recordFileManager->scan(this->indexFileHandle,indexRd,"table-id",EQ_OP,&id, CondAttrs, indexCatalogScanner)){
        if(DEBUG){
            std::cout << "[insertTuple] Fail when setting up scan in index Catalog\n";
        }
    }
    RID tempRID;
    void *record = malloc(PAGE_SIZE);
    void *tempData = malloc(PAGE_SIZE);
    void *data = malloc(PAGE_SIZE);
    rm_iter.setScanIterator(indexCatalogScanner);
    rm_iter.setRecordDescriptor(indexRd);

    uint16_t null_size = std::ceil((float)indexRd.size()/ BYTE_SIZE);
    // this is actually an expensive cost
    while(indexCatalogScanner.getNextRecord(tempRID, record) == 0){
        // for every index that's created in the index catalog file, insert
        rm_iter.prepareData(record, tempData);
        // tempData contains the indexFileName
        int indexFilename_len;
        int attributeName_len;
        memcpy(&indexFilename_len,(char*)tempData+null_size, sizeof(int));
        std::string indexFilename((char*)tempData+sizeof(int)+null_size,indexFilename_len);
        int attrName_offset = null_size + sizeof(int)+indexFilename_len;
        memcpy(&attributeName_len, (char*)tempData+attrName_offset, sizeof(int));
        std::string attributeName((char*)tempData+attrName_offset+sizeof(int), attributeName_len);
        //int key_offset = 0;
        IXFileHandle indexFileHandle;
        indexManager->openFile(indexFilename, indexFileHandle);
        for(auto attr:recordDescriptor){
            if(attributeName.compare(attr.name) == 0){
                recordFileManager->readAttribute(fileHandle, recordDescriptor,rid, attributeName, data);
                if(indexManager->deleteEntry(indexFileHandle,attr,data, rid)){
                    std::cerr << "[deleteTuple] RID("<< rid.pageNum <<"," << rid.slotNum << ") wasn't find inside the B= tree of " << attributeName << std::endl;
                }
            }
            // This switch is ugly......
            //switch(attr.type){
            //    case TypeInt:
            //        key_offset += sizeof(int);
            //        break;
            //    case TypeReal:
            //        key_offset += sizeof(float);
            //        break;
            //    case TypeVarChar:
            //        int strLen;
            //        memcpy(&strLen, (char*)data+key_offset, sizeof(int));
            //        key_offset += (sizeof(int) + strLen);
            //        break;
            //    default:
            //        break;

            //}
        }
        indexManager->closeFile(indexFileHandle);
    }
    free(data);
    free(record);
    free(tempData);


    if(recordFileManager->deleteRecord(fileHandle, recordDescriptor, rid)) {
        std::cout << "[deleteTuple] deleteRecord failed somewhere.\n";
        return -1;
    }


    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    if(tableName.compare("Tables")==0 or tableName.compare("Columns")==0){
        if(DEBUG) {
            std::cout << "[deleteTable] ERROR: User trying to update Tuple inside System Catalog\n";
        }
        return -1;
    }
    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName,id,tableFilename)){
        if(DEBUG)
        std::cout << "[updateTuple] Couldn't find the table with table-name " << tableName << std::endl;
    }


    FileHandle tableFileHandle;
    recordFileManager->openFile(tableFilename,tableFileHandle);
    std::vector<Attribute> recordDescriptor;
    if(getAttributes(tableName,recordDescriptor)){
        if(DEBUG)
        std::cout << "[updateTuple] FATAL: fail to get the attributes for the table: " << tableName << std::endl;
    }
    /* Update B+ operations */
    std::vector<Attribute> indexRd;
    buildIndexAttributesDescriptor(indexRd);
    std::vector<std::string> CondAttrs;
    CondAttrs.push_back("index-filename");
    CondAttrs.push_back("attribute-name");
    RBFM_ScanIterator indexCatalogScanner;
    RM_ScanIterator rm_iter;
    if(recordFileManager->scan(this->indexFileHandle,indexRd,"table-id",EQ_OP,&id, CondAttrs, indexCatalogScanner)){
        if(DEBUG){
            std::cout << "[insertTuple] Fail when setting up scan in index Catalog\n";
        }
    }
    RID tempRID;
    void *record = malloc(PAGE_SIZE);
    void *tempData = malloc(PAGE_SIZE);
    void *attrData = malloc(PAGE_SIZE);
    rm_iter.setScanIterator(indexCatalogScanner);
    rm_iter.setRecordDescriptor(indexRd);
    //rm_iter.setTableFileName("Index");
    uint16_t null_size = std::ceil((float)indexRd.size()/ BYTE_SIZE);

    // this is actually an expensive cost
    while(indexCatalogScanner.getNextRecord(tempRID, record) == 0){
        if(DEBUG){
            recordFileManager->printRecord(indexRd, record);
        }
        // for every index that's created in the index catalog file, insert
        rm_iter.prepareData(record, tempData);
        // tempData contains the indexFileName
        int indexFilename_len;
        int attributeName_len;
        memcpy(&indexFilename_len,(char*)tempData+null_size, sizeof(int));
        std::string indexFilename((char*)tempData+sizeof(int)+null_size,indexFilename_len);
        int attrName_offset = null_size + sizeof(int)+indexFilename_len;
        memcpy(&attributeName_len, (char*)tempData+attrName_offset, sizeof(int));
        std::string attributeName((char*)tempData+attrName_offset+sizeof(int), attributeName_len);
        if(DEBUG){
            std::cout << "[insertTuple] indexFilename_len = " << indexFilename_len << ", attributeName_len = " << attributeName_len << std::endl;
            std::cout << "[insertTuple] Inserting attribute:" << attributeName << " into file " << indexFilename << std::endl;
        }
        IXFileHandle indexAttrFileHandle;
        indexManager->openFile(indexFilename, indexAttrFileHandle);
        int key_offset = 0;
        for(auto attr:recordDescriptor){
            if(attributeName.compare(attr.name) == 0){
                recordFileManager->readAttribute(tableFileHandle, recordDescriptor, rid, attributeName, attrData);
                if(indexManager->deleteEntry(indexAttrFileHandle, attr, attrData, rid)){
                    std::cerr << "[updateTuple] RID(" << rid.pageNum << "," << rid.slotNum << ") wasn't deleted properly, probably due to non-existing RID\n";
                }
                if(indexManager->insertEntry(indexAttrFileHandle, attr,(char*)data+key_offset, rid)){
                    std::cerr << "[updateTuple] Insert after delete somehow failed\n";
                }
            }
            // This switch is ugly......
            switch(attr.type){
                case TypeInt:
                    key_offset += sizeof(int);
                    break;
                case TypeReal:
                    key_offset += sizeof(float);
                    break;
                case TypeVarChar:
                    int strLen;
                    memcpy(&strLen, (char*)data+key_offset, sizeof(int));
                    key_offset += (sizeof(int) + strLen);
                    break;
                default:
                    break;

            }
        }
        indexManager->closeFile(indexAttrFileHandle);
    }
    free(record);
    free(tempData);
    int update_result = recordFileManager->updateRecord(tableFileHandle,recordDescriptor,data,rid);
    if(update_result != 0){
        if(DEBUG){
            std::cout << "[updateTuple] Fail to update in the rbfm layer\n";
        }
        return update_result;
    }
    return update_result;

}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName,id,tableFilename)){
        if(DEBUG)
        std::cout << "[readTuple] Couldn't find the table with table-name " << tableName << std::endl;
        return -1;
    }


    FileHandle fileHandle;
    recordFileManager->openFile(tableFilename,fileHandle);
    std::vector<Attribute> recordDescriptor;
    if(getAttributes(tableName,recordDescriptor)){
        if(DEBUG)
        std::cout << "[readTuple] FATAL: fail to get the attributes for the table: " << tableName << std::endl;
        return -1;
    }

    //std::cout << "[readTuple] printRecord\n";
    //recordFileManager->printRecord(recordDescriptor, data);
    int status = recordFileManager->readRecord(fileHandle,recordDescriptor,rid,data);
    if(DEBUG){
        std::cout << "read following tuple:";
        recordFileManager->printRecord(recordDescriptor, data);
        std::cout << std::endl;
    }
    return status;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    if(DEBUG){
        for(auto attr:attrs){
            std::cout << attr.name << std::endl;
        }
    }
    return recordFileManager->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    if(DEBUG) {
        std::cout << "[readAttribute] Reading attribute " << attributeName << " for table " << tableName << " RID(" << rid.pageNum << ", " << rid.slotNum << ")\n";
    }

    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        if(DEBUG)
        std::cout << "[RM scan] Couldn't find table with table-name " << tableName << std::endl;
        return -1;
    }

    // get the record descriptor for the table name
    std::vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    // read the record at rid
    FileHandle fileHandle;
    recordFileManager->openFile(tableFilename, fileHandle);
    uint8_t recordArray[PAGE_SIZE];
    void *record = &recordArray[0];
    recordFileManager->readRecord(fileHandle, recordDescriptor, rid, record);
    if(DEBUG){
        std::cout << "[readAttribute] Retrieved the following record:\n";
        recordFileManager->printRecord(recordDescriptor, record);

    }

    uint16_t null_size = std::ceil((float)recordDescriptor.size()/BYTE_SIZE);
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, record, null_size);

    uint16_t newNullSize = 1; // because there's only one attribute
    uint8_t newNullIndicator[newNullSize];

    unsigned offset = null_size;
    if(DEBUG){
        std::cout << "[readAttribute] The offset starts out as " << offset << std::endl;
    }
    unsigned writeOffset = 1; // to write to new data
    uint16_t attrCounter = 0; // Loop Counter
    unsigned str_len = 0;
    for(auto attr : recordDescriptor) {
        unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
        unsigned int shift = 7-(attrCounter%BYTE_SIZE);
        attrCounter++;
            if (attr.name.compare(attributeName) == 0) {
                if(DEBUG){
                    std::cout << "[readAttribute] Found a match! Name = " << attributeName << std::endl;
                }
                if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)) {
                    if(DEBUG){
                        std::cout << "[readAttribute] The attribute was null.\n";
                    }
                    newNullIndicator[0] = newNullIndicator[0] + ((unsigned) 1 << (unsigned)7);
                    memcpy((char *)data, (char *)newNullIndicator, sizeof(newNullSize));
                    return 0;
                }
                // switch for doing the record retrieval
                switch (attr.type) {
                    case TypeVarChar:
                        memcpy(&str_len, (char *)record + offset, sizeof(int));
                        memcpy((char *)data + writeOffset, (char *)record + offset, sizeof(int) + str_len);
                        memcpy((char *)data, newNullIndicator, newNullSize);
                        return 0;
                    case TypeInt:
                        if(DEBUG){
                            std::cout << "[readAttribute] offset is :" << offset << std::endl;
                        }
                        //offset = 19;
                        memcpy((char *)data + writeOffset, (char *)record + offset, sizeof(int));
                        memcpy(data, newNullIndicator,newNullSize);
                        break;
                        //return 0;
                    case TypeReal:
                        memcpy((char *)data + writeOffset, (char *)record + offset, sizeof(float));
                        memcpy(data, newNullIndicator, newNullSize);
                        break;
                        //return 0;
                    default:
                        return -1;
                }
            }
            // switch for adding to offset
            switch (attr.type) {
                case TypeVarChar:
                    memcpy(&str_len, (char *)record + offset, sizeof(int));
                    offset += (sizeof(int) + str_len);
                    break;
                case TypeInt:
                    offset += sizeof(int);
                    break;
                case TypeReal:
                    offset += sizeof(float);
                    break;
                default:
                    return -1;
            }
    }

    return 0;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    // open the filename for the table to get the file handle
    int id;
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id, tableFilename)) {
        if(DEBUG){
            std::cout << "[RM scan] Couldn't find table with table-name " << tableName << std::endl;
        }
        return -1;
    }

    // get the record descriptor for this table using getAttributes
    std::vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor)) {
        if(DEBUG){
            std::cout << "[RM scan] There was an error getting attributes for table " << tableName << std::endl;
        }
        return -1;
    }

    // Get the scan iterator from the record based file manager
    RBFM_ScanIterator rbfmScanIterator;
    FileHandle *fileHandle = new FileHandle();
    recordFileManager->openFile(tableFilename, *fileHandle);

    if(recordFileManager->scan(*fileHandle, recordDescriptor,
            conditionAttribute, compOp, value,
            attributeNames, rbfmScanIterator)) {
        if(DEBUG){
            std::cout << "[RM scan] There was an error with RBFM scan for table name " << tableName << std::endl;
        }
        return -1;
    }

    // Setup the rm_ScanIterator with that iterator
    rm_ScanIterator.setScanIterator(rbfmScanIterator);
    rm_ScanIterator.setRecordDescriptor(recordDescriptor);
    rm_ScanIterator.setTableFileName(tableFilename);
    rm_ScanIterator.fileHandle = fileHandle;

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

void RM_ScanIterator::setScanIterator(RBFM_ScanIterator iterator) {
    this->scanIterator = iterator;
}

void RM_ScanIterator::setRecordDescriptor(std::vector <Attribute> descriptor) {
    this->recordDescriptor = descriptor;
}

void RM_ScanIterator::setTableFileName(std::string filename) {
    this->tableFileName = filename;
}
RC RM_ScanIterator::close(){
    //RecordBasedFileManager::instance().closeFile(*fileHandle);
    delete fileHandle;
    return 0;
}

RC RM_ScanIterator::prepareData(void *record, void *data){
    uint16_t null_size = std::ceil((float)recordDescriptor.size()/BYTE_SIZE);
    uint8_t nullIndicator[null_size];
    memcpy(nullIndicator, record, null_size);

    uint16_t newNullSize = std::ceil((float)scanIterator.attributeNames.size()/BYTE_SIZE);
    uint8_t newNullIndicator[newNullSize];
    memset(newNullIndicator,0,newNullSize);

    unsigned writeOffset = newNullSize; // to write to new data
    unsigned readOffset = null_size; // to read from record data
    uint16_t attrCounter = 0;
    uint16_t newAttrCounter = 0;
    unsigned str_len = 0;
    //for(auto name:scanIterator.attributeNames){
    //    std::cout << "attributeName = " << name << std::endl;
    //}
    for(auto attr : recordDescriptor) {
        unsigned int indexByte = floor((float)attrCounter/BYTE_SIZE);
        unsigned int shift = 7-(attrCounter%BYTE_SIZE);
        attrCounter++;

        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)) {
            unsigned int byteNum = floor((float)newAttrCounter/BYTE_SIZE);
            unsigned int shiftNum = 7-(newAttrCounter%BYTE_SIZE);
            newAttrCounter++;
            newNullIndicator[byteNum] |= ((unsigned) 1 << (unsigned)shiftNum);
            continue;
        }
        // for each attribute name wanted check for matching name in record
        for(auto name : scanIterator.attributeNames) {
            if (attr.name.compare(name) == 0) {
                if(DEBUG){
                    std::cout << "[getNextTuple] Found Attribute:" << name << std::endl; 
                }
                    switch (attr.type) {
                        case TypeVarChar:
                            memcpy(&str_len, (char *)record + readOffset, sizeof(int));
                            memcpy((char *)data + writeOffset, (char *)record + readOffset, sizeof(int) + str_len);
                            writeOffset += sizeof(int) + str_len;
                            break;
                        case TypeInt:
                            //std::cout << "[getNextTuple] readOffset = " << readOffset << std::endl;
                            //std::cout << "[getNextTuple] data = " << *(int*)((char*)record+readOffset)
                            memcpy((char*)data + writeOffset, (char *)record + readOffset, sizeof(int));
                            writeOffset += sizeof(int);
                            break;
                        case TypeReal:
                            memcpy((char*)data + writeOffset, (char *)record + readOffset, sizeof(float));
                            writeOffset += sizeof(float);
                            break;
                        default:
                            return -1;
                    }
                break; // if a attribute name matches stop checking attributeNames
            }
        }

        // switch for adding to readOffset
        switch (attr.type) {
            case TypeVarChar:
                memcpy(&str_len, (char *)record + readOffset, sizeof(int));
                readOffset += (sizeof(int) + str_len);
                break;
            case TypeInt:
                readOffset += sizeof(int);
                break;
            case TypeReal:
                readOffset += sizeof(float);
                break;
            default:
                return -1;
        }
    }

    //write the newNullIndicator to data
    //std::cout << "[getNextTuple] The newNullSize is = "<< newNullSize << std::endl;
    memcpy(data, newNullIndicator, newNullSize);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    //void *record = calloc(1,PAGE_SIZE);

    uint8_t recordArray[PAGE_SIZE];
    void *record = &recordArray[0];
    int returnCode = scanIterator.getNextRecord(rid, record);
    if(DEBUG){
        std::cout << "[getNextTuple] Retrieved the following record\n";
        RecordBasedFileManager::instance().printRecord(recordDescriptor,record);
    }

    if(returnCode == -2) {
        if(DEBUG){
            std::cout << "[getNextTuple] Error occurred while getNextRecord.\n";
        }
        return -2;
    }
    if(returnCode == RBFM_EOF) {
        if(DEBUG){
            std::cout << "[getNextTuple] EOF reached.\n";
        }
        //close();
        
        return RM_EOF;
    }
    if(DEBUG){
        std::cout << "[getNextTuple] Before reforming, printing record:";
        RecordBasedFileManager::instance().printRecord(this->recordDescriptor,record);
    }
    this->prepareData(record,data);
    if(DEBUG){
        std::cout << "[getNextTuple] After reforming, printing record:";
        RecordBasedFileManager::instance().printRecord(this->recordDescriptor,data);
    }


    //free(record);
    //free(nullIndicator);
    //free(newNullIndicator);

    return 0;
}
// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    int id; // used for tableId
    std::string tableFilename;
    if(getTableIdAndFilename(tableName, id,tableFilename)){
        if(DEBUG){
            std::cerr << "Couldn't find table with table-name" << tableName << std::endl;
            std::cerr << "Can't create Index with non-existing table\n";
        }
        return -1;
    }
    void *buffer = calloc(1,PAGE_SIZE);
    int recordSize = 0;
    prepareIndexRecord(id, tableName, attributeName, buffer, &recordSize);

    if(!fileExists(SYS_CATALOG_INDEX_FILENAME)){
        std::cerr << "[createIndex] Error: " << SYS_CATALOG_INDEX_FILENAME << " must to be created before createIndex()\n";
        return -1;
    }
    std::string indexFilename = tableName + "_" + attributeName + "_idx";
    if(fileExists(indexFilename)){
        std::cerr << "[createIndex] Error: Index " << attributeName << " has already been created\n";
        return -1;
    }
    std::vector<Attribute> recordDescriptor;
    buildIndexAttributesDescriptor(recordDescriptor);
    RID indexRID;
    recordFileManager->insertRecord(indexFileHandle,recordDescriptor, buffer, indexRID);
    //recordFileManager->printRecord(recordDescriptor,buffer);
    if(DEBUG){
        std::cout << "[creatIndex] Printing the record\n";
        recordFileManager->printRecord(recordDescriptor, buffer);
    }

    // need to scan the table to generate the key
    IXFileHandle ixFileHandle;

    if(indexManager->createFile(indexFilename)){
        if(DEBUG){
            std::cout << "[createIndex] Fail: Due to failure of creating file " << indexFilename << std::endl;
        }
        return -1;
    }
    FileHandle fileHandle;
    if(recordFileManager->openFile(tableFilename,fileHandle)){
        if(DEBUG){
            std::cout << "[createIndex] something went wrong when opening " << tableFilename << std::endl;
        }
        return -1;
    }

    if(fileHandle.appendPageCounter == 0){
        if(DEBUG){
            std::cout << "[createIndex] " << tableFilename << " empty, no need to scan\n";
        }
        return 0;
    }
    if(indexManager->openFile(indexFilename, ixFileHandle)){
        if(DEBUG){
            std::cout << "[createIndex] Fail: opening " << indexFilename << " fail! \n";
        }
        return -1;
    }

    if(!fileExists(tableFilename)){
        if(DEBUG){
            std::cout << "[createIndex] Fail: " << tableFilename << " does not exist at this point somehow\n";
        }
        return 0;
    }
    std::vector<Attribute> tableAttrs;
    getAttributes(tableName, tableAttrs);
    Attribute indexAttribute;
    for(auto attr: tableAttrs){
        if(attr.name == attributeName){
            indexAttribute = attr;
        }

    }
    
    //table Attribute names contain the attributeName
    std::vector<std::string> tableAttributeNames;
    tableAttributeNames.push_back(attributeName);
    
    RM_ScanIterator rm_iter;
    RBFM_ScanIterator rbfm_iter;
    if(recordFileManager->scan(fileHandle, tableAttrs,"",NO_OP,nullptr,tableAttributeNames,rbfm_iter)){
        if(DEBUG){
            std::cerr << "[createIndex] scan setup failed somehow\n";
        }
        return -1;
    }
    rm_iter.setScanIterator(rbfm_iter);
    rm_iter.setRecordDescriptor(tableAttrs);
    uint16_t null_size = std::ceil((float)tableAttributeNames.size()/BYTE_SIZE);
    // both indexRID and buffer can be reused at this point
    uint8_t data[PAGE_SIZE];
    while(rbfm_iter.getNextRecord(indexRID,buffer) == 0){
        rm_iter.prepareData(buffer,data);
        indexManager->insertEntry(ixFileHandle, indexAttribute, (char*)data+null_size, indexRID);
        //RecordBasedFileManager::instance().printRecord()
        //indexManager->insertEntry(ixFileHandle,)
        
    }
    if(DEBUG){
        std::cout << "Printing B tree of " << indexFilename << std::endl;
        indexManager->printBtree(ixFileHandle, indexAttribute);

    }
    indexManager->closeFile(ixFileHandle);
    
    if(DEBUG){
        std::cout << "[createIndex] Finished create the index B+ tree based on record file\n";
    }



    return 0;
}


RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    std::string tableFilename;
    int id;
    if(getTableIdAndFilename(tableName, id,tableFilename)){
        if(DEBUG){
            std::cout << "[destoryIndex] Fail: couldn't find corresponding table in the system catalog\n";
        }
        return -1;
    }

    std::string indexFilename = tableFilename + "_" + attributeName + "_idx";
    std::vector<Attribute> indexRecordDescriptor;
    buildIndexAttributesDescriptor(indexRecordDescriptor);
    std::vector<std::string> indexAttributeNames;
    indexAttributeNames.push_back("index_filename");
    RBFM_ScanIterator indexCatalogScanner;
    void *CompIndex = calloc(1,sizeof(int)+ indexFilename.length()+1);
    int nameLen = indexFilename.length();
    memcpy(CompIndex,&nameLen, sizeof(int));
    memcpy((char*)CompIndex+sizeof(int),indexFilename.c_str(),nameLen);
    if(recordFileManager->scan(this->indexFileHandle,indexRecordDescriptor,"index-filename",EQ_OP,CompIndex,indexAttributeNames, indexCatalogScanner)){
        if(DEBUG){
            std::cout << "[destroyIndex] Fail on setting up the scan\n";
        }
        return -1;
    }
    RID rid;
    uint8_t dataArray[PAGE_SIZE];
    void *data = &dataArray[0];
    while(indexCatalogScanner.getNextRecord(rid,data) == 0){
        recordFileManager->deleteRecord(this->indexFileHandle,indexRecordDescriptor, rid);
    }
    if(DEBUG){
        std::cout << "[destoryIndex] Finish deleting with all the corresponding table-id records within indexCatalog\n";
    }
    if(recordFileManager->destroyFile(indexFilename)){
        std::cout << "[destroyIndex] " << indexFilename << " destroy file fail\n";
    }
    return 0;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    const std::string attributeFilename = tableName + "_" + attributeName + "_idx";
    std::vector<Attribute> attrs;
    Attribute scanAttribute;
    this->getAttributes(tableName, attrs);
    for(auto attr: attrs){
        if(attr.name == attributeName){
            scanAttribute = attr;
        }
    }
    
    IX_ScanIterator ixScaniterator;
    IXFileHandle *ixFileHandle = new IXFileHandle();
    this->indexManager->openFile(attributeFilename, *ixFileHandle);
    if(this->indexManager->scan(*ixFileHandle, scanAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ixScaniterator)){
        if(DEBUG){
            std::cout << "[indexScan] Fail: setting up the scan fail\n";
        }

    }
    rm_IndexScanIterator.setIXScanIterator(ixScaniterator);
    rm_IndexScanIterator.ixFileHandle = ixFileHandle;
    //rm_IndexScanIterator.setRecordDescriptor(descriptor);
    return 0;
}

void RM_IndexScanIterator::setIXScanIterator(IX_ScanIterator ixScanItertor){
    this->ixScanIterator = ixScanItertor;
}
void RM_IndexScanIterator::setAttribute(Attribute attribute){
    this->attribute = attribute;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
    //this->ixScanIterator.getNextEntry(rid,key);
    return this->ixScanIterator.getNextEntry(rid,key);
}

RC RM_IndexScanIterator::close(){
    this->ixScanIterator.close();
    delete(this->ixFileHandle);
    return 0;
}


