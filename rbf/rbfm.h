#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <iomanip>
#include <bitset>

#include "helpers.h"
#include "pfm.h"
// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
    void setAttribute(std::string, AttrType,AttrLength);
    friend std::ostream& operator<<(std::ostream &output, const Attribute &attr){
        output << "attr.name = " << attr.name << ", attr.type = " << attr.type
            << ", attr.length = " << attr.length; 
        return output;
    }
};




/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
    /* My Own Class members START*/ 

    FileHandle *fileHandle;
    const void* param;
    CompOp op;
    std::string conditionAttribute;
    std::vector<Attribute> rd;
    std::vector<std::string> attributeNames;
    AttrType attrType;
    //void* pageData;

    // In theory, this can make Scan read only once per-page
    //void* pageData; // a page of Data for reducing the read count
    //PageInfo *currPageInfo;
    /* My Own Class members END  */ 

    RBFM_ScanIterator();

    ~RBFM_ScanIterator();

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    RC close(){return -1;};


    void CopyRequiredAttributes();
    // compare function should return 0 if condition match, 1 if not match
    inline void setcurrRID(PageNum pageNum, unsigned slotNum) {
        currRID.pageNum = pageNum;
        currRID.slotNum = slotNum;
    };
    void incrementRID(RID *rid, PageInfo currPageInfo){
    };
    int reachEOF(PageInfo &currPageInfo){
        if(this->fileHandle->appendPageCounter == 0){
            std::cerr << "[reachEOF] Warning: The appendPageCounter == 0, file either not opened properly or scanning an empty file.\n";
            return 1;
        }
        if(currRID.pageNum > this->fileHandle->appendPageCounter-1 or
                (currRID.pageNum == this->fileHandle->appendPageCounter-1 
                and currRID.slotNum > currPageInfo.numRecords)){
            return 1;
            // this means reach the end of the file
        }
        return 0;
    };
    RID getcurrRID(){
        return currRID;
    }
private:
    RID currRID;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

public:
    /*  My Public Members for the rbfm class START */
    uint16_t calcRecSize_and_prepareRec(const std::vector<Attribute> &rb, const void *data, void* preparedData);
    uint16_t calcRecSize_and_reformRec(const std::vector<Attribute> &rb, const void *data, void *preparedData);
    RecordInfo getRecordInfo(unsigned slotNum, const void* data);
    void insertRecordInfo(unsigned slotNum, void* pageData, RecordInfo *recInfo);
    int moveRecordsByOffset(uint16_t totalNumRecords, RecordInfo &recordBase, int16_t offset, void* pageData);
    /*  My Public Members for the rbfm class END   */


protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    static RecordBasedFileManager *_rbf_manager;
};

#endif
