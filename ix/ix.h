#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan


class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    /* ************ METHODS START ************* */

    /* ************ METHODS  END  ************* */
    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    RC insertIndexEntry(IXFileHandle &ixFileHandle, int keySize, PageNum pointer, const Attribute &attribute, const void *entry, void* &newChildEntry, PageNum &newChildPageNum, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    RC deleteIndexEntry(IXFileHandle & ixFileHandle, const int keySize, PageNum pointer, const void* entry, void* &oldChildEntry, const Attribute & attribute, const RID & rid);

    RC removeIndex(IXFileHandle &ixFileHandle, void* pageData, int recordIndex, PageInfo &pageInfo, const Attribute &attribute, int keySize, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class IX_ScanIterator {
public:

    IXFileHandle *fileHandle;
    Attribute attribute;
    int lowInt, highInt, prevInt;
    float lowReal, highReal, prevFloat;
    std::string lowStr, highStr, prevStr;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    bool lowNull;
    bool highNull;
    unsigned int ridNum;
    bool noMore;
    int prevNumRecords;
    RID prevRID;

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);
    RC getFirstLeafPage(PageNum &pageNum);
    RC checkDeletion(const void* pageData);

    // check if next is null in the current page
    bool reachLastLeaf(const void *pageData);

    inline void setcurrRID(PageNum pageNum, unsigned slotNum) {
        currRID.pageNum = pageNum;
        currRID.slotNum = slotNum;
    };

    // Terminate index scan
    RC close();

private:
    RID currRID;
};

// The index file structure would look like the following:
// | counter | counter | counter | queueSize(unused) | pg_PageNum(unused) | rootPageNum | tbl_id(unused) | currLeafPageNum |....
class IXFileHandle{
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    PageNum ixRootPageNum;
    PageNum ixCurrLeafPageNum;

    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    RC getKey(const void *pageData, const Attribute& attribute, const RecordInfo recInfo, void* key, int &keySize); // use to grab the key from pageData shoudl perform like readAttribute
    void getCounters();
    int isLeafPage(const void* pageData); // Check if a page is a Leaf or Non-Leaf Page
    int isLeafPage(int pageNum);
    PageInfo getIndexPageInfo(const void* pageData); // Takes Non-Leaf/Leaf flag into account when getting pageInfo
    RecordInfo getIndexRecordInfo(unsigned slotNum, const void* pageData);
    void insertIndexRecordInfo(unsigned slotNum, void* pageData, RecordInfo *recInfo);
    void insertIndexPageInfo(void* pageData,PageInfo *pageInfo);
    //RC insertPairIntoNonLeaf();

    // For creating a new Leaf or Non-Leaf page
    RC appendLeafPage(PageNum &pageNum);
    RC appendNonLeafPage(PageNum &pageNum);
    RC appendNonLeafPage(PageNum &pageNum, const int keySize, const void* key, const PageNum left, const PageNum right);

    // For inserting a new pair <key, pointer/RID> into a page. Assume there is ample free space and number of
    // pairs does not exceed or equal the 2d limit.
    RC insertPairIntoLeafOrNonLeaf(PageNum pageNum, const Attribute &attribute, const void* entry, int size, const void *data);
    RC insertPair(void *pageData,PageNum pageNum, const Attribute &attribute, const void* entry, int size, const void *data);

    // search for place to enter new <key, pointer/RID> pair
    // returns recordInfo # of first record that needs to be shifted to the right
    int searchEntryInsertPoint(const void* pageData, PageInfo &pageInfo, const Attribute &attribute, const void* entry);

    int moveRecordsByOffset(uint16_t totalNumRecords, int &recordBase, int16_t offset, void* pageData);
    PageNum appendNewRoot(int keySize, const void* newChildEntry, PageNum oldChildPageNum, PageNum newChildPageNum);

};

#endif
