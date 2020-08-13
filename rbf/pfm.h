#ifndef _pfm_h_
#define _pfm_h_
typedef unsigned PageNum;
typedef int RC;
typedef unsigned char byte;
// Hidden Page format 
//
// | counter | counter | counter | rootPageNum | currLeafPageNum | queueSize | pq_pageNum | tbl_id | ....

#define PAGE_SIZE 4096

#include <string>
#include <cstring>
#include <cmath>
#include <climits>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <queue>
#include <stack>
#include "helpers.h"
#include <memory>

class FileHandle;
bool fileExists(const std::string &fName);
// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


class PagedFileManager {
public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance


    /* My own class methods Starts */

    uint16_t getFreeSpace(FileHandle &fileHandle, PageNum pageNum);
    PageNum findPageWithFreeSpace(FileHandle &fileHandle, unsigned int size,PageFreeInfo &pfInfo);                        // Find a page with enough empty space
    bool removeFromPriorityQueue(FileHandle &fileHandle,PageNum pageNum);
    void setSysFlag(FileHandle &fileHandle,bool isSystem);
    bool isSystem(FileHandle &fileHandle);


    /* My own class methods Ends */

    RC createFile(const std::string &fileName);                         // Create a new file
    RC destroyFile(const std::string &fileName);                        // Destroy a file
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
    RC closeFile(FileHandle &fileHandle);                               // Close a file
    RC buildPriorityQueue(FileHandle &fileHandle);                      // Build the priority queue of free space

protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

private:
    static PagedFileManager *_pf_manager;
};

class FileHandle {
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;


    /* My own class members Start */
    // keep a copy of the queueSize and a copy of the queuePage first pageNum for quick access
    std::fstream *f;  // probably need a fstream to store the corresponding file stream?
    std::string fileName; // the fileName is used for appending at the end

    std::priority_queue<PageFreeInfo,std::vector<PageFreeInfo>,freeSpaceComparator> freePageQueue; // use this priority queue for now, probably need to implement a priority with fixed size?
    unsigned queueSize;
    PageNum queuePageNum;
    PageNum rootPageNum;
    PageNum currLeafPageNum;
    bool isSystem;

    /* My own class members End */

    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor
    
    /* My own class methods Start */

    /* Priority Queue Operation */
    int write_priority_queue(PageNum pageNum);
    int read_priority_queue();

    std::vector<unsigned> getHiddenPages();
    int compareIntAttribute(CompOp op,const int &left,const int &right);
    int compareRealAttribute(CompOp op, const float &left, const float &right);
    int compareVarcharAttribute(CompOp op, const std::string &left, const std::string &right);


    uint16_t getFreeSpace(PageNum pageNum); // return the size of freeSpace within a page
    PageInfo getPageInfo(const void* pageData); // copy the data into the pageInfo pointer
    void putPageInfo(PageInfo *pInfo, void* pageData);
    uint16_t calcOffset(uint16_t recordOffset, const void* pageData); // based on the pageInfo, calculate the offset for next record
    void updateCounters();
    //RC findPageWithFreeSpace(unsigned int size);                        // Find a page with enough empty space
    
    /* My own class methods End   */ 

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables
};

#endif
