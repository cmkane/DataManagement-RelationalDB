#ifndef _helpers_h_
#define _helpers_h_

#define PAGE_SIZE 4096

#define MAX_pfInfo_PER_PAGE 682 // PAGE_SIZE / sizeof(PageFreeInfo) = 682 mod 4
#define RecordDeleted UINT16_MAX
#define RecordMoved UINT16_MAX-1
#define MAX_RECORD_SIZE 4088 // PAGE_SIZE - sizeof(PageInfo) - sizeof(recordInfo)
#define MIN_RECORD_SIZE 8
#define SYSTEM true
#define USER false

#include <iostream>
#include <vector>
#include <algorithm>
//#include <bits/stdc++.h>
// Helper function for find a free page to insert should go into PagedFileManager

// The Hidden page has this structure
// | readPageCounter | writePageCounter | appendPageCounter | queueSize | queuePageNum | tableIdCounter

typedef struct __attribute__((__packed__)){
    unsigned pageNum;
    uint16_t freeSpace;
}PageFreeInfo;

struct PageInfo {
    uint16_t numRecords;
    uint16_t freeSpace;
    inline PageInfo& operator=(PageInfo const &pageInfo){
        this->numRecords = pageInfo.numRecords;
        this->freeSpace = pageInfo.freeSpace;
        return *this;
    };
    friend std::ostream& operator<< (std::ostream &out, PageInfo &pageInfo){
        out << "pageInfo.numRecords = " << pageInfo.numRecords << ", pageInfo.freeSpace = " << pageInfo.freeSpace << std::endl;
        return out;
    }
};

typedef struct __attribute__ ((__packed__)) {
    uint16_t offset;
    uint16_t length;
}RecordInfo;

typedef struct __attribute__ ((__packed__)) {
    uint16_t offset;
    uint16_t length;
    uint16_t ridCounts;
}IndexKeyInfo;



// Class used for the priority queue
class freeSpaceComparator{
    public:
        int operator()(const PageFreeInfo &p1, const PageFreeInfo &p2){
            return p1.freeSpace > p2.freeSpace; // for Min_heap, minimal size stay at the top
        }
};
//class Record{
//    public:
//        //Record(uint16_t numFields, std::vector<Attribute> &rd): 
//        //    numFields{numFields}{  };
//        ~Record() = default;
//        inline uint16_t getSize() {return size;};
//
//    private:
//
//        uint16_t numFields;
//        uint16_t nullIndicatorSize;
//        uint16_t size; // equivalent to offset
//};

//void display_binary(char byte){
//    std::bitset<8> b(byte);
//    std::cout<< "The bits in the byte is: " << b << '\n';
//}
#endif
