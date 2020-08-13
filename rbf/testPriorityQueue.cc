#include <iostream>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <cstdio>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int RBFTest_queue(RecordBasedFileManager &rbfm) {

    cout << endl << "***** In RBF Test Case Priority Queue *****" << endl;

    RC rc;
    string fileName = "testQueue";

    // TODO : Create test for the priority queue

    // Create a file named "test11"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "testQueue"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Close the file "testQueue"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

}

int main() {
    // To test the functionality of the record-based file manager
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    remove("testQueue");
    remove("test11rids");

    return RBFTest_queue(rbfm);
}

