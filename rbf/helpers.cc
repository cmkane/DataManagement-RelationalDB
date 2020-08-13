#include "helpers.h"
uint16_t calculateRecordSize(const std::vector<Attribute> &rd,const void *data){
    uint16_t size = 0;

    unsigned int null_size = std::ceil((float)rd.size()/8);
    size += null_size; // size of NULL indicator

    int attrCounter = 0;
    unsigned len = 0;
    std::cout << "null size is " << null_size << std::endl;
    auto *nullIndicator = (unsigned char *) malloc(null_size);
    memcpy(nullIndicator, data, null_size);
    for(auto attr: rd){
        unsigned int indexByte = floor((float)attrCounter/8);
        unsigned int shift = 7-(attrCounter%8);

        // Checking to see if the NULL bit is set, if set then raise the flag
        // or skipping directly depending on the implementation
        if(nullIndicator[indexByte] & ((unsigned) 1 << (unsigned)shift)){
            attrCounter++;
            continue;  
        }else{
            switch(attr.type) {
                case TypeVarChar:
                    memcpy(&len, (char*)data + size, sizeof(int));
                    size += sizeof(int);
                    if(DEBUG){
                        std::cout << "[calculateRecordSize] Length of the string is: " << len << std::endl;
                    }
                    size += len;
                    break;
                case TypeInt:
                    size += sizeof(int); // should be same as offset+= attr.length?
                    break;
                case TypeReal:
                    size += sizeof(float);
                    break;
                default:
                    // Should never be here
                    std::cerr << "[printRecord] Unsupported attribute type.\n";
                    return -1;
            }
        }
        attrCounter++;
    }
    free(nullIndicator);
    return size;
}
