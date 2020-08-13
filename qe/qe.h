#ifndef _qe_h_
#define _qe_h_

#include <map>
#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan


class Record{
    public:
        Record(void* data, uint16_t size):size(size){
            this->data = malloc(size);
            memcpy(this->data,data,size);
        };
        ~Record(){
            free(data);
        }
        uint16_t getSize(){return size;};
        void *data;
    private:
        uint16_t size;
};
class RecordAttribute{
    public:
        RecordAttribute(void* data, uint16_t size):size(size){
            this->data = malloc(size);
            memcpy(this->data,data,size);
        };
        RecordAttribute(Attribute attr, void* data, uint16_t size):attr(attr),size(size){
            this->data = malloc(size);
            memcpy(this->data,data,size);
        };
        ~RecordAttribute(){
            free(data);
        }
        uint16_t getSize(){return size;};
        void *data;
        Attribute attr;
    private:
        uint16_t size;
        // do I need an offset for this?
};

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;
};


/* My Own Helper functions STARTS*/
bool leftMatchRight(const void* leftData, uint16_t leftAttrsize, const void* rightData, const std::vector<Attribute> &rightRd, const Condition &cond);
bool leftMatchRight(RecordAttribute &leftAttr, const void* rightData, const std::vector<Attribute> &rightRd, const Condition &cond);
uint16_t calcRecSize(const std::vector<Attribute> &attrs, const void* data);
uint16_t getAttrOffsetAndSize(const std::vector<Attribute> &attrs, const void* data, int &keySize, Attribute *attribute, const std::string &attributeName);
void concatRightAfterLeft(const void *leftData, const std::vector<Attribute> &leftRd, const void *rightData, const std::vector<Attribute> &rightRd, void* data, Iterator *iterator);
/* My Own Helper functions ENDS */


class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName, rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;


        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
    };
};

class Filter : public Iterator {
    // Filter operator
public:
    Iterator *iterator;
    Condition condition;

    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );

    ~Filter() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        iterator->getAttributes(attrs);
    };
};

class Project : public Iterator {
    // Projection operator
public:
    std::vector<std::string> attrs;
    std::vector<Attribute> projAttrs;
    Iterator *filter;

    Project(Iterator *input,                    // Iterator of input R
            const std::vector<std::string> &attrNames) { // std::vector containing attribute names
                filter = input;
                attrs = attrNames;
                convertStringAttr();
            };
    ~Project() override = default;

    RC getNextTuple(void *data) override;

    void convertStringAttr(); // Converts the String vector of attributes to Attributes

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        attrs = projAttrs;
    };

    void getFilterAttributes(std::vector<Attribute> &attrs) {
        filter->getAttributes(attrs);
    };
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
public:
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
           ): condition(condition), numPages(numPages),leftIn(leftIn), rightIn(rightIn),first_iter(true), leftEOFreached(false){
            if(numPages <= 2){
                std::cerr << "[BLNJoin Constructor] Invalid Constructor\n";
            }
            rightData = malloc(PAGE_SIZE);
    };

    ~BNLJoin() override{
        if(!first_iter){
            for(auto tuple_iter: dataBlock){
                delete(tuple_iter.first);
                delete(tuple_iter.second);
            }
            dataBlock.clear();
        }
        free(rightData);
    };

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    // read size of B blocks into the memory
    void populateBlock();
    void freeCurrentBlock();
private:
    std::map<RecordAttribute*,Record*> dataBlock;
    const Condition condition;
    const unsigned numPages;
    Iterator *leftIn;
    TableScan *rightIn;
    bool first_iter;
    bool leftEOFreached;
    bool dataBlockExhausted;
    void *rightData;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
public:
    Iterator *left;
    IndexScan *right;
    Condition condition;
    std::vector<Attribute> joinAttrs;

    bool getNext;
    void *leftData;
    uint16_t leftOffset;
    int leftSize;

    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    ) {
        left = leftIn;
        right = rightIn;
        this->condition = condition;
        this->getNext = true;
        createAttributes();
        leftData = malloc(PAGE_SIZE);
    };

    ~INLJoin() override {
        free(leftData);
    };

    RC getNextTuple(void *data) override;

    void createAttributes();

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        attrs = joinAttrs;
    };
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );

    ~GHJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
    void createLeftPartition();
    void createRightPartition();
private:
    Iterator *leftIn;
    Iterator *rightIn;
    const Condition *condition;
    const unsigned numPartitions;
    std::vector<std::string> leftPartitions;
    std::vector<std::string> rightPartitions;
    bool first_iter;
};

class Aggregate : public Iterator {
    // Aggregation operator
public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
            );

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    ) {};

    ~Aggregate() override;

    RC getNextTuple(void *data) override;

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override {
        Attribute attr;
        attr.type = aggAttr.type;
        attr.length = aggAttr.length;
        std::string op_name;
        switch(op){
            case AVG:
                op_name = "AVG";
                break;
            case MIN:
                op_name = "MIN";
                break;
            case MAX:
                op_name = "MAX";
                break;
            case SUM:
                op_name = "SUM";
                break;
            case COUNT:
                op_name = "COUNT";
                break;
        }
        attr.name = op_name + "(" + aggAttr.name+ ")";
    }
private:
    Iterator *input;
    Attribute aggAttr;
    Attribute *groupAttr;
    AggregateOp op;
    bool basicAggr;
    bool finished;
    float result;
};

#endif
