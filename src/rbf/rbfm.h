#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <cstring>

#include "../rbf/pfm.h"

using namespace std;

typedef struct
{
    unsigned pageNum;
    unsigned slotNum;
} RID;

typedef enum { TypeInt = 0,
               TypeReal,
               TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute
{
    string name;
    AttrType type;
    AttrLength length;
};

typedef enum { EQ_OP = 0, // =
               LT_OP,     // <
               LE_OP,     // <=
               GT_OP,     // >
               GE_OP,     // >=
               NE_OP,     // !=
               NO_OP      // no condition
} CompOp;

#define RBFM_EOF (-1) // end of a scan operator

class RBFM_ScanIterator
{
  public:
    RBFM_ScanIterator(){};
    ~RBFM_ScanIterator(){};
    RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };
    RC close() { return -1; };
};

class Record
{
  public:
    static unsigned getRecordSize(const vector<Attribute> &recordDescriptor, const void *rawData);
    // return null indicator original size in record
    static unsigned parseNullIndicator(bool nullIndicators[], const vector<Attribute> &recordDescriptor, const void *rawData);
    const static string RECORD_HEAD;
    const static unsigned REC_HEADER_SIZE;

    // 0: not a ptr
    // 1: is a ptr
    // 2: is deleted
    // -1: unset! which can never happen if correct
    int ptrFlag;

    // check whether rid is original rid in upper level
    RID rid;
    char *data;

    Record(vector<Attribute> recordDescriptor, char *rawData);
    ~Record();

    // if deleted, size will be REC_HEADER_SIZE (data = NULL, ptrFlag = 2, rid = rid)
    // if pointered(moved), size will be REC_HEADER_SIZE (data = NULL, ptrFlg = 1, rid = new rid)
    unsigned sizeWithoutHeader(const vector<Attribute> &recordDescriptor);
    unsigned sizeWithHeader(const vector<Attribute> &recordDescriptor);
    
    string toString(const vector<Attribute> &recordDescriptor);
};

// DataPage: [Size][RecordNum][Records Data]...
// Record: ["Rec:"][ptrFlag][RID][Raw Data]
class DataPage
{
  public:
    const static unsigned DATA_PAGE_HEADER_SIZE;
    vector<Record *> records;
    vector<Attribute> recordDescriptor;

    // this is a DUP data, same as SUM(records.forEach.size())
    unsigned size;
    
    DataPage(vector<Attribute> recordDescriptor);
    DataPage(vector<Attribute> recordDescriptor, char *data);
    ~DataPage();

    unsigned getAvailableSize();
    void appendRecord(Record *record);
    void insertRecord(Record *record);
    void deleteRecord(unsigned slotNum);
    void getRawData(char *data);
};

class RecordBasedFileManager
{
  public:
    static RecordBasedFileManager *instance();

    RC createFile(const string &fileName);
    RC destroyFile(const string &fileName);
    RC openFile(const string &fileName, FileHandle &fileHandle);
    RC closeFile(FileHandle &fileHandle);

    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
    RC addPageAndInsert(FileHandle &fileHandle, vector<Attribute> &recordDescriptor, char *data, RID &rid);
    RC findPageAndInsert(FileHandle &fileHandle, vector<Attribute> &recordDescriptor, char *data, RID &rid);

    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

  protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

  private:
    static RecordBasedFileManager *_rbf_manager;
    PagedFileManager *pfm;
    char buffer[PAGE_SIZE];
};

#endif
