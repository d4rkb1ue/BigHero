#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

#define RM_EOF (-1) // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
  public:
    RM_ScanIterator(){};
    ~RM_ScanIterator(){};

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
    RC close() { return -1; };
};

// Relation Manager
class RelationManager
{
  public:
    const string PREFIX = ".tbl";
    const string TABLES_TBL = "Tables";
    const int TABLES_ID = 0;
    const string COLUMNS_TBL = "Columns";
    const int COLUMNS_ID = 1;
    const vector<Attribute> TABLES_ATTRS = {
        {"table-id", TypeInt, 4},
        {"table-name", TypeVarChar, 50},
        {"file-name", TypeVarChar, 50}};
    const vector<Attribute> COLUMNS_ATTRS = {
        {"table-id", TypeInt, 4},
        {"column-name", TypeVarChar, 50},
        {"column-type", TypeInt, 4},
        {"column-length", TypeInt, 4},
        {"column-position", TypeInt, 4}};

    static RelationManager *instance();

    RC createCatalog();
    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs, int tableId = -1);
    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);
    RC deleteTuple(const string &tableName, const RID &rid);
    RC updateTuple(const string &tableName, const void *data, const RID &rid);
    RC readTuple(const string &tableName, const RID &rid, void *data);
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,
            const void *value,
            const vector<string> &attributeNames,
            RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

  protected:
    RecordBasedFileManager *rbfm;
    char buffer[PAGE_SIZE];

    RelationManager();
    ~RelationManager();

    void cpyAndInc(char des[], unsigned &offset, const void *src, unsigned len = 4);
    void prepareTableRecordInBuf(const unsigned tableId, const string tableName);
    void prepareColumnRecordInBuf(const unsigned tableId, const string name, const AttrType type, const unsigned len, const unsigned pos);
};

#endif
