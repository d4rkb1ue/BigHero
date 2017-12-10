#include "rm.h"

RM_ScanIterator::RM_ScanIterator()
    : it(nullptr)
{
}

RM_ScanIterator::~RM_ScanIterator()
{
    if (it)
    {
        delete it;
    }
}

RM_ScanIterator::RM_ScanIterator(
    FileHandle *fileHandle,
    const vector<Attribute> recordDescriptor,
    const string conditionAttribute,
    const CompOp compOp,
    const char *value,
    const vector<string> attributeNames)
    : it(nullptr)
{
    it = new RBFM_ScanIterator(
        fileHandle,
        recordDescriptor,
        conditionAttribute,
        compOp,
        value,
        attributeNames
    );
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return it->getNextRecord(rid, data);
}

RC RM_ScanIterator::close()
{
    return it->close();
}

RelationManager *RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    // can't use this! since catalog haven't create!
    // createTable(TABLES_TBL, TABLES_ATTRS, TABLES_ID);
    // createTable(COLUMNS_TBL, COLUMNS_ATTRS, COLUMNS_ID);

    string tableFileName = TABLES_TBL + PREFIX,
           columnsFileName = COLUMNS_TBL + PREFIX;

    // create files
    if (rbfm->createFile(tableFileName) != 0)
    {
        cerr << "create " << tableFileName << "failed" << endl;
        return -1;
    }
    if (rbfm->createFile(columnsFileName) != 0)
    {
        cerr << "create " << columnsFileName << "failed" << endl;
        return -1;
    }

    // open & insert to Tables.tbl
    RID rid = {0, 0};
    FileHandle fileHandle;
    rbfm->openFile(tableFileName, fileHandle);

    prepareTableRecordInBuf(TABLES_ID, TABLES_TBL);
    // rbfm->printRecord(TABLES_ATTRS, buffer);
    rbfm->insertRecord(fileHandle, TABLES_ATTRS, buffer, rid);

    prepareTableRecordInBuf(COLUMNS_ID, COLUMNS_TBL);
    // rbfm->printRecord(TABLES_ATTRS, buffer);
    rbfm->insertRecord(fileHandle, TABLES_ATTRS, buffer, rid);

    rbfm->closeFile(fileHandle);

    // open & insert to Columns.tbl
    FileHandle fileHandle2;
    rbfm->openFile(columnsFileName, fileHandle2);

    for (unsigned i = 0; i < TABLES_ATTRS.size(); i++)
    {
        prepareColumnRecordInBuf(TABLES_ID, TABLES_ATTRS[i].name, TABLES_ATTRS[i].type, TABLES_ATTRS[i].length, i);
        // rbfm->printRecord(COLUMNS_ATTRS, buffer);
        rbfm->insertRecord(fileHandle2, COLUMNS_ATTRS, buffer, rid);
    }

    for (unsigned i = 0; i < COLUMNS_ATTRS.size(); i++)
    {
        prepareColumnRecordInBuf(COLUMNS_ID, COLUMNS_ATTRS[i].name, COLUMNS_ATTRS[i].type, COLUMNS_ATTRS[i].length, i);
        // rbfm->printRecord(COLUMNS_ATTRS, buffer);
        rbfm->insertRecord(fileHandle2, COLUMNS_ATTRS, buffer, rid);
    }
    rbfm->closeFile(fileHandle2);
    return 0;
}

RC RelationManager::deleteCatalog()
{
    rbfm->destroyFile(TABLES_TBL + PREFIX);
    rbfm->destroyFile(COLUMNS_TBL + PREFIX);
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs, int tableId)
{
    RID rid;

    // no default table ID given, get tableId
    if (tableId < 0)
    {
        int maxTableId = -1;

        FileHandle fileHandle2;
        RBFM_ScanIterator it;
        vector<string> allAttrs;
        for (unsigned i = 0; i < TABLES_ATTRS.size(); i++)
        {
            allAttrs.push_back(TABLES_ATTRS[i].name);
        }

        rbfm->openFile(TABLES_TBL + PREFIX, fileHandle2);
        rbfm->scan(fileHandle2, TABLES_ATTRS, "", NO_OP, nullptr, allAttrs, it);

        // cerr << "----------scanning table.tbl----------" << endl;
        while (it.getNextRecord(rid, buffer) != RM_EOF)
        {
            rbfm->readAttribute(fileHandle2, TABLES_ATTRS, rid, "table-id", &maxTableId);
            tableId = tableId > maxTableId ? tableId : maxTableId;
            // rbfm->printRecord(TABLES_ATTRS, buffer);
            // cerr << "maxTableId=" << maxTableId << endl;
        }

        tableId += 1;
        // cerr << "new TableId=" << tableId << endl;
    }

    if (rbfm->createFile(tableName + PREFIX) != 0)
    {
        cerr << "create file " << tableName << PREFIX << "failed." << endl;
        exit(-1);
    }

    // insert to Tables.tbl
    FileHandle fileHandle;
    rbfm->openFile(TABLES_TBL + PREFIX, fileHandle);
    prepareTableRecordInBuf(tableId, tableName);
    // rbfm->printRecord(TABLES_ATTRS, buffer);
    rbfm->insertRecord(fileHandle, TABLES_ATTRS, buffer, rid);

    rbfm->closeFile(fileHandle);

    // insert to Columns.tbl
    FileHandle fileHandle3;
    rbfm->openFile(COLUMNS_TBL + PREFIX, fileHandle3);

    for (unsigned i = 0; i < attrs.size(); i++)
    {
        prepareColumnRecordInBuf(tableId, attrs[i].name, attrs[i].type, attrs[i].length, i);
        // rbfm->printRecord(COLUMNS_ATTRS, buffer);
        rbfm->insertRecord(fileHandle3, COLUMNS_ATTRS, buffer, rid);
    }
    rbfm->closeFile(fileHandle3);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    Utils::assertExit("don't support deleteTable!");
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // get tableId
    int tableId = 0;
    RID rid = {0, 0};
    vector<string> allTableAttrs;
    for (unsigned i = 0; i < TABLES_ATTRS.size(); i++)
    {
        allTableAttrs.push_back(TABLES_ATTRS[i].name);
    }

    // make standard VarChar Data
    Utils::makeStandardString(tableName, buffer);

    // scan Tables.tbl
    FileHandle tableFH;
    RBFM_ScanIterator tableIt;
    Utils::assertExit("opentable filed", rbfm->openFile(TABLES_TBL + PREFIX, tableFH));

    rbfm->scan(tableFH, TABLES_ATTRS, "table-name", EQ_OP, buffer, allTableAttrs, tableIt);

    if (tableIt.getNextRecord(rid, buffer) == RBFM_EOF)
    {
        cerr << "tableName not found" << endl;
        return -1;
    }
    rbfm->readAttribute(tableFH, TABLES_ATTRS, rid, "table-id", &tableId);
    rbfm->closeFile(tableFH);
    
    // scan Columns.tbl
    vector<string> allColAttrs;
    for (unsigned i = 0; i < COLUMNS_ATTRS.size(); i++)
    {
        allColAttrs.push_back(COLUMNS_ATTRS[i].name);
    }

    FileHandle colFH;
    RBFM_ScanIterator colIt;
    Utils::assertExit("open columns.tbl failed", rbfm->openFile(COLUMNS_TBL + PREFIX, colFH));

    rbfm->scan(colFH, COLUMNS_ATTRS, "table-id", EQ_OP, &tableId, allColAttrs, colIt);

    attrs.clear();
    string name;
    AttrType type = TypeInt;
    int len = 0;
    int pos = 0;
    while (colIt.getNextRecord(rid, buffer) != RBFM_EOF)
    {
        readColumnRecordInBuf(tableId, name, type, len, pos);
        // rbfm->printRecord(COLUMNS_ATTRS, buffer);
        // cerr << name << ", " << type << ", " << len << ", " << pos << endl;
        Utils::assertExit("column position unordered.", static_cast<unsigned>(pos) != attrs.size());
        attrs.push_back({name, type, static_cast<unsigned>(len)});
    }

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        cerr << "get Attribute at " << tableName << "failed" << endl;
        return -1;
    }
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + PREFIX, fileHandle) != 0)
    {
        cerr << "can't open .tbl" + tableName << endl;
        return -1;
    }
    return rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        cerr << "get Attribute at " << tableName << "failed" << endl;
        return -1;
    }
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + PREFIX, fileHandle) != 0)
    {
        cerr << "can't open .tbl" + tableName << endl;
        return -1;
    }
    return rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        cerr << "get Attribute at " << tableName << "failed" << endl;
        return -1;
    }
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + PREFIX, fileHandle) != 0)
    {
        cerr << "can't open .tbl" + tableName << endl;
        return -1;
    }

    // make null indicator
    bool ni[recordDescriptor.size()];
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        ni[i] = recordDescriptor[i].name == attributeName ? false : true;
        // cerr << (ni[i] ? "1" : "0");
    }
    cerr << endl;

    unsigned nullIndicatorSize = Utils::makeNullIndicator(ni, recordDescriptor.size(), data);

    // rbfm return without null indicators
    return rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, static_cast<char *>(data) + nullIndicatorSize);
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        cerr << "get Attribute at " << tableName << "failed" << endl;
        return -1;
    }
    FileHandle *fileHandle = new FileHandle();
    if (rbfm->openFile(tableName + PREFIX, *fileHandle) != 0)
    {
        cerr << "can't open .tbl" + tableName << endl;
        return -1;
    }
    
    RM_ScanIterator *it = new RM_ScanIterator(
        fileHandle,
        recordDescriptor,
        conditionAttribute,
        compOp,
        static_cast<const char*>(value),
        attributeNames
    );

    rm_ScanIterator = *it;
    return 0;
}

void RelationManager::cpyAndInc(char des[], unsigned &offset, const void *src, unsigned len)
{
    memcpy(des + offset, src, len);
    offset += len;
}

void RelationManager::readAndInc(void *des, unsigned &offset, const void *src, unsigned len)
{
    const char *_src = static_cast<const char *>(src);
    memcpy(des, _src + offset, len);
    offset += len;
}

void RelationManager::prepareTableRecordInBuf(const unsigned tableId, const string tableName)
{
    memset(buffer, 0, PAGE_SIZE);
    // jump nullindicator size = 1, all not NULL
    unsigned offset = 1;
    unsigned strSize = 0;
    string fileName = tableName + PREFIX;

    cpyAndInc(buffer, offset, &tableId);
    strSize = tableName.length();
    cpyAndInc(buffer, offset, &strSize);
    cpyAndInc(buffer, offset, tableName.c_str(), tableName.length());
    strSize = fileName.length();
    cpyAndInc(buffer, offset, &strSize);
    cpyAndInc(buffer, offset, fileName.c_str(), fileName.length());
}

void RelationManager::prepareColumnRecordInBuf(const unsigned tableId, const string name, const AttrType type, const unsigned len, const unsigned pos)
{
    memset(buffer, 0, PAGE_SIZE);
    unsigned offset = 1;
    unsigned strSize = 0;
    int uType = type;

    cpyAndInc(buffer, offset, &tableId);
    strSize = name.length();
    cpyAndInc(buffer, offset, &strSize);
    cpyAndInc(buffer, offset, name.c_str(), name.length());
    cpyAndInc(buffer, offset, &uType);
    cpyAndInc(buffer, offset, &len);
    cpyAndInc(buffer, offset, &pos);
}

void RelationManager::readColumnRecordInBuf(int &tableId, string &name, AttrType &type, int &len, int &pos)
{
    unsigned offset = 1;
    unsigned strSize = 0;

    readAndInc(&tableId, offset, buffer);
    memcpy(&strSize, buffer + offset, 4);
    offset += 4;
    char nameBuf[strSize + 1];
    readAndInc(nameBuf, offset, buffer, strSize);
    nameBuf[strSize] = '\0';
    name = string(nameBuf);
    readAndInc(&type, offset, buffer);
    readAndInc(&len, offset, buffer);
    readAndInc(&pos, offset, buffer);
}