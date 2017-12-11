#include "rbfm.h"

RBFM_ScanIterator::RBFM_ScanIterator()
    : fileHandle(nullptr),
      vcSize(0),
      compOp(NO_OP),
      value(nullptr),
      nextPn(0),
      nextSn(0),
      currPg(nullptr)
{
}

RBFM_ScanIterator::RBFM_ScanIterator(
    FileHandle *fileHandle,
    const vector<Attribute> recordDescriptor,
    const string conditionAttribute,
    const CompOp compOp,
    const char *value,
    const vector<string> attributeNames)
    : fileHandle(fileHandle),
      recordDescriptor(recordDescriptor),
      vcSize(0),
      compOp(compOp),
      value(nullptr),
      attributeNames(attributeNames),
      nextPn(0),
      nextSn(0),
      currPg(nullptr)
{
    if (compOp != NO_OP)
    {
        // find attribute type
        unsigned attr = 0;
        for (; attr < recordDescriptor.size(); attr++)
        {
            if (recordDescriptor[attr].name == conditionAttribute)
            {
                break;
            }
        }
        if ((attr == recordDescriptor.size()) ||
            (attr == 0 && recordDescriptor[0].name != conditionAttribute))
        {
            cerr << "Condition attribute not found." << endl;
            exit(-1);
        }
        this->conditionAttribute = recordDescriptor[attr];

        // copy value
        if (this->conditionAttribute.type != TypeVarChar)
        {
            this->value = new char[4];
            memcpy(this->value, value, 4);
        }
        else
        {
            unsigned size = Utils::getVCSizeWithHead(value);
            this->value = new char[size];
            memcpy(this->value, value, size);
        }
    }
    getNextPage();
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
    if (value)
    {
        delete[] value;
    }
    if (currPg)
    {
        delete currPg;
    }
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    // end
    if (!currPg)
    {
        return RBFM_EOF;
    }

    if (nextSn >= currPg->records.size())
    {
        getNextPage();
        return getNextRecord(rid, data);
    }

    Record *record = currPg->records[nextSn];

    if (compOp != NO_OP)
    {
        unsigned attrSz = record->getAttribute(recordDescriptor, conditionAttribute.name, buffer);
        // no data return, may be null
        if (attrSz == 0)
        {
            nextSn++;
            return getNextRecord(rid, data);
        }

        // compareRes = target - record value
        // compareRes < 0 => (record value > target)
        // compareRes > 0 => (record value < target)
        int compareRes = compareTo(buffer);
        switch (compOp)
        {
        case EQ_OP:
        {
            if (compareRes != 0)
            {
                nextSn++;
                // 递归写法有潜在的爆栈的可能
                return getNextRecord(rid, data);
            }
            break;
        }
        case LT_OP:
        {
            if (compareRes <= 0)
            {
                nextSn++;
                return getNextRecord(rid, data);
            }
            break;
        }
        case LE_OP:
        {
            if (compareRes < 0)
            {
                nextSn++;
                return getNextRecord(rid, data);
            }
            break;
        }
        case GT_OP:
        {
            if (compareRes >= 0)
            {
                nextSn++;
                return getNextRecord(rid, data);
            }
            break;
        }
        case GE_OP:
        {
            if (compareRes > 0)
            {
                nextSn++;
                return getNextRecord(rid, data);
            }
            break;
        }
        case NE_OP:
        {
            if (compareRes == 0)
            {
                nextSn++;
                return getNextRecord(rid, data);
            }
            break;
        }
        }
    }

    Utils::assertExit("next record should not be nullptr", !record);
    Utils::assertExit("can't scan delete/updated record", record->ptrFlag != 0);

    // cerr << "next Record: " <<  record->toString(recordDescriptor) << endl;
    record->attributeProject(recordDescriptor, attributeNames, static_cast<char *>(data));
    // memcpy(data, record->data, record->sizeWithoutHeader(recordDescriptor));
    
    rid.pageNum = nextPn - 1;
    rid.slotNum = nextSn;

    nextSn++;

    return 0;
}

void RBFM_ScanIterator::getNextPage()
{
    // end of paged file
    if (fileHandle->getNumberOfPages() <= nextPn)
    {
        if (currPg)
        {
            delete currPg;
        }
        currPg = nullptr;
        return;
    }
    fileHandle->readPage(nextPn, buffer);
    if (currPg)
    {
        delete currPg;
    }
    currPg = new DataPage(recordDescriptor, buffer);
    nextPn++;
    nextSn = 0;
}

int RBFM_ScanIterator::compareTo(char *thatVal)
{
    switch (conditionAttribute.type)
    {
    case TypeInt:
    {
        int int_this = 0, int_that = 0;
        memcpy(&int_this, value, sizeof(int));
        memcpy(&int_that, thatVal, sizeof(int));
        // cerr << "comparing " << int_this << " ? " << int_that << endl;
        return int_this - int_that;
    }
    case TypeReal:
    {
        float f_this = 0, f_that = 0;
        memcpy(&f_this, value, sizeof(float));
        memcpy(&f_that, thatVal, sizeof(float));
        // cerr << "comparing " << f_this << " ? " << f_that << endl;
        if (f_this - f_that < 0.001 && f_that - f_this < 0.001)
        {
            return 0;
        }
        return ((f_this - f_that) < 0.0 ? -1 : 1);
    }
    case TypeVarChar:
    {
        unsigned size = Utils::getVCSizeWithHead(value);
        unsigned thatSize = Utils::getVCSizeWithHead(thatVal);

        // empty string
        if (size == sizeof(unsigned))
        {
            return thatSize == sizeof(unsigned) ? 0 : -1;
        }
        if (thatSize == sizeof(unsigned))
        {
            return 1;
        }

        // + '\0'
        char _c[size - sizeof(unsigned) + 1];
        memcpy(_c, value + sizeof(unsigned), size - sizeof(unsigned));
        _c[size - sizeof(unsigned)] = '\0';
        string _s(_c);

        char _c_that[thatSize - sizeof(unsigned) + 1];
        memcpy(_c_that, thatVal + sizeof(unsigned), thatSize - sizeof(unsigned));
        _c_that[thatSize - sizeof(unsigned)] = '\0';
        string _s_that(_c_that);

        // cerr << "comparing \"" << _s << "\" ? \"" << _s_that << "\"" << endl;
        return _s.compare(_s_that);
    }
    }
    return 0;
}

RC RBFM_ScanIterator::close()
{
    return 0;
}

const string Record::RECORD_HEAD = "Rec:";
const unsigned Record::REC_HEADER_SIZE = sizeof(int) + sizeof(RID) + 4;

unsigned Record::getRecordSize(const vector<Attribute> &recordDescriptor, const void *rawData)
{
    if (rawData == nullptr)
    {
        return 0;
    }
    unsigned offset = 0;
    unsigned attributeNum = recordDescriptor.size();
    const char *data = static_cast<const char *>(rawData);

    // parse record null indicator
    bool nullIndicators[attributeNum];

    // nullIndicators size as offset
    offset += parseNullIndicator(nullIndicators, recordDescriptor, rawData);

    // parse attributes
    unsigned vcSize = 0;
    for (unsigned i = 0; i < attributeNum; i++)
    {
        if (nullIndicators[i])
        {
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
        case TypeReal:
        {
            offset += 4;
            break;
        }
        case TypeVarChar:
        {
            memcpy(&vcSize, data + offset, sizeof(unsigned));
            offset += sizeof(unsigned) + vcSize;
            break;
        }
        }
    }

    return offset;
}

unsigned Record::parseNullIndicator(bool nullIndicators[], const vector<Attribute> &recordDescriptor, const void *rawData)
{
    if (!rawData)
    {
        cerr << "empty rawData" << endl;
        exit(-1);
    }
    const char *data = static_cast<const char *>(rawData);
    unsigned attributeNum = recordDescriptor.size();
    char niChar;

    for (unsigned i = 0; i < attributeNum; i++)
    {
        niChar = *(data + i / 8);
        nullIndicators[i] = !!((niChar << (i % 8)) & 0x80);
        // cout << ":" << (nullIndicators[i] ? "1" : "0");
    }
    return (attributeNum - 1) / 8 + 1;
}

Record::Record(vector<Attribute> recordDescriptor, const char *rawData)
    : ptrFlag(-1),
      data(nullptr)
{
    // empty record
    if (rawData == nullptr)
    {
        this->ptrFlag = 2;
        this->data = nullptr;
        return;
    }

    unsigned size = getRecordSize(recordDescriptor, rawData);
    data = new char[size];
    memcpy(data, rawData, size);
}

Record::~Record()
{
    if (data)
    {
        delete[] data;
    }
}

unsigned Record::sizeWithoutHeader(const vector<Attribute> &recordDescriptor)
{
    if (ptrFlag == 2)
    {
        return 0;
    }
    return getRecordSize(recordDescriptor, data);
}

unsigned Record::sizeWithHeader(const vector<Attribute> &recordDescriptor)
{
    if (ptrFlag == 2)
    {
        return REC_HEADER_SIZE;
    }
    return sizeWithoutHeader(recordDescriptor) + REC_HEADER_SIZE;
}

string Record::toString(const vector<Attribute> &recordDescriptor)
{
    if (!data)
    {
        return "[ptrFlag=" + to_string(ptrFlag) + "]";
    }

    string s;
    unsigned offset = 0;
    unsigned attributeNum = recordDescriptor.size();
    bool nullIndicators[attributeNum];
    offset += parseNullIndicator(nullIndicators, recordDescriptor, data);
    
    // parse attributes
    int _int;
    float _float;
    unsigned vcSize = 0;
    char _vc[sizeWithoutHeader(recordDescriptor)];

    for (unsigned i = 0; i < attributeNum; i++)
    {
        s += recordDescriptor[i].name + ": ";
        if (nullIndicators[i])
        {
            s += "NULL\n";
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
        {
            // can't modify data itself, since it's the original raw data
            memcpy(&_int, data + offset, sizeof(int));
            offset += sizeof(int);
            s += to_string(_int);
            break;
        }
        case TypeReal:
        {
            memcpy(&_float, data + offset, sizeof(float));
            offset += sizeof(float);
            s += to_string(_float);
            break;
        }
        case TypeVarChar:
        {
            memcpy(&vcSize, data + offset, sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy(_vc, data + offset, vcSize);
            offset += vcSize;
            _vc[vcSize] = '\0';
            s += string(_vc);
            break;
        }
        }
        s += "\n";
    }
    return s;
}

unsigned Record::getAttribute(const vector<Attribute> &recordDescriptor, const string &attributeName, char *des)
{
    unsigned offset = 0;
    unsigned vcSize = 0;
    unsigned attributeNum = recordDescriptor.size();
    bool nullIndicators[attributeNum];
    offset += parseNullIndicator(nullIndicators, recordDescriptor, data);

    unsigned attr = 0;
    for (; attr < attributeNum; attr++)
    {
        if (recordDescriptor[attr].name == attributeName)
        {
            break;
        }
    }

    if ((attr == attributeNum) ||
        (attr == 0 && recordDescriptor[attr].name != attributeName))
    {
        cerr << "can't found the attributeName" << endl;
        exit(-1);
    }

    if (nullIndicators[attr])
    {
        return 0;
    }

    // pass all useless datas
    for (unsigned i = 0; i < attr; i++)
    {
        if (nullIndicators[i])
        {
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
        case TypeReal:
        {
            offset += 4;
            break;
        }
        case TypeVarChar:
        {
            memcpy(&vcSize, data + offset, sizeof(unsigned));
            offset += sizeof(unsigned) + vcSize;
            break;
        }
        }
    }

    switch (recordDescriptor[attr].type)
    {
    case TypeInt:
    case TypeReal:
    {
        memcpy(des, data + offset, 4);
        return 4;
    }
    case TypeVarChar:
    {
        memcpy(&vcSize, data + offset, sizeof(unsigned));
        // copy both size and data
        memcpy(des, data + offset, sizeof(unsigned) + vcSize);
        return sizeof(unsigned) + vcSize;
    }
    }
    return 0;
}

unsigned Record::attributeProject(const vector<Attribute> &recordDescriptor, const vector<string> attributeNames, char *des)
{
    unsigned srcOffset = 0,
             desOffset = 0,
             vcSize = 0;
    // get src null indicators
    bool srcNullIndicators[recordDescriptor.size()];
    srcOffset += parseNullIndicator(srcNullIndicators, recordDescriptor, data);

    // make des null indicators
    bool desNullIndicators[recordDescriptor.size()];

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        desNullIndicators[i] = true;

        // if original indicator is true, then still true, since there's no data
        if (srcNullIndicators[i])
        {
            continue;
        }

        // else decide whether user need this attribute
        for (unsigned j = 0; j < attributeNames.size(); j++)
        {
            if (recordDescriptor[i].name == attributeNames[j])
            {
                desNullIndicators[i] = false;
                break;
            }
        }
    }
    
    // copy null indicator data to des
    desOffset = Utils::makeNullIndicator(desNullIndicators, recordDescriptor.size(), des);
    
    // test desNullIndicators
    bool test[recordDescriptor.size()];
    parseNullIndicator(test, recordDescriptor, des);
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (test[i] != desNullIndicators[i])
        {
            cerr << "desNullIndicators wrong!" << endl;
            exit(-1);
        }
    }


    Utils::assertExit("[ERR]desOffset != srcOffset", desOffset != srcOffset);
    
    // pick data to copy
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        // cerr << "desOffset=" << desOffset << ", srcOffset=" << srcOffset << endl;
        // cerr << "desNullIndicators com=" << memcmp(des, data, desOffset) << endl;

        // if srcNullIndicators[i], then desNullIndicators[i]
        if (srcNullIndicators[i])
        {
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
        case TypeReal:
        {
            // jump this attribute
            if (desNullIndicators[i])
            {
                srcOffset += 4;
            }
            else
            {
                memcpy(des + desOffset, data + srcOffset, 4);
                srcOffset += 4;
                desOffset += 4;
            }
            break;
        }
        case TypeVarChar:
        {
            vcSize = Utils::getVCSizeWithHead(data + srcOffset);
            if (desNullIndicators[i])
            {
                srcOffset += vcSize;
            }
            else
            {
                // copy both size and data
                memcpy(des + desOffset, data + srcOffset, vcSize);
                srcOffset += vcSize;
                desOffset += vcSize;
            }
            break;
        }
        }
    }
    // cerr << "desOffset=" << desOffset << ", srcOffset=" << srcOffset << endl;
    // cerr << "memcmp=" << memcmp(data, des, desOffset) << endl;
    return desOffset;
}

unsigned Record::attributeProjectCompress(const vector<Attribute> &recordDescriptor, const vector<string> attributeNames, char *des)
{
    unsigned srcOffset = 0,
             desOffset = 0,
             vcSize = 0;
    // get src null indicators
    bool srcNullIndicators[recordDescriptor.size()];
    srcOffset += parseNullIndicator(srcNullIndicators, recordDescriptor, data);

    // make des null indicators
    bool desNullIndicators[recordDescriptor.size()];

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        desNullIndicators[i] = true;

        // if original indicator is true, then still true, since there's no data
        if (srcNullIndicators[i])
        {
            continue;
        }

        // else decide whether user need this attribute
        for (unsigned j = 0; j < attributeNames.size(); j++)
        {
            if (recordDescriptor[i].name == attributeNames[j])
            {
                desNullIndicators[i] = false;
                break;
            }
        }
    }
    
    // copy null indicator data to des
    unsigned compressNISize = (attributeNames.size() - 1) / 8 + 1;
    memset(des, 0, compressNISize);
    desOffset += compressNISize;


    // Utils::assertExit("[ERR]desOffset != srcOffset", desOffset != srcOffset);
    
    // pick data to copy
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        // cerr << "desOffset=" << desOffset << ", srcOffset=" << srcOffset << endl;
        // cerr << "desNullIndicators com=" << memcmp(des, data, desOffset) << endl;

        // if srcNullIndicators[i], then desNullIndicators[i]
        if (srcNullIndicators[i])
        {
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
        case TypeReal:
        {
            // jump this attribute
            if (desNullIndicators[i])
            {
                srcOffset += 4;
            }
            else
            {
                memcpy(des + desOffset, data + srcOffset, 4);
                srcOffset += 4;
                desOffset += 4;
            }
            break;
        }
        case TypeVarChar:
        {
            vcSize = Utils::getVCSizeWithHead(data + srcOffset);
            if (desNullIndicators[i])
            {
                srcOffset += vcSize;
            }
            else
            {
                // copy both size and data
                memcpy(des + desOffset, data + srcOffset, vcSize);
                srcOffset += vcSize;
                desOffset += vcSize;
            }
            break;
        }
        }
    }
    // cerr << "desOffset=" << desOffset << ", srcOffset=" << srcOffset << endl;
    // cerr << "memcmp=" << memcmp(data, des, desOffset) << endl;
    return desOffset;
}


const unsigned DataPage::DATA_PAGE_HEADER_SIZE = sizeof(unsigned) * 2;

DataPage::DataPage(vector<Attribute> recordDescriptor)
    : recordDescriptor(recordDescriptor),
      size(DATA_PAGE_HEADER_SIZE)
{
}

DataPage::DataPage(vector<Attribute> recordDescriptor, char *data)
    : recordDescriptor(recordDescriptor),
      size(0)
{
    char *_data = data;
    unsigned recordNum = 0;

    // read page header, data is a copy, just modify it
    memcpy(&size, data, sizeof(unsigned));
    data += sizeof(unsigned);
    memcpy(&recordNum, data, sizeof(unsigned));
    data += sizeof(unsigned);

    // read records
    int ptrFlag;
    RID rid;

    for (unsigned i = 0; i < recordNum; i++)
    {
        // jump "Rec:"
        data += 4;
        memcpy(&ptrFlag, data, sizeof(int));
        data += sizeof(int);
        if (ptrFlag == 1)
        {
            cerr << "can't deal with ptrFlag == 1" << endl;
            exit(-1);
        }

        memcpy(&rid, data, sizeof(RID));
        data += sizeof(RID);
        Record *rec = nullptr;

        if (ptrFlag == 2)
        {
            rec = new Record(recordDescriptor, nullptr);
        }
        else
        {
            rec = new Record(recordDescriptor, data);
        }

        rec->ptrFlag = ptrFlag;
        rec->rid = rid;
        data += rec->sizeWithoutHeader(recordDescriptor);
        records.push_back(rec);
    }

    if (data - _data != size)
    {
        cerr << "DataPage::DataPage: data - _data=" << data - _data << " != size=" << size << endl;
        exit(-1);
    }
}

DataPage::~DataPage()
{
    for (unsigned i = 0; i < records.size(); i++)
    {
        delete records[i];
    }
}

unsigned DataPage::getAvailableSize()
{
    unsigned available = size;
    for (unsigned i = 0; i < records.size(); i++)
    {
        if (records[i]->ptrFlag == 2)
        {
            available -= records[i]->sizeWithHeader(recordDescriptor);
        }
    }
    return available;
}

void DataPage::appendRecord(Record *record)
{
    record->ptrFlag = 0;
    record->rid.slotNum = records.size();
    records.push_back(record);
    size += record->sizeWithHeader(recordDescriptor);
    if (size > PAGE_SIZE)
    {
        cerr << "append Record excessed PAGE_SIZE" << endl;
        exit(-1);
    }
}

void DataPage::insertRecord(Record *record)
{
    record->ptrFlag = 0;
    unsigned slotNum = records.size();

    // find a deleted record and reuse its RID
    for (unsigned i = 0; i < records.size(); i++)
    {
        if (records[i]->ptrFlag == 2)
        {
            slotNum = i;
            break;
        }
    }
    if (slotNum == records.size())
    {
        return appendRecord(record);
    }

    // reuse its RID
    record->rid.slotNum = slotNum;

    // updata size before free it
    size += record->sizeWithHeader(recordDescriptor) -
            records[slotNum]->sizeWithHeader(recordDescriptor);

    // free the deleted item
    delete records[slotNum];
    records[slotNum] = record;

    if (size > PAGE_SIZE)
    {
        cerr << "insert Record excessed PAGE_SIZE" << endl;
        exit(-1);
    }
}

void DataPage::deleteRecord(unsigned slotNum)
{
    Record *rec = records[slotNum];
    if (rec->ptrFlag != 0)
    {
        cerr << "DataPage::deleteRecord can't deal with rec->ptrFlag != 0." << endl;
        exit(-1);
    }

    // still keep the whole header
    size -= rec->sizeWithoutHeader(recordDescriptor);
    cerr << "size-=" << rec->sizeWithoutHeader(recordDescriptor) << "=" << size << endl;
    rec->ptrFlag = 2;
    delete[] rec->data;
    rec->data = nullptr;
}

void DataPage::getRawData(char *data)
{
    char *_data = data;
    unsigned recordNum = records.size();
    memset(data, 0, PAGE_SIZE);

    // copy page header
    memcpy(data, &size, sizeof(unsigned));
    data += sizeof(unsigned);
    memcpy(data, &recordNum, sizeof(unsigned));
    data += sizeof(unsigned);

    // generate records raw data
    Record *rec;
    unsigned recSize;
    for (unsigned i = 0; i < recordNum; i++)
    {
        if (data - _data > PAGE_SIZE)
        {
            cerr << "DataPage: data - _data > PAGE_SIZE" << endl;
            exit(-1);
        }
        rec = records[i];
        // add "Rec:"
        memcpy(data, Record::RECORD_HEAD.c_str(), 4);
        data += 4;
        memcpy(data, &(rec->ptrFlag), sizeof(int));
        data += sizeof(int);
        memcpy(data, &(rec->rid), sizeof(RID));
        data += sizeof(RID);

        recSize = rec->sizeWithoutHeader(recordDescriptor);
        if (recSize == 0)
        {
            continue;
        }
        memcpy(data, rec->data, recSize);
        data += recSize;
    }
    if (data - _data != size)
    {
        cerr << "DataPage::getRawData: data - _data=" << data - _data << " != size=" << size << endl;
        exit(-1);
    }
}

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    // remove all const restriction, void->char & clone
    vector<Attribute> c_recordDescriptor(recordDescriptor);
    unsigned dataSize = Record::getRecordSize(recordDescriptor, data);
    char c_data[dataSize];
    memcpy(c_data, data, dataSize);

    if (fileHandle.getNumberOfPages() == 0)
    {
        return addPageAndInsert(fileHandle, c_recordDescriptor, c_data, rid);
    }
    return findPageAndInsert(fileHandle, c_recordDescriptor, c_data, rid);
}

RC RecordBasedFileManager::addPageAndInsert(FileHandle &fileHandle, vector<Attribute> &recordDescriptor, char *data, RID &rid)
{
    DataPage page(recordDescriptor);

    // append page to get pageNum
    page.getRawData(buffer);
    fileHandle.appendPage(buffer);
    unsigned pageNum = fileHandle.getNumberOfPages() - 1;

    Record *record = new Record(recordDescriptor, data);
    record->rid.pageNum = pageNum;

    page.appendRecord(record);

    rid = record->rid;

    page.getRawData(buffer);
    fileHandle.writePage(pageNum, buffer);
    return 0;
}

RC RecordBasedFileManager::findPageAndInsert(FileHandle &fileHandle, vector<Attribute> &recordDescriptor, char *data, RID &rid)
{
    bool APPEND_ONLY = true;

    Record *record = new Record(recordDescriptor, data);
    unsigned recordSize = record->sizeWithHeader(recordDescriptor);

    // try last page firstly
    unsigned pageNum = fileHandle.getNumberOfPages() - 1;
    fileHandle.readPage(pageNum, buffer);
    DataPage *lst = new DataPage(recordDescriptor, buffer);
    DataPage *page = lst;

    // lst can't fit
    if (lst->getAvailableSize() + recordSize > PAGE_SIZE)
    {
        if (APPEND_ONLY)
        {
            delete page;
            return addPageAndInsert(fileHandle, recordDescriptor, data, rid);
        }

        // lst == first? only one page, just do append
        if (pageNum == 0)
        {
            delete lst;
            return addPageAndInsert(fileHandle, recordDescriptor, data, rid);
        }

        // have other pages
        pageNum = 0;
        for (; pageNum < fileHandle.getNumberOfPages() - 1; pageNum++)
        {
            fileHandle.readPage(pageNum, buffer);
            delete page;
            page = new DataPage(recordDescriptor, buffer);
            if (page->getAvailableSize() + recordSize <= PAGE_SIZE)
            {
                break;
            }
        }

        // still not found
        if (pageNum == fileHandle.getNumberOfPages() - 1)
        {
            delete page;
            return addPageAndInsert(fileHandle, recordDescriptor, data, rid);
        }

        // found, do other thing below
    }

    record->rid.pageNum = pageNum;
    page->insertRecord(record);

    rid = record->rid;
    page->getRawData(buffer);
    fileHandle.writePage(pageNum, buffer);

    delete page;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    if (fileHandle.readPage(rid.pageNum, buffer) != 0)
    {
        cerr << "read page failed" << endl;
        return -1;
    }

    DataPage page(recordDescriptor, buffer);
    if (rid.slotNum >= page.records.size())
    {
        cerr << "page.recordNum > rid.slotNum" << endl;
        return -1;
    }

    Record *rec = page.records[rid.slotNum];
    if (rec->ptrFlag == 1)
    {
        cerr << "can't deal with record ptr now" << endl;
        exit(-1);
    }
    if (rec->ptrFlag == 2)
    {
        // already deleted
        return -1;
    }
    unsigned dataSize = rec->sizeWithoutHeader(recordDescriptor);
    memcpy(data, rec->data, dataSize);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    // remove all const restriction, void->char & clone
    vector<Attribute> c_recordDescriptor(recordDescriptor);
    unsigned dataSize = Record::getRecordSize(recordDescriptor, data);
    char c_data[dataSize];
    memcpy(c_data, data, dataSize);

    Record rec(c_recordDescriptor, c_data);
    cerr << rec.toString(c_recordDescriptor);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    if (fileHandle.readPage(rid.pageNum, buffer) != 0)
    {
        cerr << "read page failed" << endl;
        return -1;
    }

    DataPage page(recordDescriptor, buffer);
    if (rid.slotNum >= page.records.size())
    {
        cerr << "page.recordNum > rid.slotNum" << endl;
        return -1;
    }

    Record *rec = page.records[rid.slotNum];
    if (rec->ptrFlag == 1)
    {
        cerr << "can't deal with record ptr now" << endl;
        exit(-1);
    }
    if (rec->ptrFlag == 2)
    {
        // already deleted
        return -1;
    }

    page.deleteRecord(rid.slotNum);
    page.getRawData(buffer);
    fileHandle.writePage(rid.pageNum, buffer);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    cerr << "can't update." << endl;
    exit(-1);

    // remove all const restriction, void->char & clone
    // vector<Attribute> c_recordDescriptor(recordDescriptor);
    // unsigned dataSize = Record::getRecordSize(recordDescriptor, data);
    // char c_data[dataSize];
    // memcpy(c_data, data, dataSize);

    // if (fileHandle.readPage(rid.pageNum, buffer) != 0)
    // {
    //     cerr << "read page failed" << endl;
    //     return -1;
    // }

    // DataPage page(recordDescriptor, buffer);
    // if (rid.slotNum >= page.records.size())
    // {
    //     cerr << "page.recordNum > rid.slotNum" << endl;
    //     return -1;
    // }

    // Record *oldRecord = page.records[rid.slotNum];
    // Record *newRecord = new Record(recordDescriptor, data);

    // if (oldRecord->ptrFlag == 1)
    // {
    //     cerr << "update can't deal with record ptr now" << endl;
    //     exit(-1);
    // }
    // if (oldRecord->ptrFlag == 2)
    // {
    //     // already deleted
    //     return -1;
    // }

    // // whether can fit the new record
    // unsigned oldSize = oldRecord->sizeWithHeader(recordDescriptor);
    // unsigned newSize = newRecord->sizeWithHeader(recordDescriptor);

    // bool canFit = true;
    // if (newSize > oldSize)
    // {
    //     canFit = false;
    //     if (page.size + newSize - oldSize <= PAGE_SIZE)
    //     {
    //         canFit = true;
    //     }
    // }

    // if (canFit)
    // {
    //     page.updateRecord(rid.slotNum, newRecord);
    //     return 0;
    // }
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    if (fileHandle.readPage(rid.pageNum, buffer) != 0)
    {
        cerr << "read page failed" << endl;
        return -1;
    }

    DataPage page(recordDescriptor, buffer);
    if (rid.slotNum >= page.records.size())
    {
        cerr << "page.recordNum > rid.slotNum" << endl;
        return -1;
    }

    Record *rec = page.records[rid.slotNum];

    if (rec->ptrFlag != 0)
    {
        cerr << "can't deal with record ptr now" << endl;
        exit(-1);
    }

    rec->getAttribute(recordDescriptor, attributeName, static_cast<char *>(data));
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator)
{
    RBFM_ScanIterator *ret = new RBFM_ScanIterator(&fileHandle,
                                                   recordDescriptor,
                                                   conditionAttribute,
                                                   compOp,
                                                   static_cast<const char *>(value),
                                                   attributeNames);
    rbfm_ScanIterator = *ret;
    return 0;
}