#include "rbfm.h"

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

Record::Record(vector<Attribute> recordDescriptor, char *rawData)
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
    delete [] rec->data;
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
    return -1;
}
