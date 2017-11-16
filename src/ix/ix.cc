#include "ix.h"

/****************************************************
 *                  IndexManager                    *
 ****************************************************/
IndexManager *IndexManager::_index_manager = 0;

IndexManager *IndexManager::instance()
{
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    if (!_pfm)
    {
        _pfm = PagedFileManager::instance();
    }
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return _pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if (ixfileHandle.fileName.size() > 0)
    {
        return -1;
    }

    ixfileHandle = IXFileHandle(fileName);
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return ixfileHandle.closeFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
}

/****************************************************
 *                  IX_ScanIterator                 *
 ****************************************************/
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}

/****************************************************
 *                  IXFileHandle                    *
 ****************************************************/
IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    fileName = "";
}

IXFileHandle::IXFileHandle(string indexFileName)
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    if (!_pfm)
    {
        _pfm = PagedFileManager::instance();
    }

    if (_pfm->openFile(indexFileName, _fileHandle) != 0)
    {
        cerr << "Open index file failed." << endl;
        exit(-1);
    }

    fileName = indexFileName;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data, unsigned dataSize)
{
    ixWritePageCounter++;
    return _fileHandle.writePage(pageNum, data, dataSize);
}

RC IXFileHandle::appendPage(const void *data, unsigned dataSize)
{
    ixAppendPageCounter++;
    return _fileHandle.appendPage(data, dataSize);
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    ixReadPageCounter++;
    return _fileHandle.readPage(pageNum, data);
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

RC IXFileHandle::closeFile()
{
    return _fileHandle.close();
}