#include "pfm.h"

/****************************************************
 *                  PagedFileManager                *
 ****************************************************/

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance()
{
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
    FILE *newFile;
    newFile = fopen(fileName.c_str(), "wbx");
    if (newFile == NULL)
    {
        return -1;
    }
    fputs("", newFile);
    fflush(newFile);
    fclose(newFile);
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    FILE *pfile;
    pfile = fopen(fileName.c_str(), "r+b");
    if (pfile == NULL)
    {
        return -1;
    }
    fileHandle = FileHandle{pfile};
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.close();
}

/****************************************************
 *                  DirectroyPage                   *
 ****************************************************/

RC DirectroyPage::readRawData(void *d)
{
    memcpy(dataSize, d, PAGE_SIZE);
    return 0;
}

RC DirectroyPage::getRawData(void *d)
{
    memcpy(d, dataSize, PAGE_SIZE);
    return 0;
}

RC DirectroyPage::getDataSize(PageNum &pageNum, unsigned &size)
{
    if (pageNum >= DIR_PAGE_LEN)
    {
        return -1;
    }
    size = dataSize[pageNum];
    return 0;
}

RC DirectroyPage::updateDataSize(PageNum &pageNum, unsigned size)
{
    if (pageNum >= DIR_PAGE_LEN)
    {
        return -1;
    }
    dataSize[pageNum] = size;
    return 0;
}

/****************************************************
 *                    FileHeader                    *
 ****************************************************/
FileHeader::FileHeader()
{
    data.readPageCounter = 0;
    data.writePageCounter = 0;
    data.appendPageCounter = 0;
    data.pageCount = 0;
    data.dirCount = 0;
}

FileHeader::FileHeader(unsigned readPageCounter,
                       unsigned writePageCounter,
                       unsigned appendPageCounter,
                       unsigned pageCount,
                       unsigned dirCount)
{
    data.readPageCounter = readPageCounter;
    data.writePageCounter = writePageCounter;
    data.appendPageCounter = appendPageCounter;
    data.pageCount = pageCount;
    data.dirCount = dirCount;
}

/**
 * pass d to this function, re-write the file header
 */
RC FileHeader::readRawData(void *d)
{
    memcpy(&data, d, FILEHEADER_SIZE);
    return 0;
}

/**
 * pass d as buffer, to get the file header
 */
RC FileHeader::getRawData(void *d)
{
    memcpy(d, &data, FILEHEADER_SIZE);
    return 0;
}

/****************************************************
 *                    FileHandle                    *
 ****************************************************/

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    pageCount = 0;
    dirCount = 0;

    filePtr = NULL;
}

FileHandle::FileHandle(FILE *f)
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    pageCount = 0;
    dirCount = 0;

    filePtr = f;

    char *buffer = new char[PAGE_SIZE];
    
    if (getFileSize() > 0)
    {
        // read file header
        if (_rawReadByte(0, FILEHEADER_SIZE, buffer) != 0)
        {
            cerr << "_rawReadByte failed." << endl;
            exit(-1);
        }
        fileHeader.readRawData(buffer);

        readPageCounter = fileHeader.data.readPageCounter;
        writePageCounter = fileHeader.data.writePageCounter;
        appendPageCounter = fileHeader.data.appendPageCounter;
        pageCount = fileHeader.data.pageCount;
        dirCount = fileHeader.data.dirCount;

        // read directory page(s)
        for (int i = 0, offset = FILEHEADER_SIZE; i < dirCount; i++)
        {
            dirPages.push_back(DirectroyPage());
            if (_rawReadByte(offset, PAGE_SIZE, buffer) != 0)
            {
                cerr << "_rawReadByte failed." << endl;
                exit(-1);
            }

            dirPages[i].readRawData(buffer);
            // 1st directory page
            if (i == 0)
            {
                offset += PAGE_SIZE * pageCount;
            }
            else
            {
                offset += PAGE_SIZE;
            }
        }
    }
    // init file header
    else
    {
        dirCount = 1;
        dirPages.push_back(DirectroyPage());
        // init fileHeader and DirPages
        flushAll();
    }
    delete [] buffer;
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= pageCount)
    {
        return -1;
    }
    readPageCounter++;
    return _rawReadPage(pageNum + 1, data);
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return writePage(pageNum, data, PAGE_SIZE);
}

RC FileHandle::appendPage(const void *data)
{
    return appendPage(data, PAGE_SIZE);
}

unsigned FileHandle::getNumberOfPages()
{
    return pageCount;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::_rawReadByte(unsigned start, unsigned end, void *data)
{
    if (start >= end || end >= getFileSize())
    {
        return -1;
    }
    fseek(filePtr, start, SEEK_SET);
    fread(data, 1, end - start, filePtr);
    rewind(filePtr);
    return 0;
}

RC FileHandle::_rawWriteByte(unsigned start, unsigned end, void *data)
{
    if (start >= end || start > getFileSize())
    {
        return -1;
    }
    fseek(filePtr, start, SEEK_SET);
    fwrite(data, end - start, 1, filePtr);
    fflush(filePtr);
    rewind(filePtr);
    return 0;
}

RC FileHandle::_rawReadPage(PageNum pageNum, void *data)
{
    if (pageCount + dirCount - 1 < pageNum)
    {
        return -1;
    }
    size_t pos = FILEHEADER_SIZE + pageNum * PAGE_SIZE;
    fseek(filePtr, pos, SEEK_SET);
    fread(data, PAGE_SIZE, 1, filePtr);
    rewind(filePtr);
    return 0;
}

RC FileHandle::_rawWritePage(PageNum pageNum, const void *data)
{
    if (pageCount + dirCount - 1 < pageNum)
    {
        return -1;
    }
    size_t pos = FILEHEADER_SIZE + pageNum * PAGE_SIZE;
    fseek(filePtr, pos, SEEK_SET);
    fwrite(data, PAGE_SIZE, 1, filePtr);
    fflush(filePtr);
    rewind(filePtr);
    return 0;
}
RC FileHandle::_rawAppendPage(const void *data)
{
    fseek(filePtr, 0, SEEK_END);
    fwrite(data, PAGE_SIZE, 1, filePtr);
    fflush(filePtr);
    rewind(filePtr);
    return 0;
}

/**
 * in Byte
 * http://www.cplusplus.com/reference/cstdio/fread/
 */
size_t FileHandle::getFileSize()
{
    size_t lSize = 0;

    fseek(filePtr, 0, SEEK_END);
    lSize = ftell(filePtr);
    rewind(filePtr);

    return lSize;
}

RC FileHandle::close()
{
    flushAll();
    return fclose(filePtr);
}

RC FileHandle::flushAll()
{
    // save FileHeader
    char *buffer = new char[PAGE_SIZE];
    fileHeader = FileHeader{
        readPageCounter,
        writePageCounter,
        appendPageCounter,
        pageCount,
        dirCount};
    fileHeader.getRawData(buffer);
    _rawWriteByte(0, FILEHEADER_SIZE, buffer);

    // save Directory Pages
    for (int i = 0, offset = 0; i < dirCount; i++)
    {
        dirPages[i].getRawData(buffer);
        _rawWritePage(offset, buffer);
        // 1st directory page
        if (i == 0)
        {
            // aka offset = pageCount
            offset += pageCount;
        }
        offset += 1;
    }
    fflush(filePtr);
    rewind(filePtr);
    
    delete [] buffer;

    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data, unsigned dataSize)
{
    if (pageNum >= pageCount)
    {
        return -1;
    }
    writePageCounter++;

    _rawWritePage(pageNum + 1, data);
    updateDataSize(pageNum, dataSize);
    flushAll();
    return 0;
}

RC FileHandle::appendPage(const void *data, unsigned dataSize)
{
    appendPageCounter++;
    pageCount++;
    
    // need add new directory page
    if (pageCount > DIR_PAGE_LEN * dirCount)
    {
        dirCount++;
        dirPages.push_back(DirectroyPage());
    }

    if (dirCount == 1)
    {
        _rawAppendPage(data);
    }
    // actually overwrite the 2nd directory page
    else
    {
        _rawWritePage(pageCount, data);
    }
    updateDataSize(pageCount - 1, dataSize);
    flushAll();
    return 0;
}

RC FileHandle::updateDataSize(PageNum pageNum, unsigned dataSize)
{
    unsigned dirNum = pageNum / DIR_PAGE_LEN;
    if (dirNum >= dirCount)
    {
        return -1;
    }
    pageNum = pageNum % DIR_PAGE_LEN;
    dirPages[dirNum].updateDataSize(pageNum, dataSize);
    return 0;
}

RC FileHandle::getPageSize(PageNum pageNum, unsigned &size)
{
    if (pageNum >= pageCount)
    {
        cerr << "pageNum >= pageCount" << endl;
        exit(-1);
    }
    unsigned dirNum = pageNum / DIR_PAGE_LEN;
    if (dirNum >= dirCount)
    {
        return -1;
    }
    pageNum = pageNum % DIR_PAGE_LEN;
    if (dirPages[dirNum].getDataSize(pageNum, size) != 0)
    {
        return -1;
    }
    return 0;
}