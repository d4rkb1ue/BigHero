#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

/**
 * ALL SIZE is # of Bytes
 */
#define PAGE_SIZE 4096
#define FILEHEADER_LEN 5
#define FILEHEADER_SIZE (FILEHEADER_LEN * sizeof(unsigned))

/**
 * Each Directory Page keep # DIR_PAGE_LEN of
 * data sizes for corresponding data pages
 * Aka 1024
 */
#define DIR_PAGE_LEN (PAGE_SIZE / sizeof(unsigned))

#include <string>
#include <climits>
#include <cstdio>
#include <iostream>
#include <vector>
#include <cstring>

using namespace std;

class FileHandle;

/****************************************************
 *                      Utils                       *
 ****************************************************/
class Utils
{
  public:
    // return total standard size
    static unsigned makeStandardString(const string s, char *data);
    static void assertExit(const string e, RC ret);
    static void assertExit(const string e, bool b = true);
    static unsigned getVCSizeWithHead(const char *data);
};

/****************************************************
 *                  PagedFileManager                *
 ****************************************************/
class PagedFileManager
{
  public:
    static PagedFileManager *instance(); // Access to the _pf_manager instance

    RC createFile(const string &fileName);                       // Create a new file
    RC destroyFile(const string &fileName);                      // Destroy a file
    RC openFile(const string &fileName, FileHandle &fileHandle); // Open a file
    RC closeFile(FileHandle &fileHandle);                        // Close a file

  protected:
    PagedFileManager();  // Constructor
    ~PagedFileManager(); // Destructor

  private:
    static PagedFileManager *_pf_manager;
};

/****************************************************
 *                  DirectroyPage                   *
 ****************************************************

 * Actually, directory page has been abandoned.
 * All metadata of data size in this header file can only be PAGE_SIZE, aka full for always
 * just manage your page size inside 
 */
class DirectroyPage
{
    unsigned dataSize[DIR_PAGE_LEN];

  public:
    RC readRawData(void *d);
    RC getRawData(void *d);

    RC getDataSize(PageNum &pageNum, unsigned &size);
    RC updateDataSize(PageNum &pageNum, unsigned size);
};

/****************************************************
 *                    FileHeader                    *
 ****************************************************/
class FileHeader
{
    /**
     * change this need to change FILEHEADER_SIZE also!
     */
    struct FileHeaderData
    {
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned pageCount;
        unsigned dirCount;
    };

  public:
    FileHeaderData data;
    FileHeader();
    FileHeader(unsigned readPageCounter,
               unsigned writePageCounter,
               unsigned appendPageCounter,
               unsigned pageCount,
               unsigned dirCount);
    RC readRawData(void *d);
    RC getRawData(void *d);
};

/****************************************************
 *                    FileHandle                    *
 ****************************************************/
class FileHandle
{
    FILE *filePtr;
    FileHeader fileHeader;
    vector<DirectroyPage> dirPages;

    unsigned pageCount;
    unsigned dirCount;

    RC _rawReadPage(PageNum pageNum, void *data);
    RC _rawWritePage(PageNum pageNum, const void *data);
    RC _rawAppendPage(const void *data);

    RC _rawReadByte(unsigned offset, unsigned len, void *data);
    RC _rawWriteByte(unsigned start, unsigned end, void *data);

    RC readDirPages();
    RC updateDirPages();

    RC readFileHeader();
    RC updateFileHeader();

    RC flushAll();

    size_t getFileSize();

  public:
    FileHandle(FILE *f);

    RC writePage(PageNum pageNum, const void *data, unsigned dataSize);
    RC appendPage(const void *data, unsigned dataSize);
    RC updateDataSize(PageNum pageNum, unsigned dataSize);

    RC getPageSize(PageNum pageNum, unsigned &size);
    RC close();

    /***********************
     * ORIGINAL Interfaces *
     ***********************/

    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

    FileHandle();  // Default constructor
    ~FileHandle(); // Destructor

    RC readPage(PageNum pageNum, void *data);                                                              // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                                                       // Write a specific page
    RC appendPage(const void *data);                                                                       // Append a specific page
    unsigned getNumberOfPages();                                                                           // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount); // Put the current counter values into variables
};

#endif
