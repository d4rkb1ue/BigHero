#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>

#include "../rbf/rbfm.h"

#define IX_EOF (-1) // end of the index scan

class IX_ScanIterator;
class IXFileHandle;
class BTree;
class MetaPage;
class NodePage;
class InternalPage;
class LeafPage;
class InternalEntry;
class LeafEntry;

/****************************************************
 *                  IndexManager                    *
 ****************************************************/
class IndexManager
{
private:
  PagedFileManager *_pfm;
  static IndexManager *_index_manager;

protected:
  IndexManager();
  ~IndexManager();

public:
  static IndexManager *instance();

  // Create an index file.
  RC createFile(const string &fileName);

  // Delete an index file.
  RC destroyFile(const string &fileName);

  // Open an index and return an ixfileHandle.
  RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

  // Close an ixfileHandle for an index.
  RC closeFile(IXFileHandle &ixfileHandle);

  // Insert an entry into the given index that is indicated by the given ixfileHandle.
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given ixfileHandle.
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Initialize and IX_ScanIterator to support a range search
  RC scan(IXFileHandle &ixfileHandle,
          const Attribute &attribute,
          const void *lowKey,
          const void *highKey,
          bool lowKeyInclusive,
          bool highKeyInclusive,
          IX_ScanIterator &ix_ScanIterator);

  // Print the B+ tree in pre-order (in a JSON record format)
  void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
};

/****************************************************
 *                  IX_ScanIterator                 *
 ****************************************************/
class IX_ScanIterator
{
public:
  IX_ScanIterator();
  ~IX_ScanIterator();

  // Get next matching entry
  RC getNextEntry(RID &rid, void *key);

  // Terminate index scan
  RC close();
};

/****************************************************
 *                  IXFileHandle                    *
 ****************************************************/
class IXFileHandle
{
private:
  BTree *tree;

public:
  FileHandle _fileHandle;
  PagedFileManager *_pfm;
  string fileName;

  unsigned ixReadPageCounter;
  unsigned ixWritePageCounter;
  unsigned ixAppendPageCounter;

  IXFileHandle();
  IXFileHandle(string fileName);
  ~IXFileHandle();

  BTree *getTree(AttrType type);
  // FileHandle won't care the data size anymore, the tree itself can take care
  RC writePage(PageNum pageNum, char *data);
  RC appendPage(char *data);
  RC appendPage(char *data, PageNum &pageNum);
  RC readPage(PageNum pageNum, char *data);
  unsigned getNumberOfPages();
  RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
  RC closeFile();
};

/****************************************************
 *                       BTree                      *
 ****************************************************/
class BTree
{
private:
  char buffer[PAGE_SIZE];

public:
  NodePage *root;
  PageNum rootPn;
  IXFileHandle *_fileHandle;
  AttrType attrType;

  BTree(IXFileHandle *fileHandle, AttrType attrType);
  ~BTree();

  bool isEmpty();
  RC insert(char *key, unsigned len, RID rid);
  // RC lazyRemove(char *key, unsigned len);
  // RC toJSON();
  string toString();

private:
  RC initNewTree(char *key, unsigned len, RID rid);
  RC insertToLeaf(char *key, unsigned len, RID rid);
  void updateRoot();
  // RC insertToParent();
};

/****************************************************
 *                    MetaPage                      *
 ****************************************************/
class MetaPage
{
  const static string META_PAGE;
  const static string META_PAGE_END;
public:
  PageNum rootPn;
  bool rootIsLeaf;

  MetaPage(char *rawData);
  MetaPage(PageNum rootPn, bool rootIsLeaf);
  ~MetaPage(){};

  RC getRawData(char *data);
};

/****************************************************
 *                    NodePage                      *
 ****************************************************/
class NodePage
{
public:
  NodePage *parent;
  bool isRoot;
  bool isLeaf;
  AttrType attrType;
  unsigned size;

  NodePage(NodePage *parent, AttrType attrType, unsigned size, bool isLeaf);
  virtual ~NodePage(){};

  bool tooBig() { return size > PAGE_SIZE; };
  virtual RC getRawData(char *data) = 0;
  virtual string toString() = 0;
};

/****************************************************
 *                  InternalPage                    *
 ****************************************************/
class InternalPage : public NodePage
{
private:
  vector<InternalEntry *> entries;

public:
  InternalPage(AttrType attrType, InternalPage *parent);
  InternalPage(char *rawData, AttrType attrType, InternalPage *parent);
  ~InternalPage();

  RC getRawData(char *data) override;
  string toString() override;
};

/****************************************************
 *                    LeafPage                      *
 ****************************************************
 *  
 *  [isLeaf][next leaf pageNum][entries num][entry1][entry2]...
 * 
 */
class LeafPage : public NodePage
{
  // [isLeaf][next leaf pageNum][entries num]
  const static unsigned LEAF_PAGE_HEADER_SIZE = sizeof(bool) + sizeof(unsigned) * 2;
  vector<LeafEntry *> entries;
  // 0: no nextPn, since pageNum of meta page is 0
  PageNum *nextPn;

public:
  LeafPage(AttrType attrType, InternalPage *parent);
  LeafPage(char *rawData, AttrType attrType, InternalPage *parent);
  ~LeafPage();

  RC lookupAndInsert(char *key, unsigned len, RID rid);
  LeafEntry *lookup(char *key);
  RC insert(char *key, unsigned len, RID rid);

  RC getRawData(char *data) override;
  string toString() override;
};

/****************************************************
 *                  InternalEntry                   *
 ****************************************************/
class InternalEntry
{
public:
  char *key;
  unsigned size;
  PageNum ptrNum;

  InternalEntry(char *key, unsigned len, PageNum ptrNum);
  ~InternalEntry();
};

/****************************************************
 *                    LeafEntry                     *
 ****************************************************/
class LeafEntry
{
public:
  char *key;
  unsigned size;
  RID rid;
  bool isDeleted;

  LeafEntry(char *key, unsigned len, RID rid, bool isDeleted = false);
  ~LeafEntry();

  int compareTo(LeafEntry *that, AttrType attrType);
  string toString(AttrType attrType);
};

#endif
