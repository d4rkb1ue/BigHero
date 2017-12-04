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
    RC assertIXFileHandle(IXFileHandle &ixfileHandle);

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
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, bool withMeta = false) const;
};

/****************************************************
 *                  IX_ScanIterator                 *
 ****************************************************/
class IX_ScanIterator
{
  private:
    IXFileHandle *ixfileHandle;
    Attribute attr;
    char *lowKey;
    char *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    bool toGetFirst;
    // legal next should always > 0, since No.0 is Meta page
    PageNum next;
    vector<LeafEntry *> entries;
    char buffer[PAGE_SIZE];

  public:
    IX_ScanIterator();
    IX_ScanIterator(
        IXFileHandle *ixfileHandle, Attribute attr,
        char *lowKey, char *highKey,
        bool lowKeyInclusive, bool highKeyInclusive);
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);
    void getNextLeafPage();

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
    void rebuidTree(AttrType type);
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
    RC insert(char *key, RID rid);
    RC initNewTree(char *key, RID rid);
    RC insertToLeaf(char *key, RID rid);
    // insert parent may change the oldNode(newNode)->parent, if split/create new root
    // so must pass by ptr, not pageNum
    void insertToParent(NodePage *oldNode, char *key, NodePage *newNode);

    RC lazyRemove(char *key, RID rid);

    void updateRoot();

    PageNum getBeginLeaf();
    // PageNum getEndLeaf();
    // return 0: not found, since 0 can't be any data page
    PageNum findExactLeafPage(char *key);
    // PageNum findLessThanLeafPage();
    // PageNum findGreaterThanLeafPage();

    string toString(bool withMeta = false);
    string pageToString(PageNum pn, bool withMeta = false);
    // RC toJSON();
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
    int rootIsLeaf;

    MetaPage(char *rawData);
    MetaPage(PageNum rootPn, int rootIsLeaf);
    ~MetaPage(){};

    RC getRawData(char *data);
};

/****************************************************
 *                    NodePage                      *
 ****************************************************/
class NodePage
{
  public:
    // 0: not parent node. Aka, I'm the root
    PageNum parentPn;
    int isLeaf;
    AttrType attrType;
    unsigned size;
    // pass value after create obj, since it may not know when created; after persisting
    PageNum pageNum;

    NodePage(PageNum parentPn, AttrType attrType, unsigned size, int isLeaf);
    virtual ~NodePage(){};

    bool tooBig() { return size > PAGE_SIZE; };
    virtual RC getRawData(char *data) = 0;
    virtual string toString(bool withMeta = false) = 0;
};

/****************************************************
 *                  InternalPage                    *
 ****************************************************
 *
 * should NOT change this four meta! will effect outside call! isLeaf & parentPn must lay first.
 * Else, change BTree.insertToParent() also.
 * 
 *  [isLeaf][parent PageNum][entries num][entries...]
 * 
 * Internal Entry:
 *  [key value][Node(leaf/internal) pageNum]
 * 
 */
class InternalPage : public NodePage
{
  private:
    // [isLeaf][parent PageNum][entries num]
    const static unsigned INTERNAL_PAGE_HEADER_SIZE = sizeof(unsigned) * 3;

  public:
    vector<InternalEntry *> entries;

    InternalPage(AttrType attrType, PageNum parentPn);
    InternalPage(char *rawData, AttrType attrType);
    ~InternalPage();

    void initFirstEntry(PageNum left, char *midKey, unsigned len, PageNum right);
    void insertAfter(PageNum oldNode, char *midKey, unsigned len, PageNum newNode);
    void moveHalfTo(InternalPage &that);
    InternalEntry *dummyAndPopFirstKey();
    PageNum lookup(char *key, unsigned len);
    RC getRawData(char *data) override;
    string toString(bool withMeta = false) override;
};

/****************************************************
 *                    LeafPage                      *
 ****************************************************
 *  
 * should NOT change this four meta! will effect outside call! isLeaf & parentPn must lay first
 * Else, change BTree.insertToParent() also.
 * 
 *  [isLeaf][parent PageNum][next leaf pageNum][entries num][entries...]*
 * 
 * Leaf Entry:
 *  [key value][RID.pageNum][RID.slotNum][isDeleted]
 */
class LeafPage : public NodePage
{
  private:
    // [isLeaf][parent PageNum][next leaf pageNum][entries num]
    const static unsigned LEAF_PAGE_HEADER_SIZE = sizeof(unsigned) * 4;

  public:
    vector<LeafEntry *> entries;
    // 0: no nextPn, since pageNum of meta page is 0
    PageNum nextPn;

    LeafPage(AttrType attrType, PageNum parentPn);
    LeafPage(char *rawData, AttrType attrType);
    ~LeafPage();

    // RC lookupAndInsert(char *key, unsigned len, RID rid);
    // LeafEntry *lookup(char *key);
    // after insert, the size may over PAGE_SIZE, the caller should take care of it
    RC insert(char *key, unsigned len, RID rid);
    // exact key, with no trimmed
    RC lazyRemove(char *key, RID rid);
    void moveHalfTo(LeafPage &that);
    void cloneRangeFrom(char *key, unsigned len, bool inclusive, vector<LeafEntry *> &target);
    void cloneRangeAll(vector<LeafEntry *> &target);
    // void cloneRangeTo(char *key, unsigned len, vector<LeafEntry *> &target);
    // void cloneRangeFromTo(char *key, unsigned len, vector<LeafEntry *> &target);

    RC getRawData(char *data) override;
    string toString(bool withMeta = false) override;
};

/****************************************************
 *                  InternalEntry                   *
 ****************************************************/
class InternalEntry
{
  public:
    char *key;
    // if size = 0: dummy key, the first key locate at the first of a internal page
    // for ptr less than key[1]
    unsigned size;
    PageNum ptrNum;

    InternalEntry(char *key, unsigned len, PageNum ptrNum);
    ~InternalEntry();

    int compareTo(LeafEntry *that, AttrType attrType);
    InternalEntry *clone();
    string toString(AttrType attrType);
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
    int isDeleted;

    LeafEntry(char *key, unsigned len, RID rid = {0, 0}, int isDeleted = 0);
    ~LeafEntry();

    int compareTo(LeafEntry *that, AttrType attrType);
    LeafEntry *clone();
    string toString(AttrType attrType);
};

#endif
