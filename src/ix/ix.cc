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
    // TODO: only care about int/real
    unsigned len = 4;
    char _key[4];
    const char *ckey = static_cast<const char *>(key);
    for (int i = 0; i < len; i++)
    {
        _key[i] = ckey[i];
    }
    return ixfileHandle.getTree(attribute.type)->insert(_key, len, rid);
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
    cout << endl
         << ixfileHandle.getTree(attribute.type)->toString() << endl;
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
    : tree(nullptr),
      _pfm(PagedFileManager::instance()),
      fileName(""),
      ixReadPageCounter(0),
      ixWritePageCounter(0),
      ixAppendPageCounter(0)
{
}
IXFileHandle::IXFileHandle(string fileName)
    : tree(nullptr),
      _pfm(PagedFileManager::instance()),
      fileName(fileName),
      ixReadPageCounter(0),
      ixWritePageCounter(0),
      ixAppendPageCounter(0)
{
    if (_pfm->openFile(fileName, _fileHandle) != 0)
    {
        cerr << "Open index file failed." << endl;
        exit(-1);
    }
}

IXFileHandle::~IXFileHandle()
{
    if (tree)
    {
        delete tree;
    }
}

BTree *IXFileHandle::getTree(AttrType attrType)
{
    if (!tree)
    {
        tree = new BTree(this, attrType);
    }
    return tree;
}

RC IXFileHandle::writePage(PageNum pageNum, char *data)
{
    ixWritePageCounter++;
    return _fileHandle.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(char *data)
{
    unsigned pageNum = 0;
    return appendPage(data, pageNum);
}

RC IXFileHandle::appendPage(char *data, PageNum &pageNum)
{
    ixAppendPageCounter++;
    if (_fileHandle.appendPage(data) != 0)
    {
        cerr << "append page in index file handle failed." << endl;
        exit(-1);
    }
    // the new page is the last page
    pageNum = getNumberOfPages() - 1;
    return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, char *data)
{
    ixReadPageCounter++;
    return _fileHandle.readPage(pageNum, data);
}

unsigned IXFileHandle::getNumberOfPages()
{
    return _fileHandle.getNumberOfPages();
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

/****************************************************
 *                       BTree                      *
 ****************************************************/
BTree::BTree(IXFileHandle *fileHandle, AttrType attrType)
    : _fileHandle(fileHandle),
      attrType(attrType)
{
    if (_fileHandle->getNumberOfPages() == 0)
    {
        root = nullptr;
        rootPn = 0;
        return;
    }
    cerr << "first insert shouldn't read any page." << endl;
    exit(-1);
    // // if file isn't empty, read meta page
    // _fileHandle->readPage(0, buffer);
    // MetaPage meta(buffer);
    // rootPn = meta.rootPn;

    // // read root node
    // _fileHandle->readPage(rootPn, buffer);
    // if (meta.rootIsLeaf)
    // {
    //     root = new LeafPage(buffer);
    // }
    // else
    // {
    //     root = new InternalPage(buffer);
    // }
}

BTree::~BTree()
{
    if (root)
    {
        if (root->isLeaf)
        {
            delete static_cast<LeafPage *>(root);
        }
        else
        {
            delete static_cast<InternalPage *>(root);
        }
        root = nullptr;
    }
}

bool BTree::isEmpty()
{
    return !root;
}

// insert

RC BTree::insert(char *key, unsigned len, RID rid)
{
    if (isEmpty())
    {
        return initNewTree(key, len, rid);
    }
    return insertToLeaf(key, len, rid);
}

RC BTree::initNewTree(char *key, unsigned len, RID rid)
{
    // appent meta page before append a leaf page
    updateRoot();

    LeafPage *node = new LeafPage(attrType, nullptr);
    node->insert(key, len, rid);
    root = node;

    // persist
    if (node->getRawData(buffer) != 0)
    {
        cerr << "when get raw data from node, size > PAGE_SIZE" << endl;
        exit(-1);
    }
    if (_fileHandle->appendPage(buffer, rootPn) != 0)
    {
        cerr << "append page fail when init new tree." << endl;
        exit(-1);
    }

    if (rootPn != 1)
    {
        cerr << endl
             << "new leaf page's num should be 1." << endl;
        exit(-1);
    }

    // update root pageNum in meta page
    updateRoot();
    return 0;
}

RC BTree::insertToLeaf(char *key, unsigned len, RID rid)
{
    return -1;
}

void BTree::updateRoot()
{
    // no meta page, no root
    if (!root)
    {
        if (_fileHandle->getNumberOfPages() > 0)
        {
            cerr << "append page when no root set, but already has some pages?" << endl;
            exit(-1);
        }

        // insert an pure 0 page
        memset(buffer, 0, PAGE_SIZE);
        _fileHandle->appendPage(buffer);
        return;
    }

    MetaPage meta(rootPn, root->isLeaf);
    meta.getRawData(buffer);
    _fileHandle->writePage(0, buffer);
}

// print

string BTree::toString()
{
    if (isEmpty())
    {
        return "[0_EMPTY_TREE_0]";
    }
    string s = "[" + root->toString() + "]";
    return s;
}

/****************************************************
 *                    MetaPage                      *
 ****************************************************/
MetaPage::MetaPage(PageNum rootPn, bool rootIsLeaf)
    : rootPn(rootPn),
      rootIsLeaf(rootIsLeaf)
{
}

MetaPage::MetaPage(char *rawData)
{
    memcpy(&rootPn, rawData, sizeof(unsigned));
    rawData += sizeof(unsigned);
    memcpy(&rootIsLeaf, rawData, sizeof(bool));
}

RC MetaPage::getRawData(char *data)
{
    memset(data, 0, PAGE_SIZE);
    memcpy(data, &rootPn, sizeof(unsigned));
    data += sizeof(unsigned);
    memcpy(data, &rootIsLeaf, sizeof(bool));
    return 0;
}

/****************************************************
 *                    NodePage                      *
 ****************************************************/
NodePage::NodePage(NodePage *parent, AttrType attrType, unsigned size, bool isLeaf)
    : parent(parent),
      isRoot(parent == nullptr),
      isLeaf(isLeaf),
      attrType(attrType),
      size(size)
{
}

/****************************************************
 *                  InternalPage                    *
 ****************************************************/
InternalPage::InternalPage(AttrType attrType, InternalPage *parent)
    : NodePage(parent, attrType, 0, false)
{
}
InternalPage::InternalPage(char *rawData, AttrType attrType, InternalPage *parent)
    : NodePage(parent, attrType, 0, false)
{
    cerr << "should not make internal now.." << endl;
    exit(-1);
}

InternalPage::~InternalPage()
{
    for (int i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

RC InternalPage::getRawData(char *data)
{
    return -1;
}

string InternalPage::toString()
{
    return "TODO";
}

/****************************************************
 *                    LeafPage                      *
 ****************************************************
 *  
 *  [isLeaf][next leaf pageNum][entries num][entries...]*
 * 
 */
LeafPage::LeafPage(AttrType attrType, InternalPage *parent)
    : NodePage(parent, attrType, LEAF_PAGE_HEADER_SIZE, true),
      nextPn(0)
{
}
LeafPage::LeafPage(char *rawData, AttrType attrType, InternalPage *parent)
    : NodePage(parent, attrType, LEAF_PAGE_HEADER_SIZE, true)
{
    cerr << "should not read leaf page from disk now" << endl;
    exit(-1);
    // unsigned entriesNum = 0;

    // memcpy(&isLeaf, rawData, sizeof(bool));
    // rawData += sizeof(bool);
    // memcpy(&nextPn, rawData, sizeof(PageNum));
    // rawData += sizeof(PageNum);
    // memcpy(&entriesNum, rawData, sizeof(unsigned));
    // rawData += sizeof(unsigned);

    // for (int i = 0; i < entriesNum; i++)
    // {
    //     entries.push_bask(new LeafEntry())
    // }
}

LeafPage::~LeafPage()
{
    for (int i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

RC LeafPage::lookupAndInsert(char *key, unsigned len, RID rid)
{
    return -1;
}

LeafEntry *LeafPage::lookup(char *key)
{
    return nullptr;
}

RC LeafPage::insert(char *key, unsigned len, RID rid)
{
    LeafEntry *e = new LeafEntry(key, len, rid);
    auto it2ptr = entries.begin();
    auto end = entries.end();
    for (; it2ptr != end && e->compareTo(*it2ptr, attrType) > 0; it2ptr++)
    {
    }
    entries.insert(it2ptr, e);
    size += len + sizeof(RID);
    return 0;
}

RC LeafPage::getRawData(char *data)
{
    if (tooBig())
    {
        return -1;
    }

    unsigned entryNum = entries.size();
    memset(data, 0, PAGE_SIZE);

    memcpy(data, &isLeaf, sizeof(bool));
    data += sizeof(bool);
    memcpy(data, &nextPn, sizeof(unsigned));
    data += sizeof(unsigned);
    memcpy(data, &entryNum, sizeof(unsigned));
    data += sizeof(unsigned);

    for (vector<LeafEntry *>::iterator it = entries.begin(); it != entries.end(); it++)
    {
        memcpy(data, (*it)->key, (*it)->size);
        data += (*it)->size;
        memcpy(data, &((*it)->rid), sizeof(RID));
        data += sizeof(RID);
    }
    return 0;
}

string LeafPage::toString()
{
    string s = "[";
    for (int i = 0; i < entries.size(); i++)
    {
        s += entries[i]->toString(attrType) + ", ";
    }
    s = s.substr(0, s.size() - 2);
    s += "]";
    return s;
}

/****************************************************
 *                  InternalEntry                   *
 ****************************************************/
InternalEntry::InternalEntry(char *key, unsigned len, PageNum ptrNum)
    : size(len),
      ptrNum(ptrNum)
{
    this->key = new char[size];
    memcpy(this->key, key, size);
}

InternalEntry::~InternalEntry()
{
    if (key)
    {
        delete[] key;
    }
}

/****************************************************
 *                    LeafEntry                     *
 ****************************************************/
LeafEntry::LeafEntry(char *key, unsigned len, RID rid, bool isDeleted)
    : size(len),
      rid(rid),
      isDeleted(isDeleted)
{
    this->key = new char[size];
    memcpy(this->key, key, size);
}

LeafEntry::~LeafEntry()
{
    if (key)
    {
        delete[] key;
    }
}

int LeafEntry::compareTo(LeafEntry *that, AttrType attrType)
{
    switch (attrType)
    {
    case TypeInt:
    {
        int int_this = 0, int_that = 0;
        memcpy(&int_this, key, sizeof(int));
        memcpy(&int_that, that->key, sizeof(int));
        return int_this - int_that;
    }
    case TypeReal:
    {
        float f_this = 0, f_that = 0;
        memcpy(&f_this, key, sizeof(float));
        memcpy(&f_that, that->key, sizeof(float));
        if (f_this - f_that < 0.000001)
        {
            return 0;
        }
        return f_this - f_that < 0 ? -1 : 1;
    }
    case TypeVarChar:
    {
        cerr << "TODO shouldn't compare VarChar now" << endl;
        exit(-1);
    }
    }
    return 0;
}

string LeafEntry::toString(AttrType attrType)
{
    string s_rid = "(" + to_string(rid.pageNum) + "," + to_string(rid.slotNum) + ")";
    switch (attrType)
    {
    case TypeInt:
    {
        int _int = 0;
        memcpy(&_int, key, sizeof(int));
        return to_string(_int) + ":" + s_rid;
    }
    case TypeReal:
    {
        float _f = 0.0;
        memcpy(&_f, key, sizeof(float));
        return to_string(_f) + ":" + s_rid;
    }
    case TypeVarChar:
    {
        return "[varchar..]";
    }
    }
}