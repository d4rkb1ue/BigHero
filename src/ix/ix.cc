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

RC IndexManager::assertIXFileHandle(IXFileHandle &ixfileHandle)
{
    if (ixfileHandle.fileName.size() == 0)
    {
        return -1;
    }
    return 0;
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
    if (assertIXFileHandle(ixfileHandle) != 0)
    {
        return -1;
    }
    // cerr << "openFile return 0, fileName.size() = " << ixfileHandle.fileName.size() << endl;
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    if (assertIXFileHandle(ixfileHandle) != 0)
    {
        return -1;
    }
    return ixfileHandle.closeFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (assertIXFileHandle(ixfileHandle) != 0)
    {
        return -1;
    }
    if (attribute.type == TypeVarChar)
    {
        cerr << "can't deal with var char now" << endl;
    }
    char c_key[4];
    memcpy(c_key, key, 4);
    if (ixfileHandle.getTree(attribute.type)->insert(c_key, rid) != 0)
    {
        return -1;
    }
    // every change, shoudle rebuid tree, or there's will an out-of-sync
    ixfileHandle.rebuidTree(attribute.type);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (assertIXFileHandle(ixfileHandle) != 0)
    {
        return -1;
    }
    if (attribute.type == TypeVarChar)
    {
        cerr << "can't deal with var char now" << endl;
    }
    char c_key[4];
    memcpy(c_key, key, 4);
    if (ixfileHandle.getTree(attribute.type)->lazyRemove(c_key) != 0)
    {
        return -1;
    }
    ixfileHandle.rebuidTree(attribute.type);
    return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    if (assertIXFileHandle(ixfileHandle) != 0)
    {
        return -1;
    }
    if (lowKey || highKey)
    {
        cerr << "TODO: can't deal with lowkey or highkey" << endl;
        return -1;
        // find by BTree then start
        // if findBTree == nullptr, return -1 or something
    }
    if (attribute.type == TypeVarChar)
    {
        cerr << "TODO: can't deal with VarChar key value" << endl;
        exit(-1);
    }
    char c_lowKey[4];
    char c_highKey[4];
    if (lowKey)
    {
        memcpy(c_lowKey, lowKey, 4);
    }
    if (highKey)
    {
        memcpy(c_highKey, highKey, 4);
    }
    ix_ScanIterator = IX_ScanIterator(
        &ixfileHandle, attribute,
        lowKey ? c_lowKey : nullptr, highKey ? c_highKey : nullptr,
        lowKeyInclusive, highKeyInclusive);
    return 0;
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
    : ixfileHandle(nullptr),
      lowKey(nullptr),
      highKey(nullptr),
      lowKeyInclusive(false),
      highKeyInclusive(false),
      next(0)
{
}

IX_ScanIterator::IX_ScanIterator(
    IXFileHandle *ixfileHandle, Attribute attr,
    char *lowKey, char *highKey,
    bool lowKeyInclusive, bool highKeyInclusive)
    : ixfileHandle(ixfileHandle),
      attr(attr),
      lowKey(nullptr),
      highKey(nullptr),
      lowKeyInclusive(lowKeyInclusive),
      highKeyInclusive(highKeyInclusive)
{
    if (lowKey || highKey)
    {
        cerr << "TODO: can't deal with lowkey or highkey" << endl;
        exit(-1);
    }
    if (attr.type == TypeVarChar)
    {
        cerr << "[IX_ScanIterator]TODO: can't deal with VarChar key value" << endl;
        exit(-1);
    }
    if (lowKey)
    {
        this->lowKey = new char[4];
        memcpy(this->lowKey, lowKey, 4);
    }
    if (highKey)
    {
        this->highKey = new char[4];
        memcpy(this->highKey, highKey, 4);
    }
    if (ixfileHandle->getTree(attr.type)->isEmpty())
    {
        cerr << "empty tree, nothing to do" << endl;
        exit(-1);
    }
    next = ixfileHandle->getTree(attr.type)->getBeginLeaf();
    getNextLeafPage();
}

IX_ScanIterator::~IX_ScanIterator()
{
    if (lowKey)
    {
        delete[] lowKey;
    }
    if (highKey)
    {
        delete[] highKey;
    }
    for (unsigned i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (lowKey || highKey)
    {
        cerr << "TODO: can't deal with lowkey or highkey" << endl;
        exit(-1);
    }
    if (entries.size() == 0)
    {
        // cerr << "Scan is end." << endl;
        if (next == 0)
        {
            return IX_EOF;
        }
        else
        {
            getNextLeafPage();
        }
    }
    // do shifting
    // remove all isDeleted
    vector<LeafEntry *>::iterator it = entries.begin();
    while (entries.size() > 0 && it != entries.end() && (*it)->isDeleted == 1)
    {
        cerr << "erasing entry: " << (*it)->toString(TypeInt) << endl;
        entries.erase(it);
        // be careful when delete in cycle!
        // it should not ++ since it's deleted, the new one will come to current position
        // it++;
    }
    if (entries.size() == 0)
    {
        return getNextEntry(rid, key);
    }
    LeafEntry *first = entries[0];
    rid = first->rid;
    memcpy(key, first->key, first->size);
    entries.erase(entries.begin());
    return 0;
}

void IX_ScanIterator::getNextLeafPage()
{
    if (lowKey || highKey)
    {
        cerr << "TODO: can't deal with lowkey or highkey" << endl;
        exit(-1);
    }
    if (!next)
    {
        cerr << "!next, check is required before call getNextLeafPage()" << endl;
        exit(-1);
    }
    ixfileHandle->readPage(next, buffer);
    // all leaf pages in the scan iterator have no need to know their parent, since we only go next
    LeafPage lp(buffer, attr.type);
    if (entries.size() > 0)
    {
        cerr << "Before get new leaf page's entries, current scan iterator's entries should be empty" << endl;
        exit(-1);
    }
    lp.cloneRangeAll(entries);
    next = lp.nextPn;
}

RC IX_ScanIterator::close()
{
    return 0;
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
        // cerr << "open index file failed. Set fileName to empty" << endl;
        this->fileName = "";
    }
}

IXFileHandle::~IXFileHandle()
{
    if (tree)
    {
        delete tree;
    }
}

BTree *IXFileHandle::getTree(AttrType type)
{
    if (!tree)
    {
        // cerr << "make new BTree..." << endl;
        tree = new BTree(this, type);
    }
    return tree;
}

void IXFileHandle::rebuidTree(AttrType type)
{
    if (tree)
    {
        delete tree;
        tree = new BTree(this, type);
    }
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
    // cerr << "first insert shouldn't read any page." << endl;
    // exit(-1);

    // if file isn't empty, read meta page
    _fileHandle->readPage(0, buffer);
    MetaPage meta(buffer);
    rootPn = meta.rootPn;

    // read root node
    _fileHandle->readPage(rootPn, buffer);
    if (meta.rootIsLeaf)
    {
        root = new LeafPage(buffer, attrType);
    }
    else
    {
        root = new InternalPage(buffer, attrType);
    }

    // cerr << endl
    //      << "finish reloading tree: " <<  endl
    //      << "rootRn - " << rootPn << endl
    //      << toString() << endl;
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

RC BTree::insert(char *key, RID rid)
{
    if (isEmpty())
    {
        return initNewTree(key, rid);
    }
    return insertToLeaf(key, rid);
}

RC BTree::initNewTree(char *key, RID rid)
{
    // appent meta page before append a leaf page
    updateRoot();

    if (attrType == TypeVarChar)
    {
        cerr << "can't deal with var char now." << endl;
    }
    LeafPage *node = new LeafPage(attrType, 0);
    node->insert(key, 4, rid);
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

RC BTree::insertToLeaf(char *key, RID rid)
{
    if (attrType == TypeVarChar)
    {
        cerr << "can't deal with var char now." << endl;
    }
    PageNum pn = findExactLeafPage(key);
    if (pn == 0)
    {
        cerr << "find 0?" << endl;
        return -1;
    }
    _fileHandle->readPage(pn, buffer);
    LeafPage lp(buffer, attrType);
    lp.pageNum = pn;

    if (lp.insert(key, 4, rid) != 0)
    {
        cerr << "insert to leaf failed." << endl;
        return -1;
    }
    if (lp.tooBig())
    {
        // do split
        LeafPage newLeaf(attrType, lp.parentPn);
        PageNum newPn = 0;

        // make new leaf page
        lp.moveHalfTo(newLeaf);
        newLeaf.nextPn = lp.nextPn;

        // persist
        newLeaf.getRawData(buffer);
        _fileHandle->appendPage(buffer, newPn);
        lp.nextPn = newPn;
        newLeaf.pageNum = newPn;

        // pop up
        insertToParent(&lp, newLeaf.entries[0]->key, &newLeaf);
        // since after pop up, the newLeaf's parent may change
        newLeaf.getRawData(buffer);
        _fileHandle->writePage(newPn, buffer);

        // cerr << endl
        //      << "after split: " << endl
        //      << "rootPn - " << rootPn << endl
        //      << "old pn - " << pn << endl
        //      << "new pn - " << newPn << endl;
    }
    lp.getRawData(buffer);
    _fileHandle->writePage(pn, buffer);
    // cerr << "after insert (" << rid.pageNum << ", " << rid.slotNum << "), page size = " << lp.size << endl;
    return 0;
}

void BTree::insertToParent(NodePage *oldNode, char *midKey, NodePage *newNode)
{
    // make sure the pageNum for both is valid
    if (oldNode->pageNum == 0 || newNode->pageNum == 0)
    {
        cerr << "insertToParent func need know both page's pageNum" << endl;
        exit(-1);
    }
    if (attrType == TypeVarChar)
    {
        cerr << "can't deal with var char now." << endl;
        exit(-1);
    }

    // is root
    if (oldNode->parentPn == 0)
    {
        // should new, since we'll keep it as root
        InternalPage *newIndex = new InternalPage(attrType, 0);
        root = newIndex;
        
        // append page and get pageNum
        newIndex->getRawData(buffer);
        _fileHandle->appendPage(buffer, rootPn);
        oldNode->parentPn = rootPn;
        newNode->parentPn = rootPn;
        newIndex->initFirstEntry(oldNode->pageNum, midKey, 4, newNode->pageNum);

        // persist new data
        newIndex->getRawData(buffer);
        _fileHandle->writePage(rootPn, buffer);
    }
    else
    {
        PageNum parentPn = oldNode->parentPn;
        newNode->parentPn = parentPn;

        // get it's parent
        _fileHandle->readPage(parentPn, buffer);
        InternalPage parent(buffer, attrType);
        
        // insert
        parent.insertAfter(oldNode->pageNum, midKey, 4, newNode->pageNum);

        // may need resursive call
        if (parent.tooBig())
        {
            cerr << "TODO: internal page over size" << endl;
            exit(-1);
        }

        // persist updating
        parent.getRawData(buffer);
        _fileHandle->writePage(parentPn, buffer);
    }
    updateRoot();
}

// remove

RC BTree::lazyRemove(char *key)
{
    PageNum pn = findExactLeafPage(key);
    if (pn == 0)
    {
        return -1;
    }
    _fileHandle->readPage(pn, buffer);
    LeafPage lp(buffer, attrType);
    if (lp.lazyRemove(key) != 0)
    {
        // not found
        return -1;
    }
    lp.getRawData(buffer);
    // write back
    _fileHandle->writePage(pn, buffer);
    return 0;
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

    MetaPage meta(rootPn, root->isLeaf ? 1 : 0);
    meta.getRawData(buffer);
    _fileHandle->writePage(0, buffer);
}

// find

PageNum BTree::getBeginLeaf()
{
    if (isEmpty())
    {
        return 0;
    }
    if (root->isLeaf)
    {
        return rootPn;
    }
    PageNum nodePn = rootPn;
    while (true)
    {
        _fileHandle->readPage(nodePn, buffer);

        // find if is leaf or not
        int isLeafBuffer = 0;
        memcpy(&isLeafBuffer, buffer, sizeof(int));
        if (isLeafBuffer == 1)
        {
            return nodePn;
        }
        InternalPage ip(buffer, attrType);
        if (ip.entries.size() < 2)
        {
            cerr << "empty internal page found?" << endl;
            exit(-1);
        }

        // left most dummy entry
        nodePn = ip.entries[0]->ptrNum;
    }
}

PageNum BTree::findExactLeafPage(char *key)
{
    // cout << "findExactLeafPage" << endl;
    if (isEmpty())
    {
        return 0;
    }
    if (root->isLeaf)
    {
        return rootPn;
    }
    if (attrType == TypeVarChar)
    {
        cerr << "can't deal with var char now." << endl;
        exit(-1);
    }
    PageNum pn = rootPn;
    while (true)
    {
        _fileHandle->readPage(pn, buffer);

        // find if is leaf or not
        int isLeafBuffer = 0;
        memcpy(&isLeafBuffer, buffer, sizeof(int));
        if (isLeafBuffer == 1)
        {
            // cerr << "return " << pn << endl;
            return pn;
        }
        InternalPage ip(buffer, attrType);
        if (ip.entries.size() < 2)
        {
            cerr << "empty internal page found?" << endl;
            exit(-1);
        }

        pn = ip.lookup(key, 4);
    }
}

// print

string BTree::toString()
{
    if (isEmpty())
    {
        return "[0_EMPTY_TREE_0]";
    }
    string s = "{\n" + pageToString(rootPn);
    s += "\n}";
    return s;
}

string BTree::pageToString(PageNum pn)
{
    _fileHandle->readPage(pn, buffer);
    // find if is leaf or not
    int isLeafBuffer = 0;
    memcpy(&isLeafBuffer, buffer, sizeof(int));
    if (isLeafBuffer == 1)
    {
        LeafPage leaf(buffer, attrType);
        return leaf.toString();
    }
    InternalPage ip(buffer, attrType);
    string s = ip.toString() + ",\n";
    s += "\"childern\": [\n";
    for (unsigned i = 0; i < ip.entries.size(); i++)
    {
        s += pageToString(ip.entries[i]->ptrNum) + ",\n";
    }
    s += "]";
    return s;
}

/****************************************************
 *                    MetaPage                      *
 ****************************************************/
const string MetaPage::META_PAGE = "META_PAGE:  ";
const string MetaPage::META_PAGE_END = "META_PAGE_END";

MetaPage::MetaPage(PageNum rootPn, int rootIsLeaf)
    : rootPn(rootPn),
      rootIsLeaf(rootIsLeaf)
{
}

MetaPage::MetaPage(char *rawData)
{
    // for meta page starter
    rawData += META_PAGE.size();
    memcpy(&rootPn, rawData, sizeof(unsigned));
    rawData += sizeof(unsigned);
    memcpy(&rootIsLeaf, rawData, sizeof(int));
}

RC MetaPage::getRawData(char *data)
{
    memset(data, 0, PAGE_SIZE);
    memcpy(data, META_PAGE.c_str(), META_PAGE.size());
    memcpy(data + PAGE_SIZE - META_PAGE_END.size(), META_PAGE_END.c_str(), META_PAGE_END.size());
    data += META_PAGE.size();
    memcpy(data, &rootPn, sizeof(unsigned));
    data += sizeof(unsigned);
    memcpy(data, &rootIsLeaf, sizeof(int));
    return 0;
}

/****************************************************
 *                    NodePage                      *
 ****************************************************/
NodePage::NodePage(PageNum parentPn, AttrType attrType, unsigned size, int isLeaf)
    : parentPn(parentPn),
      isLeaf(isLeaf),
      attrType(attrType),
      size(size),
      pageNum(0)
{
}

/****************************************************
 *                  InternalPage                    *
 ****************************************************
 *
 *  [isLeaf][parent PageNum][entries num][entries...]
 * 
 * Internal Entry:
 *  [key value][Node(leaf/internal) pageNum]
 * 
 */
InternalPage::InternalPage(AttrType attrType, PageNum parentPn)
    : NodePage(parentPn, attrType, INTERNAL_PAGE_HEADER_SIZE, 0)
{
}
InternalPage::InternalPage(char *rawData, AttrType attrType)
    : NodePage(parentPn, attrType, 0, 0)
{
    if (attrType == TypeVarChar)
    {
        cerr << endl
             << "TODO: can't read varchar from disk" << endl;
        exit(-1);
    }

    char *start = rawData;
    unsigned entriesNum = 0;
    char keyBuffer[PAGE_SIZE];
    PageNum pnBuffer = 0;

    memcpy(&(this->isLeaf), rawData, sizeof(int));
    rawData += sizeof(int);
    memcpy(&(this->parentPn), rawData, sizeof(PageNum));
    rawData += sizeof(PageNum);
    memcpy(&entriesNum, rawData, sizeof(unsigned));
    rawData += sizeof(unsigned);

    for (unsigned i = 0; i < entriesNum; i++)
    {
        // dummy key still has a 4 size key
        memcpy(keyBuffer, rawData, 4);
        rawData += 4;
        memcpy(&pnBuffer, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);

        entries.push_back(new InternalEntry(keyBuffer, 4, pnBuffer));
    }
    size = rawData - start;
    // cerr << "finish reading a InternalPage: " << toString();
}

InternalPage::~InternalPage()
{
    for (unsigned i = 0; i < entries.size(); i++)
    {
       delete entries[i];
    }
}

void InternalPage::initFirstEntry(PageNum left, char *key, unsigned len, PageNum right)
{
    // actually first two entry, while the first entry is a dummy entry
    entries.push_back(new InternalEntry(nullptr, 0, left));
    size += 4 + sizeof(unsigned);

    entries.push_back(new InternalEntry(key, len, right));
    size += len + sizeof(unsigned);
}

void InternalPage::insertAfter(PageNum oldNode, char *midKey, unsigned len, PageNum newNode)
{
    vector<InternalEntry *>::iterator it = entries.begin();
    for (; it != entries.end() && (*it)->ptrNum != oldNode; it++);
    if (it == entries.end())
    {
        cerr << "insert after what? don't find the old node." << endl;
        exit(-1);
    }
    entries.insert(it + 1, new InternalEntry(midKey, len, newNode));
}

PageNum InternalPage::lookup(char *key, unsigned len)
{
    LeafEntry e(key, len);
    // 0th is dummy key, can't compare. so start from 1
    for (unsigned i = 1; i < entries.size(); i++)
    {
        if (entries[i]->compareTo(&e, attrType) > 0)
        {
            return entries[i - 1]->ptrNum;
        }
    }
    // reach the last one
    return entries[entries.size() - 1]->ptrNum;
}

RC InternalPage::getRawData(char *data)
{
    if (tooBig())
    {
        cerr << "can't get raw data, since to big" << endl;
        return -1;
    }
    unsigned entryNum = entries.size();
    memset(data, 0, PAGE_SIZE);

    memcpy(data, &isLeaf, sizeof(int));
    data += sizeof(int);
    memcpy(data, &parentPn, sizeof(PageNum));
    data += sizeof(PageNum);
    memcpy(data, &entryNum, sizeof(unsigned));
    data += sizeof(unsigned);

    for (unsigned i = 0; i < entries.size(); i++)
    {
        // for dummy key, [0, ptr]
        if (entries[i]->size == 0)
        {
            if (i != 0)
            {
                cerr << "dummy key should be the first one." << endl;
                exit(-1);
            }
            memset(data, 0, 4);
            data += 4;
        }
        else
        {
            memcpy(data, entries[i]->key, entries[i]->size);
            data += entries[i]->size;
        }
        memcpy(data, &(entries[i]->ptrNum), sizeof(unsigned));
        data += sizeof(unsigned);
    }
    return 0;
}

string InternalPage::toString()
{
    string s = "\"keys\": [";
    for (unsigned i = 0; i < entries.size(); i++)
    {
        s += entries[i]->toString(attrType) + ",";
    }
    s = s.substr(0, s.size() - 1) + "]";
    return s;
}

/****************************************************
 *                    LeafPage                      *
 ****************************************************
 *  
 *  [isLeaf][parent PageNum][next leaf pageNum][entries num][entries...]*
 * 
 * Leaf Entry:
 *  [key value][RID.pageNum][RID.slotNum][isDeleted]
 */
LeafPage::LeafPage(AttrType attrType, PageNum parentPn)
    : NodePage(parentPn, attrType, LEAF_PAGE_HEADER_SIZE, 1),
      nextPn(0)
{
}
LeafPage::LeafPage(char *rawData, AttrType attrType)
    : NodePage(0, attrType, 0, 1)
{
    // cerr << "should not read leaf page from disk now" << endl;
    // exit(-1);

    if (attrType == TypeVarChar)
    {
        cerr << endl
             << "TODO: can't read varchar from disk" << endl;
        exit(-1);
    }

    char *start = rawData;
    unsigned entriesNum = 0;
    char keyBuffer[PAGE_SIZE];
    RID ridBuffer = {0, 0};
    int isDeletedBuffer = 0;

    memcpy(&isLeaf, rawData, sizeof(int));
    rawData += sizeof(int);
    memcpy(&(this->parentPn), rawData, sizeof(PageNum));
    rawData += sizeof(PageNum);
    memcpy(&nextPn, rawData, sizeof(PageNum));
    rawData += sizeof(PageNum);
    memcpy(&entriesNum, rawData, sizeof(unsigned));
    rawData += sizeof(unsigned);

    for (unsigned i = 0; i < entriesNum; i++)
    {
        // TODO: only care about the TypeInt & TypeReal
        memcpy(keyBuffer, rawData, 4);
        rawData += 4;
        memcpy(&ridBuffer.pageNum, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);
        memcpy(&ridBuffer.slotNum, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);
        memcpy(&isDeletedBuffer, rawData, sizeof(int));
        rawData += sizeof(int);

        entries.push_back(new LeafEntry(keyBuffer, 4, ridBuffer, isDeletedBuffer));
    }
    size = rawData - start;
    // cerr << "Finish reading leaf page from rawData, size = " << size << ", result: " << endl;
    // cerr << toString() << endl;
}

LeafPage::~LeafPage()
{
    for (unsigned i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

RC LeafPage::insert(char *key, unsigned len, RID rid)
{
    LeafEntry *e = new LeafEntry(key, len, rid);
    vector<LeafEntry *>::iterator it2ptr = entries.begin();
    for (; it2ptr != entries.end() && e->compareTo(*it2ptr, attrType) > 0; it2ptr++)
    {
    }
    entries.insert(it2ptr, e);
    size += len + sizeof(RID);
    return 0;
}

RC LeafPage::lazyRemove(char *key)
{
    if (attrType == TypeVarChar)
    {
        cerr << "can't deal with var char now." << endl;
        exit(-1);
    }
    LeafEntry e(key, 4);
    vector<LeafEntry *>::iterator it = entries.begin();
    for (; it != entries.end() && e.compareTo(*it, attrType) != 0; it++);
    // not found or is already deleted
    if (it == entries.end() || (*it)->isDeleted == 1)
    {
        return -1;
    }
    (*it)->isDeleted = 1;
    return 0;
}

void LeafPage::moveHalfTo(LeafPage &that)
{
    unsigned count = entries.size();
    for (unsigned i = count / 2; i < count; i++)
    {
        that.entries.push_back(entries[i]);
    }

    // remove, use count to avoid entries.size change after pop_back()
    for (unsigned i = count / 2; i < count; i++)
    {
        size -= (entries[i]->size + sizeof(RID) + sizeof(unsigned));
        entries.pop_back();
    }
}

void LeafPage::cloneRangeFrom(char *key, unsigned len, vector<LeafEntry *> &target)
{
    LeafEntry e(key, len);
    vector<LeafEntry *>::iterator it = entries.begin();
    for (; it != entries.end() && e.compareTo(*it, attrType) != 0; it++);
    for (; it != entries.end(); it++)
    {
        target.push_back((*it)->clone());
    }
}

void LeafPage::cloneRangeAll(vector<LeafEntry *> &target)
{
    vector<LeafEntry *>::iterator it = entries.begin();
    for (; it != entries.end(); it++)
    {
        target.push_back((*it)->clone());
    }
}

RC LeafPage::getRawData(char *data)
{
    if (tooBig())
    {
        cerr << "raw data > PAGE_SIZE" << endl;
        exit(-1);
    }

    unsigned entryNum = entries.size();
    memset(data, 0, PAGE_SIZE);

    memcpy(data, &isLeaf, sizeof(int));
    data += sizeof(int);
    memcpy(data, &(this->parentPn), sizeof(PageNum));
    data += sizeof(PageNum);
    memcpy(data, &nextPn, sizeof(PageNum));
    data += sizeof(PageNum);
    memcpy(data, &entryNum, sizeof(unsigned));
    data += sizeof(unsigned);

    for (unsigned i = 0; i < entries.size(); i++)
    {
        memcpy(data, entries[i]->key, entries[i]->size);
        data += entries[i]->size;
        memcpy(data, &(entries[i]->rid), sizeof(RID));
        data += sizeof(RID);
        memcpy(data, &(entries[i]->isDeleted), sizeof(int));
        data += sizeof(int);
    }
    return 0;
}

string LeafPage::toString()
{
    string s = "[";
    for (unsigned i = 0; i < entries.size(); i++)
    {
        if (entries[i]->isDeleted)
            continue;
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
    if (len > 0)
    {
        this->key = new char[size];
        memcpy(this->key, key, size);
    }
    else
    {
        // never forget this->
        this->key = nullptr;
    }
}

InternalEntry::~InternalEntry()
{
    if (key)
    {
        delete[] key;
    }
}

int InternalEntry::compareTo(LeafEntry *that, AttrType attrType)
{
    // dummy key
    if (size == 0)
    {
        return -1;
    }
    LeafEntry _this(key, size);
    return _this.compareTo(that, attrType);
}

string InternalEntry::toString(AttrType attrType)
{
    if (size == 0)
    {
        string s = "[dummy: " + ptrNum;
        s += "]";
        return s;
    }
    int _int = 0;
    float _f = 0.0;
    switch (attrType)
    {
    case TypeInt:
    {
        memcpy(&_int, key, sizeof(int));
        return to_string(_int);
    }
    case TypeReal:
    {
        memcpy(&_f, key, sizeof(float));
        return to_string(_f);
    }
    case TypeVarChar:
    {
        return "[VarChar..]";
    }
    }
    return "";
}

/****************************************************
 *                    LeafEntry                     *
 ****************************************************/
LeafEntry::LeafEntry(char *key, unsigned len, RID rid, int isDeleted)
    : size(len),
      rid(rid),
      isDeleted(isDeleted)
{
    this->key = new char[size];
    memcpy(this->key, key, size);
}

LeafEntry::~LeafEntry()
{
    if (key && size > 0)
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

LeafEntry *LeafEntry::clone()
{
    // cerr << "cloning: " << *((int *)key) << ", " << rid.pageNum << ", " << rid.slotNum << endl;
    LeafEntry *e = new LeafEntry(key, size, rid, isDeleted);
    return e;
}

string LeafEntry::toString(AttrType attrType)
{
    string s_rid = "(" + to_string(rid.pageNum) + "," + to_string(rid.slotNum) + ")";
    int _int = 0;
    float _f = 0.0;
    switch (attrType)
    {
    case TypeInt:
    {
        memcpy(&_int, key, sizeof(int));
        return to_string(_int) + ":" + s_rid;
    }
    case TypeReal:
    {
        memcpy(&_f, key, sizeof(float));
        return to_string(_f) + ":" + s_rid;
    }
    case TypeVarChar:
    {
        return "[VarChar..]";
    }
    }
    return "";
}