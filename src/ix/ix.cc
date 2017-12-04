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
    unsigned len = 4;
    if (attribute.type == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }
    char c_key[len];
    memcpy(c_key, key, len);
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
    unsigned len = 4;
    if (attribute.type == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }
    char c_key[len];
    memcpy(c_key, key, len);
    if (ixfileHandle.getTree(attribute.type)->lazyRemove(c_key, rid) != 0)
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

    // for remove const parameters restriction
    char c_lowKey[PAGE_SIZE / 2];
    char c_highKey[PAGE_SIZE / 2];

    unsigned len = 4;
    if (lowKey)
    {
        if (attribute.type == TypeVarChar)
        {
            len = getVCSizeWithHead(lowKey);
        }
        memcpy(c_lowKey, lowKey, len);
    }
    if (highKey)
    {
        if (attribute.type == TypeVarChar)
        {
            len = getVCSizeWithHead(highKey);
        }
        memcpy(c_highKey, highKey, len);
    }
    ix_ScanIterator = IX_ScanIterator(
        &ixfileHandle, attribute,
        (lowKey ? c_lowKey : nullptr), (highKey ? c_highKey : nullptr),
        lowKeyInclusive, highKeyInclusive);
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, bool withMeta) const
{
    cerr << endl
         << ixfileHandle.getTree(attribute.type)->toString(withMeta) << endl;
}

unsigned IndexManager::getVCSizeWithHead(const void *_data)
{
    const char *data = (const char *)_data;
    unsigned s = 0;
    memcpy(&s, data, sizeof(unsigned));
    return s + sizeof(unsigned);
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
      highKeyInclusive(highKeyInclusive),
      next(0)
{
    unsigned len = 4;
    if (lowKey)
    {
        if (attr.type == TypeVarChar)
        {
            len = getVCSizeWithHead(lowKey);
        }
        this->lowKey = new char[len];
        memcpy(this->lowKey, lowKey, len);
    }
    if (highKey)
    {
        if (attr.type == TypeVarChar)
        {
            len = getVCSizeWithHead(highKey);
        }
        this->highKey = new char[len];
        memcpy(this->highKey, highKey, len);
    }

    if (lowKey)
    {
        next = ixfileHandle->getTree(attr.type)->findExactLeafPage(lowKey);
    }
    else
    {
        next = ixfileHandle->getTree(attr.type)->getBeginLeaf();
    }
    toGetFirst = true;
    getNextLeafPage();
}

IX_ScanIterator::~IX_ScanIterator()
{
    // Debug: May cause segment fault.
    if (lowKey)
    {
        // delete[] lowKey;
    }
    if (highKey)
    {
        // delete[] highKey;
    }
    for (unsigned i = 0; i < entries.size(); i++)
    {
        // delete entries[i];
    }
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (ixfileHandle->getTree(attr.type)->isEmpty())
    {
        return IX_EOF;
    }
    if (entries.size() == 0)
    {
        if (next == 0)
        {
            return IX_EOF;
        }
        else
        {
            getNextLeafPage();
        }
    }
    // do shifting : remove top isDeleted entries
    vector<LeafEntry *>::iterator it = entries.begin();
    while (entries.size() > 0 && it != entries.end() && (*it)->isDeleted == 1)
    {
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

    // check if > high key
    if (highKey)
    {
        unsigned len = 4;
        if (attr.type == TypeVarChar)
        {
            len = getVCSizeWithHead(highKey);
        }

        LeafEntry upperBound(highKey, len);
        if ((highKeyInclusive &&
             first->compareTo(&upperBound, attr.type) > 0) ||
            (!highKeyInclusive &&
             first->compareTo(&upperBound, attr.type) >= 0))
        {
            entries.clear();
            next = 0;
            return IX_EOF;
        }
    }
    // return copy
    rid = first->rid;
    memcpy(key, first->key, first->size);
    entries.erase(entries.begin());
    return 0;
}

void IX_ScanIterator::getNextLeafPage()
{
    if (entries.size() > 0)
    {
        cerr << "Before get new leaf page's entries, current scan iterator's entries should be empty" << endl;
        exit(-1);
    }
    if (next == 0)
    {
        // just don't insert anything to entries, getNextEntry() will take care to return -1
        return;
    }

    ixfileHandle->readPage(next, buffer);
    // all leaf pages in the scan iterator have no need to know their parent, since we only go next
    LeafPage lp(buffer, attr.type);
    if (toGetFirst && lowKey)
    {
        toGetFirst = false;
        lp.cloneRangeFrom(lowKey, lowKeyInclusive, entries);
    }
    else
    {
        lp.cloneRangeAll(entries);
    }
    next = lp.nextPn;
}

RC IX_ScanIterator::close()
{
    return 0;
}

unsigned IX_ScanIterator::getVCSizeWithHead(char *data)
{
    unsigned s = 0;
    memcpy(&s, data, sizeof(unsigned));
    return s + sizeof(unsigned);
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

    LeafPage *node = new LeafPage(attrType, 0);
    node->insert(key, rid);
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
    PageNum pn = findExactLeafPage(key);
    if (pn == 0)
    {
        cerr << "find 0?" << endl;
        return -1;
    }
    _fileHandle->readPage(pn, buffer);
    LeafPage lp(buffer, attrType);
    lp.pageNum = pn;

    if (lp.insert(key, rid) != 0)
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
        newLeaf.parentPn = lp.parentPn;

        // pop up
        insertToParent(&lp, newLeaf.entries[0]->key, &newLeaf);

        // since after pop up, the newLeaf's parent may change
        newLeaf.getRawData(buffer);
        _fileHandle->writePage(newPn, buffer);
    }

    lp.getRawData(buffer);
    _fileHandle->writePage(pn, buffer);
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
        newIndex->initFirstEntry(oldNode->pageNum, midKey, newNode->pageNum);

        // persist new data
        newIndex->getRawData(buffer);
        _fileHandle->writePage(rootPn, buffer);
    }
    else
    {
        PageNum parentPn = oldNode->parentPn;

        // get it's parent
        _fileHandle->readPage(parentPn, buffer);
        InternalPage parent(buffer, attrType);
        parent.pageNum = parentPn;

        // insert
        parent.insertAfter(oldNode->pageNum, midKey, newNode->pageNum);

        // may need resursive call
        if (parent.tooBig())
        {
            // set its parent in next resursive
            InternalPage newIp(attrType, 0);
            PageNum newIpPn = 0;

            parent.moveHalfTo(newIp);
            newIp.parentPn = parent.parentPn;

            // persist to get pageNum
            newIp.getRawData(buffer);
            _fileHandle->appendPage(buffer, newIpPn);
            newIp.pageNum = newIpPn;

            // set new children's parent to newIp's pageNum, no need caring about dummy key, key doesn't matter
            for (unsigned i = 0; i < newIp.entries.size(); i++)
            {
                PageNum n = newIp.entries[i]->ptrNum;
                _fileHandle->readPage(n, buffer);

                // since it can be leaf or internal, just override data[4-8]. both works
                memcpy(buffer + sizeof(int), &newIpPn, sizeof(PageNum));
                _fileHandle->writePage(n, buffer);

                // should also reset the page that are already in memory
                if (oldNode->pageNum == n)
                {
                    oldNode->parentPn = newIpPn;
                }
                if (newNode->pageNum == n)
                {
                    newNode->parentPn = newIpPn;
                }
            }

            // set dummy & pop-up to resursive call
            InternalEntry *midIndexKey = newIp.dummyAndPopFirstKey();
            insertToParent(&parent, midIndexKey->key, &newIp);

            // re-persist the new parentPn
            newIp.getRawData(buffer);
            _fileHandle->writePage(newIpPn, buffer);
        }

        // persist updating
        parent.getRawData(buffer);
        _fileHandle->writePage(parentPn, buffer);
    }
    updateRoot();
}

// remove

RC BTree::lazyRemove(char *key, RID rid)
{
    PageNum pn = findExactLeafPage(key);
    if (pn == 0)
    {
        return -1;
    }
    _fileHandle->readPage(pn, buffer);
    LeafPage lp(buffer, attrType);
    if (lp.lazyRemove(key, rid) != 0)
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
    if (isEmpty())
    {
        return 0;
    }
    if (root->isLeaf)
    {
        return rootPn;
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

        pn = ip.lookup(key);
    }
}

// print

string BTree::toString(bool withMeta)
{
    if (isEmpty())
    {
        return "[EMPTY_TREE]";
    }
    string s = "{\n" + pageToString(rootPn, withMeta);
    s += "\n}";
    return s;
}

string BTree::pageToString(PageNum pn, bool withMeta)
{
    _fileHandle->readPage(pn, buffer);

    // find if is leaf or not
    int isLeafBuffer = 0;
    memcpy(&isLeafBuffer, buffer, sizeof(int));
    if (isLeafBuffer == 1)
    {
        LeafPage leaf(buffer, attrType);
        leaf.pageNum = pn;
        return leaf.toString(withMeta);
    }
    InternalPage ip(buffer, attrType);
    ip.pageNum = pn;
    string s = ip.toString(withMeta) + ",\n";
    s += "\"childern\": [\n";
    for (unsigned i = 0; i < ip.entries.size(); i++)
    {
        s += pageToString(ip.entries[i]->ptrNum, withMeta) + ",\n";
    }
    s += "]";
    return s;
}

unsigned BTree::getVCSizeWithHead(char *data)
{
    unsigned s = 0;
    memcpy(&s, data, sizeof(unsigned));
    return s + sizeof(unsigned);
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

unsigned NodePage::getVCSizeWithHead(char *data)
{
    unsigned s = 0;
    memcpy(&s, data, sizeof(unsigned));
    return s + sizeof(unsigned);
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

    // for 1st dummy key, still has a 4 size key
    if (entriesNum > 0)
    {
        memcpy(keyBuffer, rawData, 4);
        rawData += 4;
        memcpy(&pnBuffer, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);
        entries.push_back(new InternalEntry(nullptr, 0, pnBuffer));
    }

    unsigned len = 4;
    for (unsigned i = 1; i < entriesNum; i++)
    {
        if (attrType == TypeVarChar)
        {
            len = getVCSizeWithHead(rawData);
        }

        memcpy(keyBuffer, rawData, len);
        rawData += len;
        memcpy(&pnBuffer, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);

        entries.push_back(new InternalEntry(keyBuffer, len, pnBuffer));
    }

    size = rawData - start;
}

InternalPage::~InternalPage()
{
    for (unsigned i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

void InternalPage::initFirstEntry(PageNum left, char *key, PageNum right)
{
    // actually first two entry, while the first entry is a dummy entry
    entries.push_back(new InternalEntry(nullptr, 0, left));
    // dummy key still has a 4 size key
    size += 4 + sizeof(PageNum);

    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }

    entries.push_back(new InternalEntry(key, len, right));
    size += len + sizeof(PageNum);
}

void InternalPage::insertAfter(PageNum oldNode, char *midKey, PageNum newNode)
{
    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(midKey);
    }

    vector<InternalEntry *>::iterator it = entries.begin();
    for (; it != entries.end() && (*it)->ptrNum != oldNode; it++)
        ;
    if (it == entries.end())
    {
        cerr << "insert after what? don't find the old node." << endl;
        exit(-1);
    }
    size += len + sizeof(PageNum);
    entries.insert(it + 1, new InternalEntry(midKey, len, newNode));
}

void InternalPage::moveHalfTo(InternalPage &that)
{
    unsigned count = entries.size();
    for (unsigned i = count / 2; i < count; i++)
    {
        that.entries.push_back(entries[i]);
        that.size += entries[i]->size + sizeof(PageNum);
    }

    // do seperately to avoid size changing in loop
    for (unsigned i = count / 2; i < count; i++)
    {
        size -= (entries[i]->size + sizeof(PageNum));
        entries.pop_back();
    }
}

InternalEntry *InternalPage::dummyAndPopFirstKey()
{
    if (entries.size() < 2)
    {
        cerr << "dummyAndPopFirstKey should apply to half size internal page!" << endl;
        exit(-1);
    }
    // at least its original size is 4, after drop will change the total size of this page
    size -= entries[0]->size - 4;
    InternalEntry *clone = entries[0]->clone();

    // dummy it!
    entries[0]->key = nullptr;
    entries[0]->size = 0;
    return clone;
}

PageNum InternalPage::lookup(char *key)
{
    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }

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
        exit(-1);
    }
    char *_data = data;

    unsigned entryNum = entries.size();
    memset(data, 0, PAGE_SIZE);

    // header
    memcpy(data, &isLeaf, sizeof(int));
    data += sizeof(int);
    memcpy(data, &parentPn, sizeof(PageNum));
    data += sizeof(PageNum);
    memcpy(data, &entryNum, sizeof(unsigned));
    data += sizeof(unsigned);

    for (unsigned i = 0; i < entries.size(); i++)
    {
        if (data - _data >= PAGE_SIZE - entries[i]->size - sizeof(unsigned))
        {
            cerr << "raw data grow oversize..." << endl;
            exit(-1);
        }

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

string InternalPage::toString(bool withMeta)
{
    string s;
    if (withMeta)
    {
        s += "[Internal -isLeaf" + to_string(isLeaf);
        s += " -parentPn" + to_string(parentPn);
        s += " -size" + to_string(size);
        s += " -pageNum" + to_string(pageNum);
        s += " -entries.size" + to_string(entries.size());
        s += "]\n";
    }
    s += "\"keys\": [";
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

    unsigned len = 4;
    for (unsigned i = 0; i < entriesNum; i++)
    {
        if (attrType == TypeVarChar)
        {
            len = getVCSizeWithHead(rawData);
        }
        memcpy(keyBuffer, rawData, len);
        rawData += len;
        memcpy(&ridBuffer.pageNum, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);
        memcpy(&ridBuffer.slotNum, rawData, sizeof(unsigned));
        rawData += sizeof(unsigned);
        memcpy(&isDeletedBuffer, rawData, sizeof(int));
        rawData += sizeof(int);

        entries.push_back(new LeafEntry(keyBuffer, len, ridBuffer, isDeletedBuffer));
    }

    size = rawData - start;
}

LeafPage::~LeafPage()
{
    for (unsigned i = 0; i < entries.size(); i++)
    {
        delete entries[i];
    }
}

RC LeafPage::insert(char *key, RID rid)
{
    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }
    LeafEntry *e = new LeafEntry(key, len, rid);
    vector<LeafEntry *>::iterator it2ptr = entries.begin();
    for (; it2ptr != entries.end() && e->compareTo(*it2ptr, attrType) >= 0; it2ptr++)
    {
    }
    entries.insert(it2ptr, e);
    size += len + sizeof(RID);
    return 0;
}

RC LeafPage::lazyRemove(char *key, RID rid)
{
    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }
    LeafEntry e(key, len);
    vector<LeafEntry *>::iterator it = entries.begin();

    // test either not equal or is already deleted
    for (;
         it != entries.end() &&
         (e.compareTo(*it, attrType) != 0 ||
          (*it)->isDeleted == 1 ||
          rid.pageNum != (*it)->rid.pageNum ||
          rid.slotNum != (*it)->rid.slotNum);
         it++)
        ;

    // not found
    if (it == entries.end())
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
        that.size += (entries[i]->size + sizeof(RID) + sizeof(unsigned));
    }

    // remove, use count to avoid entries.size change after pop_back()
    for (unsigned i = count / 2; i < count; i++)
    {
        size -= (entries[i]->size + sizeof(RID) + sizeof(unsigned));
        entries.pop_back();
    }
}

void LeafPage::cloneRangeFrom(char *key, bool inclusive, vector<LeafEntry *> &target)
{
    unsigned len = 4;
    if (attrType == TypeVarChar)
    {
        len = getVCSizeWithHead(key);
    }
    LeafEntry e(key, len);

    vector<LeafEntry *>::iterator it = entries.begin();

    if (inclusive)
    {
        for (; it != entries.end() && e.compareTo(*it, attrType) > 0; it++)
            ;
    }
    else
    {
        for (; it != entries.end() && e.compareTo(*it, attrType) >= 0; it++)
            ;
    }

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
        cerr << "LeafPage: raw data > PAGE_SIZE" << endl;
        exit(-1);
    }
    char *_data = data;

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
        if (data - _data > PAGE_SIZE - entries[i]->size - sizeof(RID) - sizeof(int))
        {
            cerr << "leaf page grow oversize." << endl;
            exit(-1);
        }
        memcpy(data, entries[i]->key, entries[i]->size);
        data += entries[i]->size;
        memcpy(data, &(entries[i]->rid), sizeof(RID));
        data += sizeof(RID);
        memcpy(data, &(entries[i]->isDeleted), sizeof(int));
        data += sizeof(int);
    }
    return 0;
}

string LeafPage::toString(bool withMeta)
{
    string s;
    if (withMeta)
    {
        s += "[Leaf -isLeaf" + to_string(isLeaf);
        s += " -parentPn" + to_string(parentPn);
        s += " -size" + to_string(size);
        s += " -pageNum" + to_string(pageNum);
        s += " -entries.size" + to_string(entries.size());
        s += " -nextPn" + to_string(nextPn);
        s += "]\n";
    }
    s += "[";
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

InternalEntry *InternalEntry::clone()
{
    InternalEntry *c = new InternalEntry(key, size, ptrNum);
    return c;
}

string InternalEntry::toString(AttrType attrType)
{
    if (size == 0)
    {
        string s = "[dummy: " + to_string(ptrNum);
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
        if (size == sizeof(unsigned))
        {
            return "[EMPTY_VARCHAR]";
        }
        // + '\0'
        char _c[size - sizeof(unsigned) + 1];
        memcpy(_c, key + sizeof(unsigned), size - sizeof(unsigned));
        _c[size - sizeof(unsigned)] = '\0';
        return string(_c);
        // return "[" + to_string(size) + "]" + string(key + sizeof(unsigned));
    }
    }
    return "ERR";
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
        // cerr << "in LeafEntry::compareTo, this = " << f_this << ", that = " << f_that << endl;
        if (f_this - f_that < 0.001 && f_that - f_this < 0.001)
        {
            return 0;
        }
        return ((f_this - f_that) < 0.0 ? -1 : 1);
    }
    case TypeVarChar:
    {
        // empty string
        if (size == sizeof(unsigned))
        {
            return that->size == sizeof(unsigned) ? 0 : -1;
        }
        if (that->size == sizeof(unsigned))
        {
            return 1;
        }

        // + '\0'
        char _c[size - sizeof(unsigned) + 1];
        memcpy(_c, key + sizeof(unsigned), size - sizeof(unsigned));
        _c[size - sizeof(unsigned)] = '\0';
        string _s(_c);

        char _c_that[that->size - sizeof(unsigned) + 1];
        memcpy(_c_that, that->key + sizeof(unsigned), that->size - sizeof(unsigned));
        _c_that[that->size - sizeof(unsigned)] = '\0';
        string _s_that(_c_that);

        return _s.compare(_s_that);
    }
    }
    return 0;
}

LeafEntry *LeafEntry::clone()
{
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
        if (size == sizeof(unsigned))
        {
            return "[EMPTY_VARCHAR]";
        }
        // + '\0'
        char _c[size - sizeof(unsigned) + 1];
        memcpy(_c, key + sizeof(unsigned), size - sizeof(unsigned));
        _c[size - sizeof(unsigned)] = '\0';

        return string(_c) + ":" + s_rid;
        ;
    }
    }
    return "";
}