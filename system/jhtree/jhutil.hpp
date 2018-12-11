/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef JHUTIL_HPP
#define JHUTIL_HPP

#include "jlib.hpp"
#include "jqueue.tpp"
#include "jhtree.hpp"

// TABLE should be SuperHashTable derivative to contain MAPPING's
// MAPPING should be something that constructs with (KEY, ENTRY) and impl. query returning ref. to ENTRY
template <class KEY, class ENTRY, class MAPPING, class TABLE>
class CMRUCacheOf : public CInterface//, public IInterface
{
protected:
    class DTABLE : public TABLE
    {
        CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE> &owner;
    public:
        DTABLE(CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE> &_owner) : owner(_owner) { }
        virtual void onAdd(void *mapping) { owner.elementAdded((MAPPING *)mapping); TABLE::onAdd(mapping); }
        virtual void onRemove(void *mapping) { owner.elementRemoved((MAPPING *)mapping); TABLE::onRemove(mapping); }
    } table;
    QueueOf<MAPPING, false> mruList;

    void clear(int count)
    {
        for (;;)
        {
            MAPPING *tail = mruList.dequeueTail();
            if (!tail)
                break;
            table.removeExact(tail);
            if ((-1 != count) && (0 == --count))
                break;
        }
    }
public:
    typedef SuperHashIteratorOf<MAPPING> CMRUIterator;

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void add(KEY key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        if (full())
            makeSpace();

        MAPPING *mapping = table.find(key);
        if (mapping)
        {
            if (promoteIfAlreadyPresent)
                promote(mapping);
        }
        else
        {
            mapping = new MAPPING(key, entry); // owns entry
            table.replace(*mapping);
            mruList.enqueueHead(mapping);
        }
    }
    ENTRY *query(KEY key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    ENTRY *get(KEY key, bool doPromote=true)
    {
        return LINK(query(key, doPromote));
    }
    bool remove(MAPPING *_mapping)
    {   
        Linked<MAPPING> mapping = _mapping;
        if (!table.removeExact(_mapping))
            return false;
        mruList.dequeue(mapping);
        return true;
    }
    bool remove(KEY key)
    {
        Linked<MAPPING> mapping = table.find(key);
        if (!mapping)
            return false;
        table.removeExact(mapping);
        mruList.dequeue(mapping);
        return true;
    }
    void kill() { clear(-1); }
    void promote(MAPPING *mapping)
    {
        mruList.dequeue(mapping); // will still be linked in table
        mruList.enqueueHead(mapping);
    }
    CMRUIterator *getIterator()
    {
        return new SuperHashIteratorOf<MAPPING>(table);
    }
    virtual void makeSpace() { }
    virtual bool full() { return false; }
    virtual void elementAdded(MAPPING *mapping) { }
    virtual void elementRemoved(MAPPING *mapping) { }
};

interface IMemCallback
{
    virtual void *allocate(size_t) = 0;
    virtual void release(void *) = 0;
};
template <class KEY, class VALUE>
class CMRUCache2Of : public CInterface
{
    struct HTEntry
    {
        KEY key;
        unsigned hash;
        HTEntry *next;
        HTEntry *prev;
        VALUE *value; // NB: if null, HT element is empty
    };
    HTEntry *table = nullptr;
    HTEntry *mru = nullptr;
    HTEntry *lru = nullptr;
    unsigned htn = 0;
    unsigned n = 0;
    struct MemCb : implements IMemCallback
    {
        virtual void *allocate(size_t sz) override
        {
            return malloc(sz);
        }
        virtual void release(void *ptr) override
        {
            free(ptr);
        }
    } defaultMemCb;
    IMemCallback *memCallback = &defaultMemCb;
    static unsigned defaultHashFunc(const void *key)
    {
        return hashc((const unsigned char *)key, sizeof(KEY), 0);
    }
    std::function<unsigned(const void *)> hashFunc = defaultHashFunc;


    inline unsigned getHash(const KEY &key) const
    {
        return hashc((const unsigned char *)&key, sizeof(key), 0);
    }
    void expand()
    {
        htn += htn;
        HTEntry *newTable = (HTEntry *)memCallback->allocate(((memsize_t)htn)*sizeof(HTEntry));
        // could check capacity and see if higher pow2
        memset(newTable, 0, sizeof(HTEntry)*htn);
        HTEntry *cur = mru;
        HTEntry *lastNew = nullptr;
        while (cur)
        {
            unsigned i = hashFunc(&cur->key) & (htn - 1);
            while (newTable[i].value)
            {
                i++;
                if (i==htn)
                    i = 0;
            }
            newTable[i] = *cur;
            lastNew = newTable+i;
            cur = cur->next;
            if (lastNew)
                lastNew->next = cur;
            cur->prev = lastNew;
            cur->next = nullptr;
        }
        lru = lastNew;
        memCallback->release(table);
        table = newTable;
    }
protected:
    // returns position of match OR empty pos. to use if not found
    inline unsigned findPos(const KEY &key, unsigned h)
    {
        unsigned i = h & (htn - 1);
        while (true)
        {
            HTEntry *ht = table+i;
            if (nullptr == ht->value) // IOW - HT[i] is empty, irrelevant what key/rest members are
                return i;
            if ((ht->hash==h) && (key == ht->key)) // NB: not really necessary to store hash and check it, but if key comparison was expensive, a quick check on hash 1st is a win
                return i;
            if (++i==htn)
                i = 0;
        }
    }
    // same as above, except returns NotFound if no match
    inline unsigned findMatch(const KEY &key, unsigned h)
    {
        unsigned i = h & (htn - 1);
        while (true)
        {
            HTEntry *ht = table+i;
            if (nullptr == ht->value) // IOW - table[i] is empty, irrelevant what key/rest members are
                return NotFound;
            if ((ht->hash==h) && (key == ht->key)) // NB: not really necessary to store hash and check it, but if key comparison was expensive, a quick check on hash 1st is a win
                return i;
            if (++i==htn)
                i = 0;
        }
    }
    void removeMRUEntry(HTEntry *ht)
    {
        HTEntry *prev = ht->prev;
        HTEntry *next = ht->next;
        if (prev)
        {
            prev->next = next;
            if (lru == ht)
                lru = prev;
        }
        if (next)
        {
            next->prev = prev;
            if (mru == ht)
                mru = next;
        }
    }
    void removeEntry(HTEntry *ht)
    {
        remoteMRUEntry(ht);
        ht->value = nullptr;
        elementRemoved(ht->key, ht->value);
    }
public:
    CMRUCache2Of<KEY, VALUE>()
    {
        htn = 8;
        n = 0;
        table = (HTEntry *)memCallback->allocate(((memsize_t)htn)*sizeof(HTEntry));
        // could check capacity and see if higher pow2
        memset(table, 0, sizeof(HTEntry)*htn);
    }
    void setMemoryCallback(IMemCallback *_memCallback)
    {
        memCallback = _memCallback;
    }
    void setHashFunc(std::function<unsigned(const void *)> _hashFunc)
    {
        hashFunc = _hashFunc;
    }
    void add(const KEY &key, VALUE *value, bool promoteIfAlreadyPresent=true)
    {
        unsigned h = getHash(key);
        unsigned e = findPos(key, h);
        HTEntry *ht = table+e;
        if (ht->value)
        {
            if (promoteIfAlreadyPresent)
                promote(ht);
        }
        else
            addNew(e, h, key, value);
    }
    VALUE *query(const KEY &key, bool doPromote=true)
    {
        unsigned h = getHash(key);
        unsigned e = findMatch(key, h);
        if (NotFound == e)
            return nullptr;
        HTEntry *ht = table+e;
        if (doPromote)
            promote(ht);
        return ht->value;
    }
    VALUE *get(KEY key, bool doPromote=true)
    {
        return LINK(query(key, doPromote));
    }
    VALUE *getOrAdd(const KEY &key, VALUE *value, bool doPromote=true)
    {
        unsigned h = getHash(key);
        unsigned e = findMatch(key, h);
        if (NotFound == e)
        {
            addNew(e, h, key, value);
            return nullptr;
        }
        HTEntry *ht = table+e;
        if (doPromote)
            promote(ht);
        return ht->value;
    }
    bool remove(const KEY &key) // NB: not thread safe, need to protect if calling MT
    {
        unsigned h = getHash(key);
        unsigned e = findMatch(key, h);
        if (NotFound == e)
            return false;
        HTEntry *ht = table+e;
        removeEntry(ht);
        return true;
    }
    void kill()
    {
        HTEntry *cur = mru;
        while (cur)
        {
            elementRemoved(cur->key, cur->value);
            cur = cur->next;
        }
        memset(table, 0, sizeof(HTEntry)*htn);
    }
    void promote(HTEntry *ht)
    {
        if (nullptr == ht->prev) // already at top
            return;
        removeMRUEntry(ht); // NB: remains in table, meaning it is still thread safe to read from table during a promote
        HTEntry *oldMRU = mru;
        mru = ht;
        mru->next = oldMRU;
        oldMRU->prev = mru;
    }
    void shrink(std::function<bool()> shrinkUntilCondFunc) // NB: not thread safe, need to protect if calling MT
    {
        do
        {
            if (nullptr == lru)
                break;
            removeEntry(lru);
        }
        while (shrinkUntilCondFunc());
    }
    virtual void addNew(unsigned i, unsigned h, const KEY &key, VALUE *value) // Called by add() if necessary. NB: not thread safe, need to protect if calling MT
    {
        HTEntry *ht;
        if (n >= ((htn * 3) / 4)) // if over 75% full
        {
            expand();
            // re-find empty slot
            i = h & (htn - 1);
            while (true)
            {
                ht = &table[i];
                if (nullptr == ht->value)
                    break;
                if (++i==htn)
                    i = 0;
            }
        }
        else
            ht = &table[i];
        ht->value = value;
        ht->hash = h;
        ht->key = key;
        mru->prev = ht;
        mru = ht;
        if (!lru)
            lru = ht;
        n++;
        elementAdded(key, value);
    }
    virtual void elementAdded(const KEY &key, VALUE *value) { }
    virtual void elementRemoved(const KEY &key, VALUE *value) { }
};

template <class KEY, class ENTRY, class MAPPING, class TABLE>
class CMRUCacheMaxCountOf : public CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>
{
    typedef CMRUCacheMaxCountOf<KEY, ENTRY, MAPPING, TABLE> SELF;
    unsigned cacheMax;
    unsigned cacheOverflow; // # to clear if full
public:
    CMRUCacheMaxCountOf(unsigned _cacheMax) : cacheMax(_cacheMax) { cacheOverflow = 1; }
    unsigned setCacheLimit(unsigned _cacheMax)
    {
        if (SELF::table.count() > _cacheMax)
            this->clear(_cacheMax - SELF::table.count());
        unsigned oldCacheMax = cacheMax;
        cacheMax = _cacheMax;
        return oldCacheMax;
    }
    virtual void makeSpace()
    {
        SELF::clear(cacheOverflow);
    }
    virtual bool full()
    {
        return ((unsigned)-1 != cacheMax) && (SELF::table.count() > cacheMax + cacheOverflow);
    }
};

#endif
