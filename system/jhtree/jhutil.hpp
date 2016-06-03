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
class CMRUCacheOf : public CInterface, public IInterface
{
    SpinLock lock;
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
        loop
        {
            MAPPING *tail = mruList.dequeueTail();
            if (!tail)
                break;
            table.removeExact(tail);
            if ((-1 != count) && (0 == --count))
                break;
        }
    }
    void clear(int count, CIArrayOf<MAPPING> &cleared)
    {
        loop
        {
            MAPPING *tail = mruList.dequeueTail();
            if (!tail)
                break;
            MAPPING &_tail = *LINK(tail);
            cleared.append(_tail);
            table.removeExact(tail);
            if ((-1 != count) && (0 == --count))
                break;
        }
    }
    void _add(KEY &key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        CIArrayOf<MAPPING> free; // outside of spin, so not locked whilst deleting elements
        SpinBlock b(lock);
        if (full())
            makeSpace(free);

        MAPPING *mapping = table.find(key);
        if (mapping)
        {
            if (promoteIfAlreadyPresent)
                promote(mapping);
            //entry.Release();
        }
        else
        {
            mapping = new MAPPING(key, entry); // owns entry
            table.replace(*mapping);
            mruList.enqueueHead(mapping);
        }
    }
    ENTRY *_query(KEY &key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    ENTRY *_get(KEY &key, bool doPromote=true)
    {
        SpinBlock b(lock);
        return LINK(_query(key, doPromote));
    }
    bool _remove(KEY &key)
    {
        Linked<MAPPING> mapping = table.find(key);
        if (!mapping)
            return false;
        table.removeExact(mapping);
        mruList.dequeue(mapping);
        return true;
    }
public:
    typedef SuperHashIteratorOf<MAPPING> CMRUIterator;

    IMPLEMENT_IINTERFACE;

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void add(KEY key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        _add(key, entry, promoteIfAlreadyPresent);
    }
    void addByRef(KEY &key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        _add(key, entry, promoteIfAlreadyPresent);
    }
    ENTRY *query(KEY key, bool doPromote=true)
    {
        return _query(key, doPromote);
    }
    ENTRY *queryByRef(KEY &key, bool doPromote=true)
    {
        return _query(key, doPromote);
    }
    ENTRY *get(KEY key, bool doPromote=true)
    {
        return _get(key, doPromote);
    }
    ENTRY *getByRef(KEY &key, bool doPromote=true)
    {
        return _get(key, doPromote);
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
        return _remove(key);
    }
    bool removeByRef(KEY &key)
    {
        return _remove(key);
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
    virtual void makeSpace(CIArrayOf<MAPPING> &toFree) { }
    virtual bool full() { return false; }
    virtual void elementAdded(MAPPING *mapping) { }
    virtual void elementRemoved(MAPPING *mapping) { }
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
    virtual void makeSpace(CIArrayOf<MAPPING> &toFree)
    {
        SELF::clear(cacheOverflow, toFree);
    }
    virtual bool full()
    {
        return ((unsigned)-1 != cacheMax) && (SELF::table.count() > cacheMax + cacheOverflow);
    }
};

#endif
