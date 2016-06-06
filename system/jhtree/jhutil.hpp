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
    MAPPING *mruHead = nullptr, *mruTail = nullptr;

    void clear(int count)
    {
        if (-1 == count)
        {
            table.kill();
        }
        else
        {
            if (!mruHead)
            {
                WARNLOG("clear(%d) called with mruHead = null", count);
                return;
            }
            dbgassertex(mruTail);
            MAPPING *cur = mruTail;
            MAPPING *prev = mruTail->prev;
            loop
            {
                table.removeExact(cur);
                cur = prev;
                if (!cur)
                {
                    WARNLOG("mruHead = mruTail = null , tableCount=%d", table.count());
                    // no previous, consumed all.
                    mruHead = mruTail = nullptr;
                    break;
                }
                if (0 == --count)
                {
                    mruTail = cur;
                    mruTail->next = nullptr;
                    break;
                }
                prev = cur->prev;

                if (mruTail && (mruTail->prev == nullptr))
                    WARNLOG("... clear() .. mruTail->prev == null");
            }
        }
    }
    void clear(int count, CIArrayOf<MAPPING> &cleared)
    {
        if (-1 == count)
            clear(count);
        else
        {
            if (!mruHead)
            {
                WARNLOG("clear(%d) called with mruHead = null", count);
                return;
            }
            dbgassertex(mruTail);
            MAPPING *cur = mruTail;
            MAPPING *prev = mruTail->prev;
            loop
            {
                cleared.append(*LINK(cur));
                table.removeExact(cur);
                cur = prev;
                if (!cur)
                {
                    WARNLOG("mruHead = mruTail = null , tableCount=%d", table.count());
                    // no previous, consumed all.
                    mruHead = mruTail = nullptr;
                    break;
                }
                if (0 == --count)
                {
                    mruTail = cur;
                    mruTail->next = nullptr;
                    break;
                }
                prev = cur->prev;

                if (mruTail && (mruTail->prev == nullptr))
                    WARNLOG("... clear() .. mruTail->prev == null");
            }
        }
    }
    void _add(KEY &key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        CIArrayOf<MAPPING> free; // outside of spin, so not locked whilst deleting elements
        if (full())
            makeSpace(free);

        MAPPING *mapping = table.find(key);
        if (mapping)
        {
            if (promoteIfAlreadyPresent)
                _promote(mapping);
            //entry.Release();
        }
        else
        {
            mapping = new MAPPING(key, entry); // owns entry
            table.replace(*mapping);
            if (mruHead)
            {
                mapping->next = mruHead;
                mruHead->prev = mapping;
            }
            else
                mruTail = mapping;
            mruHead = mapping;
        }
        if (mruTail && (mruTail->prev == nullptr))
            WARNLOG("::add() ... mruTail->prev == null");
    }
    ENTRY *_query(KEY &key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(key);
        if (!mapping) return NULL;

        if (doPromote)
            _promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    ENTRY *_get(KEY &key, bool doPromote=true)
    {
        return LINK(_query(key, doPromote));
    }
    bool _remove(MAPPING *_mapping)
    {
        WARNLOG("_remove() called");
        Linked<MAPPING> mapping = _mapping;
        if (!table.removeExact(mapping))
            return false;
        MAPPING *prev = mapping->prev;
        MAPPING *next = mapping->next;
        if (prev)
            prev->next = next;
        if (next)
            next->prev = prev;
        if (mapping == mruHead)
            mruHead = next;
        if (mapping == mruTail)
            mruTail = prev;
        return true;
    }
    bool _remove(KEY &key)
    {
        Linked<MAPPING> mapping = table.find(key);
        if (!mapping)
            return false;
        return _remove(mapping);
    }
    void _promote(MAPPING *mapping)
    {
        if (mapping->prev == nullptr)
            return;
        MAPPING *prev = mapping->prev;
        MAPPING *next = mapping->next;
        if (prev)
        {
            if (mapping == mruTail)
            {
                mruTail = prev;
                PROGLOG("::promote() mruTail set to prev");
            }
            prev->next = next;
        }
        if (next)
            next->prev = prev;
        mapping->next = mruHead;
        mapping->prev = nullptr;
        mruHead->prev = mapping;
        mruHead = mapping;
        if (mruTail && (mruTail->prev == nullptr))
            WARNLOG("::promote() ... mruTail->prev == null");
    }
public:
    typedef SuperHashIteratorOf<MAPPING> CMRUIterator;

    IMPLEMENT_IINTERFACE;

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void add(KEY key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        SpinBlock b(lock);
        _add(key, entry, promoteIfAlreadyPresent);
    }
    void addByRef(KEY &key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        SpinBlock b(lock);
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
        SpinBlock b(lock);
        return _get(key, doPromote);
    }
    ENTRY *getByRef(KEY &key, bool doPromote=true)
    {
        SpinBlock b(lock);
        return _get(key, doPromote);
    }
    bool remove(MAPPING *mapping)
    {
        SpinBlock b(lock);
        return _remove(mapping);
    }
    bool remove(KEY key)
    {
        SpinBlock b(lock);
        return _remove(key);
    }
    bool removeByRef(KEY &key)
    {
        SpinBlock b(lock);
        return _remove(key);
    }
    void kill()
    {
        SpinBlock b(lock);
        clear(-1);
    }
    void promote(MAPPING *mapping)
    {
        SpinBlock b(lock);
        _promote(mapping);
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
