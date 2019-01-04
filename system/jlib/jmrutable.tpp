/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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



#ifndef JMRUTABLE_TPP
#define JMRUTABLE_TPP

#include "platform.h"
#include <stdio.h>
#include <vector>
#include <functional>
#include "jiface.hpp"
#include "jobserve.hpp"
#include "jiter.hpp"
#include "jsuperhash.hpp"

#include "jqueue.tpp"


template <class KEY, class VALUE, class HASHER = std::hash<KEY>>
class CMRUHashTable : public CInterface
{
    std::function<void *(size_t)> allocFunc=nullptr;
    std::function<void (void *)> deallocFunc=nullptr;

    struct MRUNode;
    struct HTEntry
    {
        HTEntry& operator=(const HTEntry &other)
        {
            key = other.key;
            hash = other.hash;
            type = other.type;
            value = other.value;
            return *this;
        }
        KEY key;
        unsigned hash;
        unsigned type;
        VALUE value;
        MRUNode *mruNode;

        void consume(HTEntry &other)
        {
            key = other.key;
            hash = other.hash;
            type = other.type;
            std::swap(value, other.value);
        }
    };
    struct MRUNode
    {
        HTEntry *entry;
        MRUNode *next;
        MRUNode *prev;
    };
    struct HTTable
    {
        // these could be calculated dynamically, but better to configure them at creation (see HTTable::create())
        HTEntry *table = nullptr;
        MRUNode **mru = nullptr;
        MRUNode **lru = nullptr;
        MRUNode *mruNodeTable;
        QueueOf<MRUNode, false> freeMRUNodes;
        unsigned *typeCount = nullptr;
        HASHER hasher;

        unsigned mruNextFree = 0;
        unsigned numTypes = 0;
        unsigned htn = 0;
        unsigned n = 0;

        HTTable(size_t size, unsigned types)
        {
            htn = size;
            numTypes = types;

            byte *auxMem = ((byte *)this) + sizeof(HTTable);
            size32_t mruLruSz = numTypes * sizeof(MRUNode *);
            size32_t mruNodeTableSz = ((memsize_t)htn) * sizeof(MRUNode);
            size32_t typeCountSz = numTypes * sizeof(unsigned);
            size32_t tableSz = ((memsize_t)htn)*sizeof(HTEntry);
            size32_t extraSz = mruLruSz*2+mruNodeTableSz+typeCountSz+tableSz;
            memset(auxMem, 0, extraSz);

            mru = (MRUNode **)auxMem;
            auxMem += mruLruSz;
            lru = (MRUNode **)auxMem;
            auxMem += mruLruSz;
            mruNodeTable = (MRUNode *)auxMem;
            auxMem += mruNodeTableSz;
            typeCount = (unsigned *)auxMem;
            auxMem += typeCountSz;
            table = (HTEntry *)auxMem;
        }

        unsigned elements() const { return n; }
        unsigned size() const { return htn; }
        unsigned queryTypeCount(unsigned type) const { return typeCount[type]; }
        HTEntry &queryFirst() { return table[0]; }
        HTEntry &queryLast() { return table[htn]; }
        unsigned queryNumTypes() const { return numTypes; }
        void setEmpty(HTEntry *entry)
        {
            entry->mruNode = nullptr;
        }
        bool isEmpty(HTEntry *entry) const { return nullptr == entry->mruNode; }
        bool full() const
        {
            return (n >= ((htn * 3) / 4)); // over 75% full.
        }
        void clean()
        {
            n = 0;
            byte *auxMem = ((byte *)this) + sizeof(HTTable);
            size32_t mruLruSz = numTypes * sizeof(MRUNode *);
            size32_t mruNodeTableSz = ((memsize_t)htn) * sizeof(MRUNode);
            size32_t typeCountSz = numTypes * sizeof(unsigned);
            size32_t tableSz = ((memsize_t)htn)*sizeof(HTEntry);
            size32_t extraSz = mruLruSz*2+mruNodeTableSz+typeCountSz+tableSz;
            memset(auxMem, 0, extraSz);
        }
        void kill(bool releaseElements=true, bool doClean=true)
        {
            if (releaseElements)
            {
                // walk table, by walking lru's of each value type
                for (unsigned t=0; t<numTypes; t++)
                {
                    MRUNode *cur = mru[t];
                    while (true)
                    {
                        if (!cur)
                            break;
                        cur->entry->value.~VALUE();
                        cur = cur->next;
                    }
                }
            }
            if (doClean)
                clean();
        }
        MRUNode *getFreeMRUNode()
        {
            if (freeMRUNodes.ordinality())
                return freeMRUNodes.dequeue();
            dbgassertex(mruNextFree < htn);
            return &mruNodeTable[mruNextFree++];
        }
        void addNew(HTEntry *ht, unsigned h, const KEY &key, const VALUE &value, unsigned type) // Called by add() if necessary. NB: not thread safe, need to protect if calling MT
        {
            ht->value = value;
            ht->hash = h; // NB: not strictly needed, other than to make a shortcut to match on lookup (but if key is cheap to compare may be no point)
            ht->type = type;
            ht->key = key;
            MRUNode *mruNode = getFreeMRUNode();
            mruNode->entry = ht;
            MRUNode *&mruNT = mru[type];
            if (mruNT)
            {
                mruNode->next = mruNT;
                mruNT->prev = mruNode;
            }
            else
                mruNode->next = nullptr;

            ht->mruNode = mruNode;
            mruNT = mruNode;
            if (!lru[type]) // head
                lru[type] = mruNode;
            typeCount[type]++;
            n++;
            dbgassertex(!isEmpty(ht)); // sanity check we haven't create an element which is deemed 'empty'
        }
        HTTable *createExpanded(std::function<void *(size_t)> allocFunc)
        {
            unsigned newHtn = size() * 2;
            HTTable *newTable = CMRUHashTable::createNewTable(newHtn, numTypes, allocFunc);

            HTEntry *newTableStart = &newTable->queryFirst();
            HTEntry *newTableEnd = &newTable->queryLast();

            // walk table, by walking lru's of each value type
            for (unsigned t=0; t<numTypes; t++)
            {
                MRUNode *curNode = mru[t];
                if (curNode)
                {
                    // mru
                    HTEntry *cur = curNode->entry;
                    unsigned i = cur->hash & (newHtn - 1);
                    HTEntry *newCur = newTableStart+i;
                    while (!isEmpty(newCur))
                    {
                        newCur++;
                        if (newCur==newTableEnd)
                            newCur = newTableStart;
                    }
                    newCur->consume(*cur);
                    MRUNode *newNode = newTable->getFreeMRUNode();
                    newCur->mruNode = newNode;
                    MRUNode *lastNew = newNode;
                    newTable->mru[t] = newNode;
                    newNode->entry = newCur;
                    curNode = curNode->next;

                    while (curNode)
                    {
                        cur = curNode->entry;
                        i = cur->hash & (newHtn - 1);
                        newCur = newTableStart+i;
                        while (!isEmpty(newCur))
                        {
                            newCur++;
                            if (newCur==newTableEnd)
                                newCur = newTableStart;
                        }
                        newCur->consume(*cur);
                        newNode = newTable->getFreeMRUNode();
                        newCur->mruNode = newNode;
                        newNode->entry = newCur;
                        newNode->prev = lastNew;
                        lastNew->next = newNode;
                        lastNew = newNode;
                        curNode = curNode->next;
                    }

                    newTable->lru[t] = lastNew;
                    newTable->typeCount[t] = typeCount[t];
                }
            }
            newTable->n = n;
            return newTable;
        }
        // returns position of match OR empty pos. to use if not found
        unsigned findPos(const KEY &key, unsigned h) const
        {
            unsigned i = h & (htn - 1);
            while (true)
            {
                HTEntry *ht = table+i;
                if (isEmpty(ht))
                    return i;
                if ((ht->hash==h) && (key == ht->key)) // NB: not really necessary to store hash and check it, but if key comparison was expensive, a quick check on hash 1st is a win
                    return i;
                if (++i==htn)
                    i = 0;
            }
        }
        // same as above, except returns NotFound if no match
        unsigned findMatch(const KEY &key, unsigned h) const
        {
            unsigned i = h & (htn - 1);
            while (true)
            {
                HTEntry *ht = table+i;
                if (isEmpty(ht))
                    return NotFound;
                if ((ht->hash==h) && (key == ht->key)) // NB: not really necessary to store hash and check it, but if key comparison was expensive, a quick check on hash 1st is a win
                    return i;
                if (++i==htn)
                    i = 0;
            }
        }
        void removeMRUEntry(HTEntry *ht)
        {
            MRUNode *mruNode = ht->mruNode;
            MRUNode *prev = mruNode->prev;
            MRUNode *next = mruNode->next;
            if (prev)
            {
                prev->next = next;
                if (lru[ht->type] == mruNode)
                    lru[ht->type] = prev;
            }
            else
                mru[ht->type] = next;
            if (next)
            {
                next->prev = prev;
                if (mru[ht->type] == mruNode)
                    mru[ht->type] = next;
            }
            freeMRUNodes.enqueue(mruNode);
        }
        void removeEntry(HTEntry *ht)
        {
            removeMRUEntry(ht);
            --typeCount[ht->type];
            --n;

            ht->value.~VALUE();
            memset(&ht->value, 0, sizeof(VALUE));

            setEmpty(ht);
        }
        bool add(const KEY &key, const VALUE &value, unsigned type, bool promoteIfAlreadyPresent=true)
        {
            unsigned h = hasher(key);
            unsigned e = findPos(key, h);
            HTEntry *ht = table+e;
            if (!isEmpty(ht))
            {
                dbgassertex(ht->type == type);
                if (promoteIfAlreadyPresent)
                    promote(ht);
                return false;
            }
            else
            {
                addNew(ht, h, key, value, type);
                return true;
            }
        }
        unsigned find(const KEY &key) const
        {
            unsigned h = hasher(key);
            unsigned e = findMatch(key, h);
            return e;
        }
        bool exists(const KEY &key) const
        {
            return NotFound != find(key);
        }
        bool query(const KEY &key, VALUE &res, unsigned *type=nullptr, bool doPromote=true)
        {
            unsigned e = find(key);
            if (NotFound == e)
                return false;
            HTEntry *ht = table+e;
            if (type)
                *type = ht->type;
            ht = table+e;
            if (doPromote)
                promote(ht);
            res = ht->value;
            return true;
        }
        bool queryOrAdd(const KEY &key, VALUE &res, const VALUE &value, unsigned type, bool doPromote=true) // NB: returns false if new item added
        {
            unsigned h = hasher(key);
            unsigned e = findPos(key, h);
            HTEntry *ht = table+e;
            if (!isEmpty(ht))
            {
                if (doPromote)
                    promote(ht);
                res = ht->value;
                return true;
            }
            else
            {
                addNew(ht, h, key, value, type);
                return false;
            }
        }
        bool remove(const KEY &key)
        {
            unsigned e = find(key);
            if (NotFound == e)
                return false;
            HTEntry *ht = table+e;
            removeEntry(ht);
            return true;
        }
        void promote(HTEntry *ht)
        {
            MRUNode *mruNode = ht->mruNode;
            MRUNode *prev = mruNode->prev;
            if (nullptr == prev) // already at top
                return;
            unsigned type = ht->type;
            MRUNode *next = mruNode->next;

            prev->next = next;
            if (next)
                next->prev = prev;

            if (lru[type] == mruNode)
                lru[type] = prev;

            mruNode->prev = nullptr;
            MRUNode *oldMRU = mru[type];
            mruNode->next = oldMRU;
            mru[type] = mruNode;
            oldMRU->prev = mruNode;
        }
        const VALUE removeLRU(unsigned type)
        {
            MRUNode *lruNode = lru[type];
            if (!lruNode)
                throw makeStringException(0, "CMRUHashTable::removeLRU() - table empty");

            MRUNode *prev = lruNode->prev;

            HTEntry *&htEntry = lruNode->entry;
            const VALUE ret = htEntry->value; // NB: copy to return
            htEntry->value.~VALUE();
            memset(&htEntry->value, 0, sizeof(VALUE));
            setEmpty(htEntry);
            --typeCount[type];
            --n;

            freeMRUNodes.enqueue(lruNode);

            lru[type] = prev;
            if (prev)
                prev->next = nullptr;
            else
                mru[type] = nullptr;
            return ret;
        }
    } *table = nullptr;

    typedef std::function<void (const VALUE &, unsigned, unsigned)> callbackFuncType;
    std::vector<unsigned> typeLimit;
    callbackFuncType limiterFunction = nullptr;

public:
    static HTTable *createNewTable(size_t size=8, size_t types=1, std::function<void *(size_t)> allocFunc=nullptr)
    {
        // check that it's a power of 2
        assertex((size > 0) && (0 == (size & (size-1))));

        // NB: have to be careful don't break alignment
        size32_t tableSz = ((memsize_t)size)*sizeof(HTEntry);
        size32_t mruLruSz = types * sizeof(HTEntry *);
        size32_t mruNodeTableSz = ((memsize_t)size) * sizeof(MRUNode);
        size32_t typeCountSz = types * sizeof(unsigned);
        size32_t extraSz = mruLruSz*2+mruNodeTableSz+typeCountSz+tableSz;
        size32_t totalSz = sizeof(HTTable)+extraSz;
        void *mem;
        if (allocFunc)
            mem = allocFunc(totalSz);
        else
            mem = malloc(totalSz);
        return new (mem) HTTable(size, types);
    }
    CMRUHashTable(size_t initialSize=8, size_t types=1, std::function<void *(size_t)> _allocFunc=nullptr, std::function<void (void *)> _deallocFunc=nullptr)
    {
        allocFunc = _allocFunc;
        deallocFunc = _deallocFunc;
        table = createNewTable(initialSize, types, allocFunc);
        for (unsigned t=0; t<types; t++)
            typeLimit.push_back((unsigned)-1); // i.e. unbound
    }
    ~CMRUHashTable()
    {
        if (table)
        {
            destroyTable(table, true);
            if (deallocFunc)
                deallocFunc(table);
            else
                free(table);
        }
    }
    void kill() { table->kill(); }
    size_t size() const { return table->size(); }
    size_t elements() const { return table->elements(); }
    bool full() const { return table->full(); }
    bool limitExceeded(unsigned type) const
    {
        return table->queryTypeCount(type) > typeLimit[type];
    }
    void enforceLimit(unsigned type, const VALUE &value)
    {
        if (limiterFunction != nullptr)
            limiterFunction(value, type, table->queryTypeCount(type));
        else
        {
            while (limitExceeded(type))
                removeLRU(type);
        }
        if (table->full())
        {
            void *oldTable = expand();
            destroyTable(oldTable, false); // false means don't iterate to release elements, because expand() has consumed them.
        }
    }
    bool add(const KEY &key, const VALUE &value, unsigned type, bool promoteIfAlreadyPresent=true)
    {
        bool res = table->add(key, value, type, promoteIfAlreadyPresent);
        enforceLimit(type, value);
        return res;
    }
    bool exists(const KEY &key) const
    {
        return table->exists(key);
    }
    bool query(const KEY &key, VALUE &res, unsigned *type=nullptr, bool doPromote=true)
    {
        return table->query(key, res, type, doPromote);
    }
    bool queryOrAdd(const KEY &key, VALUE &res, const VALUE &value, unsigned type, bool doPromote=true) // NB: returns false if new item added
    {
        if (!table->queryOrAdd(key, res, value, type, doPromote))
        {
            enforceLimit(type, value);
            return false;
        }
        return true;
    }
    bool remove(const KEY &key)
    {
        return table->remove(key);
    }
    void promote(HTEntry *ht)
    {
        table->promote(ht);
    }
    const VALUE removeLRU(unsigned type)
    {
        return table->removeLRU(type);
    }
    void *expand()
    {
        HTTable *newTable = table->createExpanded(allocFunc);
        std::swap(table, newTable);
        return newTable; // now old memory
    }
    void setTypeLimit(unsigned type, size_t limit)
    {
        typeLimit[type] = limit;
    }
    void setTypeLimiterCallback(callbackFuncType limiterFunc)
    {
        limiterFunction = limiterFunc;
    }
    void destroyTable(void *_table, bool releaeElements) // to be used after expand() returns old table
    {
        HTTable *table = (HTTable *)_table;
        table->kill(releaeElements, false);
        if (deallocFunc)
            deallocFunc(_table);
        else
            free(_table);
    }
};


#endif // JMRUTABLE_TPP
