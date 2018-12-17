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


#include "platform.h"
#include <limits.h>
#include <ctype.h>
#include <stdio.h>

#include "jhash.hpp"
#include "jmutex.hpp"

#define PSTRINGDATA INT_MIN

#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

#define GETBYTE0(n) ((unsigned)k[n])
#define GETBYTE1(n) ((unsigned)k[n+1]<<8)
#define GETBYTE2(n) ((unsigned)k[n+2]<<16)
#define GETBYTE3(n) ((unsigned)k[n+3]<<24)
#define GETWORD(k,n) (GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))

// the above looks inefficient but the compiler optimizes well

// this hash looks slow but is about twice as quick as using our CRC table
// and gives gives better results
// (see paper at http://burtleburtle.net/bob/hash/evahash.html for more info)

// Global atom table

MODULE_INIT(INIT_PRIORITY_JHASH)
{
    return true;
}

MODULE_EXIT()
{
    // do not delete globalAtomTable as it will slow things down.  If you
    // do not want these in your leak checking call releaseAtoms()
}

#define HASHONE(hash, c)        { hash *= 0x01000193; hash ^= c; }      // Fowler/Noll/Vo Hash... seems to work pretty well, and fast

unsigned hashc( const unsigned char *k, unsigned length, unsigned initval)
{
    unsigned hash = initval;        // ^_rotl(length,2)??
    unsigned char c;
    while (length >= 8)
    {
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        length-=4;
    }
    switch (length)
    {
    case 7: c = (*k++); HASHONE(hash, c); // fallthrough
    case 6: c = (*k++); HASHONE(hash, c); // fallthrough
    case 5: c = (*k++); HASHONE(hash, c); // fallthrough
    case 4: c = (*k++); HASHONE(hash, c); // fallthrough
    case 3: c = (*k++); HASHONE(hash, c); // fallthrough
    case 2: c = (*k++); HASHONE(hash, c); // fallthrough
    case 1: c = (*k++); HASHONE(hash, c);
    }
    return hash;
}

template <typename T>
inline unsigned doHashValue( T value, unsigned initval)
{
    //The values returned from this function are only consistent with those from hashn() if running on little endian architecture
    unsigned hash = initval;
    unsigned char c;
    for (unsigned i=0; i < sizeof(T); i++)
    {
        c = (byte)value;
        value >>= 8;
        HASHONE(hash, c);
    }
    return hash;
}

unsigned hashvalue( unsigned value, unsigned initval)
{
    return doHashValue(value, initval);
}

unsigned hashvalue( unsigned __int64 value, unsigned initval)
{
    return doHashValue(value, initval);
}

unsigned hashvalue( const void * value, unsigned initval)
{
    return doHashValue((memsize_t)value, initval);
}

#define GETWORDNC(k,n) ((GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))&0xdfdfdfdf)


unsigned hashnc( const unsigned char *k, unsigned length, unsigned initval)
{
    unsigned hash = initval;
    unsigned char c;
    while (length >= 8)
    {
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        length-=4;
    }
    switch (length)
    {
    case 7: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 6: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 5: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 4: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 3: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 2: c = (*k++)&0xdf; HASHONE(hash, c); // fallthrough
    case 1: c = (*k++)&0xdf; HASHONE(hash, c);
    }
    return hash;
}


MappingKey::MappingKey(const void * inKey, int keysize)
{
  int ksm = keysize;
  if (!ksm)
     ksm = (size32_t)strlen((char *) inKey) + 1;
  else if (ksm<0)
  {
     if (ksm==PSTRINGDATA) {
       ksm = (*((unsigned char *)inKey))+1;
     }
     else {
       ksm = -ksm;
       ksm += (size32_t)strlen((char *) inKey + ksm) + 1;
     }
  }

  void * temp  = malloc(ksm);
  memcpy(temp, inKey, ksm);
  key = temp;
}

//-- Mapping ---------------------------------------------------

unsigned MappingBase::getHash() const       { return hash; }
void     MappingBase::setHash(unsigned hval){ hash = hval; }

const void * Mapping::getKey() const        { return key.key; }

//-- HashMapping ---------------------------------------------------

//const void * HashMapping::getKey() const  { return key.key; }

//-- Atom ---------------------------------------------------

unsigned AtomBase::getHash() const              { return hash; }
void     AtomBase::setHash(unsigned hval)       { hash = hval; }

const void * AtomBase::getKey() const           { return key; }

//-- Case Atom ---------------------------------------------------

CaseAtom::CaseAtom(const void * k) : hash(0)
{
    text = strdup((const char *)k);
    lower = createLowerCaseAtom(text);
}

//-- HashTable ---------------------------------------------------

bool HashTable::addOwn(IMapping & donor)
{
    if(add(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

bool HashTable::replaceOwn(IMapping & donor)
{
    if(replace(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

IMapping * HashTable::findLink(const void * findParam) const
{
    IMapping * found = SuperHashTableOf<IMapping, const void>::find(findParam);
    if(found) found->Link();
    return found;
}

bool HashTable::onNotify(INotification & notify)
{
    bool ret = true;
    if (notify.getAction() == NotifyOnDispose)
    {
        IMapping * mapping = static_cast<IMapping *>(notify.querySource());
        ret = removeExact(mapping);
        assertex(ret);
    }
    return ret;
}

bool HashTable::keyeq(const void *key1, const void *key2, int ksize) const
{
    if (ksize<=0)
    {
        if (ksize<0)
        {
            if (ksize==PSTRINGDATA)
                return (memcmp(key1,key2,(*(unsigned char*)key1)) == 0);
            unsigned ksm = -ksize;
            if (memcmp(key1,key2,ksm))
                return false;
            key1 = (char *)key1 + ksm;
            key2 = (char *)key2 + ksm;
        }

        if (ignorecase)
        {
            unsigned char *k1 = (unsigned char *) key1;
            unsigned char *k2 = (unsigned char *) key2;
            for (;;)
            {
                unsigned char c1 = toupper(*k1);
                if (c1 != toupper(*k2))
                    return false;
                if (c1 == 0)
                    return true;
                k1++;
                k2++;
            }
        }
        return strcmp((char *)key1, (char *)key2) == 0;
    }
    return memcmp(key1,key2,ksize)==0;
}

unsigned HashTable::hash(const void *key, int ksize) const
{
   unsigned h = 0x811C9DC5;
   unsigned char *bp = (unsigned char *) key;
   if (ksize<=0)
   {
      if (ksize==PSTRINGDATA) {
        ksize = (*(unsigned char*)key)+1;
        goto BlockHash;
      }
      if (ksize<0) {
        h = hashc(bp,-ksize,h);
        bp -= ksize;
      }
      unsigned char* ks = bp;
      while (*bp) bp++;
      if (ignorecase)
        h = hashnc(ks,(size32_t)(bp-ks),h);
      else
        h = hashc(ks,(size32_t)(bp-ks),h);
   }
   else
   {
BlockHash:
      if (ignorecase)
        h = hashnc(bp,ksize,h);
      else
        h = hashc(bp,ksize,h);
   }
      //Strings that only differ by a single character don't generate hashes very far apart without this
   h *= 0x01000193;
   //h = ((h << 7) ^ (h >> 25));
   return h;
}

IMapping * HashTable::createLink(const void *key)
{
   IMapping * match = findLink(key);
   if (!match)
   {
      match = newMapping(key);
      if (match) add(*match);            // link for hash table
   }
   return match;
}

IMapping *HashTable::newMapping(const void *)
{
   assertex(!"Newmapping must be overridden to use HashTable::Create");
   return NULL;
}

// -- Helper functions...

void IntersectHash(HashTable & h1, HashTable & h2)
{
  HashIterator iter(h1);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = h2.find(cur.getKey());
    iter.next();
    if (!matched)
      h1.removeExact(&cur);
  }
}

void UnionHash(HashTable & h1, HashTable & h2)
{
  HashIterator iter(h2);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = h1.find(cur.getKey());
    if (!matched)
      h1.add(cur);
    iter.next();
  }
}

void SubtractHash(HashTable & main, HashTable & sub)
{
  HashIterator iter(sub);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = main.find(cur.getKey());
    iter.next();
    if (matched)
      main.removeExact(&cur);
  }
}

//===========================================================================

void KeptHashTable::onAdd(void * et)
{
    static_cast<IMapping *>(et)->Link();
}

void KeptHashTable::onRemove(void * et)
{
    static_cast<IMapping *>(et)->Release();
}

IMapping * KeptHashTable::create(const void *key)
{
   IMapping * match = find(key);
   if (!match)
   {
      match = newMapping(key);
      if (match) addOwn(*match);         // already linked for hash table
   }
   return match;
}


void ObservedHashTable::onAdd(void * et)
{
    static_cast<IMapping *>(et)->addObserver(*this);
}

void ObservedHashTable::onRemove(void * et)
{
    static_cast<IMapping *>(et)->removeObserver(*this);
}

//===========================================================================

static CriticalSection atomCrit;
static KeptLowerCaseAtomTable *globalAtomTable = NULL;
inline KeptLowerCaseAtomTable * queryGlobalAtomTable()
{
    if (!globalAtomTable)
    {
        globalAtomTable = new KeptLowerCaseAtomTable;
        globalAtomTable->reinit(2000);
    }
    return globalAtomTable;
}

extern jlib_decl IAtom * createAtom(const char *value)
{
    if (!value) return NULL;
    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(value);
}

extern jlib_decl IAtom * createAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;
    char * nullTerminated = (char *)alloca(len+1);
    memcpy(nullTerminated, value, len);
    nullTerminated[len] = 0;
    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(nullTerminated);
}

//===========================================================================

static CriticalSection caseAtomCrit;
static KeptCaseAtomTable *globalCaseAtomTable = NULL;
inline KeptCaseAtomTable * queryGlobalCaseAtomTable()
{
    if (!globalCaseAtomTable)
    {
        globalCaseAtomTable = new KeptCaseAtomTable;
        globalCaseAtomTable->reinit(2000);
    }
    return globalCaseAtomTable;
}

extern jlib_decl IIdAtom * createIdAtom(const char *value)
{
    if (!value) return NULL;
    CriticalBlock crit(caseAtomCrit);
    return queryGlobalCaseAtomTable()->addAtom(value);
}

extern jlib_decl IIdAtom * createIdAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;
    char * nullTerminated = (char *)alloca(len+1);
    memcpy(nullTerminated, value, len);
    nullTerminated[len] = 0;
    CriticalBlock crit(caseAtomCrit);
    return queryGlobalCaseAtomTable()->addAtom(nullTerminated);
}

#ifdef THE_GLOBAL_HASH_TABLE_BECOMES_CASE_SENSITIVE
extern jlib_decl IAtom * createLowerCaseAtom(const char *value)
{
    if (!value) return NULL;

    unsigned len = strlen(value);
    const byte * src = (const byte *)value;
    char * lower = (char *)alloca(len+1);
    for (unsigned i=0; i < len; i++)
        lower[i] = tolower(src[i]);
    lower[len] = 0;

    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(value);
}

extern jlib_decl IAtom * createLowerCaseAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;

    const byte * src = (const byte *)value;
    char * lower = (char *)alloca(len+1);
    for (unsigned i=0; i < len; i++)
        lower[i] = tolower(src[i]);
    lower[len] = 0;

    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(lower);
}
#endif

extern jlib_decl void releaseAtoms()
{
    delete globalCaseAtomTable;
    globalCaseAtomTable = NULL;
    delete globalAtomTable;
    globalAtomTable = NULL;
}


interface IHTTableCallback
{
    virtual void elementAdded(const KEY &key, VALUE *value, unsigned type) = 0;
    virtual void elementRemoved(const KEY &key, VALUE *value, unsigned type) = 0;
};

enum NodeType:unsigned { nt_leaf, nt_node, nt_blob, nt_preload, nt_max };


template <class KEY, class VALUE>
class CMRUHashTable : public CInterface
{
    unsigned clearPercentage = 10;

    struct HTEntry
    {
        HTEntry(const HTEntry &other) : key(other.key)
        {
            hash = other.hash;
            type = other.type;
            value = other.value;
            next = prev = nullptr;
        }
        HTEntry& operator=(const HTEntry &other)
        {
            key = other.key;
            hash = other.hash;
            type = other.type;
            value = other.value;
#ifdef _DEBUG
            next = prev = nullptr;
#endif
            return *this;
        }
        KEY key;
        unsigned hash;
        unsigned type;
        VALUE *value; // NB: if null, HT element is empty
        HTEntry *next;
        HTEntry *prev;
    };

    struct HTTable // NB: must not contain virtuals
    {
        // these could be calculated dynamically, but better to configure them at creation (see HTTable::create())
        HTEntry *table = nullptr;
        HTEntry **mru = nullptr;
        HTEntry **lru = nullptr;

        unsigned numTypes = 0;
        IHTTableCallback *cb = nullptr;
        unsigned htn = 0;
        unsigned n = 0;

        void setCallback(IHTTableCallback *_cb) { cb = _cb; }
        inline unsigned elements() const { return n; }
        inline unsigned size() const { return htn; }
        inline HTEntry &queryFirst() { return table[0]; }
        inline HTEntry &queryLast() { return table[htn-1]; }
        inline unsigned getNumTypes() const { return numTypes; }
        inline bool full() const
        {
            return (n >= ((htn * 3) / 4)); // over 75% full.
        }
        void clean()
        {
            memset(mru, 0, numTypes * sizeof(HTEntry *));
            memset(lru, 0, numTypes * sizeof(HTEntry *));
            memset(table, 0, sizeof(HTEntry)*htn);
            n = 0;
        }
        void kill()
        {
            HTEntry *cur = &queryFirst();;
            HTEntry *endTable = &queryLast();
            while (cur != endTable)
            {
                ::Release(cur->value);
                cur++;
            }
            clean(); // not strictly necessary
            for (unsigned t=0; t<numTypes; t++)
            {
                mru[t] = nullptr;
                lru[t] = nullptr;
            }
        }
        void addNew(HTEntry *ht, unsigned h, const KEY &key, VALUE *value, unsigned type) // Called by add() if necessary. NB: not thread safe, need to protect if calling MT
        {
            dbgassertex(!full()); // Should not happen, it is the responsibility of the callback to expand and replace the HT if getting full

            ht->prev = nullptr;
            ht->value = value;
            ht->hash = h; // NB: not strictly needed, other than to make a shortcut to match on lookup (but if key is cheap to compare may be no point)
            ht->type = type;
            ht->key = key;
            HTEntry *&mruNT = mru[type];
            if (mruNT)
            {
                ht->next = mruNT;
                mruNT->prev = ht;
            }
            else
                ht->next = nullptr;

            mruNT = ht;
            if (!lru[type])
                lru[type] = ht;
            n++;
        }
        HTTable *createExpanded()
        {
            unsigned newHtn = size() * 2;
            HTTable *newTable = new (newHtn, numTypes) HTTable;
            newTable->setCallback(cb);
            newTable->setMemCallback(memCb);
            newTable->n = n;

            HTEntry *newTableStart = &newTable->queryFirst();
            HTEntry *newTableEnd = &newTable->queryLast();

            // walk table, by walking lru's of each value type
            for (unsigned t=0; t<numTypes; t++)
            {
                HTEntry *cur = mru[t];
                if (cur)
                {
                    unsigned curs = 1;
                    // mru
                    unsigned i = cur->key.getHash() & (newHtn - 1);
                    HTEntry *newCur = newTableStart+i;
                    while (newCur->value)
                    {
                        newCur++;
                        if (newCur==newTableEnd)
                            newCur = newTableStart;
                    }
                    HTEntry *lastNew = newCur;
                    *lastNew = *cur;
                    mru[t] = lastNew;
                    cur = cur->next;

                    while (cur)
                    {
                        ++curs;
                        i = cur->key.getHash() & (newHtn - 1);
                        newCur = newTableStart+i;
                        while (newCur->value)
                        {
                            newCur++;
                            if (newCur==newTableEnd)
                                newCur = newTableStart;
                        }
                        *newCur = *cur;
                        newCur->prev = lastNew;
                        lastNew->next = newCur;
                        lastNew = newCur;
                        cur = cur->next;
                    }
                    lru[t] = lastNew;
                }
            }
            return newTable;
        }
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
                if (lru[ht->type] == ht)
                    lru[ht->type] = prev;
            }
            else
                mru[ht->type] = next;
            if (next)
            {
                next->prev = prev;
                if (mru[ht->type] == ht)
                    mru[ht->type] = next;
            }
        }
        void removeEntry(HTEntry *ht)
        {
            removeMRUEntry(ht);
            ht->value = nullptr;
            --n;
        }
        bool add(const KEY &key, VALUE *value, unsigned type, bool promoteIfAlreadyPresent=true)
        {
            unsigned h = key.getHash();
            unsigned e = findPos(key, h);
            HTEntry *ht = table+e;
            if (ht->value)
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
        VALUE *query(const KEY &key, HTEntry * &ht, bool doPromote=true)
        {
            unsigned h = key.getHash();
            unsigned e = findMatch(key, h);
            if (NotFound == e)
                return nullptr;
            ht = table+e;
            if (doPromote)
                promote(ht);
            return ht->value;
        }
        VALUE *queryOrAdd(const KEY &key, VALUE *value, unsigned type, bool doPromote=true)
        {
            unsigned h = key.getHash();
            unsigned e = findPos(key, h);
            HTEntry *ht = table+e;
            if (ht->value)
            {
                if (doPromote)
                    promote(ht);
                return ht->value;
            }
            else
            {
                addNew(ht, h, key, value, type);
                return nullptr;
            }
        }
        bool remove(const KEY &key) // NB: not thread safe, need to protect if calling MT
        {
            unsigned h = key.getHash();
            unsigned e = findMatch(key, h);
            if (NotFound == e)
                return false;
            HTEntry *ht = table+e;
            removeEntry(ht);
            return true;
        }
        void promote(HTEntry *ht)
        {
            if (nullptr == ht->prev) // already at top
                return;
            unsigned type = ht->type;
            removeMRUEntry(ht); // NB: remains in table, meaning it is still thread safe to read from table during a promote
            HTEntry *oldMRU = mru[type];
            ht->prev = nullptr;
            ht->next = oldMRU;
            mru[type] = ht;
            oldMRU->prev = ht;
        }
        void reduceUntil(unsigned type, std::function<bool()> untilFunc) // NB: not thread safe, need to protect if calling MT
        {
            HTEntry *cur = lru[type];
            if (cur)
            {
                unsigned numRemoved = 0;
                while (true)
                {
                    HTEntry *prev = cur->prev;

                    cur->value = nullptr;

                    ++numRemoved;
                    cur = prev;
                    if (!cur)
                        break;
                    // IOW has origSize been reduced by clearPercentage?
                    if (untilFunc())
                        break;
                }
                lru[type] = cur;
                if (cur)
                    cur->next = nullptr;
                else
                    mru[type] = nullptr;

                n -= numRemoved;
            }
        }
        static HTTable *create(unsigned elements, unsigned numTypes, std::function<void *(size_t)> allocFunc=nullptr) // numTypes only relevant for MRU/LRU
        {
            // NB: have to be careful don't break alignment
            size32_t sz = sizeof(HTTable)+((memsize_t)elements)*sizeof(HTEntry);
            size32_t numTypesSz = numTypes * sizeof(HTEntry *);

            size32_t totalSz = sz+numTypesSz*2;
            HTTable *ret;
            if (allocFunc)
                ret = (HTTable *)allocFunc(totalSz);
            else
                ret = (HTTable *)malloc(totalSz);
            memset(ret, 0, sz+numTypesSz*2);
            const byte *extraStart = ((byte *)ret) + sizeof(HTTable);
            ret->mru = (HTEntry **)extraStart;
            ret->lru = (HTEntry **)(extraStart + numTypesSz);
            ret->table = (HTEntry *)(extraStart + (numTypesSz * 2));
            ret->htn = elements;
            ret->numTypes = numTypes;
            ret->memCb = memCb;
            return ret;
        }
        static void destroy(HTTable *ptr, std::function<void(void *)> freeFunc=nullptr)
        {
            if (ptr)
            {
                HTTable *htTable = (HTTable *)ptr;
                if (freeFunc)
                    freeFunc(ptr);
                else
                    free(ptr);
            }
        }
    } *table = nullptr;

    void add(const KEY &key, VALUE *value, unsigned type, bool promoteIfAlreadyPresent=true)
    {
        table->add(key, value, type, promoteIfAlreadyPresent);
    }
    VALUE *query(const KEY &key, HTEntry * &ht, bool doPromote=true)
    {
        return table->query(key, ht, doPromote);
    }
    VALUE *queryOrAdd(const KEY &key, VALUE *value, unsigned type, bool doPromote=true)
    {
        return table->queryOrAdd(key, value, type, doPromote);
    }
    bool remove(const KEY &key) // NB: not thread safe, need to protect if calling MT
    {
        return table->remove(key);
    }
    void promote(HTEntry *ht)
    {
        table->promote(ht);
    }
    void reduceUntil(unsigned type, std::function<bool()> untilFunc) // NB: not thread safe, need to protect if calling MT
    {
        table->reduceUntil(type, untilFunc);
    }

    std::vector<size32_t> sizeLimit;
    std::vector<size32_t> totalSize;

public:
    CMRUHashTable()
    {
        table = new (8, NodeType::nt_max) HTTable; // size must be power of 2
        table->setCallback(this);
        for (unsigned t=0; t<NodeType::nt_max; t++)
        {
            totalSize.push_back(0);
            sizeLimit.push_back(0);
        }
        sizeLimit[nt_leaf] = 0x10000;
        sizeLimit[nt_node] = 0x10000;
        sizeLimit[nt_blob] = 0x10000;

    }
    void kill()
    {
        for (unsigned t=0; t<NodeType::nt_max; t++)
            totalSize[t] = 0;
        table->kill();
    }
    bool reduceUntilFunc(unsigned type, size32_t targetSize)
    {
        return totalSize[type] <= targetSize;
    }
};

