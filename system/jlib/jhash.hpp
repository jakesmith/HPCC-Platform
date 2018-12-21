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



#ifndef JHASH_HPP
#define JHASH_HPP

#include "platform.h"
#include <stdio.h>
#include <functional>
#include "jiface.hpp"
#include "jobserve.hpp"
#include "jiter.hpp"
#include "jsuperhash.hpp"

#ifndef IHASH_DEFINED       // may be defined already elsewhere
#define IHASH_DEFINED
interface IHash
{
    virtual unsigned hash(const void *data)=0;
 protected:
    virtual ~IHash() {}
};
#endif

interface jlib_decl IMapping : extends IObservable
{
 public:
    virtual const void * getKey() const = 0;
    virtual unsigned     getHash() const = 0;
    virtual void         setHash(unsigned) = 0;
};

interface jlib_decl IAtom : extends IMapping
{
 public:
    virtual const char * queryStr() const = 0;
};
inline jlib_decl const char * str(const IAtom * atom) { return atom ? atom->queryStr() : NULL; }

//This interface represents an atom which preserves its case, but also stores a lower case representation
//for efficient case insensitive comparison.
//It is deliberately NOT derived from IAtom to avoid accidentally using the wrong interface
interface jlib_decl IIdAtom : extends IMapping
{
 public:
    virtual const char * queryStr() const = 0;
 public:
    virtual IAtom * queryLower() const = 0;
};
inline jlib_decl const char * str(const IIdAtom * atom) { return atom ? atom->queryStr() : NULL; }
inline jlib_decl IAtom * lower(const IIdAtom * atom) { return atom ? atom->queryLower() : NULL; }

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 )
#endif

class jlib_decl HashTable
    : public SuperHashTableOf<IMapping, const void>, implements IObserver
{
  public:
    HashTable(int _keysize, bool _ignorecase)
        : SuperHashTableOf<IMapping, const void>(),
        keysize(_keysize), ignorecase(_ignorecase) {}
    HashTable(unsigned initsize, int _keysize, bool _ignorecase)
        : SuperHashTableOf<IMapping, const void>(initsize),
        keysize(_keysize), ignorecase(_ignorecase) {}
    ~HashTable() { _releaseAll(); }

    IMPLEMENT_IINTERFACE

    bool add(IMapping & donor)
      {
          donor.setHash(hash(donor.getKey(), keysize));
          return SuperHashTableOf<IMapping, const void>::add(donor);
      }
    bool replace(IMapping & donor)
      {
          donor.setHash(hash(donor.getKey(), keysize));
          return SuperHashTableOf<IMapping, const void>::replace(donor);
      }
    bool addOwn(IMapping & donor);
    bool replaceOwn(IMapping & donor);
    IMapping * findLink(const void *key) const;
    virtual bool onNotify(INotification & notify);
    IMapping * createLink(const void *key);

  protected:
    virtual IMapping *  newMapping(const void *key);

  private:
    unsigned   getHashFromElement(const void * et) const
      { return static_cast<const IMapping *>(et)->getHash(); }
    unsigned   getHashFromFindParam(const void *fp) const
      { return hash(fp, keysize); }
    const void * getFindParam(const void * et) const
      { return const_cast<const void *>(static_cast<const IMapping *>(et)->getKey()); }
    bool       matchesFindParam(const void * et, const void * key, unsigned fphash __attribute__((unused))) const
      { return keyeq(key, static_cast<const IMapping *>(et)->getKey(), keysize); }
    bool       keyeq(const void *key1, const void *key2, int ksize) const;
    unsigned   hash(const void *key, int ksize) const;

  private:
    int        keysize;
    bool       ignorecase;
};

class jlib_decl KeptHashTable : public HashTable
{
public:
    KeptHashTable(int _keysize, bool _ignorecase)
        : HashTable(_keysize, _ignorecase) {}
    KeptHashTable(unsigned _initsize, int _keysize, bool _ignorecase)
        : HashTable(_initsize, _keysize, _ignorecase) {}
    ~KeptHashTable() { _releaseAll(); }

    IMapping * create(const void *key);

private:
    void       onAdd(void *et);
    void       onRemove(void *et);
};

class jlib_decl ObservedHashTable : public HashTable
{
public:
    ObservedHashTable(int _keysize, bool _ignorecase)
        : HashTable(_keysize, _ignorecase) {}
    ObservedHashTable(unsigned _initsize, int _keysize, bool _ignorecase)
        : HashTable(_initsize, _keysize, _ignorecase) {}
    ~ObservedHashTable() { _releaseAll(); }

private:
    void       onAdd(void *et);
    void       onRemove(void *et);
};

class jlib_decl HashIterator : public SuperHashIteratorOf<IMapping>
{
  public:
    HashIterator(const HashTable & _table) : SuperHashIteratorOf<IMapping>(_table) {}

    IMapping & get() { IMapping & et = query(); et.Link(); return et; }
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

void IntersectHash(HashTable & h1, HashTable & h2);
void UnionHash(HashTable & h1, HashTable & h2);
void SubtractHash(HashTable & main, HashTable & sub);

#include "jhash.ipp"

typedef MappingStringTo<IInterfacePtr,IInterfacePtr> CopyMappingStringToIInterface;
typedef MapStringTo<IInterfacePtr,IInterfacePtr,CopyMappingStringToIInterface> CopyMapStringToIInterface;

template <class C> class CopyMapStringToMyClass : public CopyMapStringToIInterface
{
  public:
    CopyMapStringToMyClass() : CopyMapStringToIInterface() {};
    CopyMapStringToMyClass(bool _ignorecase) : CopyMapStringToIInterface(_ignorecase) {};
    static inline C * mapToValue(IMapping * _map)
      {
      CopyMappingStringToIInterface * map = (CopyMappingStringToIInterface *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(const char *k) const
      {
      IInterface **x = CopyMapStringToIInterface::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class C, class BASE> class CopyMapStringToMyClassViaBase : public CopyMapStringToIInterface
{
  public:
    CopyMapStringToMyClassViaBase() : CopyMapStringToIInterface() {};
    CopyMapStringToMyClassViaBase(bool _ignorecase) : CopyMapStringToIInterface(_ignorecase) {};
    C *getValue(const char *k) const
      {
      IInterface **x = CopyMapStringToIInterface::getValue(k);
      return x ? (C *)(BASE *)*x : NULL;
      }
    bool setValue(const char *k, C * v)
      {
      return CopyMapStringToIInterface::setValue(k, (BASE *)v);
      }
};

//===========================================================================

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 ) // hope this warning not significant! (may get link errors I guess)
#endif

class jlib_decl MappingStringToIInterface : public MappingStringTo<IInterfacePtr,IInterfacePtr>
{
  public:
    MappingStringToIInterface(const char * k, IInterfacePtr a) :
    MappingStringTo<IInterfacePtr,IInterfacePtr>(k,a) { ::Link(a); }
    ~MappingStringToIInterface() { ::Release(val); }
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

typedef MapStringTo<IInterfacePtr,IInterfacePtr,MappingStringToIInterface> MapStringToIInterface;

template <class C> class MapStringToMyClass : public MapStringToIInterface
{
  public:
    MapStringToMyClass() : MapStringToIInterface() {};
    MapStringToMyClass(bool _ignorecase) : MapStringToIInterface(_ignorecase) {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingStringToIInterface * map = (MappingStringToIInterface *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(const char *k) const
      {
      IInterface **x = MapStringToIInterface::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class C, class BASE> class MapStringToMyClassViaBase : public MapStringToIInterface
{
  public:
    MapStringToMyClassViaBase() : MapStringToIInterface() {};
    MapStringToMyClassViaBase(bool _ignorecase) : MapStringToIInterface(_ignorecase) {};
    C *getValue(const char *k) const
      {
      IInterface **x = MapStringToIInterface::getValue(k);
      return x ? (C *)(BASE *)*x : NULL;
      }
    bool setValue(const char *k, C * v)
      {
      return MapStringToIInterface::setValue(k, (BASE *)v);
      }
};

//===========================================================================

template <class KEY, class KEYINIT>
class CopyMapXToIInterface  : public MapBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr,MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> >
{
};

template <class KEY, class KEYINIT, class C> 
class CopyMapXToMyClass : public CopyMapXToIInterface<KEY, KEYINIT>
{
  public:
    CopyMapXToMyClass() : CopyMapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> * map = (MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = CopyMapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class KEY, class KEYINIT>
class MappingXToIInterface : public MappingBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr>
{
    typedef MappingXToIInterface<KEY, KEYINIT> SELF;
  public:
    MappingXToIInterface(KEYINIT k, IInterfacePtr a) :
    MappingBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr>(k,a) { ::Link(a); }
    ~MappingXToIInterface() { ::Release(SELF::val); }
};

template <class KEY, class KEYINIT>
class MapXToIInterface  : public MapBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr,MappingXToIInterface<KEY, KEYINIT> >
{
public:
    using ELEMENT = MappingXToIInterface<KEY, KEYINIT>;
};

template <class KEY, class KEYINIT, class C> 
class MapXToMyClass : public MapXToIInterface<KEY, KEYINIT>
{
  public:
    MapXToMyClass() : MapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingXToIInterface<KEY, KEYINIT> * map = (MappingXToIInterface<KEY, KEYINIT> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = MapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class KEY, class KEYINIT, class C, class BASE> 
class MapXToMyClassViaBase : public MapXToIInterface<KEY, KEYINIT>
{
  public:
    MapXToMyClassViaBase () : MapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingXToIInterface<KEY, KEYINIT> * map = (MappingXToIInterface<KEY, KEYINIT> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *)(BASE *)*x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = MapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *)(BASE *) *x : NULL;
      }
    bool setValue(KEYINIT k, BASE * v)
      {
      return MapXToIInterface<KEY, KEYINIT>::setValue(k, v);
      }
};


//===========================================================================
template <class KEY, class VALUE>
class MappingOwnedToOwned : public MappingBetween<Linked<KEY>, KEY *, Linked<VALUE>,VALUE *>
{
public:
    MappingOwnedToOwned(KEY * k, VALUE * a) : MappingBetween<Linked<KEY>, KEY *, Linked<VALUE>,VALUE *>(k, a) {}
};

template <class KEY, class VALUE>
class MapOwnedToOwned : public MapBetween<Linked<KEY>, KEY * , Linked<VALUE>,VALUE *,MappingOwnedToOwned<KEY, VALUE> >
{
public:
    inline MapOwnedToOwned() {}
    inline MapOwnedToOwned(unsigned initsize) : MapBetween<Linked<KEY>, KEY * , Linked<VALUE>,VALUE *,MappingOwnedToOwned<KEY, VALUE> >(initsize) {}
};


/*
 The hash tables can be used from the interfaces above.  However there are
 some helper classes/macros in jhash.ipp to make them easier to use:

 Element classes:
   HashMapping      - An implementation of IMapping that works in keep and
                      non kept hash tables.
   Atom             - An implementation of IAtom
   MappingKey       - A class for storing a hash key - used by above.

 There are also some classes to make creating hash tables easier:
 (They are actually implemented as macros to stop the compiler choking on
  the template classes).

 a) To create a hash table of a given class use:
    typedef HashTableOf<MAPPING-TYPE, HASH-KEY-SIZE> MapHashTable;

    Using a HashTableOf also adds the following method to the hash table class

    MAPPING-TYPE * CreateLink(const void * key);

    which creates entries in the hash table.

 b) To create a hash table that maps a string to another type use:

    typedef MapStringTo<TO-TYPE, TO-INIT> MyMappingName;
    where
       TO-TYPE - what is stored in the mapping
       TO-INIT - what you pass to the value to initialise it.
    e.g.
    typedef MapStringTo<StringAttr, const char *> MapStrToStr;

 c) To create a hash table that maps a non-string to another type use:

    typedef MapBetween<KEY-TYPE, KEY-INIT, MAP-TYPE, MAP-INIT> myMap;
    where MAP-... as above and
       KEY-TYPE - what is stored as the key
       KEY-INIT - what you pass to the key to initialise it.
    e.g.,
    typedef MapBetween<int, int, StringAttr, const char *> MapIntToStr;

 *** HashTable Key sizes ***
 The HashTable key size can have one of the following forms:

    +n  - The key is n bytes of data
    0   - The key is a null terminated string
    -n  - The key is n bytes of data followed by a null terminated string.
*/

// Create an atom in the global atom table
extern jlib_decl IAtom * createAtom(const char *value);
extern jlib_decl IAtom * createAtom(const char *value, size32_t len);

inline IAtom * createLowerCaseAtom(const char *value) { return createAtom(value); }
inline IAtom * createLowerCaseAtom(const char *value, size32_t len) { return createAtom(value, len); }

extern jlib_decl IIdAtom * createIdAtom(const char *value);
extern jlib_decl IIdAtom * createIdAtom(const char *value, size32_t len);

extern jlib_decl void releaseAtoms();
extern jlib_decl unsigned hashc( const unsigned char *k, unsigned length, unsigned initval);
extern jlib_decl unsigned hashnc( const unsigned char *k, unsigned length, unsigned initval);
extern jlib_decl unsigned hashvalue( unsigned value, unsigned initval);
extern jlib_decl unsigned hashvalue( unsigned __int64 value, unsigned initval);
extern jlib_decl unsigned hashvalue( const void * value, unsigned initval);

//================================================
// Minimal Hash table template - slightly less overhead that HashTable/SuperHashTable

template <class C> class CMinHashTable
{
protected:
    unsigned htn;
    unsigned n;
    C **table;

    void expand(bool expand=true)
    {
        C **t = table+htn; // more interesting going backwards
        if (expand)
            htn = htn*2+1;
        else
            htn /= 2;
        C **newtable = (C **)calloc(sizeof(C *),htn);
        while (t--!=table) {
            C *c = *t;
            if (c) {
                unsigned h = c->hash%htn;
                while (newtable[h]) {
                    h++;
                    if (h==htn)
                        h = 0;
                }
                newtable[h] = c;
            }
        }
        free(table);
        table = newtable;
    }       

public:
    CMinHashTable<C>(unsigned _initialSize = 7)
    {
        htn = _initialSize;
        n = 0;
        table = (C **)calloc(sizeof(C *),htn);
    }
    ~CMinHashTable<C>()
    {
        C **t = table+htn; 
        while (t--!=table) 
            if (*t) 
                C::destroy(*t);
        free(table);
    }

    void add(C *c)
    {
        if (n*4>htn*3)
            expand();
        unsigned i = c->hash%htn;
        while (table[i]) 
            if (++i==htn)
                i = 0;
        table[i] = c;
        n++;
    }

    C *findh(const char *key,unsigned h) 
    {
        unsigned i=h%htn;
        while (table[i]) {
            if ((table[i]->hash==h)&&table[i]->eq(key))
                return table[i];
            if (++i==htn)
                i = 0;
        }
        return NULL;
    }

    C *find(const char *key,bool add) 
    {
        unsigned h = C::getHash(key);
        unsigned i=h%htn;
        while (table[i]) {
            if ((table[i]->hash==h)&&table[i]->eq(key))
                return table[i];
            if (++i==htn)
                i = 0;
        }
        if (!add)
            return NULL;
        C *c = C::create(key);
        table[i] = c;
        n++;
        if (n*4>htn*3)
            expand();
        return c;
    }

    unsigned findIndex(const char *key, unsigned h)
    {
        unsigned i=h%htn;
        while (table[i]) {
            if ((table[i]->hash==h)&&table[i]->eq(key))
                return i;
            if (++i==htn)
                i = 0;
        }
        return (unsigned) -1;
    }

    C *getIndex(unsigned v) const
    {
        assert(v != (unsigned)-1 && v < htn);
        return table[v];
    }

    void remove(C *c)
    {
        unsigned i = c->hash%htn;
        C::destroy(c);
        while (table[i]!=c) {
            if (table[i]==NULL) {
                return;
            }
            if (++i==htn)
                i = 0;
        }
        unsigned j = i+1;
        for (;;) {
            if (j==htn)
                j = 0;
            C *cn = table[j];
            if (cn==NULL) 
                break;
            unsigned k = cn->hash%htn;
            if (j>i) {
                if ((k<=i)||(k>j)) {
                    table[i] = cn;
                    i = j;
                }
            }
            else if ((k>j)&&(k<=i)) {
                table[i] = cn;
                i = j;
            }
            j++;
        }
        table[i] = NULL;
        n--;
        if ((n>1024)&&(n<htn/4))
            expand(false);
    }

    C *first(unsigned &i)
    {
        i = 0; 
        return next(i);
    }
    C *next(unsigned &i)
    {
        while (i!=htn) {
            C* r = table[i++];
            if (r)
                return r;
        }
        return NULL;
    }

    unsigned ordinality()
    {
        return n;
    }

};

template <class KEY, class VALUE>
class CMRUHashTable : public CInterface
{
    std::function<void *(size_t)> allocFunc=nullptr;
    std::function<void (void *)> deallocFunc=nullptr;

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
        unsigned htn = 0;
        unsigned n = 0;

        unsigned elements() const { return n; }
        unsigned size() const { return htn; }
        HTEntry &queryFirst() { return table[0]; }
        HTEntry &queryLast() { return table[htn-1]; }
        unsigned getNumTypes() const { return numTypes; }
        bool full() const
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
        HTTable *createExpanded(std::function<void *(size_t)> allocFunc)
        {
            unsigned newHtn = size() * 2;
            HTTable *newTable = CMRUHashTable::createNewTable(newHtn, numTypes, allocFunc);

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
        unsigned findPos(const KEY &key, unsigned h)
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
        unsigned findMatch(const KEY &key, unsigned h)
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
        VALUE *query(const KEY &key, unsigned *type=nullptr, bool doPromote=true)
        {
            unsigned h = key.getHash();
            unsigned e = findMatch(key, h);
            if (NotFound == e)
                return nullptr;
            HTEntry *ht = table+e;
            if (type)
                *type = ht->type;
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
        VALUE *removeLRU(unsigned type) // NB: not thread safe, need to protect if calling MT
        {
            HTEntry *cur = lru[type];
            if (!cur)
                return nullptr;

            VALUE *ret = cur->value;
            cur->value = nullptr;
            --n;

            HTEntry *prev = cur->prev;
            cur = prev;
            lru[type] = cur;
            if (cur)
                cur->next = nullptr;
            else
                mru[type] = nullptr;
            return ret;
        }
    } *table = nullptr;

public:
    static HTTable *createNewTable(size_t size=8, size_t types=1, std::function<void *(size_t)> allocFunc=nullptr)
    {
        // check that it's a power of 2
        assertex((size > 0) && (0 == (size & (size-1))));

        // NB: have to be careful don't break alignment
        size32_t sz = sizeof(HTTable)+((memsize_t)size)*sizeof(HTEntry);
        size32_t numTypesSz = types * sizeof(HTEntry *);

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
        ret->htn = size;
        ret->numTypes = types;
        return ret;
    }
    CMRUHashTable(size_t initialSize=8, size_t types=1, std::function<void *(size_t)> _allocFunc=nullptr, std::function<void (void *)> _deallocFunc=nullptr)
    {
        allocFunc = _allocFunc;
        deallocFunc = _deallocFunc;
        table = createNewTable(initialSize, types, allocFunc);

        // check that it's a power of 2
        assertex((initialSize > 0) && (0 == (initialSize & (initialSize-1))));

        // NB: have to be careful don't break alignment
        size32_t sz = sizeof(HTTable)+((memsize_t)initialSize)*sizeof(HTEntry);
        size32_t numTypesSz = types * sizeof(HTEntry *);

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
        ret->htn = initialSize;
        ret->numTypes = types;
    }
    ~CMRUHashTable()
    {
        if (table)
        {
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

    void add(const KEY &key, VALUE *value, unsigned type, bool promoteIfAlreadyPresent=true)
    {
        table->add(key, value, type, promoteIfAlreadyPresent);
    }
    VALUE *query(const  KEY &key, unsigned *type=nullptr, bool doPromote=true)
    {
        return table->query(key, type, doPromote);
    }
    VALUE *queryOrAdd(const KEY &key, VALUE *value, unsigned type, bool doPromote=true)
    {
        return table->queryOrAdd(key, value, type, doPromote);
    }
    bool remove(const KEY &key)
    {
        return table->remove(key);
    }
    void promote(HTEntry *ht)
    {
        table->promote(ht);
    }
    VALUE *removeLRU(unsigned type)
    {
        return table->removeLRU(type);
    }
    void *expand()
    {
        HTTable *newTable = table->createExpanded(allocFunc);
        std::swap(table, newTable);
        return newTable; // now old memory
    }
};


template <class KEY, class VALUE>
CMRUHashTable<KEY, VALUE> *createTypedMRUCache(size_t initialSize=8, size_t types=1, std::function<void *(size_t)> allocFunc=nullptr, std::function<void (void *)> deallocFunc=nullptr)
{
    return new CMRUHashTable<KEY, VALUE>(initialSize, types, allocFunc, deallocFunc);
}


#endif
