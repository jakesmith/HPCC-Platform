/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef __THMEM__
#define __THMEM__

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif

#include "jexcept.hpp"
#include "jbuff.hpp"
#include "jsort.hpp"
#include "thormisc.hpp"
#include "eclhelper.hpp"
#include "rtlread_imp.hpp"
#include "roxiemem.hpp"
#include "roxierowbuff.hpp"

#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.hpp"
#include "thorcommon.ipp"

interface IRecordSize;
interface ILargeMemLimitNotify;
interface ISortKeySerializer;
interface ICompare;

//#define INCLUDE_POINTER_ARRAY_SIZE


#define ReleaseThorRow(row) ReleaseRoxieRow(row)
#define ReleaseClearThorRow(row) ReleaseClearRoxieRow(row)
#define LinkThorRow(row) LinkRoxieRow(row)



graph_decl void setThorInABox(unsigned num);


// used for non-row allocations
#define ThorMalloc(a) malloc(a)
#define ThorRealloc(p,a) realloc(p,a)
#define ThorFree(p) free(p)


// ---------------------------------------------------------
// Thor link counted rows

// these may be inline later

#ifdef TEST_ROW_LINKS
#define TESTROW(r) if (r) { LinkThorRow(r); ReleaseThorRow(r); }
#else
#define TESTROW(r) 
#endif
#ifdef PARANOID_TEST_ROW_LINKS
#define PARANOIDTESTROW(r) if (r) { LinkThorRow(r); ReleaseThorRow(r); }
#else
#define PARANOIDTESTROW(r) 
#endif



class OwnedConstThorRow 
{
public:
    inline OwnedConstThorRow()                              { ptr = NULL; }
    inline OwnedConstThorRow(const void * _ptr)             { TESTROW(_ptr); ptr = _ptr; }
    inline OwnedConstThorRow(const OwnedConstThorRow & other)   { ptr = other.getLink(); }

    inline ~OwnedConstThorRow()                             { ReleaseThorRow(ptr); }
    
private: 
    /* these overloaded operators are the devil of memory leak. Use set, setown instead. */
    void operator = (const void * _ptr)          { set(_ptr);  }
    void operator = (const OwnedConstThorRow & other) { set(other.get());  }

    /* this causes -ve memory leak */
    void setown(const OwnedConstThorRow &other) {  }

public:
    inline const void * operator -> () const        { PARANOIDTESTROW(ptr); return ptr; } 
    inline operator const void *() const            { PARANOIDTESTROW(ptr); return ptr; } 
    
    inline void clear()                         { const void *temp=ptr; ptr=NULL; ReleaseThorRow(temp); }
    inline const void * get() const             { PARANOIDTESTROW(ptr); return ptr; }
    inline const void * getClear()              
    { 
        const void * ret = ptr; 
        ptr = NULL; 
        TESTROW(ret);
        return ret; 
    }
    inline const void * getLink() const         { LinkThorRow(ptr); return ptr; }
    inline void set(const void * _ptr)          
    { 
        const void * temp = ptr; 
        LinkThorRow(_ptr); 
        ptr = _ptr; 
        if (temp)
            ReleaseThorRow(temp); 
    }
    inline void setown(const void * _ptr)       
    { 
        TESTROW(_ptr);
        const void * temp = ptr; 
        ptr = _ptr; 
        if (temp)
            ReleaseThorRow(temp); 
    }
    
    inline void set(const OwnedConstThorRow &other) { set(other.get()); }

    inline void deserialize(IRowInterfaces *rowif, size32_t memsz, const void *mem)
    {
        if (memsz) {
            RtlDynamicRowBuilder rowBuilder(rowif->queryRowAllocator());
            //GH->NH This now has a higher overhead than you are likely to want at this point...
            CThorStreamDeserializerSource dsz(memsz,mem);
            size32_t size = rowif->queryRowDeserializer()->deserialize(rowBuilder,dsz);
            setown(rowBuilder.finalizeRowClear(size));
        }
        else
            clear();
    }
    
private:
    const void * ptr;
};

interface IThorAllocator : extends IInterface
{
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const = 0;
    virtual roxiemem::IRowManager *queryRowManager() const = 0;
};

IThorAllocator *createThorAllocator(memsize_t memSize);

extern graph_decl IOutputMetaData *createOutputMetaDataWithExtra(IOutputMetaData *meta, size32_t sz);
extern graph_decl IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz);

 
class CThorRowLinkCounter: public CSimpleInterface, implements IRowLinkCounter
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    virtual void releaseRow(const void *row)
    {
        ReleaseThorRow(row);
    }
    virtual void linkRow(const void *row)
    {
        LinkThorRow(row);
    }
};


extern graph_decl memsize_t ThorRowMemoryAvailable();



// ---------------------------------------------------------


interface CLargeThorLinkedRowArrayOutOfMemException: extends IException
{
};

interface IThorRowSequenceCompare: implements ICompare, extends IInterface
{
};

#define PERROWOVERHEAD    (sizeof(atomic_t) + sizeof(unsigned) + sizeof(void *))
                          //    link + activityid + stable sort ptr

interface IThorRowArrayException: extends IException
{
};

extern graph_decl IThorRowArrayException *createRowArrayException(size32_t sz);

extern graph_decl void checkMultiThorMemoryThreshold(bool inc);
extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify);

extern graph_decl memsize_t setLargeMemSize(unsigned limit);

class graph_decl CLegacyThorRowArray
{
    MemoryBuffer ptrbuf;
    unsigned numelem;
    memsize_t totalsize;
    memsize_t maxtotal;
    size32_t overhead;
    Linked<IOutputRowSerializer> serializer;
    bool keepsize;
    bool sizing;
    bool raiseexceptions;

    void adjSize(const void *row, bool inc);


public:
    CLegacyThorRowArray();

    ~CLegacyThorRowArray()
    {
        reset(true);
    }

    void reset(bool freeptrs)
    {
        const void ** row = (const void **)base();
        unsigned remn = 0;
        while (numelem) {
            const void * r = *(row++);
            if (r) {
                remn++;
                ReleaseThorRow(r);
            }
            numelem--;
        }
        if (freeptrs)
            ptrbuf.resetBuffer();
        else
            ptrbuf.setLength(0);
        if (sizing&&remn) 
            checkMultiThorMemoryThreshold(false);
        totalsize = 0;
        overhead = 0;
    }

    inline void clear() { reset(true); }

    void append(const void *row) // takes ownership
    {
        if (sizing) 
            adjSize(row,true);
        ptrbuf.append(sizeof(row),&row);
        numelem++;
    }

    void removeRows(unsigned i,unsigned n);

    inline const byte * item(unsigned idx) const
    {
        if (idx>=numelem)
            return NULL;
        return *(((const byte **)ptrbuf.toByteArray())+idx);

    }
    inline const byte ** base() const
    {
        return (const byte **)ptrbuf.toByteArray();
    }

    inline const byte * itemClear(unsigned idx) // sets old to NULL 
    {
        if (idx>=numelem)
            return NULL;
        byte ** rp = ((byte **)ptrbuf.toByteArray())+idx;
        const byte *ret = *rp;
        if (sizing)
            adjSize(ret,false);
        *rp = NULL;
        return ret;

    }

    inline unsigned ordinality() const
    {
        return numelem;
    }

    inline memsize_t totalSize() const
    {
#ifdef _DEBUG
        assertex(sizing); 
#endif
        return totalsize;
    }

    void setMaxTotal(memsize_t tot)
    {   
        maxtotal = tot;
    }

    inline memsize_t totalMem()
    {
        return 
#ifdef INCLUDE_POINTER_ARRAY_SIZE           
        ptrbuf.length()+ptrbuf.capacity()+
#endif
        totalsize+overhead;
    }

    inline bool isFull()
    {
        memsize_t sz = totalMem();
#ifdef _DEBUG
        assertex(sizing&&!raiseexceptions);
#endif
        if (sz>maxtotal) {
#ifdef _DEBUG
            PROGLOG("CLegacyThorRowArray isFull(totalsize=%"I64F"u,ptrbuf.length()=%u,ptrbuf.capacity()=%u,overhead=%u,maxtotal=%"I64F"u",
                     (unsigned __int64) totalsize,ptrbuf.length(),ptrbuf.capacity(),overhead,(unsigned __int64) maxtotal);
#endif
            return true;
        }
        else
            return false;
    }

    void sort(ICompare & compare, bool stable, unsigned maxcores)
    {
        unsigned n = ordinality();
        if (n>1) {
            const byte ** res = base();
            if (stable) {
                MemoryAttr tmp;
                void ** ptrs = (void **)tmp.allocate(n*sizeof(void *));
                memcpy(ptrs,res,n*sizeof(void **));
                parqsortvecstable(ptrs, n, compare, (void ***)res, maxcores); // use res for index
                while (n--) {
                    *res = **((byte ***)res);
                    res++;
                }
            }
            else 
                parqsortvec((void **)res, n, compare, maxcores);
        }
    }

    void partition(ICompare & compare,unsigned num,UnsignedArray &out) // returns num+1 points
    {
        unsigned p=0;
        unsigned n = ordinality();
        const byte **ptrs = (const byte **)ptrbuf.toByteArray();
        while (num) {
            out.append(p);
            if (p<n) {
                unsigned q = p+(n-p)/num;
                if (p==q) { // skip to next group
                    while (q<n) {
                        q++;
                        if ((q<n)&&(compare.docompare(ptrs[p],ptrs[q])!=0)) // ensure at next group
                            break;
                    }
                }
                else {
                    while ((q<n)&&(q!=p)&&(compare.docompare(ptrs[q-1],ptrs[q])==0)) // ensure at start of group
                        q--;
                }
                p = q;
            }
            num--;
        }
        out.append(n);
    }

    void setSizing(bool _sizing,bool _raiseexceptions) // ,IOutputRowSerializer *_serializer)
    {
        sizing = _sizing;
        raiseexceptions = _raiseexceptions;
    }

    unsigned load(IRowStream &stream,bool ungroup); // doesn't check for overflow
    unsigned load(IRowStream &stream, bool ungroup, bool &abort, bool *overflowed=NULL);
    
    IRowStream *createRowStream(unsigned start=0,unsigned num=(unsigned)-1, bool streamowns=true);
    unsigned save(IRowWriter *writer,unsigned start=0,unsigned num=(unsigned)-1, bool streamowns=true);
    void setNull(unsigned idx);
    void transfer(CLegacyThorRowArray &from);
    void swapWith(CLegacyThorRowArray &from);

    void serialize(IOutputRowSerializer *_serializer,IRowSerializerTarget &out);
    void serialize(IOutputRowSerializer *_serializer,MemoryBuffer &mb,bool hasnulls);
    unsigned serializeblk(IOutputRowSerializer *_serializer,MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count);
    void deserialize(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,size32_t sz,const void *buf,bool hasnulls);
    void deserializerow(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,IRowDeserializerSource &in); // NB single row not NULL

    void reorder(unsigned start,unsigned num, unsigned *neworder);

    void setRaiseExceptions(bool on=true) { raiseexceptions=on; }

    void reserve(unsigned n);
    void setRow(unsigned idx,const void *row) // takes ownership of row
    {
        assertex(idx<numelem);
        const byte ** rp = ((const byte **)ptrbuf.toByteArray())+idx;
        OwnedConstThorRow old = *rp;
        if (old&&sizing) 
            adjSize(old,false);
        *rp = (const byte *)row;
        if (sizing) 
            adjSize(row,true);
    }
    void ensure(unsigned size)
    {
        if (size<=numelem) return;
        reserve(size-numelem);
    }
};

//////////////

// JCSMORE
enum {
    InitialSortElements = 0,
    //The number of rows that can be added without entering a critical section, and therefore also the number
    //of rows that might not get freed when memory gets tight.
    CommitStep=32
};

graph_decl StringBuffer &getRecordString(const void *key, IOutputRowSerializer *serializer, const char *prefix, StringBuffer &out);

#define SPILL_PRIORITY_JOIN 10
#define SPILL_PRIORITY_SELFJOIN 10
#define SPILL_PRIORITY_HASHJOIN 10
#define SPILL_PRIORITY_LARGESORT 10
#define SPILL_PRIORITY_GROUPSORT 20
#define SPILL_PRIORITY_OVERFLOWABLE_BUFFER 50
#define SPILL_PRIORITY_SPILLABLE_STREAM 50

#if 1
class graph_decl CThorExpandingRowArray : public CSimpleInterface
{
protected:
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;
    IOutputRowDeserializer *deserializer;

    roxiemem::IRowManager *rowManager;
    const void **rows;
    void **stableSortTmp;
    bool stableSort, throwOnOom, allowNulls;
    roxiemem::rowidx_t maxRows;  // Number of rows that can fit in the allocated memory.
    roxiemem::rowidx_t numRows;  // rows that have been added can only be updated by writing thread.

    void init(roxiemem::rowidx_t initialSize, bool stable);
    const void *allocateNewRows(roxiemem::rowidx_t requiredRows, OwnedConstThorRow &newStableSortTmp);
    void doSort(unsigned n, void **const rows, ICompare &compare, unsigned maxCores);
    void doSave(unsigned n, const void **rows, bool grouped, IFile &file);
    virtual bool ensure(roxiemem::rowidx_t requiredRows);

public:
    CThorExpandingRowArray(CActivityBase &activity, roxiemem::rowidx_t initialSize=InitialSortElements);
    CThorExpandingRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool stable=false, roxiemem::rowidx_t initialSize=InitialSortElements);
    ~CThorExpandingRowArray();
    // NB: throws error on OOM by default
    void setup(IRowInterfaces *rowIf, bool stableSort=false, bool throwOnOom=true, bool allowNulls=false);

    void clearRows();
    void kill();

    void setRow(roxiemem::rowidx_t idx, const void *row) // NB: takes ownership
    {
        OwnedConstThorRow _row = row;
        assertex(idx < maxRows);
        const void *oldRow = rows[idx];
        if (oldRow)
            ReleaseThorRow(oldRow);
        rows[idx] = _row.getClear();
    }
    inline bool append(const void *row) // NB: takes ownership
    {
        assertex(row || allowNulls);
        OwnedConstThorRow _row = row;
        if (numRows >= maxRows)
        {
            if (!ensure(numRows+1))
                return false;
        }
        rows[numRows++] = _row.getClear();
        return true;
    }
    inline const void *query(roxiemem::rowidx_t i) const
    {
        if (i>=numRows)
            return NULL;
        return rows[i];
    }
    inline const void *get(roxiemem::rowidx_t i) const
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        if (row)
            LinkRoxieRow(row);
        return row;
    }
    inline const void *getClear(roxiemem::rowidx_t i)
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        rows[i] = NULL;
        return row;
    }
    inline roxiemem::rowidx_t ordinality() const { return numRows; }

    inline const void **getRowArray() { return rows; }
    void swap(CThorExpandingRowArray &src);
    void transfer(CThorExpandingRowArray &from)
    {
        kill();
        swap(from);
    }
    void transferRows(roxiemem::rowidx_t & outNumRows, const void * * & outRows);
    void transferFrom(CThorExpandingRowArray &src);
    void removeRows(roxiemem::rowidx_t start, roxiemem::rowidx_t n);
    void sort(ICompare &compare, unsigned maxCores);
    void reorder(roxiemem::rowidx_t start, roxiemem::rowidx_t num, unsigned *neworder);

    unsigned save(IFile &file, bool preserveGrouping);
    IRowStream *createRowStream(roxiemem::rowidx_t start=0, roxiemem::rowidx_t num=(roxiemem::rowidx_t)-1, bool streamOwns=true);

    void partition(ICompare &compare, unsigned num, UnsignedArray &out); // returns num+1 points

    offset_t serializedSize();
    void serialize(IRowSerializerTarget &out);
    void serialize(MemoryBuffer &mb,bool hasnulls);
    unsigned serializeBlock(MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count);
    void deserialize(size32_t sz, const void *buf, bool hasnulls);
    void deserializeRow(IRowDeserializerSource &in); // NB single row not NULL
};

class graph_decl CThorSpillableRowArray : private CThorExpandingRowArray
{
    const size32_t commitDelta;  // How many rows need to be written before they are added to the committed region?
    roxiemem::rowidx_t firstRow; // Only rows firstRow..numRows are considered initialized.  Only read/write within cs.
    roxiemem::rowidx_t commitRows;  // can only be updated by writing thread within a critical section
    mutable CriticalSection cs;

protected:
    virtual bool ensure(roxiemem::rowidx_t requiredRows);

public:
    class CThorSpillableRowArrayLock
    {
        CThorSpillableRowArrayLock(CThorSpillableRowArrayLock &); // avoid accidental use
        const CThorSpillableRowArray & rows;
    public:
        inline CThorSpillableRowArrayLock(const CThorSpillableRowArray &_rows) : rows(_rows) { rows.lock(); }
        inline ~CThorSpillableRowArrayLock() { rows.unlock(); }
    };


    CThorSpillableRowArray(CActivityBase &activity, roxiemem::rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    CThorSpillableRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool stable=false, roxiemem::rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    // NB: throwOnOom false
    void setup(IRowInterfaces *rowIf, bool stableSort=false, bool throwOnOom=false, bool allowNulls=false)
    {
        CThorExpandingRowArray::setup(rowIf, stableSort, throwOnOom, allowNulls);
    }
    void kill() { CThorExpandingRowArray::kill(); }
    void clearRows()
    {
        assertex(0 == firstRow);
        CThorExpandingRowArray::clearRows();
        firstRow = 0;
        commitRows = 0;
    }
    void transferRows(roxiemem::rowidx_t & outNumRows, const void * * & outRows);
    void flush();
    inline bool append(const void *row)
    {
        assertex(row || allowNulls);
        OwnedConstThorRow _row = row;
        if (numRows >= maxRows)
        {
            if (!ensure(numRows+1))
            {
                flush();
                if (numRows >= maxRows)
                    return false;
            }
        }
        rows[numRows++] = _row.getClear();
        return true;
    }

    //The following can be accessed from the reader without any need to lock
    inline const void *query(roxiemem::rowidx_t i) const
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::query(i);
    }
    inline const void *get(roxiemem::rowidx_t i) const
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::get(i);
    }
    inline const void *getClear(roxiemem::rowidx_t i)
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::getClear(i);
    }

    //A thread calling the following functions must own the lock, or guarantee no other thread will access
    void sort(ICompare & compare, unsigned maxcores);
    unsigned save(IFile &file, bool preserveGrouping);
    const void **getBlock(roxiemem::rowidx_t readRows);
    inline void noteSpilled(roxiemem::rowidx_t spilledRows)
    {
        firstRow += spilledRows;
    }

    //The block returned is only valid until the critical section is released

    inline roxiemem::rowidx_t firstCommitted() const { return firstRow; }
    inline roxiemem::rowidx_t numCommitted() const { return commitRows - firstRow; }

    //Locking functions - use CThorSpillableRowArrayLock above
    inline void lock() const { cs.enter(); }
    inline void unlock() const { cs.leave(); }

// access to
    void swap(CThorSpillableRowArray &src);
    void transfer(CThorSpillableRowArray &from)
    {
        kill();
        swap(from);
    }
    void transferFrom(CThorExpandingRowArray &src);

    IRowStream *createRowStream();

    offset_t serializedSize()
    {
        if (firstRow > 0)
            throwUnexpected();
        return CThorExpandingRowArray::serializedSize();
    }
    void serialize(IRowSerializerTarget &out)
    {
        if (firstRow > 0)
            throwUnexpected();
        CThorExpandingRowArray::serialize(out);
    }
    void serialize(MemoryBuffer &mb,bool hasNulls)
    {
        if (firstRow > 0)
            throwUnexpected();
        CThorExpandingRowArray::serialize(mb, hasNulls);
    }
    void deserialize(size32_t sz, const void *buf, bool hasNulls){ CThorExpandingRowArray::deserialize(sz, buf, hasNulls); }
    void deserializeRow(IRowDeserializerSource &in) { CThorExpandingRowArray::deserializeRow(in); }
};


#else
class graph_decl CThorRowFixedSizeArray : public roxiemem::RoxieSimpleInputRowArray
{
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;

public:
    CThorRowFixedSizeArray(CActivityBase &_activity);
    CThorRowFixedSizeArray(CActivityBase &_activity, IRowInterfaces *rowIf);
    void setup(IRowInterfaces *_rowIf);

    void swap(CThorRowFixedSizeArray &src);
    void transferRows(roxiemem::rowidx_t &outNumRows, const void **&outRows);
    const void **getRowArray();

    void removeRows(roxiemem::rowidx_t start, roxiemem::rowidx_t n);

    void partition(ICompare &compare, unsigned num, UnsignedArray &out); // returns num+1 points
    IRowStream *createRowStream(unsigned start=0,unsigned num=(unsigned)-1, bool streamowns=true);
    unsigned save(IFile &file, unsigned start=0, unsigned num=(unsigned)-1, bool streamowns=true);

    offset_t serializedSize();
    void serialize(IRowSerializerTarget &out);
    void serialize(MemoryBuffer &mb,bool hasnulls);
    unsigned serializeBlock(MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count);
};



class graph_decl CThorExpandingRowArray : public roxiemem::RoxieOutputRowArray
{
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;
    IOutputRowDeserializer *deserializer;
    roxiemem::rowidx_t initialSize;

    void **stableSortTmp;
    bool stableSort;

public:
    CThorExpandingRowArray(CActivityBase &activity, roxiemem::rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    CThorExpandingRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool stable=false, roxiemem::rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    ~CThorExpandingRowArray();
    void setup(IRowInterfaces *rowIf, bool stable=false);

    void swap(CThorExpandingRowArray &src);
    void transfer(CThorExpandingRowArray &from)
    {
        kill();
        swap(from);
    }
	void transferFrom(CThorRowFixedSizeArray &src);
    void removeRows(roxiemem::rowidx_t start, roxiemem::rowidx_t n);
    void sort(ICompare & compare, unsigned maxcores);
    void reorder(unsigned start, unsigned num, unsigned *neworder);

    unsigned save(IFile &file, bool grouped);
    IRowStream *createRowStream();

    offset_t serializedSize();
    void serialize(IRowSerializerTarget &out);
    void serialize(MemoryBuffer &mb,bool hasnulls);
    void deserialize(size32_t sz, const void *buf, bool hasnulls);
    void deserializeRow(IRowDeserializerSource &in); // NB single row not NULL

    virtual bool ensure(roxiemem::rowidx_t requiredRows);
};
#endif


enum RowCollectorFlags { rc_mixed, rc_allMem, rc_allDisk, rc_allDiskOrAllMem };
interface IThorRowCollectorCommon : extends IInterface
{
    virtual rowcount_t numRows() const = 0;
    virtual unsigned numOverflows() const = 0;
    virtual unsigned overflowScale() const = 0;
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort=true) = 0;
    virtual void transferRowsIn(CThorExpandingRowArray &src) = 0;
};

interface IThorRowLoader : extends IThorRowCollectorCommon
{
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50) = 0;
    virtual IRowStream *load(IRowStream *in, bool &abort, bool preserveGrouping=false, CThorExpandingRowArray *allMemRows=NULL) = 0;
    virtual IRowStream *loadGroup(IRowStream *in, bool &abort, CThorExpandingRowArray *allMemRows=NULL);
};

interface IThorRowCollector : extends IThorRowCollectorCommon
{
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50, bool preserveGrouping=false) = 0;
    virtual IRowWriter *getWriter() = 0;
    virtual void reset() = 0;
    virtual IRowStream *getStream() = 0;
};

extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity);
extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50, bool preserveGrouping=false);




class CSDSServerStatus;


extern graph_decl ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *status=NULL);

extern graph_decl void setThorVMSwapDirectory(const char *swapdir);

class IPerfMonHook; 
extern graph_decl IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

extern graph_decl void setLCRrowCRCchecking(bool on=true);



#endif
