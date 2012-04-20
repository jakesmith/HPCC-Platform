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

#include "platform.h"

#include "jmisc.hpp"
#include "jio.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jvmem.hpp"

#include "thbufdef.hpp"
#include "thor.hpp"
#include "thormisc.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "daclient.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "eclrtl.hpp"
#include "roxiemem.hpp"
#include "roxierow.hpp"

#include "thmem.hpp"
#include "thgraph.hpp"

#include "thalloc.hpp"

#undef ALLOCATE
#undef CLONE
#undef RESIZEROW
#undef SHRINKROW
#undef MEMACTIVITYTRACESTRING 


#include "thbuf.hpp"
#include "thmem.hpp"

#ifdef _DEBUG
//#define _TESTING
#define ASSERTEX(c) assertex(c)
#else
#define ASSERTEX(c)
#endif

static memsize_t MTthreshold=0; 
static CriticalSection MTcritsect;  // held when blocked 
static Owned<ILargeMemLimitNotify> MTthresholdnotify;
static bool MTlocked = false;


class CThorRowArrayException: public CSimpleInterface, public IThorRowArrayException
{
    size32_t sz;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CThorRowArrayException(size32_t _sz) 
    {
//      DebugBreak();
        sz = _sz;
    };
    
    int             errorCode() const { return 101; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        return str.append("CThorRowArray: Group too large: ").append(sz).append(" bytes");
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

IThorRowArrayException *createRowArrayException(size32_t sz)
{
    return new CThorRowArrayException(sz);
}


void checkMultiThorMemoryThreshold(bool inc)
{
    if (MTthresholdnotify.get()) {
        CriticalBlock block(MTcritsect);
        memsize_t used = 0; // JCSMORE - might work via callback in new scheme
        if (MTlocked) {
            if (used<MTthreshold/2) {
                DBGLOG("Multi Thor threshold lock released: %"I64F"d",(offset_t)used);
                MTlocked = false;
                MTthresholdnotify->give(used);
            }
        }
        else if (used>MTthreshold) {
            DBGLOG("Multi Thor threshold  exceeded: %"I64F"d",(offset_t)used);
            if (!MTthresholdnotify->take(used)) {
                throw createOutOfMemException(-9,
                    1024,  // dummy value
                    used);
            }
            DBGLOG("Multi Thor lock taken");
            MTlocked = true;
        }
    }
}

extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify)
{
    CriticalBlock block(MTcritsect);
    if (MTthresholdnotify.get()&&!notify&&MTlocked) {
        MTlocked = false;
        MTthresholdnotify->give(0);
    }
    MTthreshold = size;
    MTthresholdnotify.set(notify);
    if (notify)
        checkMultiThorMemoryThreshold(true);
}


static memsize_t largeMemSize = 0;
memsize_t setLargeMemSize(unsigned limitMB)
{
    memsize_t prevLargeMemSize = largeMemSize;
    largeMemSize = 1024*1024*(memsize_t)limitMB;
    return prevLargeMemSize;
}

memsize_t queryLargeMemSize()
{
    if (0 == largeMemSize)
        throwUnexpected();
    return largeMemSize;
}


CLegacyThorRowArray::CLegacyThorRowArray()
{
    numelem = 0;
    totalsize = 0;
    overhead = 0;
    sizing = false;
    raiseexceptions = false;
    memsize_t tmp = queryLargeMemSize();

    if (tmp>0xffffffff)
        maxtotal = 0xffffffff;
    else
        maxtotal = (unsigned)tmp;
    if (maxtotal<0x100000)
        maxtotal = 0x100000;
}

void CLegacyThorRowArray::adjSize(const void *row, bool inc)
{
    if (!row)
        return;
    size32_t size = thorRowMemoryFootprint(NULL, row);
    size32_t prevtot = totalsize;
    if (inc) {
        if (raiseexceptions) {
            size32_t tot = size+totalMem()+PERROWOVERHEAD;
            if (tot>maxtotal)
                throw new CThorRowArrayException(tot);
        }
        totalsize += size;
        overhead += PERROWOVERHEAD;  // more - take into acount memory manager granularity?
    }
    else {
        totalsize -= size;
        overhead -= PERROWOVERHEAD;  // more - take into acount memory manager granularity?
    }
    if (!prevtot||!totalsize||(prevtot/0x100000!=totalsize/0x100000)) { // just check on transitions or when 0
        checkMultiThorMemoryThreshold(inc);
    }
}

void CLegacyThorRowArray::setNull(unsigned idx)
{
    OwnedConstThorRow row = itemClear(idx);
}

void CLegacyThorRowArray::removeRows(unsigned i,unsigned n)
{
    unsigned o = ordinality();
    if (i>=o)
        return;
    if (i+n>o) 
        n = o-i;
    if (n==0)
        return;
    if (n==o) {
        reset(false);
        return;
    }
    const byte **from = ((const byte **)ptrbuf.toByteArray())+i;
    for (unsigned j=0; j<n; j++) {
        if (sizing)
            adjSize(from[j],false);
        ReleaseThorRow(from[j]);
    }
    memmove(&from[0],&from[n],(o-n-i)*sizeof(const void *));
    ptrbuf.setLength(ptrbuf.length()-n*sizeof(const void *));
    numelem -= n;
}

        

unsigned CLegacyThorRowArray::load(IRowStream &stream,bool ungrouped)
{
    unsigned n = 0;
    loop {
        OwnedConstThorRow row = stream.nextRow();
        if (!row) {
            if (ungrouped)
                row.setown(stream.nextRow());
            if (!row)
                break;
        }
        append(row.getLink());      // use getLink incase throws exception
        n++;
    }       
    return n;
}

unsigned CLegacyThorRowArray::load(IRowStream &stream, bool ungrouped, bool &abort, bool *overflowed)
{
    unsigned n = 0;
    if (overflowed)
        *overflowed = false;
    loop {
        OwnedConstThorRow row = stream.nextRow();
        if (!row) {
            if (ungrouped)
                row.setown(stream.nextRow());
            if (!row)
                break;
        }
        append(row.getLink());      // use getLink incase throws exception
        n++;
        if (overflowed&&isFull()) {
            *overflowed=true; 
            break;
        }
    }       
    return n;
}

void CLegacyThorRowArray::transfer(CLegacyThorRowArray &from)
{
    clear();
    swapWith(from);

}

void CLegacyThorRowArray::swapWith(CLegacyThorRowArray &from)
{
    ptrbuf.swapWith(from.ptrbuf);
    unsigned t = numelem;
    numelem = from.numelem;
    from.numelem = t;
    size32_t ts = totalsize;
    totalsize = from.totalsize; 
    from.totalsize = ts;
    ts = overhead;
    overhead = from.overhead;
    from.overhead = ts;
    ts = maxtotal;
    maxtotal = from.maxtotal;
    from.maxtotal = ts;
    IOutputRowSerializer *sz = serializer.getClear();
    serializer.setown(from.serializer.getClear());
    from.serializer.setown(sz);
}


void CLegacyThorRowArray::serialize(IOutputRowSerializer *serializer,IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    for (unsigned i=0;i<numelem;i++)
    {
        const void *row = item(i); 
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull)
        {
            WARNLOG("CLegacyThorRowArray::serialize ignoring NULL row");
            warnnull = false;
        }

    }

}

void CLegacyThorRowArray::serialize(IOutputRowSerializer *serializer,MemoryBuffer &mb,bool hasnulls)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (!hasnulls)
        serialize(serializer,s);
    else
    {
        unsigned short guard = 0x7631;
        mb.append(guard);
        for (unsigned i=0;i<numelem;i++)
        {
            const void *row = item(i); 
            bool isnull = (row==NULL);
            mb.append(isnull);
            if (!isnull) 
                serializer->serialize(s,(const byte *)row);
        }
    }
}

unsigned CLegacyThorRowArray::serializeblk(IOutputRowSerializer *serializer,MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count)
{
    assertex(serializer);
    CMemoryRowSerializer out(mb);
    bool warnnull = true;
    if (idx>=numelem)
        return 0;
    if (numelem-idx<count)
        count = numelem-idx;
    unsigned ret = 0;
    for (unsigned i=0;i<count;i++) {
        size32_t ln = mb.length();
        const void *row = item(i+idx); 
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull) {
            WARNLOG("CLegacyThorRowArray::serialize ignoring NULL row");
            warnnull = false;
        }
        if (mb.length()>dstmax) {
            if (ln)
                mb.setLength(ln);   // make sure one row
            break;
        }
        ret++;
    }
    return ret;
}


void CLegacyThorRowArray::deserializerow(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,IRowDeserializerSource &in)
{
    RtlDynamicRowBuilder rowBuilder(&allocator);
    size32_t sz = deserializer->deserialize(rowBuilder,in);
    append(rowBuilder.finalizeRowClear(sz));
}


void CLegacyThorRowArray::deserialize(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,size32_t sz,const void *buf, bool hasnulls)
{
    if (hasnulls) {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos()) {
        if (hasnulls) {
            bool nullrow;
            d.read(sizeof(bool),&nullrow);
            if (nullrow) {
                append(NULL);
                continue;
            }
        }
        deserializerow(allocator,deserializer,d);
    }
}



IRowStream *CLegacyThorRowArray::createRowStream(unsigned start,unsigned num, bool streamowns)
{
    class cStream: public CSimpleInterface, implements IRowStream
    {
    public:
        unsigned pos;
        unsigned num;
        bool owns;
        CLegacyThorRowArray* parent;

        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        const void *nextRow()
        {
            if (!num) 
                return NULL;
            num--;
            if (owns)
                return parent->itemClear(pos++);
            const void *ret = parent->item(pos++);
            LinkThorRow(ret);
            return ret;
        }

        void stop()
        {
            num = 0;
            // could remove rest possibly
        }

    } *ret = new cStream();
    if (start>ordinality()) {
        start = ordinality();
        num = 0;
    }
    else if ((num==(unsigned)-1)||(start+num>ordinality()))
        num = ordinality()-start;
    ret->pos = start;
    ret->num = num;
    ret->owns = streamowns;
    ret->parent = this;
    return ret;
}

unsigned CLegacyThorRowArray::save(IRowWriter *writer, unsigned pos,unsigned num, bool owns)
{
    if (pos>ordinality()) {
        pos = ordinality();
        num = 0;
    }
    else if ((num==(unsigned)-1)||(pos+num>ordinality()))
        num = ordinality()-pos;
    if (!num) 
        return 0;
    PROGLOG("CLegacyThorRowArray::save %d rows",num);
    unsigned ret = 0; 
    while (num--) {
        OwnedConstThorRow row;
        if (owns) 
            row.setown(itemClear(pos++));
        else 
            row.set(item(pos++));
        writer->putRow(row.getClear());
        ret++;
    }
    PROGLOG("CLegacyThorRowArray::save done");
    return ret;
}

void CLegacyThorRowArray::reorder(unsigned start,unsigned num, unsigned *neworder)
{
    if (start>=ordinality())
        return;
    if (start+num>ordinality())
        num = ordinality()-start;
    if (!num)
        return;
    MemoryAttr ma;
    byte **tmp = (byte **)ma.allocate(num*sizeof(void *));
    byte **p = ((byte **)ptrbuf.toByteArray())+start;
    memcpy(tmp,p,num*sizeof(void *));
    for (unsigned i=0;i<num;i++) 
        p[i] = tmp[neworder[i]];
}

void CLegacyThorRowArray::reserve(unsigned n)
{
    size32_t sz = sizeof(const byte *)*n;
    if (raiseexceptions) {
        size32_t tot = sz+totalMem();
        if (tot>maxtotal)
            throw new CThorRowArrayException(tot);
    }
    memset(ptrbuf.reserve(sz),0,sz);
    numelem+=n;
}

// =================================

StringBuffer &getRecordString(const void *key, IOutputRowSerializer *serializer, const char *prefix, StringBuffer &out)
{
    MemoryBuffer mb;
    const byte *k = (const byte *)key;
    size32_t sz = 0;
    if (serializer&&k) {
        CMemoryRowSerializer mbsz(mb);
        serializer->serialize(mbsz,(const byte *)k);
        k = (const byte *)mb.bufferBase();
        sz = mb.length();
    }
    if (sz)
        out.appendf("%s(%d): ",prefix,sz);
    else {
        out.append(prefix).append(": ");
        if (k)
            sz = 16;
        else
            out.append("NULL");
    }
    bool first=false;
    while (sz) {
        if (first)
            first=false;
        else
            out.append(',');
        if ((sz>=3)&&isprint(k[0])&&isprint(k[1])&&isprint(k[2])) {
            out.append('"');
            do {
                out.append(*k);
                sz--;
                if (sz==0)
                    break;
                if (out.length()>1024)
                    break;
                k++;
            } while (isprint(*k));
            out.append('"');
        }
        if (out.length()>1024) {
            out.append("...");
            break;
        }
        if (sz) {
            out.appendf("%2x",(unsigned)*k);
            k++;
            sz--;
        }
    }
    return out;
}

CThorRowFixedSizeArray::CThorRowFixedSizeArray(CActivityBase &_activity) : activity(_activity)
{
    rowIf = NULL;
    allocator = NULL;
    serializer = NULL;
}

CThorRowFixedSizeArray::CThorRowFixedSizeArray(CActivityBase &_activity, IRowInterfaces *_rowIf) : activity(_activity), rowIf(_rowIf)
{
    setup(rowIf);
}

void CThorRowFixedSizeArray::setup(IRowInterfaces *_rowIf)
{
    rowIf = _rowIf;
    allocator = rowIf->queryRowAllocator();
    serializer = rowIf->queryRowSerializer();
}

void CThorRowFixedSizeArray::swap(CThorRowFixedSizeArray &other)
{
    IRowInterfaces *otherRowIf = other.rowIf;
    const void **otherRows = other.rows;
    roxiemem::rowidx_t otherNumRows = other.numRows;
    roxiemem::rowidx_t otherFirstRow = other.firstRow;

    other.setup(rowIf);
    other.rows = rows;
    other.numRows = numRows;
    other.firstRow = firstRow;

    setup(otherRowIf);
    rows = otherRows;
    numRows = otherNumRows;
    firstRow = otherFirstRow;
}

void CThorRowFixedSizeArray::transferRows(roxiemem::rowidx_t &outNumRows, const void **&outRows)
{
    assertex(0 == firstRow);
    outNumRows = numRows;
    outRows = rows;
    numRows = 0;
    rows = NULL;
}

const void **CThorRowFixedSizeArray::getRowArray()
{
    return rows;
}

offset_t CThorRowFixedSizeArray::serializedSize()
{
    roxiemem::rowidx_t c = ordinality();
    offset_t total = 0;
    for (unsigned i=0; i<c; i++)
    {
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)rows[i]);
        total += ssz.size();
    }
    return total;
}

void CThorRowFixedSizeArray::serialize(IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    for (unsigned i=0;i<ordinality();i++)
    {
        const void *row = query(i);
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull)
        {
            WARNLOG("CThorRowFixedSizeArray::serialize ignoring NULL row");
            warnnull = false;
        }
    }
}

void CThorRowFixedSizeArray::serialize(MemoryBuffer &mb, bool hasnulls)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (!hasnulls)
        serialize(s);
    else {
        unsigned short guard = 0x7631;
        mb.append(guard);
        for (unsigned i=0;i<ordinality();i++)
        {
            const void *row = query(i);
            bool isnull = (row==NULL);
            mb.append(isnull);
            if (!isnull)
                serializer->serialize(s,(const byte *)row);
        }
    }
}

unsigned CThorRowFixedSizeArray::serializeBlock(MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count)
{
    assertex(serializer);
    CMemoryRowSerializer out(mb);
    bool warnnull = true;
    unsigned num=ordinality();
    if (idx>=num)
        return 0;
    if (num-idx<count)
        count = num-idx;
    unsigned ret = 0;
    for (unsigned i=0;i<count;i++) {
        size32_t ln = mb.length();
        const void *row = query(i+idx);
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull) {
            WARNLOG("CThorRowFixedSizeArray::serialize ignoring NULL row");
            warnnull = false;
        }
        if (mb.length()>dstmax) {
            if (ln)
                mb.setLength(ln);   // make sure one row
            break;
        }
        ret++;
    }
    return ret;
}

void CThorRowFixedSizeArray::partition(ICompare &compare, unsigned num, UnsignedArray &out) // returns num+1 points
{
    unsigned p=0;
    unsigned n = ordinality();
    while (num)
    {
        out.append(p);
        if (p<n)
        {
            unsigned q = p+(n-p)/num;
            if (p==q){ // skip to next group
                while (q<n)
                {
                    q++;
                    if ((q<n)&&(compare.docompare(rows[p],rows[q])!=0)) // ensure at next group
                        break;
                }
            }
            else
            {
                while ((q<n)&&(q!=p)&&(compare.docompare(rows[q-1],rows[q])==0)) // ensure at start of group
                    q--;
            }
            p = q;
        }
        num--;
    }
    out.append(n);
}

IRowStream *CThorRowFixedSizeArray::createRowStream(unsigned start, unsigned num, bool streamowns)
{
    class CStream : public CSimpleInterface, implements IRowStream
    {
        bool owns;
        rowcount_t pos, lastRow;
        CThorRowFixedSizeArray &parent;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStream(CThorRowFixedSizeArray &_parent, bool _owns, rowcount_t firstRow, rowcount_t _lastRow)
            : parent(_parent), owns(_owns), pos(firstRow), lastRow(_lastRow)
        {
        }
        const void *nextRow()
        {
            if (pos >= lastRow)
                return NULL;
            if (owns)
                return parent.getClear(pos++);
            else
                return parent.get(pos++);
        }
        void stop() { }
    };
    if (start>ordinality())
        start = ordinality();
    rowcount_t lastRow;
    if ((num==(unsigned)-1)||(start+num>ordinality()))
        lastRow = ordinality();
    else
        lastRow = start+num;

    return new CStream(*this, streamowns, start, lastRow); // NB: links this
}

unsigned CThorRowFixedSizeArray::save(IFile &file, unsigned pos, unsigned num, bool owns)
{
    Owned<IExtRowWriter> writer = createRowWriter(&file, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), false, false, true);
    if (pos>ordinality()) { // JCSMORE why/how/when??
        pos = ordinality();
        num = 0;
    }
    else if ((num==(unsigned)-1)||(pos+num>ordinality()))
        num = ordinality()-pos;
    if (!num)
        return 0;
    PROGLOG("CThorRowFixedSizeArray::save %d rows",num);
    unsigned ret = 0;
    while (num--) {
        OwnedConstThorRow row;
        if (owns)
            row.setown(getClear(pos++));
        else
            row.set(query(pos++));
        writer->putRow(row.getClear());
        ret++;
    }
    writer.clear();
    PROGLOG("CThorRowFixedSizeArray::save done");
    return ret;
}

void CThorRowFixedSizeArray::removeRows(roxiemem::rowidx_t start, roxiemem::rowidx_t n)
{
    assertex(start>=firstRow);
    assertex(numRows-start >= n);
    assertex((0 == n) || (NULL != rows));
    if (rows)
    {
        for (roxiemem::rowidx_t i = start; i < start+n; i++)
            ReleaseRoxieRow(rows[i]);
        //firstRow = 0;
        numRows -= n;
        const void **from = rows+start;
        memmove(from, from+n, n * sizeof(void *));
    }
}

//====

class CSpillableStream : public CSimpleInterface, implements IRowStream, implements roxiemem::IBufferedRowCallback
{
    CActivityBase &activity;
    roxiemem::rowidx_t pos, numReadRows, granularity;
    IRowInterfaces *rowIf;
    const void **readRows;
    CThorExpandingRowArray rows;
    OwnedIFile spillFile;
    Owned<IRowStream> spillStream;

    bool spillRows()
    {
        roxiemem::rowidx_t numRows = rows.numCommitted();
        if (0 == numRows)
            return false;

        CThorRowFixedSizeArray outRows(activity);
        outRows.transferFrom(rows);

        StringBuffer tempname;
        GetTempName(tempname,"streamspill", true);
        spillFile.setown(createIFile(tempname.str()));

        outRows.save(*spillFile);
        rows.noteSpilled(numRows);
        return true;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSpillableStream(CActivityBase &_activity, CThorExpandingRowArray &inRows, IRowInterfaces *_rowIf)
        : activity(_activity), rowIf(_rowIf), rows(_activity)
    {
        rows.setup(rowIf);
        rows.swap(inRows);
        pos = numReadRows = 0;
        granularity = 500; // JCSMORE - rows

        // a small amount of rows to read from swappable rows
        roxiemem::IRowManager *rowManager = activity.queryJob().queryRowManager();
        readRows = static_cast<const void * *>(rowManager->allocate(granularity * sizeof(void*), activity.queryContainer().queryId()));
        activity.queryJob().queryRowManager()->addRowBuffer(this);
    }
    ~CSpillableStream()
    {
        activity.queryJob().queryRowManager()->removeRowBuffer(this);
        spillStream.clear();
        if (spillFile)
            spillFile->remove();
        ::ReleaseThorRow(readRows);
    }

// IRowStream
    virtual const void *nextRow()
    {
        if (spillStream)
            return spillStream->nextRow();
        if (pos == numReadRows)
        {
            roxiemem::RoxieOutputRowArrayLock block(rows);
            if (spillFile)
            {
                spillStream.setown(createSimpleRowStream(spillFile, rowIf));
                return spillStream->nextRow();
            }
            roxiemem::rowidx_t fetch = rows.numCommitted();
            if (0 == fetch)
                return NULL;
            if (fetch >= granularity)
                fetch = granularity;
            const void **toRead = rows.getBlock(fetch);
            memcpy(readRows, toRead, fetch * sizeof(void *));
            rows.noteSpilled(fetch);
            rows.flush();
            numReadRows = fetch;
            pos = 0;
        }
        const void *row = readRows[pos];
        readRows[pos] = NULL;
        ++pos;
        return row;
    }
    virtual void stop() { }

// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return SPILL_PRIORITY_SPILLABLE_STREAM;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        roxiemem::RoxieOutputRowArrayLock block(rows);
        return spillRows();
    }
};


//====

#if 1
void CThorExpandingRowArray::init(roxiemem::rowidx_t initialSize, bool stable)
{
    rowManager = _activity.queryJob().queryRowManager();
    stableSortTmp = NULL;
    if (initialSize)
    {
        rows = static_cast<const void * *>(rowManager->allocate(initialSize * sizeof(void*), RowArrayActivityId));
        maxRows = RoxieRowCapacity(rows) / sizeof(void *);
        if (stableSort)
            stableSortTmp = static_cast<void **>(rowManager->allocate(initialSize * sizeof(void*), activity.queryContainer().queryId()));
    }
    else
    {
        rows = NULL;
        maxRows = 0;
    }
    numRows = 0;
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, roxiemem::rowidx_t initialSize) : activity(_activity)
{
    init(initialSize, false);
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, IRowInterfaces *_rowIf, bool _stableSort, roxiemem::rowidx_t initialSize) : activity(_activity)
{
    init(initialSize, _stableSort);
    setup(_rowIf, _stableSort);
}

CThorExpandingRowArray::~CThorExpandingRowArray()
{
    if (stableSortTmp)
        ReleaseThorRow(stableSortTmp);
}

void CThorExpandingRowArray::setup(IRowInterfaces *_rowIf, bool _stableSort)
{
    rowIf = _rowIf;
    stableSort = _stableSort;
    allocator = rowIf->queryRowAllocator();
    deserializer = rowIf->queryRowDeserializer();
    serializer = rowIf->queryRowSerializer();
}

void CThorExpandingRowArray::swap(CThorExpandingRowArray &other)
{
    roxiemem::IRowManager *otherRowManager = other.rowManager;
    IRowInterfaces *otherRowIf = other.rowIf;
    const void **otherRows = other.rows;
    void **otherStableSortTmp = other.stableSortTmp;
    bool otherStableSort = other.stableSort;
    roxiemem::rowidx_t otherMaxRows = other.maxRows;
    roxiemem::rowidx_t otherFirstRow = other.firstRow;
    roxiemem::rowidx_t otherNumRows = other.numRows;
    roxiemem::rowidx_t otherCommitRows = other.commitRows;

    other.rowManager = rowManager;
    other.setup(rowIf, stableSort);
    other.rows = rows;
    other.stableSortTmp = stableSortTmp;
    other.maxRows = maxRows;
    other.firstRow = firstRow;
    other.numRows = numRows;
    other.commitRows = commitRows;

    rowManager = otherRowManager;
    setup(otherRowIf, otherStableSort);
    rows = otherRows;
    stableSortTmp = otherStableSortTmp;
    maxRows = otherMaxRows;
    firstRow = otherFirstRow;
    numRows = otherNumRows;
    commitRows = otherCommitRows;
}

void CThorExpandingRowArray::transferFrom(CThorRowFixedSizeArray &donor)
{
    roxiemem::RoxieOutputRowArrayLock block(*this);
    kill();
    donor.transferRows(numRows, rows);
    commitRows = maxRows = numRows;
    if (stableSort && maxRows)
        ensure(maxRows);
}

bool CThorExpandingRowArray::ensure(roxiemem::rowidx_t requiredRows)
{
    // JCSMORE very similar to DynamicRoxieOutputRowArray::ensure
    unsigned newSize = maxRows;
    //This condition must be <= at least 1/scaling factor below otherwise you'll get an infinite loop.
    if (newSize <= 4)
        newSize = requiredRows;
    else
    {
        //What algorithm should we use to increase the size?  Trading memory usage against copying row pointers.
        // adding 50% would reduce the number of allocations.
        // anything below 32% would mean that blocks n,n+1 when freed have enough space for block n+3 which might
        //   reduce fragmentation.
        //Use 25% for the moment.  It should possibly be configurable - e.g., higher for thor global sort.
        while (newSize < requiredRows)
            newSize += newSize/4;
    }

    const void **newRows = NULL;
    void **newStableSortTmp = NULL;
    try
    {
        newRows = static_cast<const void **>(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
        if (!newRows)
            return false;
        if (stableSort)
        {
            newStableSortTmp = static_cast<void **>(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
            if (!newStableSortTmp)
            {
                ReleaseThorRow(newRows);
                return false;
            }
        }
    }
    catch (IException * e)
    {
        if (newRows)
            ReleaseThorRow(newRows);
        //Pahological cases - not enough memory to reallocate the target row buffer, or no contiguous pages available.
        unsigned code = e->errorCode();
        if ((code == ROXIEMM_MEMORY_LIMIT_EXCEEDED) || (code == ROXIEMM_MEMORY_POOL_EXHAUSTED))
        {
            e->Release();
            return false;
        }
        throw;
    }

    //Only the writer is allowed to reallocate rows (otherwise append can't be optimized), so rows is valid outside the lock
    const void **oldRows = rows;
    void **oldStableSortTmp = stableSortTmp;
    {
        roxiemem::RoxieOutputRowArrayLock block(*this);
        oldRows = rows;
        memcpy(newRows, oldRows+firstRow, (numRows - firstRow) * sizeof(void*));
        numRows -= firstRow;
        commitRows -= firstRow;
        firstRow = 0;
        rows = newRows;
        maxRows = RoxieRowCapacity(newRows) / sizeof(void *);
        stableSortTmp = newStableSortTmp;
    }
    ReleaseRoxieRow(oldRows);
    ReleaseThorRow(oldStableSortTmp);
    return true;
}

void CThorExpandingRowArray::sort(ICompare & compare, unsigned maxcores)
{
    unsigned n = numCommitted();
    if (n>1)
    {
        void **rows = (void **const)getBlock(n);
        if (stableSort)
        {
            void **_rows = rows;
            memcpy(stableSortTmp, _rows, n*sizeof(void **));
            parqsortvecstable(stableSortTmp, n, compare, (void ***)_rows, maxcores);
            while (n--)
            {
                *_rows = **((void ***)_rows);
                _rows++;
            }
        }
        else
            parqsortvec((void **const)rows, n, compare, maxcores);
    }
}

void CThorExpandingRowArray::reorder(unsigned start, unsigned num, unsigned *neworder)
{
    if (start>=numRows())
        return;
    if (start+num>numRows())
        num = numRows()-start;
    if (!num)
        return;
    MemoryAttr ma;
    void **tmp = (void **)ma.allocate(num*sizeof(void *));
    const void **p = rows + start;
    memcpy(tmp, p, num*sizeof(void *));
    for (unsigned i=0; i<num; i++)
        p[i] = tmp[neworder[i]];
}

unsigned CThorExpandingRowArray::save(IFile &file, bool grouped)
{
    Owned<IExtRowWriter> writer = createRowWriter(&file, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), false, false, true);
    roxiemem::rowidx_t numRows = numCommitted();
    if (0 == numRows)
        return 0;
    PROGLOG("CThorExpandingRowArray::save %d rows", numRows);
    const void **rows = getBlock(numRows);
    for (roxiemem::rowidx_t i=0; i < numRows; i++)
    {
        writer->putRow(rows[i]);
        rows[i] = NULL;
    }
    writer.clear();
    PROGLOG("CThorExpandingRowArray::save done");
    return numRows;
}

IRowStream *CThorExpandingRowArray::createRowStream()
{
    // NB: should only be called if locked
    return new CSpillableStream(activity, *this, rowIf);
}

offset_t CThorExpandingRowArray::serializedSize()
{
    roxiemem::rowidx_t c = numCommitted();
    assertex(serializer);
    offset_t total = 0;
    for (unsigned i=0; i<c; i++)
    {
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)rows[i]);
        total += ssz.size();
    }
    return total;
}

void CThorExpandingRowArray::serialize(IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    roxiemem::rowidx_t n = numCommitted();
    if (n)
    {
        for (roxiemem::rowidx_t i = 0; i < n; i++)
        {
            const void *row = query(i);
            if (row)
                serializer->serialize(out, (const byte *)row);
            else if (warnnull)
            {
                WARNLOG("CThorRowFixedSizeArray::serialize ignoring NULL row");
                warnnull = false;
            }
        }
    }
}

void CThorExpandingRowArray::serialize(MemoryBuffer &mb, bool hasnulls)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (!hasnulls)
        serialize(s);
    else
    {
        unsigned short guard = 0x7631;
        mb.append(guard);
        roxiemem::rowidx_t n = numCommitted();
        if (n)
        {
            for (roxiemem::rowidx_t i = 0; i < n; i++)
            {
                const void *row = query(i);
                bool isnull = (row==NULL);
                mb.append(isnull);
                if (!isnull)
                    serializer->serialize(s, (const byte *)row);
            }
        }
    }
}

void CThorExpandingRowArray::deserializeRow(IRowDeserializerSource &in)
{
    RtlDynamicRowBuilder rowBuilder(allocator);
    size32_t sz = deserializer->deserialize(rowBuilder,in);
    append(rowBuilder.finalizeRowClear(sz));
}

void CThorExpandingRowArray::deserialize(size32_t sz,const void *buf, bool hasnulls)
{
    if (hasnulls) {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos()) {
        if (hasnulls) {
            bool nullrow;
            d.read(sizeof(bool),&nullrow);
            if (nullrow) {
                append(NULL);
                continue;
            }
        }
        deserializeRow(d);
    }
}

//////////////////

CThorSpillableRowArray::CThorSpillableRowArray(CActivityBase &activity, roxiemem::rowidx_t initialSize, size32_t _commitDelta)
    : CThorExpandingRowArray(activity, initialSize), commitDelta(_commitDelta)
{

}

CThorSpillableRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool stable=false, roxiemem::rowidx_t initialSize, size32_t _commitDelta)
    : CThorExpandingRowArray(activity, rowIf, stable, initialSize), commitDelta(_commitDelta)
{
}

#else

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, roxiemem::rowidx_t _initialSize, size32_t commitDelta)
    : RoxieOutputRowArray(_activity.queryJob().queryRowManager(), _initialSize, commitDelta), activity(_activity), initialSize(_initialSize)
{
    stableSortTmp = NULL;
    rowIf = NULL;
    stableSort = false;
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, IRowInterfaces *_rowIf, bool _stableSort, roxiemem::rowidx_t _initialSize, size32_t commitDelta)
    : RoxieOutputRowArray(_activity.queryJob().queryRowManager(), _initialSize, commitDelta), activity(_activity), initialSize(_initialSize)
{
    setup(_rowIf, _stableSort);
    if (stableSort && initialSize)
        stableSortTmp = static_cast<void **>(rowManager->allocate(initialSize * sizeof(void*), activity.queryContainer().queryId()));
    else
        stableSortTmp = NULL;
}


CThorExpandingRowArray::~CThorExpandingRowArray()
{
    if (stableSortTmp)
        ReleaseThorRow(stableSortTmp);
}

void CThorExpandingRowArray::setup(IRowInterfaces *_rowIf, bool _stableSort)
{
    rowIf = _rowIf;
    stableSort = _stableSort;
    allocator = rowIf->queryRowAllocator();
    deserializer = rowIf->queryRowDeserializer();
    serializer = rowIf->queryRowSerializer();
}

void CThorExpandingRowArray::swap(CThorExpandingRowArray &other)
{
    roxiemem::IRowManager *otherRowManager = other.rowManager;
    IRowInterfaces *otherRowIf = other.rowIf;
    const void **otherRows = other.rows;
    void **otherStableSortTmp = other.stableSortTmp;
    bool otherStableSort = other.stableSort;
    roxiemem::rowidx_t otherMaxRows = other.maxRows;
    roxiemem::rowidx_t otherFirstRow = other.firstRow;
    roxiemem::rowidx_t otherNumRows = other.numRows;
    roxiemem::rowidx_t otherCommitRows = other.commitRows;

    other.rowManager = rowManager;
    other.setup(rowIf, stableSort);
    other.rows = rows;
    other.stableSortTmp = stableSortTmp;
    other.maxRows = maxRows;
    other.firstRow = firstRow;
    other.numRows = numRows;
    other.commitRows = commitRows;

    rowManager = otherRowManager;
    setup(otherRowIf, otherStableSort);
    rows = otherRows;
    stableSortTmp = otherStableSortTmp;
    maxRows = otherMaxRows;
    firstRow = otherFirstRow;
    numRows = otherNumRows;
    commitRows = otherCommitRows;
}

void CThorExpandingRowArray::transferFrom(CThorRowFixedSizeArray &donor)
{
    roxiemem::RoxieOutputRowArrayLock block(*this);
    kill();
    donor.transferRows(numRows, rows);
    commitRows = maxRows = numRows;
    if (stableSort && maxRows)
        ensure(maxRows);
}

bool CThorExpandingRowArray::ensure(roxiemem::rowidx_t requiredRows)
{
    // JCSMORE very similar to DynamicRoxieOutputRowArray::ensure
    unsigned newSize = maxRows;
    //This condition must be <= at least 1/scaling factor below otherwise you'll get an infinite loop.
    if (newSize <= 4)
        newSize = requiredRows;
    else
    {
        //What algorithm should we use to increase the size?  Trading memory usage against copying row pointers.
        // adding 50% would reduce the number of allocations.
        // anything below 32% would mean that blocks n,n+1 when freed have enough space for block n+3 which might
        //   reduce fragmentation.
        //Use 25% for the moment.  It should possibly be configurable - e.g., higher for thor global sort.
        while (newSize < requiredRows)
            newSize += newSize/4;
    }

    const void **newRows = NULL;
    void **newStableSortTmp = NULL;
    try
    {
        newRows = static_cast<const void **>(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
        if (!newRows)
            return false;
        if (stableSort)
        {
            newStableSortTmp = static_cast<void **>(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
            if (!newStableSortTmp)
            {
                ReleaseThorRow(newRows);
                return false;
            }
        }
    }
    catch (IException * e)
    {
        if (newRows)
            ReleaseThorRow(newRows);
        //Pahological cases - not enough memory to reallocate the target row buffer, or no contiguous pages available.
        unsigned code = e->errorCode();
        if ((code == ROXIEMM_MEMORY_LIMIT_EXCEEDED) || (code == ROXIEMM_MEMORY_POOL_EXHAUSTED))
        {
            e->Release();
            return false;
        }
        throw;
    }

    //Only the writer is allowed to reallocate rows (otherwise append can't be optimized), so rows is valid outside the lock
    const void **oldRows = rows;
    void **oldStableSortTmp = stableSortTmp;
    {
        roxiemem::RoxieOutputRowArrayLock block(*this);
        oldRows = rows;
        memcpy(newRows, oldRows+firstRow, (numRows - firstRow) * sizeof(void*));
        numRows -= firstRow;
        commitRows -= firstRow;
        firstRow = 0;
        rows = newRows;
        maxRows = RoxieRowCapacity(newRows) / sizeof(void *);
        stableSortTmp = newStableSortTmp;
    }
    ReleaseRoxieRow(oldRows);
    ReleaseThorRow(oldStableSortTmp);
    return true;
}

void CThorExpandingRowArray::sort(ICompare & compare, unsigned maxcores)
{
    unsigned n = numCommitted();
    if (n>1)
    {
        void **rows = (void **const)getBlock(n);
        if (stableSort)
        {
            void **_rows = rows;
            memcpy(stableSortTmp, _rows, n*sizeof(void **));
            parqsortvecstable(stableSortTmp, n, compare, (void ***)_rows, maxcores);
            while (n--)
            {
                *_rows = **((void ***)_rows);
                _rows++;
            }
        }
        else
            parqsortvec((void **const)rows, n, compare, maxcores);
    }
}

void CThorExpandingRowArray::reorder(unsigned start, unsigned num, unsigned *neworder)
{
    if (start>=numRows())
        return;
    if (start+num>numRows())
        num = numRows()-start;
    if (!num)
        return;
    MemoryAttr ma;
    void **tmp = (void **)ma.allocate(num*sizeof(void *));
    const void **p = rows + start;
    memcpy(tmp, p, num*sizeof(void *));
    for (unsigned i=0; i<num; i++)
        p[i] = tmp[neworder[i]];
}

unsigned CThorExpandingRowArray::save(IFile &file, bool grouped)
{
    Owned<IExtRowWriter> writer = createRowWriter(&file, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), false, false, true);
    roxiemem::rowidx_t numRows = numCommitted();
    if (0 == numRows)
        return 0;
    PROGLOG("CThorExpandingRowArray::save %d rows", numRows);
    const void **rows = getBlock(numRows);
    for (roxiemem::rowidx_t i=0; i < numRows; i++)
    {
        writer->putRow(rows[i]);
        rows[i] = NULL;
    }
    writer.clear();
    PROGLOG("CThorExpandingRowArray::save done");
    return numRows;
}

IRowStream *CThorExpandingRowArray::createRowStream()
{
    // NB: should only be called if locked
    return new CSpillableStream(activity, *this, rowIf);
}

offset_t CThorExpandingRowArray::serializedSize()
{
    roxiemem::rowidx_t c = numCommitted();
    assertex(serializer);
    offset_t total = 0;
    for (unsigned i=0; i<c; i++)
    {
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)rows[i]);
        total += ssz.size();
    }
    return total;
}

void CThorExpandingRowArray::serialize(IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    roxiemem::rowidx_t n = numCommitted();
    if (n)
    {
        for (roxiemem::rowidx_t i = 0; i < n; i++)
        {
            const void *row = query(i);
            if (row)
                serializer->serialize(out, (const byte *)row);
            else if (warnnull)
            {
                WARNLOG("CThorRowFixedSizeArray::serialize ignoring NULL row");
                warnnull = false;
            }
        }
    }
}

void CThorExpandingRowArray::serialize(MemoryBuffer &mb, bool hasnulls)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (!hasnulls)
        serialize(s);
    else
    {
        unsigned short guard = 0x7631;
        mb.append(guard);
        roxiemem::rowidx_t n = numCommitted();
        if (n)
        {
            for (roxiemem::rowidx_t i = 0; i < n; i++)
            {
                const void *row = query(i);
                bool isnull = (row==NULL);
                mb.append(isnull);
                if (!isnull)
                    serializer->serialize(s, (const byte *)row);
            }
        }
    }
}

void CThorExpandingRowArray::deserializeRow(IRowDeserializerSource &in)
{
    RtlDynamicRowBuilder rowBuilder(allocator);
    size32_t sz = deserializer->deserialize(rowBuilder,in);
    append(rowBuilder.finalizeRowClear(sz));
}

void CThorExpandingRowArray::deserialize(size32_t sz,const void *buf, bool hasnulls)
{
    if (hasnulls) {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos()) {
        if (hasnulls) {
            bool nullrow;
            d.read(sizeof(bool),&nullrow);
            if (nullrow) {
                append(NULL);
                continue;
            }
        }
        deserializeRow(d);
    }
}

///////////

CThorRowArrayNew::CThorRowArrayNew(CActivityBase &activity, roxiemem::rowidx_t initialSize)
    : CThorExpandingRowArray(activity, &activity, false, initialSize)
{
}

bool CThorRowArrayNew::ensure(roxiemem::rowidx_t requiredRows)
{
    if (!CThorExpandingRowArray::ensure(requiredRows))
        throw createRowArrayException((size32_t)serializedSize());
    return true;
}

///////////
void CThorExpandingRowArray::removeRows(roxiemem::rowidx_t start, roxiemem::rowidx_t n)
{
    assertex(start>=firstRow);
    assertex(numRows-start >= n);
    assertex(!n || !rows);
    if (rows)
    {
        for (roxiemem::rowidx_t i = start; i < n; i++)
            ReleaseThorRow(rows[i]);
        //firstRow = 0;
        numRows -= n;
        const void **from = rows+start;
        memmove(from, from+n, n * sizeof(void *));
    }
}
#endif

class CThorRowCollectorBase : public CSimpleInterface, implements roxiemem::IBufferedRowCallback
{
protected:
    CActivityBase &activity;
    CThorExpandingRowArray spillableRows;
    PointerIArrayOf<IFile> spillFiles;
    Owned<IOutputRowSerializer> serializer;
    RowCollectorFlags diskMemMix;
    unsigned spillPriority;
    unsigned totalRows;
    unsigned overflowCount;
    unsigned maxCores;
    unsigned outStreams;
    ICompare *iCompare;
    bool isStable, preserveGrouping;
    IRowInterfaces *rowIf;
    SpinLock readerLock;
    bool mmRegistered;

    bool spillRows()
    {
        roxiemem::rowidx_t numRows = spillableRows.numCommitted();
        if (numRows == 0)
            return false;

        totalRows += numRows;
        if (iCompare)
            spillableRows.sort(*iCompare, maxCores);

        StringBuffer tempname;
        GetTempName(tempname,"srtspill",true);
        Owned<IFile> iFile = createIFile(tempname.str());
        spillFiles.append(iFile.getLink());
        spillableRows.save(*iFile, preserveGrouping);
        spillableRows.noteSpilled(numRows);

        ++overflowCount;

        return true;
    }
    void setPreserveGrouping(bool _preserveGrouping)
    {
        preserveGrouping = _preserveGrouping;
    }
    void putRow(const void *row)
    {
        if (!spillableRows.append(row))
        {
            {
                roxiemem::RoxieOutputRowArrayLock block(spillableRows);
                //We should have been called back to free any committed rows, but occasionally it may not (e.g., if
                //the problem is global memory is exhausted) - in which case force a spill here (but add any pending
                //rows first).
                if (spillableRows.numCommitted() != 0)
                {
                    spillableRows.flush();
                    spillRows();
                }
                //Ensure new rows are written to the head of the array.  It needs to be a separate call because
                //spillRows() cannot shift active row pointer since it can be called from any thread
                spillableRows.flush();
            }
            if (!spillableRows.append(row))
            {
                ReleaseThorRow(row);
                throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Insufficient memory to append sort row");
            }
        }
    }
    IRowStream *getStream(roxiemem::RoxieSimpleInputRowArray *allMemRows)
    {
        SpinBlock b(readerLock);
        if (0 == outStreams)
        {
            spillableRows.flush();
            if ((rc_allDisk == diskMemMix) || ((rc_allDiskOrAllMem == diskMemMix) && overflowCount))
            {
                roxiemem::RoxieOutputRowArrayLock block(spillableRows);
                if (spillableRows.numCommitted())
                {
                    spillRows();
                    spillableRows.kill();
                }
            }
        }
        if (outStreams)
            assertex(rc_allDisk == diskMemMix); // JCSMORE for now, needs sharing/spilling mem/disk combo. reader
        ++outStreams;

        class CStreamFileOwner : public CSimpleInterface, implements IExtRowStream
        {
            Linked<IFile> iFile;
            IExtRowStream *stream;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
            CStreamFileOwner(IFile *_iFile, IExtRowStream *_stream) : iFile(_iFile)
            {
                stream = LINK(_stream);
            }
            ~CStreamFileOwner()
            {
                stream->Release();
                iFile->remove();
            }
        // IExtRowStream
            virtual const void *nextRow() { return stream->nextRow(); }
            virtual void stop() { stream->stop(); }
            virtual offset_t getOffset() { return stream->getOffset(); }
            virtual void stop(CRC32 *crcout=NULL) { stream->stop(); }
            virtual const void *prefetchRow(size32_t *sz=NULL) { return stream->prefetchRow(sz); }
            virtual void prefetchDone() { stream->prefetchDone(); }
            virtual void reinit(offset_t offset, offset_t len, unsigned __int64 maxRows)
            {
                stream->reinit(offset, len, maxRows);
            }
        };

        IArrayOf<IRowStream> instrms;
        ForEachItemIn(f, spillFiles)
        {
            IFile *iFile = spillFiles.item(f);
            Owned<IExtRowStream> strm = createRowStream(iFile, rowIf, 0, (offset_t) -1, (unsigned __int64)-1, false, preserveGrouping);
			spillFiles.replace(NULL, f); // stream now owns (and will delete)
            instrms.append(* new CStreamFileOwner(iFile, strm));
        }

        {
            roxiemem::RoxieOutputRowArrayLock block(spillableRows);
            if (spillableRows.numCommitted())
            {
                totalRows += spillableRows.numCommitted();
                if (iCompare)
                    spillableRows.sort(*iCompare, maxCores);
                if (rc_allDiskOrAllMem == diskMemMix)
                {
                    assertex(allMemRows);
                    allMemRows->transferFrom(spillableRows);
                    // stream cannot be used
                    return NULL;
                }
                instrms.append(*spillableRows.createRowStream()); // NB: stream will take ownership of rows in spillableRows
            }
        }
        if (instrms.ordinality()==1)
            return LINK(&instrms.item(0));
        if (iCompare)
        {
            Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
            return createRowStreamMerger(instrms.ordinality(), instrms.getArray(), iCompare, false, linkcounter);
        }
        else
            return createConcatRowStream(instrms.ordinality(),instrms.getArray());
    }
    void reset()
    {
        spillableRows.kill();
        totalRows = overflowCount = outStreams = 0;
    }
public:
    CThorRowCollectorBase(CActivityBase &_activity)
        : activity(_activity),
          spillableRows(_activity, InitialSortElements, CommitStep)
    {
        rowIf = NULL;
        iCompare = NULL;
        preserveGrouping = isStable = false;
        totalRows = 0;
        overflowCount = 0;
        diskMemMix = rc_mixed;
        spillPriority = 50;
        outStreams = 0;
        activity.queryJob().queryRowManager()->addRowBuffer(this);
        mmRegistered = true;
        maxCores = activity.queryMaxCores();
    }
    ~CThorRowCollectorBase()
    {
        reset();
        if (mmRegistered)
            activity.queryJob().queryRowManager()->removeRowBuffer(this);
    }
    void setup(IRowInterfaces *_rowIf, ICompare *_iCompare, bool _isStable, RowCollectorFlags _diskMemMix, unsigned _spillPriority)
    {
        rowIf = _rowIf;
        iCompare = _iCompare;
        isStable = _isStable;
        diskMemMix = _diskMemMix;
        if (mmRegistered && (UINT_MAX == spillPriority))
        {
            mmRegistered = false;
            activity.queryJob().queryRowManager()->removeRowBuffer(this);
        }
        spillPriority = _spillPriority;
        spillableRows.setup(rowIf, isStable);
    }
    void transferRowsOut(CThorRowFixedSizeArray &out, bool sort)
    {
        roxiemem::RoxieOutputRowArrayLock block(spillableRows);
        totalRows += spillableRows.numCommitted();
        if (sort && iCompare)
            spillableRows.sort(*iCompare, maxCores);
        out.transferFrom(spillableRows);
    }
    bool hasOverflowed() const
    {
        return 0 != overflowCount;
    }
    rowcount_t numRows() const
    {
        return totalRows;
    }
    unsigned numOverflows() const
    {
        return overflowCount;
    }
    unsigned overflowScale() const
    {
        // 1 if no spill
        if (!overflowCount)
            return 1;
        return overflowCount*2+3; // bit arbitrary
    }
    void transferRowsIn(CThorRowFixedSizeArray &src)
    {
        reset();
        spillableRows.transferFrom(src);
    }
// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return spillPriority;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        if (UINT_MAX == spillPriority)
            return false;
        roxiemem::RoxieOutputRowArrayLock block(spillableRows);
        return spillRows();
    }
};

enum TRLGroupFlag { trl_ungroup, trl_preserveGrouping, trl_stopAtEog };
class CThorRowLoader : public CThorRowCollectorBase, implements IThorRowLoader
{
    IRowStream *load(IRowStream *in, bool &abort, TRLGroupFlag grouping, roxiemem::RoxieSimpleInputRowArray *allMemRows)
    {
        reset();
        setPreserveGrouping(trl_preserveGrouping == grouping);
        while (!abort)
        {
            const void *next = in->nextRow();
            if (!next)
            {
                if (grouping == trl_stopAtEog)
                    break;
                else
                {
                    next = in->nextRow();
                    if (!next)
                        break;
                    if (grouping == trl_preserveGrouping)
                        putRow(NULL);
                }
            }
            putRow(next);
        }
        return getStream(allMemRows);
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowLoader(CActivityBase &activity) : CThorRowCollectorBase(activity)
    {
    }
// IThorRowCollectorCommon
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority)
    {
        CThorRowCollectorBase::setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
    }
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorRowFixedSizeArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorRowFixedSizeArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
// IThorRowLoader
    virtual IRowStream *load(IRowStream *in, bool &abort, bool preserveGrouping, roxiemem::RoxieSimpleInputRowArray *allMemRows)
    {
        assertex(!iCompare || !preserveGrouping); // can't sort if group preserving
        return load(in, abort, preserveGrouping?trl_preserveGrouping:trl_ungroup, allMemRows);
    }
    virtual IRowStream *loadGroup(IRowStream *in, bool &abort, roxiemem::RoxieSimpleInputRowArray *allMemRows)
    {
        return load(in, abort, trl_stopAtEog, allMemRows);
    }
};

IThorRowLoader *createThorRowLoader(CActivityBase &activity)
{
    return new CThorRowLoader(activity);
}

IThorRowLoader *createThorRowLoader(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority)
{
    Owned<IThorRowLoader> loader = new CThorRowLoader(activity);
    loader->setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
    return loader.getClear();
}

class CThorRowCollector : public CThorRowCollectorBase, implements IThorRowCollector
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowCollector(CActivityBase &activity) : CThorRowCollectorBase(activity)
    {
    }
// IThorRowCollectorCommon
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority, bool preserveGrouping)
    {
        assertex(!iCompare || !preserveGrouping); // can't sort if group preserving
        CThorRowCollectorBase::setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
        setPreserveGrouping(preserveGrouping);
    }
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorRowFixedSizeArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorRowFixedSizeArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
// IThorRowCollector
    virtual IRowWriter *getWriter()
    {
        class CWriter : public CSimpleInterface, implements IRowWriter
        {
            Linked<CThorRowCollector> parent;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWriter(CThorRowCollector *_parent) : parent(_parent)
            {
            }
        // IRowWriter
            virtual void putRow(const void *row)
            {
                parent->putRow(row);
            }
            virtual void flush()
            {
            }
        };
        return new CWriter(this);
    }
    virtual void reset()
    {
        CThorRowCollectorBase::reset();
    }
    virtual IRowStream *getStream()
    {
        return CThorRowCollectorBase::getStream(NULL);
    }
};

IThorRowCollector *createThorRowCollector(CActivityBase &activity)
{
    return new CThorRowCollector(activity);
}

IThorRowCollector *createThorRowCollector(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority, bool preserveGrouping)
{
    Owned<IThorRowCollector> collector = new CThorRowCollector(activity);
    collector->setup(rowIf, iCompare, isStable, diskMemMix, spillPriority, preserveGrouping);
    return collector.getClear();
}



void setThorInABox(unsigned num)
{
}


class cMultiThorResourceMutex: public CSimpleInterface, implements ILargeMemLimitNotify, implements IDaliMutexNotifyWaiting
{
    class cMultiThorResourceMutexThread: public Thread
    {
        cMultiThorResourceMutex &parent;
    public:
        cMultiThorResourceMutexThread(cMultiThorResourceMutex &_parent)
            : Thread("cMultiThorResourceMutexThread"),parent(_parent)
        {
        }

        int run() 
        {
            parent.run();
            return 0;
        }
    };
    Owned<cMultiThorResourceMutexThread> thread;
    Owned<IDaliMutex> mutex;
    bool stopping;
    Linked<ICommunicator> clusterComm;
    CSDSServerStatus *status;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    cMultiThorResourceMutex(const char *groupname,CSDSServerStatus *_status)
    {
        status = _status;
        stopping = false;
        clusterComm.set(&queryClusterComm());
        if (clusterComm->queryGroup().rank(queryMyNode())==0) { // master so start thread
            thread.setown(new cMultiThorResourceMutexThread(*this));
            thread->start();
            StringBuffer mname("thorres:");
            mname.append(groupname);
            mutex.setown(createDaliMutex(mname.str()));
        }

    }

    ~cMultiThorResourceMutex()
    {
        stopping = true;
        if (thread) 
            stop();
    }

    void run() // on master
    {
        PROGLOG("cMultiThorResourceMutex thread run");
        try {
            CMessageBuffer mbuf;
            while (!stopping) {
                mbuf.clear();
                rank_t from;
                unsigned timeout = 1000*60*5;
                if (clusterComm->recv(mbuf,RANK_ALL,MPTAG_THORRESOURCELOCK,&from,timeout)) {
                    byte req;
                    mbuf.read(req);
                    if (req==1) {
                        if (mutex) 
                            mutex->enter();
                    }
                    else if (req==0) {
                        if (mutex) 
                            mutex->leave();
                    }
                    clusterComm->reply(mbuf,1000*60*5);
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::run");
        }
    }

    void stop()
    {
        PROGLOG("cMultiThorResourceMutex::stop enter");
        stopping = true;
        if (mutex) 
            mutex->kill();
        try {
            clusterComm->cancel(RANK_ALL,MPTAG_THORRESOURCELOCK);
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::stop");
        }
        if (thread)
            thread->join();
        mutex.clear();
        PROGLOG("cMultiThorResourceMutex::stop leave");
    }

    bool take(memsize_t tot)
    {
        if (stopping)
            return true;
        if (mutex) 
            return mutex->enter();
        if (stopping)
            return false;
        CMessageBuffer mbuf;
        byte req = 1;
        mbuf.append(req);
        try {
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::take");
        }
        return !stopping;
    }
                                            // will raise oom exception if false returned
    void give(memsize_t tot)
    {
        if (mutex) {
            mutex->leave();
            return;
        }
        if (stopping)
            return;
        CMessageBuffer mbuf;
        byte req = 0;
        mbuf.append(req);
        try {
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::give");
        }

    }

    //IDaliMutexNotifyWaiting
    void startWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",1);
    }
    void cycleWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",status->queryProperties()->getPropInt("@memoryBlocked")+1);
    }
    void stopWait(bool got)
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",0);
    }

};


ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *_status)
{
    return new cMultiThorResourceMutex(grpname,_status);
}


class CThorAllocator : public CSimpleInterface, implements roxiemem::IRowAllocatorCache, implements IRtlRowCallback, implements IThorAllocator
{
    mutable IArrayOf<IEngineRowAllocator> allAllocators;
    mutable SpinLock allAllocatorsLock;
    Owned<roxiemem::IRowManager> rowManager;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorAllocator(memsize_t memSize)
    {
        rowManager.setown(roxiemem::createRowManager(memSize, NULL, queryDummyContextLogger(), this, false));
        rtlSetReleaseRowHook(this);
    }
    ~CThorAllocator()
    {
        rowManager.clear();
        allAllocators.kill();
        rtlSetReleaseRowHook(NULL); // nothing should use it beyond this point anyway
    }

// IThorAllocator
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        // MORE - may need to do some caching/commoning up here otherwise GRAPH in a child query may use too many
        SpinBlock b(allAllocatorsLock);
        IEngineRowAllocator *ret = createRoxieRowAllocator(*rowManager, meta, activityId, allAllocators.ordinality(), false);
        LINK(ret);
        allAllocators.append(*ret);
        return ret;
    }
    virtual roxiemem::IRowManager *queryRowManager() const
    {
        return rowManager;
    }

// IRowAllocatorCache
    virtual unsigned getActivityId(unsigned cacheId) const
    {
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(cacheId))
            return allAllocators.item(cacheId).queryActivityId();
        else
        {
            //assert(false);
            return 12345678; // Used for tracing, better than a crash...
        }
    }
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const
    {
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(cacheId))
            return allAllocators.item(cacheId).getId(out);
        else
        {
            assert(false);
            return out.append("unknown"); // Used for tracing, better than a crash...
        }
    }
    virtual void onDestroy(unsigned cacheId, void *row) const
    {
        IEngineRowAllocator *allocator;
        {
            SpinBlock b(allAllocatorsLock); // just protect the access to the array - don't keep locked for the call of destruct or may deadlock
            if (allAllocators.isItem(cacheId))
                allocator = &allAllocators.item(cacheId);
            else
            {
                assert(false);
                return;
            }
        }
        allocator->queryOutputMeta()->destruct((byte *) row);
    }
    virtual void checkValid(unsigned cacheId, const void *row) const
    {
        // JCSMORE
    }
// IRtlRowCallback
    virtual void releaseRow(const void * row) const
    {
        ReleaseRoxieRow(row);
    }
    virtual void releaseRowset(unsigned count, byte * * rowset) const
    {
        if (rowset)
        {
            if (!roxiemem::HeapletBase::isShared(rowset))
            {
                byte * * finger = rowset;
                while (count--)
                    ReleaseRoxieRow(*finger++);
            }
            ReleaseRoxieRow(rowset);
        }
    }
    virtual void *linkRow(const void * row) const
    {
        if (row) 
            LinkRoxieRow(row);
        return const_cast<void *>(row);
    }
    virtual byte * * linkRowset(byte * * rowset) const
    {
        if (rowset)
            LinkRoxieRow(rowset);
        return const_cast<byte * *>(rowset);
    }
};

IThorAllocator *createThorAllocator(memsize_t memSize)
{
    return new CThorAllocator(memSize);
}


#define OUTPUTMETACHILDROW_VERSION 2 // for now, it's only significant that non-zero
class COutputMetaWithChildRow : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IEngineRowAllocator> childAllocator;
    IOutputMetaData *childMeta;
    size32_t extraSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<ISourceRowPrefetcher> prefetcher;

    class CSerializer : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> childSerializer;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerializer(IOutputRowSerializer *_childSerializer, size32_t _extraSz) : childSerializer(_childSerializer), extraSz(_extraSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(extraSz, self);
            const byte *childRow = *(const byte **)(self+extraSz);
            if (childRow)
            {
                byte b=1;
                out.put(1, &b);
                childSerializer->serialize(out, childRow);
            }
            else
            {
                byte b=0;
                out.put(1, &b);
            }
        }
    };
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> childDeserializer;
        Linked<IEngineRowAllocator> childAllocator;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_childDeserializer, IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childDeserializer(_childDeserializer), childAllocator(_childAllocator), extraSz(_extraSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            byte * self = rowBuilder.getSelf();
            in.read(extraSz, self);
            byte b;
            in.read(1, &b);
            const void *fChildRow;
            if (b)
            {
                RtlDynamicRowBuilder childBuilder(childAllocator);
                size32_t sz = childDeserializer->deserialize(childBuilder, in);
                fChildRow = childBuilder.finalizeRowClear(sz);
            }
            else
                fChildRow = NULL;
            memcpy(self+extraSz, &fChildRow, sizeof(const void *));
            return extraSz + sizeof(const void *);
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _extraSz) : childPrefetcher(_childPrefetcher), extraSz(_extraSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(extraSz);
            byte b;
            in.read(1, &b);
            if (b)
                childPrefetcher->readAhead(in);
        }
    };


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithChildRow(IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childAllocator(_childAllocator), extraSz(_extraSz)
    {
        childMeta = childAllocator->queryOutputMeta();
    }
    virtual size32_t getRecordSize(const void *) { return extraSz + sizeof(const void *); }
    virtual size32_t getMinRecordSize() const { return extraSz + sizeof(const void *); }
    virtual size32_t getFixedSize() const { return extraSz + sizeof(const void *); }
    virtual void toXML(const byte * self, IXmlWriter & out) 
    { 
         // ignoring xml'ing extra
        //GH: I think this is what it should do
        childMeta->toXML(*(const byte **)(self+extraSz), out); 
    }
    virtual unsigned getVersion() const { return OUTPUTMETACHILDROW_VERSION; }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return MDFneeddestruct|childMeta->getMetaFlags(); }
    virtual void destruct(byte * self)
    {
        OwnedConstThorRow childRow = *(const void **)(self+extraSz);
    }
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerializer(childMeta->createRowSerializer(ctx, activityId), extraSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(childMeta->createRowDeserializer(ctx, activityId), childAllocator, extraSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(childMeta->createRowPrefetcher(ctx, activityId), extraSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() { return this; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) 
    {
        //GH: I think this is what it should do, please check
        visitor.visitRow(*(const byte **)(self+extraSz)); 
    }
};

IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz)
{
    return new COutputMetaWithChildRow(childAllocator, extraSz);
}


class COutputMetaWithExtra : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IOutputMetaData> meta;
    size32_t metaSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<ISourceRowPrefetcher> prefetcher;
    Owned<IOutputMetaData> serializedmeta;

    class CSerialization : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> serializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerialization(IOutputRowSerializer *_serializer, size32_t _metaSz) : serializer(_serializer), metaSz(_metaSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(metaSz, self);
            serializer->serialize(out, self+metaSz);
        }
    };
    //GH - This code is the same as CPrefixedRowDeserializer
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> deserializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_deserializer, size32_t _metaSz) : deserializer(_deserializer), metaSz(_metaSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            in.read(metaSz, rowBuilder.getSelf());
            CPrefixedRowBuilder prefixedBuilder(metaSz, rowBuilder);
            size32_t sz = deserializer->deserialize(prefixedBuilder, in);
            return sz+metaSz;
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _metaSz) : childPrefetcher(_childPrefetcher), metaSz(_metaSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(metaSz);
            childPrefetcher->readAhead(in);
        }
    };

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithExtra(IOutputMetaData *_meta, size32_t _metaSz) : meta(_meta), metaSz(_metaSz)
    {
    }
    virtual size32_t getRecordSize(const void *rec) 
    {
        size32_t sz = meta->getRecordSize(rec?((byte *)rec)+metaSz:NULL); 
        return sz+metaSz;
    }
    virtual size32_t getMinRecordSize() const 
    { 
        return meta->getMinRecordSize() + metaSz;
    }
    virtual size32_t getFixedSize() const 
    {
        size32_t sz = meta->getFixedSize();
        if (!sz)
            return 0;
        return sz+metaSz;
    }

    virtual void toXML(const byte * self, IXmlWriter & out) { meta->toXML(self, out); }
    virtual unsigned getVersion() const { return meta->getVersion(); }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return meta->getMetaFlags(); }
    virtual void destruct(byte * self) { meta->destruct(self); }
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerialization(meta->createRowSerializer(ctx, activityId), metaSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(meta->createRowDeserializer(ctx, activityId), metaSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(meta->createRowPrefetcher(ctx, activityId), metaSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() 
    { 
        IOutputMetaData *sm = meta->querySerializedMeta();
        if (sm==meta.get())
            return this;
        if (!serializedmeta.get())
            serializedmeta.setown(new COutputMetaWithExtra(sm,metaSz));
        return serializedmeta.get();
    } 
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)
    {
        meta->walkIndirectMembers(self, visitor);
    }
};

IOutputMetaData *createOutputMetaDataWithExtra(IOutputMetaData *meta, size32_t sz)
{
    return new COutputMetaWithExtra(meta, sz);
}



IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain)
{
    return LINK(chain);
}

memsize_t ThorRowMemoryAvailable()
{
    //JCSMORE!
    return 1800*0x100000;
 }

void setLCRrowCRCchecking(bool on)
{
    // JCSMORE!
}
