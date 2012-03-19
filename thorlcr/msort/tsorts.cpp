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
#include <limits.h>
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include "thorport.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"
#include "thormisc.hpp"
#include "jlib.hpp"
#include "jsort.hpp"
#include "tsorts.hpp"
#include "tsorta.hpp"
#include "tsortm.hpp"
#include "tsortmp.hpp"
#include "thbuf.hpp"
#include "thcrc.hpp"
#include "thgraph.hpp"

#define _TRACE

#ifdef _DEBUG
//#define TRACE_UNIQUE
//#define _FULL_TRACE
//#define _FULLMPTRACE
//#define TRACE_PARTITION
//#define TRACE_PARTITION_OVERFLOW
#endif

#define MINISORT_PARALLEL_TRANSFER 16


inline void traceWait(const char *name,Semaphore &sem,unsigned interval=60*1000)
{
    while (!sem.wait(interval))
        PROGLOG("Waiting for %s",name);
}



#define TRANSFERBLOCKSIZE 0x100000 // 1MB
#define MINCOMPRESSEDROWSIZE 16
#define MAXCOMPRESSEDROWSIZE  0x2000

#define MPBLOCKTIMEOUT (1000*60*15)             


class CWriteIntercept : public CSimpleInterface, implements roxiemem::IBufferedRowCallback
{
    // sampling adapter

    // JCSMORE
    enum {
        InitialSortElements = 0,
        //The number of rows that can be added without entering a critical section, and therefore also the number
        //of rows that might not get freed when memory gets tight.
        CommitStep=32
    };
    CActivityBase &activity;
    CriticalSection crit;
    IRowInterfaces *rowIf;
    size32_t overflowidxfixed;
    Owned<IFile> dataFile, idxFile;
    Owned<IFileIO> dataFileIO, idxFileIO;
    Owned<ISerialStream> dataFileStrm;
    CThorStreamDeserializerSource dataFileDeserialzierSource;
    unsigned interval;
    unsigned idx;
	roxiemem::DynamicRoxieOutputRowArray sampleRows;
    Linked<IFileIOStream> outidx;
    offset_t overflowsize;
    size32_t fixedsize;
    offset_t lastofs;

    // JCSMORE - writeidxofs is a NOP for fixed size records by the looks of it (at least if serializer writes fixed sizes)
    // bit weird if always true, would look a lot clearer if explictly tested for var length case.
    void writeidxofs(offset_t o)
    {
        // lazy index write

        if (outidx.get()) {
            outidx->write(sizeof(o),&o);
            return;
        }
        if (lastofs) {
            if (fixedsize!=o-lastofs) {
                // right create idx
                StringBuffer tempname;
                GetTempName(tempname.clear(),"srtidx",false);
                idxFile.setown(createIFile(tempname.str()));
                Owned<IFileIO> fileioidx = idxFile->open(IFOcreate);
                outidx.setown(fileioidx?createBufferedIOStream(fileioidx,0x100000):NULL);
                if (outidx.get()==NULL) {
                    StringBuffer err;
                    err.append("Cannot create ").append(idxFile->queryFilename());
                    LOG(MCerror, thorJob, "%s", err.str());
                    throw MakeStringException(-1, "%s", err.str());
                }
                offset_t s = 0;
                while (s<=lastofs) {
                    outidx->write(sizeof(s),&s);
                    s += fixedsize;
                }
                assertex(s==lastofs+fixedsize);
                fixedsize = 0;
                writeidxofs(o);
                return;
            }
        }
        else
            fixedsize = (size32_t)(o-lastofs);
        lastofs = o;
    }
    size32_t _readOverflowPos(rowmap_t pos, unsigned n, offset_t *ofs, bool closeIO)
    {
        if (overflowidxfixed)
        {
            offset_t o = (offset_t)overflowidxfixed*(offset_t)pos;
            for(unsigned i=0;i<n;i++)
            {
                *(ofs++) = o;
                o += overflowidxfixed;
            }
            return n*sizeof(offset_t);
        }
        if (!idxFileIO.get())
        {
            assertex(idxFile);
            idxFileIO.setown(idxFile->open(IFOread));
        }
        size32_t rd = idxFileIO->read((offset_t)pos*(offset_t)sizeof(offset_t),sizeof(*ofs)*n,ofs);
        if (closeIO)
            idxFileIO.clear();
        return rd;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWriteIntercept(CActivityBase &_activity, IRowInterfaces *_rowIf, unsigned _interval)
		: activity(_activity), rowIf(_rowIf), interval(_interval), sampleRows(activity.queryJob().queryRowManager(), InitialSortElements, CommitStep)
    {
        interval = _interval;
        idx = 0;
        overflowsize = 0;
        fixedsize = 0;      
        lastofs = 0;
    }
    ~CWriteIntercept()
    {
        closeFiles();
        if (dataFile)
        {
            dataFile->remove();
            dataFile.clear();
        }
        if (idxFile)
        {
            idxFile->remove();
            idxFile.clear();
        }
    }
    offset_t write(IRowStream *input)
    {
        StringBuffer tempname;
        GetTempName(tempname,"srtmrg",false);
        dataFile.setown(createIFile(tempname.str()));
        Owned<IExtRowWriter> output = createRowWriter(dataFile, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), false, false, false);

        bool overflowed = false;
        PROGLOG("Local Overflow Merge start");
        unsigned ret=0;
        loop
        {
            const void *_row = input->nextRow();
            if (!_row)
                break;
            ret++;

            offset_t start = output->getPosition();
            OwnedConstThorRow row = _row;
            output->putRow(row.getLink());
            idx++;
            if (idx==interval)
            {
                idx = 0;
                // JCSMORE used to check if 'sampleRows.isFull()' here, but only to warn
                if (!sampleRows.append(row.getClear()))
                {
                    overflowsize = output->getPosition();
                    if (!overflowed)
                    {
                        PROGLOG("Sample buffer full");
                        overflowed = true;
                    }
                }
            }
            writeidxofs(start);
        }
        output->flush();
        offset_t end = output->getPosition();
        writeidxofs(end);
        if (outidx)
            outidx->flush();
        output.clear();
        if (overflowed)
            PROGLOG("Overflowed by %"I64F"d", overflowsize);
        PROGLOG("Local Overflow Merge done: overflow file %s size %"I64F"d", dataFile->queryFilename(), dataFile->size());
        return end;
    }
    IRowStream *getStream(offset_t startOffset, unsigned __int64 max)
    {
        return createRowStream(dataFile, rowIf, (offset_t)startOffset, (offset_t)-1, max, false, false);
    }
    void closeFiles()
    {
        dataFileStrm.clear();
        dataFileIO.clear();
        idxFileIO.clear();
    }
    size32_t readOverflowPos(rowmap_t pos, unsigned n, offset_t *ofs, bool closeIO)
    {
        CriticalBlock block(crit);
        return _readOverflowPos(pos, n, ofs, closeIO);
    }
    const void *getRow(rowmap_t pos)
    {
        CriticalBlock block(crit);
        offset_t ofs[2]; // JCSMORE doesn't really need 2, only to verify read right amount below
        size32_t rd = _readOverflowPos(pos-1, 2, ofs, false);
        assertex(rd==sizeof(ofs));
        size32_t idxSz = (size32_t)(ofs[1]-ofs[0]);
        if (!dataFileIO)
        {
            dataFileIO.setown(dataFile->open(IFOread));
            dataFileStrm.setown(createFileSerialStream(dataFileIO, 0));
            dataFileDeserialzierSource.setStream(dataFileStrm);
        }
        dataFileStrm->reset(ofs[0], (offset_t)-1);
        RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
        size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, dataFileDeserialzierSource);
        assertex(sz == idxSz);
        return rowBuilder.finalizeRowClear(sz);
    }
    void transferRows(CThorRowFixedSizeArray &rows)
    {
        rows.transferFrom(sampleRows);
    }

// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return 0; // JCSMORE
    }
    virtual bool freeBufferedRows(bool critical)
    {
        // JCSMORE - I don't think there's much you can do here, sample should fit in memory
        return false;
    }
};


class CSortSlaveBase :  implements ISortSlaveMP
{
    // JCSMORE
    enum {
        InitialSortElements = 0,
        //The number of rows that can be added without entering a critical section, and therefore also the number
        //of rows that might not get freed when memory gets tight.
        CommitStep=32
    };
private:
    byte *          recbufp;
    unsigned        maxelem;

    unsigned        overflowinterval; // aka overflowscale
    rowcount_t      grandtotal;
    offset_t        grandtotalsize; 
    rowmap_t *      overflowmap;
    bool            gatherdone;
    IDiskUsage *    iDiskUsage;
    Owned<CWriteIntercept> intercept;
    Owned<IMergeTransferServer> transferserver;
    size32_t        overflowidxfixed;
    ICompare*       icompare;
    bool            nosort;
    bool            isstable;
    OwnedConstThorRow   partitionrow;
    ICompare*       icollate; // used for co-sort
    ICompare*       icollateupper; // used in between join
    ISortKeySerializer *keyserializer;      // used on partition calculation
    unsigned        partno;
    unsigned        numnodes;
    void           *midkeybuf;
    size32_t        midkeybufsize;
    unsigned        multibinchopnum;
    rowmap_t       *multibinchoppos;
    Owned<IRowStream>   merger;
    Semaphore       startgathersem;
    Semaphore       startmergesem;
    Semaphore       finishedmergesem;
    Semaphore       closedownsem;
    rowcount_t      totalrows;
    mptag_t         mpTagRPC;
    CThorRowFixedSizeArray rowArray;
    Linked<IRowInterfaces> auxrowif;
    bool stopping;
    Owned<IException> closeexc; 

    class cThread:  public Thread
    {
        CSortSlaveBase &parent;
    public:
        cThread(CSortSlaveBase &_parent) 
            : Thread("CSortSlaveThread"), parent(_parent)
        {
        }
        int run()
        {
            return parent.run();
        }
    } thread;

protected:
    CActivityBase *activity;
    Linked<IRowInterfaces> rowif;
public:

    SocketEndpoint  myendpoint;
    size32_t        transferblocksize;
    Linked<ICommunicator> clusterComm;
    Owned<IException> exc;


    CSortSlaveBase(CActivityBase *_activity, SocketEndpoint &ep,IDiskUsage *_iDiskUsage,ICommunicator *_clusterComm, mptag_t _mpTagRPC)
        : activity(_activity), thread(*this), iDiskUsage(_iDiskUsage), rowArray(*activity)
    {
        clusterComm.set(_clusterComm);
        numnodes = 0;
        partno = 0;
        myendpoint = ep;
        mpTagRPC = _mpTagRPC;
        icompare = NULL;
        nosort = false;
        icollate = NULL;
        icollateupper = NULL;
        midkeybuf = NULL;
        multibinchoppos = NULL;
        transferblocksize = TRANSFERBLOCKSIZE;
        isstable = true;
        stopping = false;
    }


    ~CSortSlaveBase()
    {
        stop();
        ActPrintLog(activity, "Joining Sort Slave Server");
        verifyex(thread.join(10*60*1000));
        myendpoint.set(NULL,0);
        ActPrintLog(activity, "~CSortSlaveBase");
    }

    void start()
    {
        thread.start();
    }

    void stop()
    {
        if (!stopping) {
            stopping = true; 
            ActPrintLog(activity,"stopmarshall");
            SortSlaveMP::stopmarshall(clusterComm,mpTagRPC);
            ActPrintLog(activity,"stopmarshalldone");
        }
        if (exc)
            throw exc.getClear();
    }

    int run()
    {
        try {

            ActPrintLog(activity, "Creating SortSlaveServer on tag %d MP",mpTagRPC);    
            while(SortSlaveMP::marshall(*this,clusterComm,mpTagRPC)&&!stopping);
               ;
            stopping = true;
            ActPrintLog(activity, "Exiting SortSlaveServer on tag %d",mpTagRPC);    
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode()!=JSOCKERR_cancel_accept) {
                PrintExceptionLog(e,"**Exception(9)");
                if (exc.get())
                    e->Release();
                else
                    exc.setown(e);
                e->Release();
            }
        }
        catch (IException *e) {
            PrintExceptionLog(e,"**Exception(10)");
            if (exc.get())
                e->Release();
            else
                exc.setown(e);
        }
        stopping = true;
        return 0;
    }

    void clearOverflow()
    {
        intercept.clear();
    }

    ICompare *queryCmpFn(byte cmpfn)
    {
        switch (cmpfn) {
        case CMPFN_NORMAL: return icompare;
        case CMPFN_COLLATE: return icollate;
        case CMPFN_UPPER: return icollateupper;
        }
        return NULL;
    }


    unsigned BinChop(const byte * row,bool lesseq,bool firstdup,byte cmpfn)
    {
        unsigned n = rowArray.ordinality();
        unsigned l=0;
        unsigned r=n;
        ICompare* icmp=queryCmpFn(cmpfn);
        while (l<r) {
            unsigned m = (l+r)/2;
            const void *p = rowArray.get(m);
            int cmp = icmp->docompare(row, p);
            if (cmp < 0)
                r = m;
            else if (cmp > 0)
                l = m+1;
            else {
                if (firstdup) {
                    while ((m>0)&&(icmp->docompare(row,rowArray.get(m-1))==0))
                        m--;
                }
                else {
                    while ((m+1<n)&&(icmp->docompare(row,rowArray.get(m+1))==0))
                        m++;
                }
                return m;
            }
        }
        if (lesseq)
            return l-1;
        return l;
    }
    void doBinChop(VarElemArray &keys,rowmap_t * pos,unsigned num, byte cmpfn)
    {
        MemoryBuffer tmp;
        for (unsigned n=0;n<num;n++) {
            unsigned i = n;
            loop {                                      // adjustment for empty keys
                if (i>=keys.ordinality()) {
                    pos[n] = rowArray.ordinality();
                    break;
                }
                if (!keys.isNull(i)) {
                    pos[n] = BinChop(keys.item(i),false,true,cmpfn);
                    break;
                }
                i++;
            }
        }
    }

    void MultiBinChop(size32_t keybufsize, const byte * keybuf, unsigned num, rowmap_t * pos, byte cmpfn, bool useaux)
    {
        VarElemArray keys(useaux?auxrowif:rowif,NULL);
        keys.deserialize(keybuf,keybufsize,false);
        doBinChop(keys,pos,num,cmpfn);
    }

    void MultiBinChopStart(size32_t keybufsize, const byte * keybuf, byte cmpfn)
    {
        VarElemArray keys(rowif,keyserializer);
        keys.deserializeExpand(keybuf,keybufsize,false);
        assertex(multibinchoppos==NULL); // check for reentrancy
        multibinchopnum = keys.ordinality();
        multibinchoppos = (rowmap_t *)malloc(sizeof(rowmap_t)*multibinchopnum);
        doBinChop(keys,multibinchoppos,multibinchopnum,cmpfn);
    }

    void MultiBinChopStop(unsigned num, rowmap_t * pos)
    {
        assertex(multibinchoppos);
        assertex(multibinchopnum==num);
        memcpy(pos,multibinchoppos,num*sizeof(rowmap_t));
        free(multibinchoppos);
        multibinchoppos = NULL;
    }

    rowmap_t SingleBinChop(size32_t keysize, const byte * key,byte cmpfn)
    {
        OwnedConstThorRow row;
        row.deserialize(rowif,keysize,key);
        return BinChop((const byte *)row.get(),false,true,cmpfn);
    }


#ifdef _TRACE

    void TraceKey(const char *s, const void *k)
    {
        traceKey(rowif->queryRowSerializer(), s, k);
    }
#endif

    rowmap_t GetMinMax(size32_t &keybufsize,void *&keybuf,size32_t &avrecsize)
    {
        VarElemArray ret(rowif,keyserializer);
        avrecsize = 0;
        if (rowArray.ordinality()>0) {
            const void *kp = rowArray.get(0);
#ifdef _TRACE
            TraceKey("Min =", kp);
#endif
            ret.appendLink(kp);
            kp = rowArray.get(rowArray.ordinality()-1);
#ifdef _TRACE
            TraceKey("Max =", kp);
#endif
            ret.appendLink(kp);
            avrecsize = (size32_t)(grandtotalsize/grandtotal);
#ifdef _TRACE
            PROGLOG("Ave Rec Size = %u",avrecsize);
#endif
        }
        MemoryBuffer mb;
        ret.serialize(mb);
        keybufsize = mb.length();
        keybuf = mb.detach();
        return rowArray.ordinality();
    }

    bool GetMidPoint(size32_t lsize,const byte *lkeymem,
                    size32_t hsize,const byte *hkeymem,
                    size32_t &msize,byte *&mkeymem)
    {
        // finds the keys within the ranges specified
        // uses empty keys (0 size) if none found
        // try to avoid endpoints if possible
        if (rowArray.ordinality()!=0) {
            OwnedConstThorRow lkey;
            lkey.deserialize(rowif,lsize,lkeymem);
            OwnedConstThorRow hkey;
            hkey.deserialize(rowif,hsize,hkeymem);
            unsigned p1 = BinChop((const byte *)lkey.get(),false,false,false); 
            if (p1==(unsigned)-1)
                p1 = 0;
            unsigned p2 = BinChop((const byte *)hkey.get(),true,true,false);
            if (p2>=rowArray.ordinality()) 
                p2 = rowArray.ordinality()-1;
            if (p1<=p2) {
                unsigned pm=(p1+p2+1)/2;
                const void *kp=rowArray.get(pm);
                if ((icompare->docompare(lkey,kp)<=0)&&
                        (icompare->docompare(hkey,kp)>=0)) { // paranoia
                    MemoryBuffer mb;
                    CMemoryRowSerializer mbsz(mb);
                    rowif->queryRowSerializer()->serialize(mbsz, (const byte *)kp);
                    msize = mb.length();
                    mkeymem = (byte *)mb.detach();;
                    return true;
                }
            }
        }
        mkeymem = NULL;
        msize = 0;
        return false;
    }


    void GetMultiMidPoint(size32_t lbufsize,const void *lkeybuf,
                          size32_t hbufsize,const void *hkeybuf,
                          size32_t &mbufsize,void *&mkeybuf)
    {
        // finds the keys within the ranges specified
        // uses empty keys (0 size) if none found
        VarElemArray low(rowif,keyserializer);
        VarElemArray high(rowif,keyserializer);
        VarElemArray mid(rowif,keyserializer);
        low.deserializeExpand(lkeybuf,lbufsize,false);
        high.deserializeExpand(hkeybuf,hbufsize,false);
        unsigned n=low.ordinality();
        assertex(n==high.ordinality());
        unsigned i;
        for (i=0;i<n;i++) {
            if (rowArray.ordinality()!=0) {
                unsigned p1 = BinChop(low.item(i),false,false,false); 
                if (p1==(unsigned)-1)
                    p1 = 0;
                unsigned p2 = BinChop(high.item(i),true,true,false);
                if (p2>=rowArray.ordinality()) 
                    p2 = rowArray.ordinality()-1;
                if (p1<=p2) { 
                    unsigned pm=(p1+p2+1)/2;
                    const void *kp = rowArray.get(pm);
                    if ((icompare->docompare(low.item(i),kp)<=0)&&
                        (icompare->docompare(high.item(i),kp)>=0)) { // paranoia
                        mid.appendLink(kp);
                    }
                    else
                        mid.appendNull();
                }
                else
                    mid.appendNull();
            }
            else
                mid.appendNull();
        }
        MemoryBuffer mb;
        mid.serializeCompress(mb);
        mbufsize = mb.length();
        mkeybuf = mb.detach();
    }

    void GetMultiMidPointStart(size32_t lbufsize,const void *lkeybuf,
                               size32_t hbufsize,const void *hkeybuf)
    {
        assertex(midkeybuf==NULL); // just incase I ever allow re-entrancy
        GetMultiMidPoint(lbufsize,lkeybuf,hbufsize,hkeybuf,midkeybufsize,midkeybuf);
    }

    void GetMultiMidPointStop(size32_t &mbufsize,void *&mkeybuf)
    {
        assertex(midkeybuf); // just incase I ever allow re-entrancy
        mkeybuf = midkeybuf;
        mbufsize = midkeybufsize;
        midkeybuf = NULL;
    }

    void AdjustOverflow(rowmap_t &apos,const byte *key, byte cmpfn)
    {
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "AdjustOverflow: in (%"RMF"d)",apos);
        TraceKey(" ",(byte *)key);
#endif
        rowmap_t pos = (rowmap_t)(apos+1)*(rowmap_t)overflowinterval;
        if (pos>grandtotal)
            pos = (rowmap_t)grandtotal;
        assertex(intercept);
        MemoryBuffer bufma;
        while (pos>0) {
            OwnedConstThorRow row = intercept->getRow(pos-1);
#ifdef TRACE_PARTITION_OVERFLOW
            ActPrintLog(activity, "Compare to (%"RMF"d)",pos-1);
            TraceKey(" ",(const byte *)row.get());
#endif
            if (queryCmpFn(cmpfn)->docompare(key,row)>0)
                break;
            pos--;
        }
        intercept->closeFiles();
        apos = pos;
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "AdjustOverflow: out (%"RMF"d)",apos);
#endif
    }


    void OverflowAdjustMapStart(unsigned mapsize, rowmap_t * map,
                               size32_t keybufsize, const byte * keybuf, byte cmpfn)
    {
        assertex(intercept);
        overflowmap = (rowmap_t *)malloc(mapsize*sizeof(rowmap_t));
        memcpy(overflowmap,map,mapsize*sizeof(rowmap_t));
        unsigned i;
#ifdef TRACE_PARTITION_OVERFLOW

        ActPrintLog(activity, "OverflowAdjustMap: interval = %d",overflowinterval);
        ActPrintLog(activity, "In: ");
        for (i=0;i<mapsize;i++)
            ActPrintLog(activity, "%"RMF"d ",overflowmap[i]);
#endif
        VarElemArray keys(rowif,NULL);
        keys.deserialize(keybuf,keybufsize,false);
        for (i=0;i<mapsize-1;i++)
            AdjustOverflow(overflowmap[i],keys.item(i),cmpfn);
        assertex(grandtotal==(unsigned)grandtotal);
        overflowmap[mapsize-1] = (unsigned)grandtotal;
#ifdef TRACE_PARTITION_OVERFLOW
        ActPrintLog(activity, "Out: ");
        for (i=0;i<mapsize;i++)
            ActPrintLog(activity, "%"RMF"u ",overflowmap[i]);
#endif
    }

    rowmap_t OverflowAdjustMapStop(unsigned mapsize, rowmap_t * map)
    {
        memcpy(map,overflowmap,mapsize*sizeof(rowmap_t));
        free(overflowmap);
        assertex(grandtotal==(rowmap_t)grandtotal);
        return (rowmap_t)grandtotal;
    }


    void MultiMerge(unsigned mapsize,rowmap_t *map,
                    unsigned num,SocketEndpoint* endpoints)
    {
        MultiMergeBetween(mapsize,map,NULL,num,endpoints);
    }

    

    void StartGather()
    {
        ActPrintLog(activity, "Start Gather");
        gatherdone = false;
        startgathersem.signal();
    }

    void Gather(
            IRowInterfaces *_rowif, 
            IRowStream *in,
            ICompare *_icompare,
            ICompare *_icollate,
            ICompare *_icollateupper,
            ISortKeySerializer *_keyserializer,
            const void *_partitionrow, 
            bool _nosort,
            bool _unstable, 
            bool &abort,
            IRowInterfaces *_auxrowif
            )
    {
        ActPrintLog(activity, "Gather in");
        loop {
            if (abort)
                return;
            if (startgathersem.wait(10000))
                break;
        }
        ActPrintLog(activity, "SORT: Gather");
        assertex(!rowif);
        rowif.set(_rowif);
        if (transferserver)
            transferserver->setRowIF(rowif);
        else
            WARNLOG("SORT: transfer server not started!");
        if (_auxrowif&&_auxrowif->queryRowMetaData())
            auxrowif.set(_auxrowif);
        else {
            if (_partitionrow)
                PROGLOG("SORT: partitionrow passed but no aux serializer");
            auxrowif.set(_rowif);
        }
        keyserializer = _keyserializer;
        if (!keyserializer)
            PROGLOG("No key serializer");
        nosort = _nosort;
        if (nosort) 
            PROGLOG("SORT: Gather not sorting");
        isstable = !_unstable;
        if (_unstable) 
            PROGLOG("SORT: UNSTABLE");
        icompare = _icompare;
        icollate = _icollate?_icollate:_icompare;
        icollateupper = _icollateupper?_icollateupper:icollate;

        Linked<IThorRowSortedLoader2> sortedloader = createThorRowSortedLoader2(*activity, icompare, false, isstable, activity->queryMaxCores());
        Owned<IRowStream> overflowstream;
        try {
            overflowstream.setown(sortedloader->load(in, rowif, sl_alldiskifoverflow, abort));
            ActPrintLog(activity, "Local run sort(s) done");
        }
        catch (IException *e) {
            PrintExceptionLog(e,"**Exception(2)");
            throw;
        }
        if (!abort) {
            transferblocksize = TRANSFERBLOCKSIZE;
            grandtotal = sortedloader->numRows();
            unsigned numoverflows = sortedloader->numOverflows();
            ActPrintLog(activity, "Sort done, rows sorted = %"RCPF"d, bytes sorted = %"I64F"d overflowed to disk %d time%c",grandtotal,grandtotalsize,numoverflows,(numoverflows==1)?' ':'s');
            if (_partitionrow)
                partitionrow.set(_partitionrow);

            if (sortedloader->hasOverflowed()) { // need to write to file
                assertex(!intercept);
                overflowinterval=sortedloader->overflowScale();
                intercept.setown(new CWriteIntercept(*activity, rowif, overflowinterval));
                grandtotalsize = intercept->write(overflowstream);
                intercept->transferRows(rowArray); // get sample rows
            }
            else
            {
                overflowinterval = 1;
                sortedloader->transferRows(rowArray);
                grandtotalsize = rowArray.serializedSize(); // expensive, all in mem, need to know size
            }
        }
        ActPrintLog(activity, "Gather finished %s",abort?"ABORTED":"");
        gatherdone = true;
    }

    virtual void GetGatherInfo(rowmap_t &numlocal, offset_t &totalsize, unsigned &_overflowscale, bool haskeyserializer)
    {
        if (!gatherdone)
            ERRLOG("GetGatherInfo:***Error called before gather complete");
        if (!haskeyserializer&&keyserializer) {
            PROGLOG("Suppressing key serializer on slave");
            keyserializer = NULL;       // when cosorting and LHS empty
        }
        else if (haskeyserializer&&!keyserializer) {
            WARNLOG("Mismatched key serializer (master has, slave doesn't");
        }
        numlocal = rowArray.ordinality(); // JCSMORE - this is sample total, why not return actuall spill total?
        _overflowscale = overflowinterval;
        totalsize = grandtotalsize; // used by master, if nothing overflowed to see if can MiniSort
    }

    IRowStream * startMerge(rowcount_t &_totalrows)
    {
        ActPrintLog(activity, "SORT Merge Waiting");
        traceWait("startmergesem",startmergesem);
        ActPrintLog(activity, "SORT Merge Start");
        _totalrows = totalrows;
        return merger.getLink();
    }


    void stopMerge()
    {
        ActPrintLog(activity, "Local merge finishing");
        finishedmergesem.signal();
        ActPrintLog(activity, "Local merge finished");
        rowif.clear();
    }

    bool FirstRowOfFile(const char *filename,
                        size32_t &rowbufsize, byte * &rowbuf)
    {
        if (!*filename) { // partition row wanted
            if (partitionrow) {
                MemoryBuffer mb;
                CMemoryRowSerializer ssz(mb);
                auxrowif->queryRowSerializer()->serialize(ssz,(const byte *)partitionrow.get());
                rowbufsize = mb.length();
                rowbuf = (byte *)mb.detach();   
                partitionrow.clear(); // only one attempt! 
            }
            else {
                rowbuf = NULL;
                rowbufsize = 0;
            }
            return true;
        }
        Owned<IFile> file = createIFile(filename);
        Owned<IExtRowStream> rowstream = createSimpleRowStream(file,auxrowif);
        OwnedConstThorRow row = rowstream->nextRow();
        if (!row) {
            rowbuf = NULL;
            rowbufsize = 0;
            return true;
        }
        MemoryBuffer mb;
        CMemoryRowSerializer msz(mb);
        auxrowif->queryRowSerializer()->serialize(msz,(const byte *)row.get());
        rowbufsize = mb.length();
        rowbuf = (byte *)mb.detach();
        return true;
    }

    void GetMultiNthRow(unsigned numsplits,size32_t &outbufsize,void *&outkeybuf)
    {
        // actually doesn't get Nth row but numsplits samples distributed evenly through the rows
        assertex(numsplits);
        VarElemArray ret(rowif,keyserializer);
        unsigned numrows = rowArray.ordinality();
        if (numrows) {
            for (unsigned i=0;i<numsplits;i++) {
                count_t pos = ((i*2+1)*(count_t)numrows)/(2*(count_t)numsplits);
                if (pos>=numrows) 
                    pos = numrows-1;
                const void *kp = rowArray.get((unsigned)pos);
                ret.appendLink(kp);
            }
        }
        MemoryBuffer mb;
        ret.serializeCompress(mb);
        outbufsize = mb.length();
        outkeybuf = mb.detach();
    }

    void startMerging(unsigned numreaders,IRowStream **readers,rowcount_t _totalrows)
    {
        totalrows = _totalrows;
        if (numreaders==0) {
            merger.setown(createNullRowStream());
        }
        else if (numreaders==1) 
            merger.set(readers[0]);
        else {
            Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
            merger.setown(createRowStreamMerger(numreaders, readers, icompare, false, linkcounter));
        }
        ActPrintLog(activity, "Global Merger Created: %d streams",numreaders);
        startmergesem.signal();
        traceWait("finishedmergesem",finishedmergesem);
        ActPrintLog(activity, "Global Merged completed");
        merger.clear();
        for (unsigned i=0;i<numreaders;i++) {
            if (readers[i]) {
                readers[i]->Release();
                readers[i] = NULL;
            }
        }
        clearOverflow();
        ActPrintLog(activity, "Global Merge exit");
    }

    bool Connect(unsigned _partno, unsigned _numnodes)
    {
        ActPrintLog(activity, "Connected to slave %d of %d",_partno,_numnodes);
        numnodes = _numnodes;
        partno = _partno;
        transferserver.clear();
        transferserver.setown(createTransferServer());
        transferserver->start();
        return true; // used to establish link
    }

    void MultiMergeBetween(unsigned mapsize, rowmap_t * map, rowmap_t * mapupper, unsigned num, SocketEndpoint * endpoints)
    {
        assertex(transferserver.get()!=NULL);
        if (intercept)
            rowArray.kill();   // don't need samples any more
        transferserver->merge(mapsize,map,mapupper,num,endpoints,partno);
    }


    void Disconnect()
    {
        ActPrintLog(activity, "Disconnecting from slave %d of %d",partno,numnodes);
        if (transferserver) {
            transferserver->stop(); 
            transferserver.clear();
        }
        ActPrintLog(activity, "Disconnected from slave %d of %d",partno,numnodes);
    }

    void Close()
    {
        ActPrintLog(activity, "Close");
        try {
            if (transferserver)
                transferserver->subjoin(); // need to have finished merge threads 
        }
        catch (IException *e) {
            EXCLOG(e,"CSortSlaveBase");
            if (closeexc.get())
                e->Release();
            else
                closeexc.setown(e);
        }

        clearOverflow();
    }
    void CloseWait()
    {
        ActPrintLog(activity, "Close finished");
        if (closeexc.get())
            throw closeexc.getClear();
    }

    IRowStream *createMergeInputStream(rowmap_t sstart, rowcount_t snum)
    {
        unsigned _snum = (unsigned)snum;    // only support 2^32 rows locally
        assertex(snum==_snum);
        if (intercept) {
            offset_t startofs;  
            size32_t rd = intercept->readOverflowPos(sstart, 1, &startofs, true);
            assertex(rd==sizeof(startofs));
            return intercept->getStream(startofs, snum);
        }
        return rowArray.createRowStream((unsigned)sstart, _snum, false); // must be false as rows may overlap (between join)
    }

    void SingleMerge()
    {
        ActPrintLog(activity, "SingleMerge start");
        assertex(numnodes==1);
        totalrows = grandtotal;
        merger.setown(createMergeInputStream(0,totalrows));
        startmergesem.signal();
        traceWait("finishedmergesem(2)",finishedmergesem);
        ActPrintLog(activity, "Merged");
        merger.clear();
        clearOverflow();
    }


    virtual IMergeTransferServer *createTransferServer() = 0;

    void sendToPrimaryNode(CMessageBuffer &mb)
    {
        //compression TBD
        CMessageBuffer mbin;
#ifdef  _FULL_TRACE
        PROGLOG("MiniSort sendToPrimaryNode waiting");
#endif
        clusterComm->recv(mbin,1,mpTagRPC);
#ifdef  _FULL_TRACE
        PROGLOG("MiniSort sendToPrimaryNode continue",mbin.length());
#endif
        if (mbin.length()==0) {
            PROGLOG("aborting sendToPrimaryNode");
            // TBD?
            return;
        }
        byte fn;
        mbin.read(fn);
        if (fn!=255)
            throw MakeStringException(-1,"MiniSort sendToPrimaryNode: Protocol error(1) %d",(int)fn);
        mb.init(mbin.getSender(),mpTagRPC,mbin.getReplyTag());
#ifdef  _FULL_TRACE
        PROGLOG("MiniSort sendToPrimaryNode %u bytes",mb.length());
#endif
        clusterComm->reply(mb);
    }

    void sendToSecondaryNode(rank_t node,unsigned from, unsigned to, CriticalSection &sect,size32_t blksize)
    {
        //compression TBD
        CMessageBuffer mbout;
        unsigned done;
        do {
            done = rowArray.serializeblk(rowif->queryRowSerializer(),mbout.clear(),blksize,from,to-from);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort serialized %u rows, %u bytes",done,mbout.length());
#endif
            from += done;
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort sendToSecondaryNode(%d) send %u",(int)node,mbout.length());
#endif
            clusterComm->sendRecv(mbout,node,mpTagRPC);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort sendToSecondaryNode(%d) got %u",(int)node,mbout.length());
#endif
            byte fn;
            mbout.read(fn);
            if (fn!=254)
                throw MakeStringException(-1,"MiniSort sendToPrimaryNode: Protocol error(2) %d",(int)fn);
        } while (done!=0);
    }


    void appendFromPrimaryNode(CThorExpandingRowArray &dst)
    {
#ifdef  _FULL_TRACE
        PROGLOG("MiniSort appending from primary node");
#endif
        CMessageBuffer mbin;
        loop {
            mbin.clear();
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort appendFromPrimaryNode waiting");
#endif
            clusterComm->recv(mbin,1,mpTagRPC);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort appendFromPrimaryNode continue",mbin.length());
#endif
            CMessageBuffer mbout;
            mbout.init(mbin.getSender(),mpTagRPC,mbin.getReplyTag());
            byte fn=254;
            mbout.append(fn);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort appendFromPrimaryNode reply",mbout.length());
#endif
            clusterComm->reply(mbout);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort got from primary node %d",mbin.length());
#endif
            if (mbin.length()==0)
                break;
            dst.deserialize(*rowif->queryRowAllocator(),rowif->queryRowDeserializer(),mbin.length(),mbin.bufferBase(),false);
        }
    }

    void appendFromSecondaryNode(CThorExpandingRowArray &dst, rank_t node,Semaphore &sem)
    {
#ifdef  _FULL_TRACE
        PROGLOG("MiniSort appending from node %d",(int)node);
#endif
        CMessageBuffer mbin;
        bool first = true;
        loop {
            mbin.clear();
            byte fn = 255;
            mbin.append(fn);
            clusterComm->sendRecv(mbin,node,mpTagRPC);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort got %u from node %d",mbin.length(),(int)node);
#endif
            if (first) { 
                sem.wait();
                first = false;
            }
            if (mbin.length()==0) // nb don't exit before wait!
                break;
            dst.deserialize(*rowif->queryRowAllocator(),rowif->queryRowDeserializer(),mbin.length(),mbin.bufferBase(),false);
        }
    }
    void StartMiniSort(rowcount_t _totalrows)
    {
        PrintLog("Minisort started: %s, totalrows=%"RCPF"d", partno?"seconday node":"primary node",_totalrows);
        size32_t blksize = 0x100000;

		CThorExpandingRowArray rowsToSort(*activity, InitialSortElements, CommitStep, false);
		if (partno) {
            CMessageBuffer mb;
            unsigned idx = 0;
            loop {
                unsigned done = rowArray.serializeblk(rowif->queryRowSerializer(),mb.clear(),blksize,idx,(unsigned) -1);
#ifdef  _FULL_TRACE
                PROGLOG("MiniSort serialized %u rows, %u bytes",done,mb.length());
#endif
                sendToPrimaryNode(mb);
                if (!done)
                    break;
                idx += done;
            }
            rowArray.kill();
            appendFromPrimaryNode(rowsToSort);
        }
        else
		{
			rowsToSort.transferFrom(rowArray);
            class casyncfor: public CAsyncFor
            {
                CSortSlaveBase &base;
                Semaphore *nextsem;
            public:
                casyncfor(CSortSlaveBase &_base,unsigned numnodes) 
                    : base(_base) 
                { 
                    nextsem = new Semaphore[numnodes];  // 1 extra
                    nextsem[0].signal();
                }
                ~casyncfor()
                {
                    delete [] nextsem;
                }
                void Do(unsigned i)
                {
                    base.appendFromSecondaryNode(rowsToSort, (rank_t)(i+2),nextsem[i]); // +2 as master is 0 self is 1
                    nextsem[i+1].signal();
                }
            } afor1(*this,numnodes);
            afor1.For(numnodes-1, MINISORT_PARALLEL_TRANSFER);          
            // JCSMORE - the blocks sent from other nodes were already sorted..
            // appended to local sorted chunk, and this is resorting the whole lot..
            // It's sorting on remote side, before it knows how big..
            // But shouldn't it merge sort here instead?
            rowsToSort.sort(*icompare, isstable, activity->queryMaxCores(), rowArray);
#ifdef  _FULL_TRACE
            PROGLOG("MiniSort got %d rows %"I64F"d bytes",rowArray.ordinality(),(__int64)(rowArray.totalSize()));
#endif
            UnsignedArray points; 
            rowArray.partition(*icompare, numnodes, points);
#ifdef  _FULL_TRACE
            for (unsigned pi=0;pi<points.ordinality();pi++)
                PROGLOG("points[%d] = %u",pi, points.item(pi));
#endif
            class casyncfor2: public CAsyncFor
            {
                CSortSlaveBase &base;
                unsigned *points;
                size32_t blksize;
                CriticalSection crit;
            public:
                casyncfor2(CSortSlaveBase &_base,unsigned *_points,size32_t _blksize) 
                    : base(_base) 
                { 
                    points = _points;
                    blksize = _blksize;
                }
                void Do(unsigned i)
                {
                    base.sendToSecondaryNode((rank_t)(i+2),points[i+1],points[i+2],crit,blksize); // +2 as master is 0 self is 1
                }
            } afor2(*this,points.getArray(),blksize);
            afor2.For(numnodes-1, MINISORT_PARALLEL_TRANSFER);          
            // get rid of rows sent
            rowArray.removeRows(points.item(1),rowArray.ordinality()-points.item(1));
        }
        merger.setown(rowArray.createRowStream());
        totalrows = rowArray.ordinality();
        startmergesem.signal();
        PROGLOG("StartMiniSort output started");
        traceWait("finishedmergesem(2)",finishedmergesem);
        PrintLog("StartMiniSort output done");
        merger.clear();
        clearOverflow();
        PrintLog("StartMiniSort exit");
    }
};



class CSortSlave : public CSortSlaveBase, implements ISortSlaveBase 
{
    Owned<IGroup> nodegroup;
public:
    CSortSlave(CActivityBase *activity, SocketEndpoint &ep,IDiskUsage *_iDiskUsage,ICommunicator *_clusterComm, mptag_t _mpTagRPC)
        : CSortSlaveBase(activity, ep,_iDiskUsage,_clusterComm,_mpTagRPC)
    {
    }

    size32_t getTransferBlockSize()
    {
        return transferblocksize;
    }

    unsigned getTransferPort()
    {
        return myendpoint.port+SOCKETSERVERINC;
    }

    IRowStream *createMergeInputStream(rowmap_t sstart,rowcount_t _snum)
    {
        return CSortSlaveBase::createMergeInputStream(sstart,_snum);
    }


    IMergeTransferServer *createTransferServer()
    {
        return createMergeTransferServer(this);
    }


    void startMerging(unsigned numreaders,IRowStream **readers,rowcount_t _totalrows)
    {
        CSortSlaveBase::startMerging(numreaders,readers,_totalrows);
    }

    IGroup &queryNodeGroup()
    {
        if (!nodegroup.get()) {
            assertex(clusterComm.get());
            nodegroup.setown(clusterComm->queryGroup().subset(1,clusterComm->queryGroup().ordinality()-1));
        }
        return *nodegroup;
    }


};



//==============================================================================


class CThorSorter: public CSimpleInterface, public IThorSorter
{
    CActivityBase *activity;
    CSortSlave slave;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CThorSorter(CActivityBase *_activity, SocketEndpoint &ep,IDiskUsage *iDiskUsage,ICommunicator *clusterComm, mptag_t _mpTagRPC) 
        : activity(_activity), slave(activity, ep, iDiskUsage, clusterComm, _mpTagRPC)
    {
        slave.start();
    }
    virtual void Gather(
        IRowInterfaces *rowif,
        IRowStream *in,
        ICompare *icompare,
        ICompare *icollate,
        ICompare *icollateupper,
        ISortKeySerializer *keyserializer,
        const void *partitionrow, 
        bool nosort,
        bool unstable,
        bool &abort,
        IRowInterfaces *auxrowif
        )
    {
        slave.Gather(rowif,in,icompare,icollate,icollateupper,keyserializer,partitionrow,nosort,unstable,abort,auxrowif);
    }
    virtual IRowStream * startMerge(rowcount_t &totalrows) { return slave.startMerge(totalrows); }
    virtual void stopMerge() { slave.stopMerge(); }
};


IThorSorter *CreateThorSorter(CActivityBase *activity, SocketEndpoint &ep,IDiskUsage *iDiskUsage,ICommunicator *clusterComm, mptag_t _mpTagRPC)
{
    return new CThorSorter(activity, ep, iDiskUsage, clusterComm, _mpTagRPC);
}

