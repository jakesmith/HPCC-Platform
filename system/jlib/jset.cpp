/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#include "jlib.hpp"
#include "jset.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

//-----------------------------------------------------------------------

enum { BitsPerItem = sizeof(bits_t) * 8 };

class CBitSetArrayHelper
{
protected:
    typedef bits_t bits_settype;
    ArrayOf<bits_t> bits;
    mutable CriticalSection crit;

    inline bits_settype getBitSet(unsigned i) { return bits.item(i); }
    inline void setBitSet(unsigned i, bits_t m)
    {
        bits.replace(m, i);
    }
    inline void addBitSet(bits_t m)
    {
        bits.append((bits_t)-1);
    }
    inline unsigned getWidth() const { return bits.ordinality(); }
    inline void reset()
    {
        CriticalBlock block(crit);
        bits.kill();
    }
    inline void serialize(MemoryBuffer &buffer) const
    {
        CriticalBlock block(crit);
        buffer.append(bits.ordinality());
        ForEachItemIn(b, bits)
            buffer.append(bits.item(b));
    }
};

class CBitSetMemoryHelper
{
    bits_t *mem;
    unsigned bitSetUnits;
protected:
    typedef bits_t &bits_settype;
    ArrayOf<bits_t> bits;
    mutable CriticalSection crit;

    inline bits_settype getBitSet(unsigned i) { return bits[i]; }
    inline void setBitSet(unsigned i, bits_t m) { } // NOP, getBitset returns ref. in this impl and bits_t set directly
    inline void addBitSet(bits_t m) { UNIMPLEMENTED; }
    inline unsigned getWidth() const { return bitSetUnits; }
    inline void reset()
    {
        memset(mem, 0, sizeof(bits_t)*bitSetUnits);
    }
    inline void serialize(MemoryBuffer &buffer) const
    {
        buffer.append((unsigned)(sizeof(bits_t)*bitSetUnits));
        buffer.append(bitSetUnits/sizeof(byte), mem);
    }
    void deserialize(MemoryBuffer &buffer)
    {
    }
};

template <class BITSETHELPER>
class CBitSetBase : public BITSETHELPER
{
    typedef BITSETHELPER PARENT;
//    using PARENT::bits_settype;
    using PARENT::getWidth;
    using PARENT::getBitSet;
    using PARENT::setBitSet;
    using PARENT::addBitSet;
protected:
    typedef bits_t bits_settype;
    unsigned _scan(unsigned from, bool tst, bool scninv)
    {
        bits_t noMatchMask=tst?0:(bits_t)-1;
        unsigned j=from%BitsPerItem;
        // returns index of first = val >= from
        unsigned n=getWidth();
        unsigned i;
        for (i=from/BitsPerItem;i<n;i++)
        {
//            bits_settype m = getBitSet(i);
            bits_t m = getBitSet(i);
            if (m!=noMatchMask)
            {
#if defined(__GNUC__)
                //Use the __builtin_ffs instead of a loop to find the first bit set/cleared
                bits_t testMask = m;
                if (j != 0)
                {
                    //Set all the bottom bits to the value we're not searching for
                    bits_t mask = (((bits_t)1)<<j)-1;
                    if (tst)
                        testMask &= ~mask;
                    else
                        testMask |= mask;

                    //May possibly match exactly - if so continue main loop
                    if (testMask==noMatchMask)
                    {
                        j = 0;
                        continue;
                    }
                }

                //Guaranteed a match at this point
                if (tst)
                {
                    //Returns one plus the index of the least significant 1-bit of testMask
                    //(testMask != 0) since that has been checked above (noMatchMask == 0)
                    unsigned pos = __builtin_ffs(testMask)-1;
                    if (scninv)
                    {
                        bits_t t = ((bits_t)1)<<pos;
                        m &= ~t;
                        setBitSet(i, m);
                    }
                    return i*BitsPerItem+pos;
                }
                else
                {
                    //Same as above but invert the bitmask
                    unsigned pos = __builtin_ffs(~testMask)-1;
                    if (scninv)
                    {
                        bits_t t = ((bits_t)1)<<pos;
                        m |= t;
                        setBitSet(i, m);
                    }
                    return i*BitsPerItem+pos;
                }
#else
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++)
                {
                    if (t&m)
                    {
                        if (tst)
                        {
                            if (scninv)
                            {
                                m &= ~t;
                                setBitSet(i, m);
                            }
                            return i*BitsPerItem+j;
                        }
                    }
                    else
                    {
                        if (!tst)
                        {
                            if (scninv)
                            {
                                m |= t;
                                setbitSet(i, m);
                            }
                            return i*BitsPerItem+j;
                        }
                    }
                    t <<= 1;
                }
#endif
            }
            j = 0;
        }
        if (tst)
            return (unsigned)-1;
        unsigned ret = n*BitsPerItem;
        if (n*BitsPerItem<from)
            ret = from;
        if (scninv)
            set(ret,true);
        return ret;
    }
    void _incl(unsigned lo, unsigned hi, bool val)
    {
        if (hi<lo)
            return;
        unsigned j=lo%BitsPerItem;
        unsigned nb=(hi-lo)+1;
        unsigned n=getWidth();
        unsigned i;
        for (i=lo/BitsPerItem;i<n;i++)
        {
            bits_settype m = getBitSet(i);
            if ((nb>=BitsPerItem)&&(j==0))
            {
                if (val)
                    m = (bits_t)-1;
                else
                    m = 0;
                nb -= BitsPerItem;
            }
            else
            {
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++)
                {
                    if (val)
                        m |= t;
                    else
                        m &= ~t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
            }
            setBitSet(i, m);
            if (nb==0)
                return;
            j = 0;
        }
        if (val)
        {
            while (nb>=BitsPerItem)
            {
                addBitSet((bits_t)-1);
                nb -= BitsPerItem;
            }
            if (nb>0)
            {
                bits_t m=0;
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++)
                {
                    m |= t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
                addBitSet(m);
            }
        }
    }

protected:
    void set(unsigned n, bool val)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i = n/BitsPerItem;
        if (i>=getWidth())
        {
            if (!val)
                return; // don't bother
            while (i>getWidth())
                addBitSet(0);
            addBitSet(t);
        }
        else
        {
            bits_t m = getBitSet();
            if (val)
                m |= t;
            else
                m &= ~t;
            setBitSet(i, m);
        }
    }
    bool invert(unsigned n)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        bool ret;
        if (i>=getWidth())
        {
            while (i>getWidth())
                addBitSet(0);
            addBitSet(t);
            ret = true;
        }
        else
        {
            bits_settype m = getBitSet(i);
            ret = 0 == (m&t);
            if (ret)
                m |= t;
            else
                m &= ~t;
            setBitSet(i, m);
        }
        return ret;
    }
    bool test(unsigned n)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        if (i<getWidth())
        {
            bits_settype m = getBitSet(i);
            if (m&t)
                return true;
        }
        return false;
    }
    bool testSet(unsigned n,bool val)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        bool ret;
        if (i>=getWidth())
        {
            ret = false;
            if (!val)
                return false; // don't bother
            while (i>getWidth())
                addBitSet(0);
            addBitSet(t);
        }
        else
        {
            bits_settype m = getBitSet(i);
            ret = 0 != (m&t);
            if (val)
                m |= t;
            else
                m &= ~t;
            setBitSet(i, m);
        }
        return ret;
    }
};

// Simple BitSet // 0 based all, intermediate items exist, operations threadsafe and atomic
class CBitSet : public CBitSetBase<CBitSetArrayHelper>
{
    unsigned _scan(unsigned from, bool tst, bool scninv)
    {
        CriticalBlock block(crit);
        return CBitSetBase::_scan(from, tst, scninv);
    }
    void _incl(unsigned lo, unsigned hi, bool val)
    {
        CriticalBlock block(crit);
        CBitSetBase::_incl(lo, hi, val);
    }
    void deserialize(MemoryBuffer &buffer)
    {
        CriticalBlock block(crit);
        bits.kill();
        unsigned count;
        buffer.read(count);
        if (count)
        {
            bits.ensure(count);
            while (count--)
            {
                bits_t b;
                buffer.read(b);
                bits.append(b);
            }
        }
    }
    void deserialize(MemoryBuffer &buffer)
    {
        CriticalBlock block(crit);
        bits.kill();
        unsigned count;
        buffer.read(count);
        if (count)
        {
            bits.ensure(count);
            while (count--)
            {
                bits_t b;
                buffer.read(b);
                bits.append(b);
            }
        }
    }
public:
    CBitSet() { }
    CBitSet(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
    virtual void set(unsigned n, bool val)
    {
        CriticalBlock block(crit);
        CBitSetBase::set(n, val);
    }
    virtual bool invert(unsigned n)
    {
        CriticalBlock block(crit);
        return CBitSetBase::invert(n);)
    }
    virtual bool test(unsigned n)
    {
        CriticalBlock block(crit);
        return CBitSetBase::test(n);
    }
    virtual bool testSet(unsigned n, bool val)
    {
        CriticalBlock block(crit);
        return CBitSetBase::testSet(n, val);
    }
    virtual unsigned scan(unsigned from,bool tst)
    {
        return _scan(from,tst,false);
    }
    virtual unsigned scanInvert(unsigned from,bool tst) // like scan but inverts bit as well
    {
        return _scan(from,tst,true);
    }
    virtual void incl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,true);
    }
    virtual void excl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,false);
    }
    virtual void reset()
    {
        CBitSetBase::reset();
    }
    virtual void serialize(MemoryBuffer &buffer) const
    {
        CBitSetBase::serialize(buffer);
    }
};

extern jlib_decl IBitSet *createBitSet()
{
    return new CBitSet();
}


// Does not expand
class CBitSetThreadUnsafe : public CBitSetBase<CBitSetMemoryHelper>
{
    MemoryBuffer memBuffer; // used if managing own mem

    void deserialize(MemoryBuffer &buffer)
    {
        unsigned count;
        buffer.read(count);
        if (count)
        {
            unsigned bitSets = count/sizeof(bits_t);
            assertex(bitSets<=bitSetUnits);
            memcpy(mem, buffer.readDirect(bitSets*sizeof(byte)), bitSets*sizeof(byte));
            unsigned remaining = bitSetUnits-bitSets;
            if (remaining)
                memset(&mem[bitSets], 0, sizeof(bits_t)*remaining);
        }
    }
public:
    CBitSetThreadUnsafe()
    {
        UNIMPLEMENTED;
    }
    CBitSetThreadUnsafe(size32_t memSz, const void *_mem)
    {
        bitSetUnits = memSz*sizeof(byte) / sizeof(bits_t);
        mem = (bits_t *)_mem;
        memset(mem, 0, bitSetUnits*sizeof(bits_t));
    }
    CBitSetThreadUnsafe(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
    virtual void set(unsigned n, bool val)
    {
        CBitSetBase::set(n, val);
    }
    virtual bool invert(unsigned n)
    {
        return CBitSetBase::invert(n);
    }
    virtual bool test(unsigned n)
    {
        return CBitSetBase::test(n);
    }
    virtual bool testSet(unsigned n, bool val)
    {
        return CBitSetBase::testSet(n, val);
    }
    virtual unsigned scan(unsigned from,bool tst)
    {
        return _scan(from,tst,false);
    }
    virtual unsigned scanInvert(unsigned from,bool tst) // like scan but inverts bit as well
    {
        return _scan(from,tst,true);
    }
    virtual void incl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,true);
    }
    virtual void excl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,false);
    }
    virtual void reset()
    {
        CBitSetBase::reset();
    }
    virtual void serialize(MemoryBuffer &buffer) const
    {
        CBitSetBase::serialize(buffer);
    }
};

extern jlib_decl IBitSet *createBitSetThreadUnsafe(unsigned maxBits, const void *mem)
{
    return new CBitSetThreadUnsafe(maxBits, mem);
}

// NB: Doubt you'd want to interchange, but serialization format is compatible
extern jlib_decl IBitSet *deserializeIBitSet(MemoryBuffer &mb)
{
    return new CBitSet(mb);
}

extern jlib_decl IBitSet *deserializeIBitSetThreadUnsafe(MemoryBuffer &mb)
{
    return new CBitSetThreadUnsafe(mb);
}



