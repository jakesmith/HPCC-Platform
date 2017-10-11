#include "jiface.hpp"
#include "jbuff.hpp"
#include "jstring.hpp"
#include "junicode.hpp"
#include "rtlcommon.hpp"

CThorContiguousRowBuffer::CThorContiguousRowBuffer(ISerialStream * _in) : in(_in)
{
    buffer = NULL;
    maxOffset = 0;
    readOffset = 0;
}

void CThorContiguousRowBuffer::doRead(size32_t len, void * ptr)
{
    ensureAccessible(readOffset + len);
    memcpy(ptr, buffer+readOffset, len);
    readOffset += len;
}


size32_t CThorContiguousRowBuffer::read(size32_t len, void * ptr)
{
    doRead(len, ptr);
    return len;
}

size32_t CThorContiguousRowBuffer::readSize()
{
    size32_t value;
    doRead(sizeof(value), &value);
    return value;
}

size32_t CThorContiguousRowBuffer::readPackedInt(void * ptr)
{
    size32_t size = sizePackedInt();
    doRead(size, ptr);
    return size;
}

size32_t CThorContiguousRowBuffer::readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len)
{
    if (len == 0)
        return 0;

    size32_t size = sizeUtf8(len);
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVStr();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVUni();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}


size32_t CThorContiguousRowBuffer::sizePackedInt()
{
    ensureAccessible(readOffset+1);
    return rtlGetPackedSizeFromFirst(buffer[readOffset]);
}

size32_t CThorContiguousRowBuffer::sizeUtf8(size32_t len)
{
    if (len == 0)
        return 0;

    //The len is the number of utf characters, size depends on which characters are included.
    size32_t nextOffset = readOffset;
    while (len)
    {
        ensureAccessible(nextOffset+1);

        for (;nextOffset < maxOffset;)
        {
            nextOffset += readUtf8Size(buffer+nextOffset);  // This function only accesses the first byte
            if (--len == 0)
                break;
        }
    }
    return nextOffset - readOffset;
}

size32_t CThorContiguousRowBuffer::sizeVStr()
{
    size32_t nextOffset = readOffset;
    for (;;)
    {
        ensureAccessible(nextOffset+1);

        for (; nextOffset < maxOffset; nextOffset++)
        {
            if (buffer[nextOffset] == 0)
                return (nextOffset + 1) - readOffset;
        }
    }
}

size32_t CThorContiguousRowBuffer::sizeVUni()
{
    size32_t nextOffset = readOffset;
    const size32_t sizeOfUChar = 2;
    for (;;)
    {
        ensureAccessible(nextOffset+sizeOfUChar);

        for (; nextOffset+1 < maxOffset; nextOffset += sizeOfUChar)
        {
            if (buffer[nextOffset] == 0 && buffer[nextOffset+1] == 0)
                return (nextOffset + sizeOfUChar) - readOffset;
        }
    }
}


void CThorContiguousRowBuffer::reportReadFail()
{
    throwUnexpected();
}


const byte * CThorContiguousRowBuffer::peek(size32_t maxSize)
{
    if (maxSize+readOffset > maxOffset)
        doPeek(maxSize+readOffset);
    return buffer + readOffset;
}

offset_t CThorContiguousRowBuffer::beginNested()
{
    size32_t len = readSize();
    return len+readOffset;
}

bool CThorContiguousRowBuffer::finishedNested(offset_t & endPos)
{
    return readOffset >= endPos;
}

void CThorContiguousRowBuffer::skip(size32_t size)
{
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipPackedInt()
{
    size32_t size = sizePackedInt();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipUtf8(size32_t len)
{
    size32_t size = sizeUtf8(len);
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVStr()
{
    size32_t size = sizeVStr();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVUni()
{
    size32_t size = sizeVUni();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

//=====================================================================================

SimpleOutputWriter::SimpleOutputWriter()
{
    separatorNeeded = false;
}

void SimpleOutputWriter::outputFieldSeparator()
{
    if (separatorNeeded)
        out.append(',');
    separatorNeeded = true;
}

SimpleOutputWriter & SimpleOutputWriter::clear()
{
    out.clear();
    separatorNeeded = false;
    return *this;
}

void SimpleOutputWriter::outputQuoted(const char *text)
{
    out.append(text);
}

void SimpleOutputWriter::outputString(unsigned len, const char *field, const char *)
{
    outputFieldSeparator();
    out.append(len, field);
}

void SimpleOutputWriter::outputQString(unsigned len, const char *field, const char *fieldname)
{
    MemoryAttr tempBuffer;
    char * temp;
    if (len <= 100)
        temp = (char *)alloca(len);
    else
        temp = (char *)tempBuffer.allocate(len);
    rtlQStrToStr(len, temp, len, field);
    outputString(len, temp, fieldname);
}

void SimpleOutputWriter::outputBool(bool field, const char *)
{
    outputFieldSeparator();
    outputXmlBool(field, NULL, out);
}

void SimpleOutputWriter::outputData(unsigned len, const void *field, const char *)
{
    outputFieldSeparator();
    outputXmlData(len, field, NULL, out);
}

void SimpleOutputWriter::outputInt(__int64 field, unsigned size, const char *)
{
    outputFieldSeparator();
    outputXmlInt(field, NULL, out);
}

void SimpleOutputWriter::outputUInt(unsigned __int64 field, unsigned size, const char *)
{
    outputFieldSeparator();
    outputXmlUInt(field, NULL, out);
}

void SimpleOutputWriter::outputReal(double field, const char *)
{
    outputFieldSeparator();
    outputXmlReal(field, NULL, out);
}

void SimpleOutputWriter::outputDecimal(const void *field, unsigned size, unsigned precision, const char *)
{
    outputFieldSeparator();
    outputXmlDecimal(field, size, precision, NULL, out);
}

void SimpleOutputWriter::outputUDecimal(const void *field, unsigned size, unsigned precision, const char *)
{
    outputFieldSeparator();
    outputXmlUDecimal(field, size, precision, NULL, out);
}

void SimpleOutputWriter::outputUnicode(unsigned len, const UChar *field, const char *)
{
    outputFieldSeparator();
    outputXmlUnicode(len, field, NULL, out);
}

void SimpleOutputWriter::outputUtf8(unsigned len, const char *field, const char *)
{
    outputFieldSeparator();
    outputXmlUtf8(len, field, NULL, out);
}

void SimpleOutputWriter::outputBeginNested(const char *s, bool)
{
    if (!s || !*s)
        return;
    outputFieldSeparator();
    out.append('[');
    separatorNeeded = false;
}

void SimpleOutputWriter::outputEndNested(const char *s)
{
    if (!s || !*s)
        return;
    out.append(']');
    separatorNeeded = true;
}

void SimpleOutputWriter::outputSetAll()
{
    out.append('*');
}

void SimpleOutputWriter::newline()
{
    out.append('\n');
}

