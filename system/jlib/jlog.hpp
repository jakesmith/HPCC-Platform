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



#ifndef JLOG_HPP
#define JLOG_HPP

// Control whether XML reports have newlines between category/system/job/text sections
#ifdef _DEBUG
#define LOG_MSG_NEWLINE
#endif

#include "stdio.h"
#include "time.h"
#include "jiface.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jutil.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jtime.hpp"
#include "jtrace.hpp"

typedef enum
{
    LogMsgAttType_class          = 0x01,
    LogMsgAttType_Audience       = 0x02,
    LogMsgAttType_fields         = 0x04,
} LogMsgAttType;

/****************************************************************************************/
/* LOG MESSAGE AUDIENCES:                                                               *
 * MSGAUD_operator - This should be used when the message may be normally monitored by, *
 *                   acted on, or resolved by sys admins. E.g. out of memory alerts,    *
 *                   configuration issues, possible hardware/network related issues,    *
 * MSGAUD_user     - Messages targetted at the end-users of HPCC (including ecl coders  *
 *                   E.g. ECL code issues, workunit issues, data file related issues,   *
 *                   authentication issues                                              *
 * MSGAUD_programmer - Messages targetted at platform developers. E.g. debugging        *
 *                   and tracing messages, internal errors that would not normally be   *
 *                   resolved by sys admins or users, unexpected internal error         *
 * MSGAUD_audit    - Audit messages related to file access and authentication           *
 * ------------------------------------------------------------------------------------ *
 * LOG MESSAGE CLASS:                                                                   */
typedef enum
{
    MSGCLS_unknown     = 0x00, // Invalid/unknown log message class
    MSGCLS_disaster    = 0x01, // Any unrecoverable or critical system errors
    MSGCLS_error       = 0x02, // Recoverable/not critical Errors
    MSGCLS_warning     = 0x04, // Warnings
    MSGCLS_information = 0x08, // Config, environmental and internal status  info
    MSGCLS_progress    = 0x10, // Progress of workunits. Status of file operations
    MSGCLS_metric      = 0x20, // A metric line
    MSGCLS_addid       = 0x40, // Internal use within log system
    MSGCLS_removeid    = 0x80, // Internal use within log system
    MSGCLS_event       = 0x100, // A timestamp or event where the time it occurs is significant
    MSGCLS_all         = 0xFFFF  // Use as a filter to select all messages
} LogMsgClass;
/* ------------------------------------------------------------------------------------ *
 * NOTES:                                                                               *
 * Every message will have an audience and message class. The job number is optional    *
 *                                                                                      *
 * Standard categories of Audience + Message class are defined with LogMsgCategory.     *
 *                                                                                      *
 * The following are common logging functions (for common audience+message class)       *
 * 1) For temporary logs whilst debugging (may be disabled for releases ):              *
 *    DBGLOG([LogMsgCode,] format,..)          - uses MCdebugInfo                       *
 *                                                                                      *
 * 2) For fatal errors or unrecoverable errors:                                         *
 *    DISLOG([LogMsgCode,] format,..)          - uses MCoperatorDisaster                        *
 *                                                                                      *
 * 3) For warning messages:                                                             *
 *    (i) Messages for End-users (including ECL coders) should use:                     *
 *        UWARNLOG([LogMsgCode,] format,..)    - uses MCuserWarning                     *
 *                                                                                      *
 *    (ii) Messages for SysAdmins:                                                      *
 *        OWARNLOG([LogMsgCode,] format,..)    - uses MCoperatorWarning                 *
 *                                                                                      *
 *    (iii) Messages for platform developers:                                           *
 *        IWARNLOG([LogMsgCode,] format,..)    - uses MCdebugWarning                 *
 *                                                                                      *
 * 4) For error messages:                                                               *
 *    (i) Messages for End-users (including ECL coders) should use:                     *
 *        UERRLOG([LogMsgCode,] format,..)     - uses MCuserError                       *
 *                                                                                      *
 *    (ii) Messages for SysAdmins:                                                      *
 *        OERRLOG([LogMsgCode,] format,..)     - uses MCoperatorError                   *
 *                                                                                      *
 *    (iii) Messages for platform developers:                                           *
 *        IERRLOG([LogMsgCode,] format,..)     - uses MCdebugError                   *
 *                                                                                      *
 *    (iv) Messages for audit:                                                          *
 *        AERRLOG([LogMsgCode,] format,..)     - uses MCdebugError                   *
 *                                                                                      *
 *                                                                                      *
 * 5) For progress messages:                                                            *
 *    PROGLOG([LogMsgCode,] format,..)         - uses MCuserProgress                    *
 *                                                                                      *
 * More general logging functions include:                                              *
 * 1) Full control over the log message:                                                *
 *   LOG(LogMsgCategory, [job,] [code,] format, ...)                                    *
 * 2) Takes code, message, & audience from an exception, class is error                 *
 *    EXCLOG(exception, prefix)                                                         *
 * 3) More control over logging exceptions:                                             *
 *    LOG(LogMsgCategory, [job,] exception [, prefix])                                  *
 *                                                                                      *
 * LogMsgCategory detail level may be modified from the default with a numeric paramter *
 * For example as MCdebugInfo(50).                                                      *
 ****************************************************************************************/


// ENUMS, TYPEDEFS, CONSTS ETC.

// Enums, typedefs, and consts for LogMsgCategory, plus enum-to-string functions

// When changing this enum, be sure to update (a) the string functions, and (b) NUM value

typedef MessageAudience LogMsgAudience;

inline const char * LogMsgAudienceToVarString(LogMsgAudience audience)
{
    switch(audience)
    {
    case MSGAUD_operator:
        return("Operator");
    case MSGAUD_user:
        return("User");
    case MSGAUD_programmer:
        return("Programmer");
    case MSGAUD_monitor:
        return("Monitor");
    case MSGAUD_audit:
        return("Audit");
    default:
        return("UNKNOWN");
    }
}

inline const char * LogMsgAudienceToFixString(LogMsgAudience audience)
{
    switch(audience)
    {
    case MSGAUD_operator:
        return("OPR");
    case MSGAUD_user:
        return("USR");
    case MSGAUD_programmer:
        return("PRG");
    case MSGAUD_monitor:
        return("MON");
    case MSGAUD_audit:
        return("AUD");
    default:
        return("UNK");
    }
}
inline MessageAudience LogMsgAudFromAbbrev(char const * abbrev)
{
    if(strnicmp(abbrev, "OPR", 3)==0)
        return MSGAUD_operator;
    if(strnicmp(abbrev, "USR", 3)==0)
        return MSGAUD_user;
    if(strnicmp(abbrev, "PRO", 3)==0)
        return MSGAUD_programmer;
    if(strnicmp(abbrev, "MON", 3)==0)
        return MSGAUD_monitor;
    if(strnicmp(abbrev, "ADT", 3)==0)
        return MSGAUD_audit;
    if(strnicmp(abbrev, "ALL", 3)==0)
        return MSGAUD_all;
    return MSGAUD_unknown;
}

inline const char * LogMsgClassToVarString(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_disaster:
        return("Disaster");
    case MSGCLS_error:
        return("Error");
    case MSGCLS_warning:
        return("Warning");
    case MSGCLS_information:
        return("Information");
    case MSGCLS_progress:
        return("Progress");
    case MSGCLS_metric:
        return("Metric");
    case MSGCLS_event:
        return("Event");
    default:
        return("UNKNOWN");
    }
}

inline const char * LogMsgClassToFixString(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_disaster:
        return("DIS");
    case MSGCLS_error:
        return("ERR");
    case MSGCLS_warning:
        return("WRN");
    case MSGCLS_information:
        return("INF");
    case MSGCLS_progress:
        return("PRO");
    case MSGCLS_metric:
        return("MET");
    case MSGCLS_event:
        return("EVT");
    default:
        return("UNK");
    }
}

inline LogMsgClass LogMsgClassFromAbbrev(char const * abbrev)
{
    if(strnicmp(abbrev, "DIS", 3)==0)
        return MSGCLS_disaster;
    if(strnicmp(abbrev, "ERR", 3)==0)
        return MSGCLS_error;
    if(strnicmp(abbrev, "WRN", 3)==0)
        return MSGCLS_warning;
    if(strnicmp(abbrev, "INF", 3)==0)
        return MSGCLS_information;
    if(strnicmp(abbrev, "PRO", 3)==0)
        return MSGCLS_progress;
    if(strnicmp(abbrev, "MET", 3)==0)
        return MSGCLS_metric;
    if(strnicmp(abbrev, "EVT", 3)==0)
        return MSGCLS_event;
    if(strnicmp(abbrev, "ALL", 3)==0)
        return MSGCLS_all;
    return MSGCLS_unknown;
}

typedef unsigned LogMsgDetail;
#define DefaultDetail   DebugMsgThreshold
#define TopDetail (LogMsgDetail)-1

/*
 * Log message thresholds, assigned to log message category types.
 * It represents the lowest logging level (detail) required to output
 * messages of the given category.
 */
constexpr LogMsgDetail CriticalMsgThreshold    = 1;  //Use to declare categories reporting critical events (log level => 1)
constexpr LogMsgDetail FatalMsgThreshold       = 1;  //Use to declare categories reporting Fatal events (log level => 1)
constexpr LogMsgDetail ErrMsgThreshold         = 10; //Use to declare categories reporting Err messages (log level => 10)
constexpr LogMsgDetail WarnMsgThreshold        = 20; //Use to declare categories reporting Warn messages (log level => 20)
constexpr LogMsgDetail AudMsgThreshold         = 30; //Use to declare categories reporting Aud messages (log level => 30)
constexpr LogMsgDetail ProgressMsgThreshold    = 50; //Use to declare categories reporting Progress messages (log level => 50)
constexpr LogMsgDetail InfoMsgThreshold        = 60; //Use to declare categories reporting Info messages (log level => 60)
constexpr LogMsgDetail DebugMsgThreshold       = 80; //Use to declare categories reporting Debug messages (log level => 80)
constexpr LogMsgDetail ExtraneousMsgThreshold  = 90; //Use to declare categories reporting Extraneous messages (log level => 90)

// Typedef for LogMsgSysInfo

typedef unsigned LogMsgId;

// Typedefs and consts for LogMsgJobInfo

typedef unsigned __int64 LogMsgJobId;
typedef unsigned __int64 LogMsgUserId;
typedef unsigned __int64 LogMsgSessionId;
constexpr LogMsgJobId UnknownJob = (LogMsgJobId)-1;
constexpr LogMsgUserId UnknownUser = (LogMsgUserId)-1;
constexpr LogMsgSessionId UnknownSession = (LogMsgSessionId)-1;

// Other enums, typedefs, and consts

typedef int LogMsgCode;
constexpr LogMsgCode NoLogMsgCode = (LogMsgCode)-1;

// When changing this enum, be sure to update (a) the string function, and (b) the abbrev function

typedef enum
{
    MSGFIELD_audience    = 0x000001,
    MSGFIELD_class       = 0x000002,
    MSGFIELD_detail      = 0x000004,
    MSGFIELD_allCategory = 0x000007,
    MSGFIELD_msgID       = 0x000008,
    MSGFIELD_time        = 0x000010,
    MSGFIELD_date        = 0x000020,
    MSGFIELD_timeDate    = 0x000030,
    MSGFIELD_process     = 0x000040,
    MSGFIELD_thread      = 0x000080,
    MSGFIELD_node        = 0x000100,
    MSGFIELD_allSysInfo  = 0x00F1F8,
    MSGFIELD_job         = 0x000200,
    MSGFIELD_user        = 0x000400,
    MSGFIELD_session     = 0x000800,
    MSGFIELD_allJobInfo  = 0x000E00,
    MSGFIELD_code        = 0x001000,
    MSGFIELD_milliTime   = 0x002000,
    MSGFIELD_microTime   = 0x004000,
    MSGFIELD_nanoTime    = 0x008000,  // Not supported
    MSGFIELD_component   = 0x010000,
    MSGFIELD_quote       = 0x020000,
    MSGFIELD_prefix      = 0x040000,
    MSGFIELD_trace       = 0x080000,
    MSGFIELD_span        = 0x100000,
    MSGFIELD_last        = 0x100000,
    MSGFIELD_all         = 0xFFFFFF
} LogMsgField;

#ifdef _WIN32
#define MSGFIELD_STANDARD LogMsgField(MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix | MSGFIELD_audience)
#define MSGFIELD_LEGACY LogMsgField(MSGFIELD_timeDate | MSGFIELD_milliTime | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix)
#else
#ifdef _CONTAINERIZED
#define MSGFIELD_STANDARD LogMsgField( MSGFIELD_job | MSGFIELD_timeDate | MSGFIELD_milliTime | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_class | MSGFIELD_audience)
#else
#define MSGFIELD_STANDARD LogMsgField( MSGFIELD_timeDate | MSGFIELD_milliTime | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix | MSGFIELD_audience)
#endif
#define MSGFIELD_LEGACY LogMsgField(MSGFIELD_timeDate | MSGFIELD_milliTime | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix)
#endif

inline const char * LogMsgFieldToString(LogMsgField field)
{
    switch(field)
    {
    case MSGFIELD_audience:
        return("Audience");
    case MSGFIELD_class:
        return("Class");
    case MSGFIELD_detail:
        return("Detail");
    case MSGFIELD_msgID:
        return("Message ID");
    case MSGFIELD_time:
        return("Time");
    case MSGFIELD_date:
        return("Date");
    case MSGFIELD_process:
        return("Process ID");
    case MSGFIELD_thread:
        return("Thread ID");
    case MSGFIELD_node:
        return("Node");
    case MSGFIELD_job:
        return("Job");
    case MSGFIELD_user:
        return("User");
    case MSGFIELD_session:
        return("Session");
    case MSGFIELD_code:
        return("Code");
    case MSGFIELD_milliTime:
        return("Timer (milli)");
    case MSGFIELD_microTime:
        return("Timer (micro)");
    case MSGFIELD_nanoTime:
        return("Timer (nano)");
    case MSGFIELD_component:
        return("Component");
    case MSGFIELD_quote:
        return("Quote");
    case MSGFIELD_trace:
        return("Trace ID");
    case MSGFIELD_span:
        return("Span ID");
    default:
        return("UNKNOWN");
    }
}

inline unsigned LogMsgFieldFromAbbrev(char const * abbrev)
{
    if(strnicmp(abbrev, "AUD", 3)==0)
        return MSGFIELD_audience;
    if(strnicmp(abbrev, "CLS", 3)==0)
        return MSGFIELD_class;
    if(strnicmp(abbrev, "DET", 3)==0)
        return MSGFIELD_detail;
    if(strnicmp(abbrev, "MID", 3)==0)
        return MSGFIELD_msgID;
    if(strnicmp(abbrev, "TIM", 3)==0)
        return MSGFIELD_time;
    if(strnicmp(abbrev, "DAT", 3)==0)
        return MSGFIELD_date;
    if(strnicmp(abbrev, "PID", 3)==0)
        return MSGFIELD_process;
    if(strnicmp(abbrev, "TID", 3)==0)
        return MSGFIELD_thread;
    if(strnicmp(abbrev, "NOD", 3)==0)
        return MSGFIELD_node;
    if(strnicmp(abbrev, "JOB", 3)==0)
        return MSGFIELD_job;
    if(strnicmp(abbrev, "USE", 3)==0)
        return MSGFIELD_user;
    if(strnicmp(abbrev, "TRC", 3)==0)
        return MSGFIELD_trace;
    if(strnicmp(abbrev, "SPN", 3)==0)
        return MSGFIELD_span;
    if(strnicmp(abbrev, "SES", 3)==0)
        return MSGFIELD_session;
    if(strnicmp(abbrev, "COD", 3)==0)
        return MSGFIELD_code;
    if(strnicmp(abbrev, "MLT", 3)==0)
        return MSGFIELD_milliTime;
    if(strnicmp(abbrev, "MCT", 3)==0)
        return MSGFIELD_microTime;
    if(strnicmp(abbrev, "NNT", 3)==0)
        return MSGFIELD_nanoTime;
    if(strnicmp(abbrev, "COM", 3)==0)
        return MSGFIELD_component;
    if(strnicmp(abbrev, "QUO", 3)==0)
        return MSGFIELD_quote;
    if(strnicmp(abbrev, "PFX", 3)==0)
        return MSGFIELD_prefix;
    if(strnicmp(abbrev, "ALL", 3)==0)
        return MSGFIELD_all;
    if(strnicmp(abbrev, "STD", 3)==0)
        return MSGFIELD_STANDARD;
    return 0;
}

inline unsigned processAbbrevsString(char const * abbrevs, LogMsgAttType type)
{
    unsigned values = 0;
    bool negate = false;
    bool more = true;
    while(more)
    {
        if(strlen(abbrevs) < 3)
            break;
        unsigned value = 0;
        switch(type)
        {
            case LogMsgAttType_Audience:
                value = LogMsgAudFromAbbrev(abbrevs);
                break;
            case LogMsgAttType_fields:
                value = LogMsgFieldFromAbbrev(abbrevs);
                break;
            case LogMsgAttType_class:
                value = LogMsgClassFromAbbrev(abbrevs);
                break;
            default:
                throwUnexpected();
        }

        if(value)
        {
            if(negate)
                values &= ~value;
            else
                values |= value;
        }
        switch(abbrevs[3])
        {
            case '+':
                negate = false;
                abbrevs += 4;
                break;
            case '-':
                negate = true;
                abbrevs += 4;
                break;
            default:
                more = false;
        }
    }
    return values;
}

// This function parses strings such as "ADT+PRO+USR" and "ALL+ADT-PRO"
inline unsigned logMsgAudsFromAbbrevs(const char * abbrevs)
{
    return processAbbrevsString(abbrevs, LogMsgAttType_Audience);
}

// This function parses strings such as "DIS+ERR+WRN+INF" and "ALL+PRO-INF"
inline unsigned logMsgClassesFromAbbrevs(const char * abbrevs)
{
    return processAbbrevsString(abbrevs, LogMsgAttType_class);
}

// This function parses strings such as "AUD+CLS+DET+COD" and "STD+MIT-PID", and is used for fields attribute in XML handler descriptions
inline unsigned logMsgFieldsFromAbbrevs(const char * abbrevs)
{
    return processAbbrevsString(abbrevs, LogMsgAttType_fields);
}

inline char const * msgPrefix(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_error:
        return "ERROR: ";
    case MSGCLS_warning:
        return "WARNING: ";
    default:
        return "";
    }
}

// LOG MESSAGE CLASS AND ITS COMPONENTS

// Info about category of log message, provided by user (this info is static, chosen during coding)

class jlib_decl LogMsgCategory
{
public:
    constexpr LogMsgCategory(LogMsgAudience _audience = MSGAUD_programmer, LogMsgClass _class = MSGCLS_information, LogMsgDetail _detail = DefaultDetail) : audience(_audience), msgClass(_class), detail(_detail) {}
    constexpr LogMsgAudience  queryAudience() const { return audience; }
    constexpr LogMsgClass     queryClass() const { return msgClass; }
    constexpr LogMsgDetail    queryDetail() const { return detail; }
    void                      serialize(MemoryBuffer & out) const { out.append(audience).append(msgClass).append(detail); }
    void                      deserialize(MemoryBuffer & in)
    {
        unsigned a, c, d; in.read(a).read(c).read(d);
        audience = (LogMsgAudience) a;
        msgClass = (LogMsgClass) c;
        detail = (LogMsgDetail) d;
    }
    constexpr LogMsgCategory  operator ()(unsigned newDetail) const { return LogMsgCategory(audience, msgClass, newDetail); }
private:
    LogMsgAudience            audience;
    LogMsgClass               msgClass;
    LogMsgDetail              detail;
};

// Info about log message determined automatically by system

class jlib_decl LogMsgSysInfo
{
public:
    LogMsgSysInfo(LogMsgId _id = (LogMsgId)-1, unsigned port = 0, LogMsgSessionId session = UnknownSession);
    inline LogMsgId           queryMsgID() const { return id; }
#ifdef _WIN32
    inline time_t             queryTime() const { return timeStarted; }
    inline unsigned           queryUSecs() const { return 0; }
#else
    inline time_t             queryTime() const { return timeStarted.tv_sec; }
    inline unsigned           queryUSecs() const { return (unsigned)timeStarted.tv_usec; }
#endif
    inline unsigned           queryProcessID() const { return processID; }
    inline unsigned           queryThreadID() const { return threadID; }
    inline LogMsgSessionId    querySessionID() const { return sessionID; }
    inline const SocketEndpoint * queryNode() const { return &node; }
    void                      serialize(MemoryBuffer & out) const;
    void                      deserialize(MemoryBuffer & in);
private:
    LogMsgId                  id;
#ifdef _WIN32
    time_t                     timeStarted;
#else
    struct timeval            timeStarted;
#endif
    unsigned                  processID;
    unsigned                  threadID;
    LogMsgSessionId           sessionID;
    SocketEndpoint            node;
};

// Info about job generating log message, provided by user (this info is dynamic, determined at run-time)

class jlib_decl LogMsgJobInfo
{
public:
    constexpr LogMsgJobInfo(LogMsgJobId _job = UnknownJob, LogMsgUserId _user = UnknownUser) : jobID(_job), userID(_user) {}
    ~LogMsgJobInfo();
    LogMsgJobId queryJobID() const;
    const char * queryJobIDStr() const;
    inline LogMsgUserId queryUserID() const { return userID; }
    void setJobID(LogMsgUserId id);
    void serialize(MemoryBuffer & out) const;
    void deserialize(MemoryBuffer & in);
private:
    union {
        LogMsgJobId           jobID;
        const char *          jobIDStr;
    };
    LogMsgUserId              userID;
    bool                      isDeserialized = false;
};

#define UNK_LOG_ENTRY "UNK"
class jlib_decl LogMsgTraceInfo
{
public:
    LogMsgTraceInfo() = default;
    LogMsgTraceInfo(ISpan * _span) : span(_span)
    {
    }

    const char * queryTraceID() const
    {
        if (span && span->isValid())
        {
            const char * traceId = span->queryTraceId();
            if (traceId)
                return traceId;
        }
        return UNK_LOG_ENTRY;
    }

    const char * querySpanID() const
    {
        if (span && span->isValid())
        {
            const char * spanId = span->querySpanId();
            if (spanId)
                return spanId;
        }
        return UNK_LOG_ENTRY;
    }

private:
    Linked<ISpan> span;
};

class jlib_decl LogMsg : public CInterface
{
public:
    LogMsg() : category(), sysInfo(), jobInfo(), remoteFlag(false) {}
    LogMsg(LogMsgJobId id, const char *job);  // Used for tracking job ids
    LogMsg(const LogMsgCategory & _cat, LogMsgId _id, LogMsgCode _code, const char * _text, unsigned port, LogMsgSessionId session);
    LogMsg(const LogMsgCategory & _cat, LogMsgId _id, LogMsgCode _code, size32_t sz, const char * _text, unsigned port, LogMsgSessionId session);
    LogMsg(const LogMsgCategory & _cat, LogMsgId _id, LogMsgCode _code, const char * format, va_list args,
           unsigned port, LogMsgSessionId session)  __attribute__((format(printf,5, 0)));
    StringBuffer &            toStringPlain(StringBuffer & out, unsigned fields) const;
    StringBuffer &            toStringXML(StringBuffer & out, unsigned fields) const;
    StringBuffer &            toStringJSON(StringBuffer & out, unsigned fields) const;
    StringBuffer &            toStringTable(StringBuffer & out, unsigned fields) const;
    static StringBuffer &     toStringTableHead(StringBuffer & out, unsigned fields);
    static void               fprintTableHead(FILE * handle, unsigned fields);
    inline const LogMsgCategory  queryCategory() const { return category; }
    inline const LogMsgSysInfo & querySysInfo() const { return sysInfo; }
    inline const LogMsgJobInfo & queryJobInfo() const { return jobInfo; }
    inline LogMsgCode         queryCode() const { return msgCode; }
    inline const char *       queryText() const { return text.str(); }
    void                      serialize(MemoryBuffer & out) const { category.serialize(out); sysInfo.serialize(out); jobInfo.serialize(out); out.append(msgCode); text.serialize(out); }
    void                      deserialize(MemoryBuffer & in);
    bool                      queryRemoteFlag() const { return remoteFlag; }
protected:
    LogMsgCategory            category;
    LogMsgSysInfo             sysInfo;
    LogMsgJobInfo             jobInfo;
    LogMsgTraceInfo           traceInfo;
    LogMsgCode                msgCode = NoLogMsgCode;
    StringBuffer              text;
    bool                      remoteFlag = false;
};

jlib_decl unsigned getPositionOfField(unsigned logfields, unsigned positionoffield);
jlib_decl unsigned getMessageFieldsFromHeader(const char * line);
jlib_decl unsigned getMessageFieldsFromHeader(FILE *handle);

// INTERFACES

// Filter for log messages --- contains method to accept or reject messages

interface jlib_decl ILogMsgFilter : public IInterface
{
 public:
    virtual bool              includeMessage(const LogMsg & msg) const = 0;
    virtual bool              mayIncludeCategory(const LogMsgCategory & cat) const = 0;
    virtual unsigned          queryAudienceMask() const = 0;
    virtual unsigned          queryClassMask() const = 0;
    virtual LogMsgDetail      queryMaxDetail() const = 0;
    virtual bool              isCategoryFilter() const { return false; }
    virtual void              serialize(MemoryBuffer & out, bool preserveLocal) const = 0;
    virtual void              addToPTree(IPropertyTree * tree) const = 0;
    virtual bool              queryLocalFlag() const { return false; }
};

// Handler for log messages --- contains method to write or send messages

typedef enum
{
    LOGFORMAT_undefined,
    LOGFORMAT_xml,
    LOGFORMAT_json,
    LOGFORMAT_table
} LogHandlerFormat;

interface jlib_decl ILogMsgHandler : public IInterface
{
 public:
    virtual void              handleMessage(const LogMsg & msg) = 0;
    virtual bool              needsPrep() const = 0;
    virtual void              prep() = 0;
    virtual unsigned          queryMessageFields() const = 0;
    virtual void              setMessageFields(unsigned _fields = MSGFIELD_all) = 0;
    virtual void              addToPTree(IPropertyTree * parent) const = 0;
    virtual int               flush() { return 0; }
    virtual bool              getLogName(StringBuffer &name) const = 0;
    virtual offset_t          getLogPosition(StringBuffer &logFileName) const = 0;
    virtual LogHandlerFormat  queryFormatType() const = 0;
};

// Class on manager's list of children which sends new filters to children, and holds thread which receives log messages

class jlib_decl ILogMsgLinkToChild : public IInterface
{
public:
    virtual void              sendFilter(ILogMsgFilter * filter) const = 0;
    virtual void              sendFilterOwn(ILogMsgFilter * filter) const = 0;
    virtual void              connect() = 0;
    virtual void              disconnect() = 0;
    virtual bool              queryConnected() const = 0;
    virtual void              markDisconnected() = 0;
};

// Manager to receive log messages, filter, and pass to handlers

interface jlib_decl ILogMsgListener : public IInterface
{
    virtual bool              addMonitor(ILogMsgHandler * handler, ILogMsgFilter * filter) = 0;
    virtual bool              addMonitorOwn(ILogMsgHandler * handler, ILogMsgFilter * filter) = 0;
    virtual bool              removeMonitor(ILogMsgHandler * handler) = 0;
    typedef bool HandlerTest(ILogMsgHandler * handler);
    virtual unsigned          removeMonitorsMatching(HandlerTest & test) = 0;
    virtual void              removeAllMonitors() = 0;
    virtual bool              isActiveMonitor(const ILogMsgHandler * handler) const = 0;
    virtual ILogMsgFilter *   queryMonitorFilter(const ILogMsgHandler * handler) const = 0;
    virtual ILogMsgFilter *   getMonitorFilter(const ILogMsgHandler * handler) const = 0;
    virtual bool              changeMonitorFilter(const ILogMsgHandler * handler, ILogMsgFilter * newFilter) = 0;
    virtual bool              changeMonitorFilterOwn(const ILogMsgHandler * handler, ILogMsgFilter * newFilter) = 0;
    virtual void              prepAllHandlers() const = 0;
    virtual void              addChildOwn(ILogMsgLinkToChild * child) = 0;
    virtual void              removeChild(ILogMsgLinkToChild * child) = 0;
    virtual void              removeAllChildren() = 0;
    virtual ILogMsgFilter *   getCompoundFilter(bool locked = false) const = 0;
    virtual void              suspendChildren() = 0;
    virtual void              unsuspendChildren() = 0;
    virtual bool              addMonitorToPTree(const ILogMsgHandler * handler, IPropertyTree * tree) const = 0;
    virtual void              addAllMonitorsToPTree(IPropertyTree * tree) const = 0;
    virtual void              setPort(unsigned _port) = 0;
    virtual unsigned          queryPort() const = 0;
    virtual void              setSession(LogMsgSessionId _session) = 0;
    virtual LogMsgSessionId   querySession() const = 0;
};

interface jlib_decl ILogMsgManager : public ILogMsgListener
{
 public:
    virtual void              enterQueueingMode() = 0;
    virtual void              setQueueBlockingLimit(unsigned lim) = 0;
    virtual void              setQueueDroppingLimit(unsigned lim, unsigned numToDrop) = 0;
    virtual void              resetQueueLimit() = 0;
    virtual bool              flushQueue(unsigned timeout) = 0;
    virtual void              resetMonitors() = 0;
    virtual void              report(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 3, 4))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 4, 5))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, LogMsgCode code , const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL) = 0;
    virtual void              mreport_direct(const LogMsgCategory & cat, const char * msg) = 0;
    virtual void              mreport_va(const LogMsgCategory & cat, const char * format, va_list args) = 0;
    virtual void              report(const LogMsg & msg) const = 0;
    virtual LogMsgId          getNextID() = 0;
    virtual bool              rejectsCategory(const LogMsgCategory & cat) const = 0;
    virtual offset_t          getLogPosition(StringBuffer &logFileName, const ILogMsgHandler * handler) const = 0;
    virtual LogMsgJobId       addJobId(const char *job) = 0;
    virtual void              removeJobId(LogMsgJobId) = 0;
    virtual const char *      queryJobId(LogMsgJobId id) const = 0;
};

// CONCRETE CLASSES

// Class which mimics the report methods of a manager, prepending the given file and line (intended only for use in the FLLOG macro, below)

class jlib_decl LogMsgPrepender
{
public:
    LogMsgPrepender(char const * f, unsigned l) : file(sanitizeSourceFile(f)), line(l) { }
    void                      report(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void                      report_va(const LogMsgCategory & cat, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 4, 5)));
    void                      report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
    IException *              report(IException * e, const char * prefix = NULL, LogMsgClass cls = MSGCLS_error); // uses MCexception(e, cls),  handy for EXCLOG
private:
    char const *              file;
    unsigned                  line;
};

// FUNCTIONS, DATA, AND MACROS

// Function to get filters and handlers

extern jlib_decl ILogMsgFilter * getDefaultLogMsgFilter();
extern jlib_decl ILogMsgFilter * getPassAllLogMsgFilter();
extern jlib_decl ILogMsgFilter * getLocalLogMsgFilter();
extern jlib_decl ILogMsgFilter * getPassNoneLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryPassAllLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryLocalLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryPassNoneLogMsgFilter();
extern jlib_decl ILogMsgFilter * getCategoryLogMsgFilter(unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail maxDetail = TopDetail, bool local = false);
extern jlib_decl ILogMsgFilter * getPIDLogMsgFilter(unsigned pid, bool local = false);
extern jlib_decl ILogMsgFilter * getTIDLogMsgFilter(unsigned tid, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(const char * name, unsigned port = 0, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(const IpAddress & ip, unsigned port = 0, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(unsigned port, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(const char * name, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(const IpAddress & ip, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(bool local = false);
extern jlib_decl ILogMsgFilter * getJobLogMsgFilter(LogMsgJobId job, bool local = false);
extern jlib_decl ILogMsgFilter * getUserLogMsgFilter(LogMsgUserId user, bool local = false);
extern jlib_decl ILogMsgFilter * getSessionLogMsgFilter(LogMsgSessionId session, bool local = false);
extern jlib_decl ILogMsgFilter * getRegexLogMsgFilter(const char *regex, bool local = false);
extern jlib_decl ILogMsgFilter * getNotLogMsgFilter(ILogMsgFilter * arg);
extern jlib_decl ILogMsgFilter * getNotLogMsgFilterOwn(ILogMsgFilter * arg);
extern jlib_decl ILogMsgFilter * getAndLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getAndLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getOrLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getOrLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getSwitchLogMsgFilterOwn(ILogMsgFilter * switchFilter, ILogMsgFilter * yesFilter, ILogMsgFilter * noFilter);

extern jlib_decl ILogMsgHandler * getHandleLogMsgHandler(FILE * handle = stderr, unsigned fields = MSGFIELD_all, LogHandlerFormat logFormat = LOGFORMAT_table);
extern jlib_decl ILogMsgHandler * getFileLogMsgHandler(const char * filename, const char * headertext = 0, unsigned fields = MSGFIELD_all, LogHandlerFormat logFormat = LOGFORMAT_table, bool append = false, bool flushes = true);
extern jlib_decl ILogMsgHandler * getRollingFileLogMsgHandler(const char * filebase, const char * fileextn, unsigned fields = MSGFIELD_all, bool append = false, bool flushes = true, const char *initialName = NULL, const char *alias = NULL, bool daily = false, long maxLogSize = 0);
extern jlib_decl ILogMsgHandler * getBinLogMsgHandler(const char * filename, bool append = false);
extern jlib_decl ILogMsgHandler * getPostMortemLogMsgHandler(const char * filebase, unsigned maxLinesToKeep, unsigned _messageFields=MSGFIELD_all);

// Function to install switch filter into a monitor, switch some messages to new filter whilst leaving rest to previous filter

extern jlib_decl void installLogMsgFilterSwitch(ILogMsgHandler * handler, ILogMsgFilter * switchFilter, ILogMsgFilter * newFilter);

// Functions to make standard handlers and catagory filters and add to manager

extern jlib_decl ILogMsgHandler * attachStandardFileLogMsgMonitor(const char * filename, const char * headertext = 0, unsigned fields = MSGFIELD_all, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, LogHandlerFormat logFormat = LOGFORMAT_table, bool append = false, bool flushes = true, bool local = false);
extern jlib_decl ILogMsgHandler * attachStandardBinLogMsgMonitor(const char * filename, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, bool append = false, bool local = false);
extern jlib_decl ILogMsgHandler * attachStandardHandleLogMsgMonitor(FILE * handle = stderr, unsigned fields = MSGFIELD_all, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, LogHandlerFormat logFormat = LOGFORMAT_table, bool local = false);

// Function to construct filter from serialized and XML forms, and construct handler from XML form, and attach monitor(s) from XML form

extern jlib_decl ILogMsgFilter * getDeserializedLogMsgFilter(MemoryBuffer & in);
extern jlib_decl ILogMsgFilter * getLogMsgFilterFromPTree(IPropertyTree * tree);
extern jlib_decl ILogMsgHandler * getLogMsgHandlerFromPTree(IPropertyTree * tree);
extern jlib_decl ILogMsgHandler * attachLogMsgMonitorFromPTree(IPropertyTree * tree);     // Takes tree containing <handler> and <filter> elements
extern jlib_decl void attachManyLogMsgMonitorsFromPTree(IPropertyTree * tree);            // Takes tree containing many <monitor> elements

// Standard categories and unknown jobInfo
constexpr LogMsgCategory MCoperatorDisaster(MSGAUD_operator, MSGCLS_disaster, FatalMsgThreshold);
constexpr LogMsgCategory MCuserError(MSGAUD_user, MSGCLS_error, ErrMsgThreshold);
constexpr LogMsgCategory MCoperatorError(MSGAUD_operator, MSGCLS_error, ErrMsgThreshold);
constexpr LogMsgCategory MCdebugError(MSGAUD_programmer, MSGCLS_error, ErrMsgThreshold);
constexpr LogMsgCategory MCauditError(MSGAUD_audit, MSGCLS_error, ErrMsgThreshold);
constexpr LogMsgCategory MCuserWarning(MSGAUD_user, MSGCLS_warning, WarnMsgThreshold);
constexpr LogMsgCategory MCoperatorWarning(MSGAUD_operator, MSGCLS_warning, WarnMsgThreshold);
constexpr LogMsgCategory MCdebugWarning(MSGAUD_programmer, MSGCLS_warning, WarnMsgThreshold);
constexpr LogMsgCategory MCauditWarning(MSGAUD_audit, MSGCLS_warning, WarnMsgThreshold);
constexpr LogMsgCategory MCuserProgress(MSGAUD_user, MSGCLS_progress, ProgressMsgThreshold);
constexpr LogMsgCategory MCoperatorProgress(MSGAUD_operator, MSGCLS_progress, ProgressMsgThreshold);
constexpr LogMsgCategory MCdebugProgress(MSGAUD_programmer, MSGCLS_progress, DebugMsgThreshold);
constexpr LogMsgCategory MCuserInfo(MSGAUD_user, MSGCLS_information, InfoMsgThreshold);
constexpr LogMsgCategory MCdebugInfo(MSGAUD_programmer, MSGCLS_information, DebugMsgThreshold);
constexpr LogMsgCategory MCauditInfo(MSGAUD_audit, MSGCLS_information, AudMsgThreshold);
constexpr LogMsgCategory MCoperatorInfo(MSGAUD_operator, MSGCLS_information, InfoMsgThreshold);
constexpr LogMsgCategory MCmonitorMetric(MSGAUD_monitor, MSGCLS_metric, ErrMsgThreshold);
constexpr LogMsgCategory MCmonitorEvent(MSGAUD_monitor, MSGCLS_event, ProgressMsgThreshold);

/*
 * Function to determine log level (detail) for exceptions, based on log message class
 */
inline LogMsgDetail mapClassToDefaultDetailLevel(LogMsgClass cls)
{
    switch (cls)
    {
    case MSGCLS_disaster:
    case MSGCLS_all:
        return FatalMsgThreshold;
    case MSGCLS_error:
        return ErrMsgThreshold;
    case MSGCLS_warning:
        return WarnMsgThreshold;
    case MSGCLS_information:
        return InfoMsgThreshold;
    case MSGCLS_progress:
        return ProgressMsgThreshold;
    default:
        return DefaultDetail;
    }
}

inline LogMsgCategory MCexception(IException * e, LogMsgClass cls = MSGCLS_error)
{
    return LogMsgCategory((e)->errorAudience(),cls, mapClassToDefaultDetailLevel(cls));
}

#define MCerror MCuserError
#define MCwarning MCuserWarning
#define MCprogress MCuserProgress

// Function to return manager, standard handler and the reporters, and the handler's message fields

extern jlib_decl ILogMsgManager * queryLogMsgManager();
extern jlib_decl ILogMsgHandler * queryStderrLogMsgHandler();
extern jlib_decl ILogMsgHandler * queryPostMortemLogMsgHandler();
extern jlib_decl bool copyPostMortemLogging(const char *target, bool clear);
extern jlib_decl bool copyPortMortemPIDFiles(HANDLE pid, const char *target, bool clear);
extern jlib_decl bool getDebugInstanceDir(StringBuffer &dir, const char *componentType, const char *instance);
extern jlib_decl void setupContainerizedLogMsgHandler();

//extern jlib_decl ILogMsgManager * createLogMsgManager(); // use with care! (needed by mplog listener facility)

extern jlib_decl void setDefaultJobName(const char *id);

//A class for mapping job names to ids
class jlib_decl JobNameScope
{
public:
    JobNameScope() = default;
    JobNameScope(const char * name);
    ~JobNameScope() { clear(); }

    void clear();
    void set(const char * name);
    LogMsgJobId queryId() const { return id; }

protected:
    LogMsgJobId id = UnknownJob;
    LogMsgJobId prevId = UnknownJob;
};

// Macros to make logging as simple as possible

#define LOGMSGREPORTER queryLogMsgManager()
#define FLLOG LogMsgPrepender(__FILE__, __LINE__).report

#if 0
#ifdef _PROFILING_WITH_TRUETIME_
//It can't cope with the macro definition..... at least v6.5 can't.
inline void LOG(const LogMsg & msg)
{
    LOGMSGREPORTER->report(msg);
}
void LOG(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 2, 3)));
inline void LOG(const LogMsgCategory & cat, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, format, args);
    va_end(args);
}
void LOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 3, 4)));
inline void LOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, code , format, args);
    va_end(args);
}
inline void LOG(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL)
{
    LOGMSGREPORTER->report(cat, e, prefix);
}
inline void VALOG(const LogMsgCategory & cat, const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, format, args);
}
inline void VALOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, code , format, args);
}
#else
#define LOG LOGMSGREPORTER->report
#define VALOG LOGMSGREPORTER->report_va
#endif
#else
extern jlib_decl void ctxlogReport(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 2, 3)));
extern jlib_decl void ctxlogReportVA(const LogMsgCategory & cat, const char * format, va_list args) __attribute__((format(printf, 2, 0)));
extern jlib_decl void ctxlogReport(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 3, 4)));
extern jlib_decl void ctxlogReportVA(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args) __attribute__((format(printf, 3, 0)));
extern jlib_decl void ctxlogReport(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
extern jlib_decl IException * ctxlogReport(IException * e, const char * prefix = NULL, LogMsgClass cls = MSGCLS_error); // uses MCexception(e, cls),  handy for EXCLOG
#define LOG ::ctxlogReport
#define VALOG ::ctxlogReportVA

#endif

inline void DBGLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void DBGLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugInfo, format, args);
    va_end(args);
}

inline void DISLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void DISLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorDisaster, format, args);
    va_end(args);
}

inline void UERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void UERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserError, format, args);
    va_end(args);
}

// TODO: Remove the following #define once all ERRLOG has been removed from code
#define ERRLOG UERRLOG

inline void OERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void OERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorError, format, args);
    va_end(args);
}

inline void IERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void IERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugError, format, args);
    va_end(args);
}

inline void AERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void AERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCauditError, format, args);
    va_end(args);
}


inline void UWARNLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void UWARNLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserWarning, format, args);
    va_end(args);
}

// TODO: Remove the following #define once all WARNLOG has been removed from code
#define WARNLOG UWARNLOG

inline void OWARNLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void OWARNLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorWarning, format, args);
    va_end(args);
}

inline void IWARNLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void IWARNLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugWarning, format, args);
    va_end(args);
}

inline void PROGLOG(const char * format, ...) __attribute__((format(printf, 1, 2)));
inline void PROGLOG(const char * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserProgress, format, args);
    va_end(args);
}

inline void MLOG(const LogMsgCategory & cat, const char * msg)
{
    LOGMSGREPORTER->mreport_direct(cat, msg);
}

inline void MLOG(const LogMsgCategory & cat, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->mreport_va(cat, format, args);
    va_end(args);
}

inline void MLOG(const char * format, ...) __attribute__((format(printf, 1, 2)));
inline void MLOG(const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->mreport_va(MCuserProgress, format, args);
    va_end(args);
}

inline void DBGLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void DBGLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugInfo, code, format, args);
    va_end(args);
}

inline void DISLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void DISLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorDisaster, code, format, args);
    va_end(args);
}

inline void UWARNLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void UWARNLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserWarning, code, format, args);
    va_end(args);
}

inline void OWARNLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void OWARNLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorWarning, code, format, args);
    va_end(args);
}

inline void IWARNLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void IWARNLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugWarning, code, format, args);
    va_end(args);
}

inline void IWARNLOG(IException *except, const char *prefix=nullptr)
{
    LOG(MCdebugWarning, except, prefix);
}

inline void UWARNLOG(IException *except, const char *prefix=nullptr)
{
    LOG(MCuserWarning, except, prefix);
}

inline void OWARNLOG(IException *except, const char *prefix=nullptr)
{
    LOG(MCoperatorWarning, except, prefix);
}

inline void OERRLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void OERRLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorError, code, format, args);
    va_end(args);
}

inline void IERRLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void IERRLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugError, code, format, args);
    va_end(args);
}

inline void UERRLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void UERRLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserError, code, format, args);
    va_end(args);
}

inline void PROGLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void PROGLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserProgress, code, format, args);
    va_end(args);
}

inline void DBGLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCdebugInfo, except, prefix);
}

inline void IERRLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCdebugError, except, prefix);
}

inline void UERRLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCuserError, except, prefix);
}

inline void OERRLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCoperatorError, except, prefix);
}

inline void DISLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCoperatorDisaster, except, prefix);
}

#define EXCLOG FLLOG

#define FILELOG attachStandardFileLogMsgMonitor
#define BINLOG attachStandardBinLogMsgMonitor
#define HANDLELOG attachStandardHandleLogMsgMonitor
inline void removeLog() { queryLogMsgManager()->removeAllMonitors(); queryLogMsgManager()->removeAllChildren(); }
inline void resetLog()  { queryLogMsgManager()->resetMonitors();     queryLogMsgManager()->removeAllChildren(); }
#define PREPLOG queryLogMsgManager()->prepAllHandlers
#define REJECTLOG queryLogMsgManager()->rejectsCategory

#define AUDIT_TYPES_BEGIN typedef enum {
#define MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level) AUDIT_TYPE_##name,
#define AUDIT_TYPES_END NUM_AUDIT_TYPES } AuditType;
#include "jelogtype.hpp"
#undef AUDIT_TYPES_BEGIN
#undef MAKE_AUDIT_TYPE
#undef AUDIT_TYPES_END

class jlib_decl ISysLogEventLogger : public IInterface
{
public:
    virtual bool              log(AuditType auditType, char const * msg) = 0;
    virtual bool              log(AuditType auditType, char const * msg, size32_t datasize, void const * data) = 0;
};

extern jlib_decl ISysLogEventLogger * querySysLogEventLogger();
extern jlib_decl ILogMsgHandler * getSysLogMsgHandler(unsigned fields = MSGFIELD_all);
extern jlib_decl void UseSysLogForOperatorMessages(bool use=true);

#define SYSLOG querySysLogEventLogger()->log
#define AUDIT SYSLOG                               // bwd compatibility

extern jlib_decl void AuditSystemAccess(const char *userid, bool success, char const * msg,...) __attribute__((format(printf, 3, 4)));


interface jlib_decl IContextLogger : extends IInterface
{
    virtual void CTXLOG(const char *format, ...) const  __attribute__((format(printf, 2, 3)));
    virtual void mCTXLOG(const char *format, ...) const  __attribute__((format(printf, 2, 3)));
    virtual void CTXLOGva(const LogMsgCategory & cat, LogMsgCode code, const char *format, va_list args) const __attribute__((format(printf,4,0))) = 0;
    void logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const  __attribute__((format(printf, 5, 6)));
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const __attribute__((format(printf,5,0))) = 0;
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const = 0;
    virtual void setStatistic(StatisticKind kind, unsigned __int64 value) const = 0;
    virtual void mergeStats(unsigned activityId, const CRuntimeStatisticCollection &from) const = 0;
    virtual unsigned queryTraceLevel() const = 0;

    virtual const char *queryGlobalId() const = 0;
    virtual const char *queryLocalId() const = 0;
    virtual const char *queryCallerId() const = 0;
    virtual const CRuntimeStatisticCollection & queryStats() const = 0;
    virtual ISpan * queryActiveSpan() const = 0;
    virtual void setActiveSpan(ISpan * span) = 0; // Only call this function directly if this object's lifetime matches the lifetime of the span.  If there is any doubt use ContextSpanScope(ctx, span)
    virtual IProperties * getSpanContext() const = 0;
    virtual IProperties * getClientHeaders() const = 0;
    virtual void setSpanAttribute(const char *name, const char *value) const = 0;
    virtual void setSpanAttribute(const char *name, __uint64 value) const = 0;
    virtual void recordStatistics(IStatisticGatherer &progress) const = 0;
};

using ContextSpanScope = ContextSpanScopeImp<IContextLogger>;


extern jlib_decl StringBuffer &appendGloballyUniqueId(StringBuffer &s);

extern jlib_decl const IContextLogger &queryDummyContextLogger();
extern jlib_decl IContextLogger &updateDummyContextLogger();

//---------------------------------------------------------------------------

interface IComponentLogFileCreator : extends IInterface
{
    //IComponentLogFileCreator set methods
    virtual void setExtension(const char * _ext) = 0;       //log filename extension (eg ".log")
    virtual void setPrefix(const char * _prefix) = 0;       //filename prefix (eg "master")
    virtual void setName(const char * _name) = 0;           //log filename, overrides default of component name (without extension)
    virtual void setPostfix(const char * _postfix) = 0;     //filename postfix (eg "coalesce")
    virtual void setCreateAliasFile(bool _create) = 0;      //controls creation of hardlink alias file
    virtual void setAliasName(const char * _aliasName) = 0; //alias file name, overrides default of component name
    virtual void setLogDirSubdir(const char * _subdir) = 0; //subdir be appended to config log dir (eg "server" or "audit")
    virtual void setRolling(const bool _rolls) = 0;         //daily rollover to new file
    virtual void setMaxLogFileSize(const long _size) = 0;   //maximum log file size (files too large rolled over)
    virtual void setCompleteFilespec(const char * _fs) = 0; //Full filespec (path/fn.ext), overrides everything else

    //ILogMsgHandler fields
    virtual void setAppend(const bool _append) = 0;         //append to existing logfile
    virtual void setFlushes(const bool _flushes) = 0;       //automatically flush
    virtual void setMsgFields(const unsigned _fields) = 0;  //fields/columns to be included in log

    //ILogMsgFilter fields
    virtual void setMsgAudiences(const unsigned _audiences) = 0;    //log audience
    virtual void setMsgClasses(const unsigned _classes) = 0;        //message class
    virtual void setMaxDetail(const LogMsgDetail _maxDetail) = 0;   //message detail
    virtual void setLocal(const bool _local) = 0;                   //local logging

    //query methods (not valid until logging started)
    virtual const char * queryLogDir() const = 0;           //Location of component logfile
    virtual const char * queryLogFileSpec() const = 0;      //Full log filespec
    virtual const char * queryAliasFileSpec() const = 0;    //Full alias filespec, if created

    virtual ILogMsgHandler * beginLogging() = 0;    //begin logging to specified file(s)
};

extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(IPropertyTree * _properties, const char *_component);
extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(const char *_logDir, const char *_component);
extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(const char *_component);

struct LogAccessTimeRange
{
private:
    CDateTime startt;
    CDateTime endt;

public:
    void setStart(const CDateTime start)
    {
        startt = start;
    }

    void setEnd(const CDateTime  end)
    {
        endt = end;
    }

    void setStart(const char * start, bool local = true)
    {
        startt.setString(start,nullptr,local);
    }

    void setEnd(const char * end, bool local = true)
    {
        endt.setString(end,nullptr,local);
    }

    const CDateTime & getEndt() const
    {
        return endt;
    }

    const CDateTime & getStartt() const
    {
        return startt;
    }
};

//---------------------------------------------------------------------------
// Log retrieval interfaces and definitions

typedef enum
{
    LOGACCESS_FILTER_jobid,
    LOGACCESS_FILTER_class,
    LOGACCESS_FILTER_audience,
    LOGACCESS_FILTER_component,
    LOGACCESS_FILTER_or,
    LOGACCESS_FILTER_and,
    LOGACCESS_FILTER_wildcard,
    LOGACCESS_FILTER_instance,
    LOGACCESS_FILTER_host,
    LOGACCESS_FILTER_column,
    LOGACCESS_FILTER_pod,
    LOGACCESS_FILTER_trace,
    LOGACCESS_FILTER_span,
    LOGACCESS_FILTER_unknown
} LogAccessFilterType;

inline const char * logAccessFilterTypeToString(LogAccessFilterType field)
{
    switch(field)
    {
    case LOGACCESS_FILTER_jobid:
        return "jobid";
    case LOGACCESS_FILTER_class:
        return "class";
    case LOGACCESS_FILTER_audience:
        return "audience";
    case LOGACCESS_FILTER_component:
        return "component";
    case LOGACCESS_FILTER_pod:
        return "pod";
    case LOGACCESS_FILTER_instance:
        return "instance";
    case LOGACCESS_FILTER_host:
        return "host";
    case LOGACCESS_FILTER_trace:
        return "trace";
    case LOGACCESS_FILTER_span:
        return "span";
    case LOGACCESS_FILTER_or:
        return "OR";
    case LOGACCESS_FILTER_and:
        return "AND";
    case LOGACCESS_FILTER_wildcard:
         return "*" ;
    default:
        return "UNKNOWN";
    }
}

inline unsigned logAccessFilterTypeFromName(char const * name)
{
    if (isEmptyString(name))
        return LOGACCESS_FILTER_unknown;

    if(strieq(name, "jobid"))
        return LOGACCESS_FILTER_jobid;
    if(strieq(name, "class"))
        return LOGACCESS_FILTER_class;
    if(strieq(name, "audience"))
        return LOGACCESS_FILTER_audience;
    if(strieq(name, "component"))
        return LOGACCESS_FILTER_component;
    if(strieq(name, "pod"))
        return LOGACCESS_FILTER_pod;
    if(strieq(name, "instance"))
        return LOGACCESS_FILTER_instance;
    if(strieq(name, "trace"))
        return LOGACCESS_FILTER_trace;
    if(strieq(name, "span"))
        return LOGACCESS_FILTER_span;
    if(strieq(name, "host"))
        return LOGACCESS_FILTER_host;
    if(strieq(name, "OR"))
        return LOGACCESS_FILTER_or;
    if(strieq(name, "AND"))
        return LOGACCESS_FILTER_and;
    return LOGACCESS_FILTER_unknown;
}

interface jlib_decl ILogAccessFilter : public IInterface
{
 public:
    virtual void addToPTree(IPropertyTree * tree) const = 0;
    virtual void toString(StringBuffer & out) const = 0;
    virtual LogAccessFilterType filterType() const = 0;
    virtual ILogAccessFilter * leftFilterClause() const
    {
       return nullptr;
    }

    virtual ILogAccessFilter * rightFilterClause() const
    {
       return nullptr;
    }

    virtual const char * getFieldName() const
    {
       return nullptr;
    }

};

enum LogAccessReturnColsMode
{
    RETURNCOLS_MODE_min,
    RETURNCOLS_MODE_default,
    RETURNCOLS_MODE_custom,
    RETURNCOLS_MODE_all
};

enum LogAccessMappedField
{
    LOGACCESS_MAPPEDFIELD_timestamp,
    LOGACCESS_MAPPEDFIELD_jobid,
    LOGACCESS_MAPPEDFIELD_component,
    LOGACCESS_MAPPEDFIELD_class,
    LOGACCESS_MAPPEDFIELD_audience,
    LOGACCESS_MAPPEDFIELD_instance,
    LOGACCESS_MAPPEDFIELD_pod,
    LOGACCESS_MAPPEDFIELD_host,
    LOGACCESS_MAPPEDFIELD_traceid,
    LOGACCESS_MAPPEDFIELD_spanid,
    LOGACCESS_MAPPEDFIELD_global,
    LOGACCESS_MAPPEDFIELD_container,
    LOGACCESS_MAPPEDFIELD_message,
    LOGACCESS_MAPPEDFIELD_unmapped
};

inline const char * MappedFieldTypeToString(LogAccessMappedField mappedField)
{
    if (mappedField == LOGACCESS_MAPPEDFIELD_timestamp)
        return "timestamp";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_jobid)
        return "jobid";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_component)
        return "component";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_class)
        return "class";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_audience)
        return "audience";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_instance)
        return "instance";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_pod)
        return "pod";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_host)
        return "host";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_traceid)
        return "traceID";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_spanid)
        return "spanID";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_global)
        return "global";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_container)
        return "container";
    else if (mappedField == LOGACCESS_MAPPEDFIELD_message)
        return "message";
    else
        return "UNKNOWNFIELDTYPE";
}

enum SortByDirection
{
    SORTBY_DIRECTION_none,
    SORTBY_DIRECTION_ascending,
    SORTBY_DIRECTION_descending
};

struct SortByCondition
{
    LogAccessMappedField byKnownField;
    StringAttr fieldName;
    SortByDirection direction = SORTBY_DIRECTION_none;
};

typedef ArrayOf<SortByCondition> SortByConditions;

struct LogAccessConditions
{
private:
    Owned<ILogAccessFilter> filter;
    StringArray logFieldNames;
    LogAccessReturnColsMode returnColsMode = RETURNCOLS_MODE_default;
    LogAccessTimeRange timeRange;
    unsigned limit = 100;
    offset_t startFrom = 0;
    SortByConditions sortByConditions;

public:
    LogAccessConditions & operator = (const LogAccessConditions & l)
    {
        copySortByConditions(l.sortByConditions);
        copyLogFieldNames(l.logFieldNames);
        limit = l.limit;
        timeRange = l.timeRange;
        setFilter(LINK(l.filter));
        startFrom = l.startFrom;
        returnColsMode = l.returnColsMode;
        return *this;
    }

    LogAccessReturnColsMode getReturnColsMode() const
    {
        return returnColsMode;
    }

    void setReturnColsMode(LogAccessReturnColsMode retColsMode)
    {
        returnColsMode = retColsMode;
    }

    ILogAccessFilter * queryFilter() const
    {
        return filter.get();
    }

    void setFilter(ILogAccessFilter * _filter)
    {
        filter.setown(_filter);
    }

    void appendLogFieldName(const char * fieldname)
    {
        returnColsMode = RETURNCOLS_MODE_custom;
        if (!logFieldNames.contains(fieldname))
            logFieldNames.append(fieldname);
    }

    void copyLogFieldNames(const StringArray & fields)
    {
        ForEachItemIn(fieldsindex,fields)
        {
            appendLogFieldName(fields.item(fieldsindex));
        }
    }

    void addSortByCondition(LogAccessMappedField knownCol, const char * fieldName, SortByDirection direction)
    {
        SortByCondition condition;
        condition.byKnownField = knownCol;
        condition.fieldName.set(fieldName);
        condition.direction = direction;

        sortByConditions.append(condition);
    }

    const SortByConditions& getSortByConditions() const
    {
        return sortByConditions;
    }

    void copySortByConditions(const SortByConditions & SourceSortByConditions)
    {
        ForEachItemIn(conditionsIndex,SourceSortByConditions)
        {
           sortByConditions.append(SourceSortByConditions.item(conditionsIndex));
        }
    }

    unsigned getLimit() const
    {
        return limit;
    }

    void setLimit(unsigned limit = 100)
    {
        this->limit = limit;
    }

    const StringArray& getLogFieldNames() const
    {
        return logFieldNames;
    }

    void setLogFieldNames(const StringArray &logFieldNames)
    {
        this->logFieldNames.kill();
        this->logFieldNames.appendArray(logFieldNames);
    }

    offset_t getStartFrom() const
    {
        return startFrom;
    }

    void setStartFrom(offset_t startFrom)
    {
        this->startFrom = startFrom;
    }

    const LogAccessTimeRange & getTimeRange() const
    {
        return timeRange;
    }

    void setTimeRange(const LogAccessTimeRange &timeRange)
    {
        this->timeRange = timeRange;
    }
};

typedef enum
{
    LOGACCESS_LOGFORMAT_xml,
    LOGACCESS_LOGFORMAT_json,
    LOGACCESS_LOGFORMAT_csv
} LogAccessLogFormat;

inline LogAccessLogFormat logAccessFormatFromName(const char * name)
{
    if (isEmptyString(name))
        throw makeStringException(-1, "Encountered empty Log Access Format name");

    if(strieq(name, "xml"))
        return LOGACCESS_LOGFORMAT_xml;
    else if(strieq(name, "json"))
        return LOGACCESS_LOGFORMAT_json;
    else if(strieq(name, "csv"))
        return LOGACCESS_LOGFORMAT_csv;
    else
        throw makeStringExceptionV(-1, "Encountered unknown Log Access Format name: '%s'", name);
}

interface IRemoteLogAccessStream : extends IInterface
{
    virtual bool readLogEntries(StringBuffer & record, unsigned & recsRead) = 0;
};

struct LogQueryResultDetails
{
    unsigned int totalReceived;
    unsigned int totalAvailable;
};

typedef enum
{
    LOGACCESS_STATUS_unknown = 0,
    LOGACCESS_STATUS_success = 1,
    LOGACCESS_STATUS_warning = 2,
    LOGACCESS_STATUS_fail = 3
} LogAccessHealthStatusCode;

struct LogAccessHealthStatus
{
private:
    LogAccessHealthStatusCode code = LOGACCESS_STATUS_unknown;
    std::vector<std::string> messages;

public:
    LogAccessHealthStatus(LogAccessHealthStatusCode code_)
    {
        code = code_;
    }


    void appendMessage(const char * message)
    {
        if (!isEmptyString(message))
             messages.push_back(message);
    }

    std::vector<std::string> queryMessages()
    {
        return messages;
    }


    bool escalateStatusCode(LogAccessHealthStatusCode newCode)
    {
        if (newCode <= code) //Takes advantage of enum value assignment
            return false; //not escalated
        else
            code = newCode;

        return true; //escalated
    }

    LogAccessHealthStatusCode getCode() const {return code;}
};

inline const char * LogAccessHealthStatusToString(LogAccessHealthStatusCode statusCode)
{
    switch(statusCode)
    {
    case LOGACCESS_STATUS_success:
        return "Success";
    case LOGACCESS_STATUS_warning:
        return "Warning";
    case LOGACCESS_STATUS_fail:
        return "Fail";
    default:
        return "Unknown";
    }
};

struct LogAccessDebugReport
{
    StringBuffer SampleQueryReport;
    StringBuffer PluginDebugReport;
    StringBuffer ServerDebugReport;
};

struct LogAccessHealthReportDetails
{
    LogAccessHealthStatus status = LOGACCESS_STATUS_unknown;
    LogAccessDebugReport DebugReport;
    StringAttr Configuration;
};

struct LogAccessHealthReportOptions
{
    bool IncludeConfiguration = true;
    bool IncludeDebugReport = true;
    bool IncludeSampleQuery = true;
};

// Log Access Interface - Provides filtered access to persistent logging - independent of the log storage mechanism
//                      -- Declares method to retrieve log entries based on options set
//                      -- Declares method to retrieve remote log access type (eg elasticstack, etc)
//                      -- Declares method to retrieve active logmap (mapping between target log store and known log columns)
//                      -- Declares method to retrieve target log store connectivity information
interface IRemoteLogAccess : extends IInterface
{
    virtual bool fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format) = 0;
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format) = 0;
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize) = 0;
    virtual const char * getRemoteLogAccessType() const = 0;
    virtual IPropertyTree * queryLogMap() const = 0;
    virtual const char * fetchConnectionStr() const = 0;
    virtual bool supportsResultPaging() const = 0;
    virtual void healthReport(LogAccessHealthReportOptions options, LogAccessHealthReportDetails & report) = 0;
};

// Helper functions to construct log access filters
extern jlib_decl ILogAccessFilter * getLogAccessFilterFromPTree(IPropertyTree * tree);
extern jlib_decl ILogAccessFilter * getInstanceLogAccessFilter(const char * instancename);
extern jlib_decl ILogAccessFilter * getHostLogAccessFilter(const char * host);
extern jlib_decl ILogAccessFilter * getJobIDLogAccessFilter(const char * jobId);
extern jlib_decl ILogAccessFilter * getComponentLogAccessFilter(const char * component);
extern jlib_decl ILogAccessFilter * getPodLogAccessFilter(const char * podName);
extern jlib_decl ILogAccessFilter * getTraceIDLogAccessFilter(const char * traceId);
extern jlib_decl ILogAccessFilter * getSpanIDLogAccessFilter(const char * spanId);
extern jlib_decl ILogAccessFilter * getAudienceLogAccessFilter(MessageAudience audience);
extern jlib_decl ILogAccessFilter * getClassLogAccessFilter(LogMsgClass logclass);
extern jlib_decl ILogAccessFilter * getBinaryLogAccessFilter(ILogAccessFilter * arg1, ILogAccessFilter * arg2, LogAccessFilterType type);
extern jlib_decl ILogAccessFilter * getBinaryLogAccessFilterOwn(ILogAccessFilter * arg1, ILogAccessFilter * arg2, LogAccessFilterType type);
extern jlib_decl ILogAccessFilter * getCompoundLogAccessFilter(ILogAccessFilter * arg1, ILogAccessFilter * arg2, LogAccessFilterType type);
extern jlib_decl ILogAccessFilter * getWildCardLogAccessFilter();
extern jlib_decl ILogAccessFilter * getWildCardLogAccessFilter(const char * wildcardfilter);
extern jlib_decl ILogAccessFilter * getColumnLogAccessFilter(const char * columnName, const char * value);

// Helper functions to actuate log access query
extern jlib_decl bool fetchLog(LogQueryResultDetails & resultDetails, StringBuffer & returnbuf, IRemoteLogAccess & logAccess, ILogAccessFilter * filter, LogAccessTimeRange timeRange, const StringArray & cols, LogAccessLogFormat format, unsigned int & totalReceived, unsigned int & totalAvailable);
extern jlib_decl bool fetchJobIDLog(LogQueryResultDetails & resultDetails, StringBuffer & returnbuf, IRemoteLogAccess & logAccess, const char *jobid, LogAccessTimeRange timeRange, StringArray & cols, LogAccessLogFormat format);
extern jlib_decl bool fetchComponentLog(LogQueryResultDetails & resultDetails, StringBuffer & returnbuf, IRemoteLogAccess & logAccess, const char * component, LogAccessTimeRange timeRange, StringArray & cols, LogAccessLogFormat format);
extern jlib_decl bool fetchLogByAudience(LogQueryResultDetails & resultDetails, StringBuffer & returnbuf, IRemoteLogAccess & logAccess, MessageAudience audience, LogAccessTimeRange timeRange, StringArray & cols, LogAccessLogFormat format);
extern jlib_decl bool fetchLogByClass(LogQueryResultDetails & resultDetails, StringBuffer & returnbuf, IRemoteLogAccess & logAccess, LogMsgClass logclass, LogAccessTimeRange timeRange, StringArray & cols, LogAccessLogFormat format);
extern jlib_decl IRemoteLogAccess * queryRemoteLogAccessor();

#endif
