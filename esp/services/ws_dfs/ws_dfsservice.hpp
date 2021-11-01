/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _WS_DFSSERVICE_HPP__
#define _WS_DFSSERVICE_HPP__

#include "ws_dfs_esp.ipp"

#include "dadfs.hpp"
#include <atomic>


class CWsDfsEx : public CWsDfs
{
    std::atomic<bool> m_daliDetached{false};

public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsDfsEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual bool onDFSFileLookup(IEspContext &context, IEspDFSFileLookupRequest &req, IEspDFSFileLookupResponse &resp);

private:

    bool attachServiceToDali() override
    {
        m_daliDetached = false;
        return true;
    }

    bool detachServiceFromDali() override
    {
        m_daliDetached = true;
        return true;
    }

    bool isDetachedFromDali()
    {
        return m_daliDetached;
    }
};



#endif //_WS_DFSSERVICE_HPP__

