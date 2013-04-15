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

#ifndef ZZOOKEEPER_HPP
#define ZZOOKEEPER_HPP

#include "platform.h"
#include "jiface.hpp"
#include "jscm.hpp"

#define ZOOLOCK_READ 1
#define ZOOLOCK_WRITE 2

namespace zoo {

interface ILock : extends IInterface
{
    virtual bool changeMode(unsigned newMode, unsigned timeout) = 0;
};

interface IZooClient : extends IInterface
{
    virtual ILock *createLock(const char *path, unsigned mode, unsigned timeout) = 0;
};

IZooClient *createZooClient(const char *serverList);


} // namespace zoo


#endif
