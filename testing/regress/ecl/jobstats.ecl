/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

//class=file
//class=index
//version forceKJLocal=false,forceKJRemote=false
//version forceKJLocal=false,forceKJRemote=true
//version forceKJLocal=true,forceKJRemote=false

#onwarning (4522, ignore); // ignore implicit limit

import ^ as root;
import Std.File;
import lib_WorkunitServices.WorkunitServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;
 
forceKJRemote := #IFDEFINED(root.forceKJRemote, false);
forceKJLocal := #IFDEFINED(root.forceKJLocal, false);

rec := RECORD
 unsigned id;
 string s;
END;

//iRecs := 1000000;
//lhsRecs := 1000000;
iRecs := 10000;
lhsRecs := 10000;

lhs := DATASET(lhsRecs, TRANSFORM(rec, SELF.id := HASH(COUNTER) % iRecs; SELF.s := (string)HASH(SELF.id)), DISTRIBUTED);
rhs := DATASET(iRecs, TRANSFORM(rec, SELF.id := COUNTER; SELF.s := (string)HASH(SELF.id)), DISTRIBUTED);
 
iFilename := prefix + 'i';
i := INDEX(rhs, {id}, {rhs}, iFilename);
 
statRec := RECORD
 string stat;
END;

iWStatKinds := DATASET([
 { 'SizeDiskWrite' },
 { 'NumDiskWrites' },
 { 'NumDuplicateKeys' }
], statRec);


kJStatKinds := DATASET([
 { 'NumIndexSeeks' },
 { 'NumIndexScans' },
 { 'NumIndexWildSeeks' }
], statRec);

wuid := WORKUNIT;
iWFilter := 'w1:graph1:sg1:a5';
kJFilter := 'w1:graph2:sg6:a8';

getStats(string wuid, string scope, DATASET(statRec) statKinds) := FUNCTION
 filter := 'scope[' + scope + ']';
 stats := NOTHOR(WorkunitServices.WorkunitStatistics(wuid, false, filter));
 j := JOIN(stats, statKinds, LEFT.name = RIGHT.stat);
 RETURN TABLE(j, {name, value});
END;

kj := IF(forceKJRemote,
         JOIN(lhs, i, LEFT.id = RIGHT.id, TRANSFORM(rec, SELF := LEFT), HINT(forceRemoteKeyedLookup(true))),
         IF(forceKJLocal,
            JOIN(lhs, i, LEFT.id = RIGHT.id, TRANSFORM(rec, SELF := LEFT), HINT(remoteKeyedLookup(false))),
            JOIN(lhs, i, LEFT.id = RIGHT.id, TRANSFORM(rec, SELF := LEFT))
           )
        );

SEQUENTIAL(
 BUILD(i, OVERWRITE);
 
 COUNT(kj);

 OUTPUT(getStats(wuid, iWFilter, iWStatKinds), ALL);
 OUTPUT(getStats(wuid, kJFilter, kJStatKinds), ALL);
);


