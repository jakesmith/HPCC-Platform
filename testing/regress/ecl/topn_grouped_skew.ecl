/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

largeGroupSize := 100000 : STORED('largeGroupSize');
numTinyGroups := 100000 : STORED('numTinyGroups');

rec := RECORD
    UNSIGNED4 gid;
    UNSIGNED4 seq;
    UNSIGNED8 score;
END;

seedRec := RECORD
    UNSIGNED1 dummy;
END;

seed := DATASET([{1}], seedRec);

largeGroup := NORMALIZE(seed, largeGroupSize,
    TRANSFORM(rec,
        SELF.gid := 1;
        SELF.seq := COUNTER;
        // Reverse ordering so TOPN must examine all rows in the large group.
        SELF.score := (UNSIGNED8)largeGroupSize - COUNTER + 1;
    )
);

tinyGroups := NORMALIZE(seed, numTinyGroups,
    TRANSFORM(rec,
        SELF.gid := COUNTER + 1;
        SELF.seq := 1;
        SELF.score := (UNSIGNED8)1000000000 + COUNTER;
    )
);

allRows := NOFOLD(largeGroup + tinyGroups);

// Grouped TOPN test shape: 1 very large group and many singleton groups.
groupedRows := GROUP(SORT(allRows, gid, score), gid);
result := TOPN(groupedRows, 1, score);

OUTPUT(COUNT(result), NAMED('resultCount'));
OUTPUT(COUNT(result(gid=1 AND score=1)), NAMED('largeGroupWinnerCount'));
OUTPUT(COUNT(result(gid>1)), NAMED('tinyGroupWinnerCount'));
OUTPUT(SUM(result(gid>1), score), NAMED('tinyGroupWinnerScoreSum'));
