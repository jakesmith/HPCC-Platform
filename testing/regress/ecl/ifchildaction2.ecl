/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//Test if actions in child queries - that are never executed

NR := 10;
ds := DATASET(NR, transform({ unsigned id }, SELF.id := COUNTER));

doAggregate(unsigned n) := FUNCTION
    childds := DATASET((n*200)%1000, transform({ unsigned id, unsigned x }, SELF.id := COUNTER; SELF.x := COUNTER % 200));
    agg := TABLE(childds, { x, cnt := COUNT(GROUP) }, x);

    //Define an action
    o := output(DATASET([transform({unsigned value},self.value := n)]),named('ids'),extend);

    // always true
    condo2 := IF(n <= NR, o);
    wh := WHEN(agg, condo2, success);
    agg2 := wh[1].cnt;
    return agg2;
END;

p1 := PROJECT(nofold(ds), transform({unsigned x},
        SELF.x := doAggregate(LEFT.id);
    ));

output(nofold(p1));
