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

NR := 1000;
ds := DATASET(NR, transform({ unsigned id }, SELF.id := COUNTER));

doAggregate(unsigned n) := FUNCTION
    childds := DATASET(1000+n, transform({ unsigned id, unsigned x }, SELF.id := COUNTER; SELF.x := COUNTER % 200));
    agg := TABLE(childds, { x, cnt := COUNT(GROUP) }, x);

    //Define an action
    o := output(agg,named('x'),extend);

    // never true
    condo := IF(n > NR, o);

    // always true
    condo2 := IF(n <= NR, condo);
    wh := WHEN(agg, condo2, success);
    agg2 := wh[1].cnt;
    return agg2;
END;

p1 := PROJECT(nofold(ds), transform({unsigned x},
        SELF.x := IF(LEFT.id <= NR, LEFT.id, doAggregate(LEFT.id));
    ));

p2 := PROJECT(nofold(ds), transform({unsigned x},
        SELF.x := IF(LEFT.id <= 1, LEFT.id, doAggregate(LEFT.id));
    ));

sequential(
    output(count(nofold(p1)));
    output(count(nofold(p2)));
);
