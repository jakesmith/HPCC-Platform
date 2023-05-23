#option('pickBestEngine', false);

numRecs := 10;
rec := RECORD
 unsigned8 id;
 unsigned8 other;
 string984 rest := 'rest';
 string24  to1k := '1k';
END;

ds := DATASET(numRecs, TRANSFORM(rec, SELF.id := HASH(COUNTER), SELF.other := COUNTER), DISTRIBUTED);

sd1 := SORT(NOFOLD(ds), id);
sd2 := SORT(NOFOLD(ds), other);
sd2p := PROJECT(sd2, TRANSFORM({sd2.id}, SELF := LEFT));

PARALLEL(
 OUTPUT(DEDUP(sd1, other, HASH));
 OUTPUT(DEDUP(sd2p, id, HASH));
);
