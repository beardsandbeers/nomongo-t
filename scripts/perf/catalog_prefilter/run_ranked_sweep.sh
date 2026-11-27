#!/usr/bin/env bash
# CLASSPATH must include the compiled benchmark classes, pinned Lucene core and mongot deploy JAR.
set -euo pipefail
if [[ $# != 5 ]]; then
  echo "Usage: $0 leaf0 leaf1 tuning.i8 holdout.i8 new-output-directory" >&2
  exit 1
fi
leaf0=$1
leaf1=$2
tuning=$3
holdout=$4
out=$5
mkdir "$out"
java_args=(-Xms1g -Xmx3g --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED)
run() {
  local name=$1 mode=$2 filter=$3 queries=$4
  shift 4
  local props=()
  if [[ -n ${CATALOG_LIMITS:-} ]]; then props+=("-Dcatalog.limits=$CATALOG_LIMITS"); fi
  if [[ $filter != all ]]; then props+=("-Dcatalog.filter=$filter"); fi
  java "${java_args[@]}" "${props[@]}" -cp "${CLASSPATH:?Set CLASSPATH to the benchmark classpath}" \
    RankedStreamBenchmark "$mode" "$leaf0" "$leaf1" "$queries" "$@" > "$out/$name.csv"
}
grid=({16,64,256,1024}/{256,2048,16384}/{8,32,128}/{8,64,256} 64/1024/8/8 64/1024/32/128)
run screen stage categories_6 "$tuning" "${grid[@]}"
JAVA_TOOL_OPTIONS=-Dcatalog.shufflePrefixes=true run prefix-screen prefix categories_6 "$tuning" "${grid[@]}"
run edge stage categories_6 "$tuning" 1024/256/8/256 1024/256/8/512 2048/32768/256/512
run holdout-broad stage categories_6 "$holdout" \
  64/1024/8/8 64/1024/32/128 256/2048/128/256 1024/2048/128/256 1024/256/8/256
run holdout-filters stage all "$holdout" 64/1024/8/8 1024/2048/128/256
JAVA_TOOL_OPTIONS="-Dcatalog.shufflePrefixes=true -Dcatalog.deepPrefix=true" run deep prefix categories_6 "$holdout" \
  64/1024/8/8 256/4096/8/8 1024/2048/128/256 1024/16384/8/256 1024/16384/32/64 1024/16384/128/256 \
  2048/32768/32/64 4096/65536/32/64
for k in 128 500 2000 8000; do
  for full in false true; do
    JAVA_TOOL_OPTIONS="-Dcatalog.candidates=$k -Dcatalog.rescoreAll=$full" \
      run "pool-$k-$full" stage categories_6 "$tuning" 1024/2048/128/256
  done
done

for k in 500 2000; do
  JAVA_TOOL_OPTIONS="-Dcatalog.candidates=$k -Dcatalog.rescoreAll=true" \
    run "confirm-$k" stage categories_6 "$holdout" 1024/2048/128/256
done
JAVA_TOOL_OPTIONS="-Dcatalog.shufflePrefixes=true -Dcatalog.deepPrefix=true" run width-edge prefix categories_6 "$tuning" \
  4096/65536/32/64 8192/131072/32/64 10000/200000/32/64
JAVA_TOOL_OPTIONS="-Dcatalog.candidates=8000 -Dcatalog.rescoreAll=true" \
  run confirm-8000 stage categories_6 "$holdout" 1024/2048/128/256
