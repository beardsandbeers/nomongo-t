#!/usr/bin/env bash
# Refresh the Maven pin files (@maven, @lucene_fork, @protobuf_maven) and
# regenerate bazel/java/generated_pom/pom.xml (used by SNYK and Dependabot to
# scan for vulnerabilities).
#
# rules_jvm_external's coursier needs a JRE on disk to execute. Use Bazel's
# bundled Java runtime so re-pinning does not require host Java. A downloaded JDK
# repository also contains Bazel metadata, which collides with the files
# rules_java creates when importing JAVA_HOME as the local_jdk repository.
set -eu

JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${JAVA_DIR}/../vars.sh"

cd "${MONGOT_PATH}"

BAZEL_JAVA_HOME="$(bazel info java-home)"
export PATH="${BAZEL_JAVA_HOME}/bin:${PATH}"
unset JAVA_HOME

bazel run @lucene_fork//:pin
bazel run @maven//:pin
bazel run @protobuf_maven//:pin

"${JAVA_DIR}/update-pom-file.sh"
