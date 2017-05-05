SNAPSHOT_VER=$(mvn help:evaluate -Dexpression=project.version | grep ^[^\[].*SNAPSHOT$ | cut -d'-' -f1)
GIT_SHA=$(git rev-parse HEAD | cut -b 1-7)
PROJ_VER=${SNAPSHOT_VER}'-'${GIT_SHA}
mvn versions:set -DnewVersion=${PROJ_VER}
mvn -B -Pcore --fail-at-end clean install
