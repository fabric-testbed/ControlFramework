#!/bin/bash
# Swagger generate server stub based on specification, them merge it into the project.
# Use carefully! Commit always before using this script!
# The following structure is assumed:
# .
# +-- my_server
# |   +-- swagger_server
# User is expected to replace swagger_server with my_server/swagger_server after executing this script

STUB_DIR=my_server

FILES_TO_COPY=(
  swagger_server/__init__.py
  swagger_server/__main__.py
)


DIRS_TO_COPY=(
  swagger_server/response
)

swagger-codegen generate -i openapi.json -l python-flask -o ${STUB_DIR}
find ${STUB_DIR}/swagger_server \( -type d -name .git -prune \) -o -type f -print0 | xargs -0 sed -i '' 's/swagger_server/fabric.orchestrator.swagger_server/g'


# check for new swagger_server directory
if [ ! -d "$STUB_DIR" ]; then
  echo "[ERROR] Unable to find ${STUB_DIR}"
fi

# copy directories from server to new swagger_server
for f in "${DIRS_TO_COPY[@]}"; do
  echo "[INFO] copy directory: ${f} to new swagger_server"
  cp -r ${f} $STUB_DIR/${f}
done

# copy files from server to new swagger_server
for f in "${FILES_TO_COPY[@]}"; do
  echo "[INFO] copy file: ${f} to new swagger_server"
  cp ${f} $STUB_DIR/${f}
done

# update controllers
echo "[INFO] update controllers to include response import"
while read f; do
  echo "---------------------------------------------------"
  echo "[INFO] updating file: ${f}"
  sed -i '' "s/from fabric.orchestrator.swagger_server import util/from fabric.orchestrator.swagger_server import util\\"$'\n'\\"from fabric.orchestrator.swagger_server.response import ${f%???} as rc/g" \
    $STUB_DIR/swagger_server/controllers/${f}
  while read line; do
    if [[ $line == def* ]]; then
      echo "  - ${line}"
      func_name=$(echo $line | cut -d ':' -f 1 | cut -d ' ' -f 2-)
      echo "    ${func_name//=None/}"
      sed -i '' "0,/'do some magic!'/s//rc.${func_name//=None/}/" $STUB_DIR/swagger_server/controllers/${f}
    fi
  done < <(cat $STUB_DIR/swagger_server/controllers/${f})
done < <(ls -1 $STUB_DIR/swagger_server/controllers | grep -v '^__*')

# replace server with new server-stub
echo "[TODO] remove existing swagger_server directory and move my_server/swagger_server to swagger_server"
