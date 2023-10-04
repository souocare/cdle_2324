#!/bin/bash

ROOT_DIR=${HOME}

echo -e "\nSetting write permission on output directories..."
CMD="chmod -R g+w ${ROOT_DIR}/examples/output/"
echo "${CMD}"

sudo chmod -R g+w ${ROOT_DIR}/examples/output/

echo -e "\nSetting owner permission for hadoop group on output directories..."
CMD="chown -R usermr:hadoop ${ROOT_DIR}/examples/output/"
echo "${CMD}"

sudo chown -R usermr:hadoop ${ROOT_DIR}/examples/output/

echo -e "\n"
