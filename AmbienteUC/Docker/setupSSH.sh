#!/bin/bash

KEY_FILE="./.ssh/id_rsa"

echo "Generating RSA key..."
ssh-keygen -t rsa -f ${KEY_FILE} -q -N ""

echo "Configuring login using authorized keys..."
cat ./.ssh/id_rsa.pub > ./.ssh/authorized_keys

cat ./.ssh/userKey.pub >> ./.ssh/authorized_keys
