#!/bin/bash

echo -e "\n\nSetting execution permission on shell scripts..."
CMD="find . -type f -name '*.sh' -print0 | xargs -0 chmod 755"
echo -e "\n${CMD}"

find . -type f -name '*.sh' -print0 | xargs -0 chmod 755

echo -e ""