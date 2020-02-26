#!/usr/bin/env bash
set -euo pipefail

go generate ./...
set +e
diffline=$(git status -s | awk '{print $2}' | xargs grep '^// Code generated .* DO NOT EDIT\.$' 2>/dev/null)
set -e
if [[ $diffline != "" ]]
then
  echo "Your commit is changed after running go generate ./..., it should not hanppen."
  exit 1
fi
