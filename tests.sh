#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")"; "pwd")"; # "
pushd $SCRIPT_PATH >/dev/null

py.test tests -x

popd >/dev/null
