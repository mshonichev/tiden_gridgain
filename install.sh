#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")"; "pwd")"; # "
pushd $SCRIPT_PATH >/dev/null

sudo -H pip3.7 install dist/tiden_gridgain-*-py3-none-any.whl

popd >/dev/null
