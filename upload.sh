#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")"; "pwd")"; # "
pushd $SCRIPT_PATH >/dev/null

twine upload --repository testpypi --verbose dist/*

popd >/dev/null
