#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

exec "${BASE_DIR}/scripts/debot_lighter_execute.sh" "$1"
