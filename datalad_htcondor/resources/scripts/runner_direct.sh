#!/bin/sh
#

set -u -e

# run in root of dataset
cd dataset

exec "$@"
