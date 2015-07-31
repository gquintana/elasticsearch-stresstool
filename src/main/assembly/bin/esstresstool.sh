#!/usr/bin/env bash
TOOL_DIR=$(dirname $0)/..
java -jar $TOOL_DIR/lib/elasticsearch-stresstool-${project.version}.jar $*
