#!/usr/bin/env bash

# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Run a BIGDATA command on all hosts defined in $BIGDATA_HOME/conf/nodes.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/bigdata-config.sh

exec "$bin/nodes.sh" --config $BIGDATA_CONF_DIR cd "$BIGDATA_HOME" \; rm -rf /path/to/clog
