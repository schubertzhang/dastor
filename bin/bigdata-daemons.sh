#!/usr/bin/env bash

# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Run a BIGDATA command on all hosts defined in $BIGDATA_HOME/conf/nodes.

usage="Usage: bigdata-daemons.sh [--config <confdir>] [--nodes <nodesfilename>] [--copts <coptsfilepath>] (start|stop) command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/bigdata-config.sh

if [ "$BIGDATA_COPTS_FILE" != "" ] ; then
  COPTS_FILE_ARG="--copts $BIGDATA_COPTS_FILE"
fi

exec "$bin/nodes.sh" --config $BIGDATA_CONF_DIR cd "$BIGDATA_HOME" \; "$bin/bigdata-daemon.sh" --config $BIGDATA_CONF_DIR $COPTS_FILE_ARG "$@"
