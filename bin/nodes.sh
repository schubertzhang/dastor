#!/usr/bin/env bash

# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Run a shell command on all all hosts defined in $BIGDATA_HOME/conf/nodes.
#
# Environment Variables
#
#   BIGDATA_NODES       File naming remote hosts.
#                       Default null, $HOSTLIST use $BIGDATA_CONF_DIR/nodes.
#                       See bigdata-env.sh. Can define in command, 
#                       see bigdata-config.sh.
#   BIGDATA_CONF_DIR    Alternate conf dir. Default is $BIGDATA_HOME/conf.
#                       Defined in bigdata-config.sh.
#   BIGDATA_NODE_SLEEP  Seconds to sleep between spawning remote commands.
#                       Default null, see bigdata-env.sh.
#   BIGDATA_SSH_OPTS    Options passed to ssh when running remote commands.
#                       Default null, see bigdata-env.sh.
##

usage="Usage: nodes.sh [--config <confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bigdata-config.sh

# If the nodes file is specified in the command line,
# then it takes precedence over the definition in 
# bigdata-env.sh. Save it here.
HOSTLIST=$BIGDATA_NODES

if [ -f "$bin"/bigdata-env.sh ]; then
  . "$bin"/bigdata-env.sh
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$BIGDATA_NODES" = "" ]; then
    export HOSTLIST="$BIGDATA_CONF_DIR/nodes"
  else
    export HOSTLIST="$BIGDATA_NODES"
  fi
fi

for node in `cat "$HOSTLIST"`; do
 ssh $BIGDATA_SSH_OPTS $node $"${@// /\\ }" \
   2>&1 | sed "s/^/$node: /" &
 if [ "$BIGDATA_NODE_SLEEP" != "" ]; then
   sleep $BIGDATA_NODE_SLEEP
 fi
done

wait
