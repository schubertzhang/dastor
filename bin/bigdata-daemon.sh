#!/usr/bin/env bash

# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Runs a BIGDATA command as a daemon.
#
# Environment Variables
#
#   BIGDATA_CONF_DIR     Alternate conf dir. Default is $BIGDATA_HOME/conf.
#                        Defined in bigdata-config.sh.
#   BIGDATA_LOG_DIR      Where log files are stored.
#                        Defined here.See bigdata-env.sh.
#   BIGDATA_PID_DIR      The pid files are stored. $BIGDATA_HOME/pids by default.
#                        Defined here. See bigdata-env.sh.
#   BIGDATA_IDENT_STRING A string representing this instance of bigdata. $USER by default
#                        Defined here. See bigdata-env.sh.
#   BIGDATA_NICENESS     The scheduling priority for daemons. Defaults to 0.
#                        Defined here. See bigdata-env.sh.
##

usage="Usage: bigdata-daemon.sh [--config <confdir>] [--nodes <nodesfilename>] [--copts <coptsfilepath>] (start|stop) <command> <args...>"

# if no args specified, show usage.
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bigdata-config.sh

if [ "$BIGDATA_COPTS_FILE" != "" ] ; then
  COPTS_FILE_ARG="--copts $BIGDATA_COPTS_FILE"
fi

# local env-setting.
if [ -f "$bin"/bigdata-env.sh ]; then
  . "$bin"/bigdata-env.sh
fi

# get arguments.
startStop=$1
shift
command=$1
shift

bigdata_rotate_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
    num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
      prev=`expr $num - 1`
      [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
      num=$prev
    done
    mv "$log" "$log.$num";
  fi
}

# get log directory.
if [ "$BIGDATA_LOG_DIR" = "" ]; then
  export BIGDATA_LOG_DIR="$BIGDATA_HOME/logs"
fi
mkdir -p "$BIGDATA_LOG_DIR"

if [ "$BIGDATA_PID_DIR" = "" ]; then
  export BIGDATA_PID_DIR=$BIGDATA_HOME/pids
fi

if [ "${BIGDATA_IDENT_STRING}" = "" ]; then
  export BIGDATA_IDENT_STRING="$USER"
fi

# some variables
export BIGDATA_LOGFILE=bigdata-$BIGDATA_IDENT_STRING-$command-$HOSTNAME.log
export BIGDATA_ROOT_LOGGER="INFO,DRFA"
out=$BIGDATA_LOG_DIR/bigdata-$BIGDATA_IDENT_STRING-$command-$HOSTNAME.out
pid=$BIGDATA_PID_DIR/bigdata-$BIGDATA_IDENT_STRING-$command.pid
export BIGDATA_PIDFILE=$pid

# Set default scheduling priority
if [ "$BIGDATA_NICENESS" = "" ]; then
  export BIGDATA_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$BIGDATA_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`. Stop it first.
        exit 1
      fi
    fi

    bigdata_rotate_log $out
    echo starting $command, outputting to $out, logging to $BIGDATA_LOGFILE
    
    cd "$BIGDATA_HOME"
    nohup nice -n $BIGDATA_NICENESS "$BIGDATA_HOME"/bin/bigdata --config $BIGDATA_CONF_DIR $COPTS_FILE_ARG $command "$@" > "$out" 2>&1 < /dev/null &
    # "$BIGDATA_HOME"/bin/bigdata --config $BIGDATA_CONF_DIR $COPTS_FILE_ARG $command "$@" &
    echo $! > $pid
    sleep 1; head "$out"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
