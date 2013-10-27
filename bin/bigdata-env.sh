# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Set BIGDATA specific environment variables here.

# The only required environment variable is JAVA_HOME. All others are
# optional. When running a distributed configuration it is best to set
# JAVA_HOME in this file, so that it is correctly defined on remote nodes.
##

# The java implementation to use. Required.
export JAVA_HOME=/usr/java/latest

# Include customized options defined file.
if [ "$BIGDATA_COPTS_FILE" != "" ] ; then
  if [ -f $BIGDATA_COPTS_FILE ]; then
    . $BIGDATA_COPTS_FILE
  fi
fi

# Extra Java CLASSPATH elements. 
# Apps scripts may set their specified classpath, setting here appends it.
export BIGDATA_CLASSPATH=$BIGDATA_CLASSPATH

# The minimum and maximum size of heap to use, with unit.
# Apps scripts may set their specified heap size, setting here overwrites them.
# export BIGDATA_HEAP_MIN=128m
# export BIGDATA_HEAP_MAX=1024m

# Extra Java runtime options.
# Apps scripts may set their specified options, setting here appends it.
# export BIGDATA_OPTS="$BIGDATA_OPTS -ea -XX:+HeapDumpOnOutOfMemoryError"
export BIGDATA_OPTS="$BIGDATA_COPTS \
                    -XX:+UseParNewGC \
                    -XX:+UseConcMarkSweepGC \
                    -XX:+CMSParallelRemarkEnabled \
                    -XX:SurvivorRatio=8 \
                    -XX:MaxTenuringThreshold=1 \
                    -XX:CMSInitiatingOccupancyFraction=75 \
                    -XX:+UseCMSInitiatingOccupancyOnly \
                    -Djava.net.preferIPv4Stack=true"

# Extra ssh options. Empty by default.
# export BIGDATA_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=BIGDATA_CONF_DIR"

# Where log files are stored.
export BIGDATA_LOG_DIR=$BIGDATA_HOME/logs

# The directory where pid files are stored.
export BIGDATA_PID_DIR=$BIGDATA_HOME/pids

# File naming remote hosts.
export BIGDATA_NODES=$BIGDATA_HOME/conf/nodes

# Seconds to sleep between slave commands. Unset by default. This can be 
# useful in large clusters.
export BIGDATA_NODE_SLEEP=0.1

# A string representing this instance of BIGDATA.
export BIGDATA_IDENT_STRING=$USER

# The scheduling priority for daemon processes. See 'man nice'.
export BIGDATA_NICENESS=0
