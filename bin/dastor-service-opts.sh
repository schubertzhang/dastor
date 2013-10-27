#!/usr/bin/env bash

# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# Define the dastor-service specified BIGDATA_OPTS here.


# ----------------------------------------------------------------------
# HEAP size definition.
# ----------------------------------------------------------------------
export BIGDATA_HEAP_MIN=1024m
export BIGDATA_HEAP_MAX=1024m

# ----------------------------------------------------------------------
# Build a dastor-specified BIGDATA_COPTS. (eg. -ea for assert)
#   - JMX options
# ----------------------------------------------------------------------
export BIGDATA_COPTS="-Dcom.sun.management.jmxremote.port=8081 \
                      -Dcom.sun.management.jmxremote.ssl=false \
                      -Dcom.sun.management.jmxremote.authenticate=false"

# ----------------------------------------------------------------------
# Build a dastor-specified BIGDATA_COPTS.
#   - ThreadPriorities options
#     (1)enable thread priorities, primarily so we can give periodic tasks
#        a lower priority to avoid interfering with client workload
#     (2)allows lowering thread priority without being root. see
# http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workaround.html
# ----------------------------------------------------------------------
export BIGDATA_COPTS="$BIGDATA_COPTS \
                      -XX:+UseThreadPriorities \
                      -XX:ThreadPriorityPolicy=42 \
                      -Dbigdata.dastor.deputytransfer.priority=1 \
                      -Dbigdata.dastor.compaction.priority=2"

# ----------------------------------------------------------------------
# javaagent for JMX --> Ganglia
# ----------------------------------------------------------------------
if [ -f "$BIGDATA_CONF_DIR/jmxetric.xml" ]; then
  # jmxetric jar path
  JMXETRIC_JARPATH=$BIGDATA_HOME/lib/jmxetric-0.0.4.jar
    
  # jmxetric options
  JMXETRIC_CONF="config=$BIGDATA_CONF_DIR/jmxetric.xml"
  
  # javaagent
  JAVAAGENT="-javaagent:$JMXETRIC_JARPATH=$JMXETRIC_CONF"
  
  # append BIGDATA_COPTS
  if [ "x${JAVAAGENT}" != "x" ]; then
    export BIGDATA_COPTS="$BIGDATA_COPTS $JAVAAGENT"
  fi
fi
