# *************************************************************************
# Copyright (c) 2009~ , BIGDATA. All Rights Reserved.
# *************************************************************************

# BIGDATA runtime entironment setup script.
#
# Included in all the BIGDATA scripts with source command should not be 
# executable directly, also should not be passed any arguments, since 
# we need original $*

# Following variables will be defined here:
#
#   BIGDATA_HOME        The home dir of the BIGDATA installation.
#   BIGDATA_CONF_DIR    The conf dir of the BIGDATA installation.
#   BIGDATA_NODES       The file path of distributed nodes in cluster.
#   BIGDATA_COPTS_FILE  The file path defines customized options. 
##

# resolve links - $0 may be a softlink.
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path.
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the BIGDATA installation.
export BIGDATA_HOME=`dirname "$this"`/..

# check to see if the conf dir is given as an optional argument.
if [ $# -gt 1 ]
then
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    BIGDATA_CONF_DIR=$confdir
  fi
fi
 
# allow alternate conf dir location.
export BIGDATA_CONF_DIR="${BIGDATA_CONF_DIR:-$BIGDATA_HOME/conf}"

# check to see it is specified whether to use the nodes file or only local.
if [ $# -gt 1 ]
then
  if [ "--nodes" = "$1" ]
  then
    shift
    nodesfile=$1
    shift
    export BIGDATA_NODES="$BIGDATA_CONF_DIR/$nodesfile"
  fi
fi

# check to see it is specified whether to use the customized options defined file.
if [ $# -gt 1 ]
then
  if [ "--copts" = "$1" ]
  then
    shift
    coptsfile=$1
    shift
    export BIGDATA_COPTS_FILE="$coptsfile"
  fi
fi
