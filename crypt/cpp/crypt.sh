CURRENT_PATH=`pwd`
cd $1
TARGET_PATH=`pwd`
cd $CURRENT_PATH
echo "CURRENT_PATH : "$CURRENT_PATH
echo "TARGET_PATH : "$TARGET_PATH

srcdir=`dirname $0`
cd $srcdir
srcdir=$PWD

CRPYT_HOME=$PWD

echo "CRPYT_HOME : "$CRPYT_HOME

LIBCRYPT=""

if [ 0 -eq `uname -a|grep x86_64|wc -l` ]; then
  LIBCRYPT=$CRPYT_HOME/lib/32/libcryptopp.a
else
  LIBCRYPT=$CRPYT_HOME/lib/64/libcryptopp.a
fi

echo "=========================crypt for dir "$1"========================"

echo "========================generate key file, cpp====================="
mkdir -p gen
$CRPYT_HOME/bin/gen $CRPYT_HOME/gen/ase.key $CRPYT_HOME/gen/genkey.cpp

rm -rf "$CRPYT_HOME/data_crypt"
mkdir -p $CRPYT_HOME/data_crypt
chmod +x $CRPYT_HOME/data_crypt

echo "=====================crypt " $TARGET_PATH "/classes================"
$CRPYT_HOME/bin/crypt $TARGET_PATH/classes $CRPYT_HOME/data_crypt $CRPYT_HOME/gen/ase.key

echo "==============crypt "$TARGET_PATH"/interface/classes==============="
$CRPYT_HOME/bin/crypt $TARGET_PATH/interface/classes $CRPYT_HOME/data_crypt $CRPYT_HOME/gen/ase.key

echo "========================archive data_crypt========================="
rm -f $CRPYT_HOME/gen/dastor.dar
$CRPYT_HOME/bin/archive $CRPYT_HOME/data_crypt $CRPYT_HOME/gen/dastor.dar $CRPYT_HOME/gen/ase.key

echo "=========================remove data_crypt========================="
rm -rf $CRPYT_HOME/data_crypt

echo "=====================gen classloader signature====================="
ant -f $CRPYT_HOME/../java/build.xml compile
DASTOR_LOADER=$CRPYT_HOME/../java/classes/com/bigdata/dastor/release/DastorClassLoader.class
unzip $CRPYT_HOME/../java/lib/apache-dastor.jar -d $CRPYT_HOME/../java/lib/tmp
TOMCAT_LOADER=$CRPYT_HOME/../java/lib/tmp/org/apache/catalina/loader/WebappClassLoader.class
$CRPYT_HOME/bin/sig_gen $DASTOR_LOADER $TOMCAT_LOADER $CRPYT_HOME/gen/gensig.cpp
rm -rf $CRPYT_HOME/../java/lib/tmp

echo "==============generate license server==============="
g++ -o $CRPYT_HOME/gen/licensesvr $CRPYT_HOME/src/DaStorLicenseServer.cpp $CRPYT_HOME/src/DaStorRandom.cpp $CRPYT_HOME/gen/genkey.cpp $CRPYT_HOME/gen/gensig.cpp $LIBCRYPT -I$CRPYT_HOME/src -I$CRPYT_HOME/include

