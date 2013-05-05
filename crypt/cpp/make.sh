
srcdir=`dirname $0`
cd $srcdir
srcdir=$PWD

CRPYT_HOME=$PWD

LIBCRYPT=""

if [ 0 -eq `uname -a|grep x86_64|wc -l` ]; then
  LIBCRYPT=$CRPYT_HOME/lib/32/libcryptopp.a
else
  LIBCRYPT=$CRPYT_HOME/lib/64/libcryptopp.a
fi

echo "make bin dir"
mkdir -p $CRPYT_HOME/bin

echo "build gen..."
g++ -o $CRPYT_HOME/bin/gen $CRPYT_HOME/src/DaStorKeyGenerator.cpp $LIBCRYPT -I$CRPYT_HOME/src -I$CRPYT_HOME/include

echo "build crypt..."
g++ -o $CRPYT_HOME/bin/crypt $CRPYT_HOME/src/DaStorCrypt.cpp $LIBCRYPT -I$CRPYT_HOME/src -I$CRPYT_HOME/include

echo "build archive..."
g++ -o $CRPYT_HOME/bin/archive $CRPYT_HOME/src/DaStorArchive.cpp $LIBCRYPT -I$CRPYT_HOME/src -I$CRPYT_HOME/include

echo "build sig_gen..."
g++ -o $CRPYT_HOME/bin/sig_gen $CRPYT_HOME/src/DaStorSignatureGenerator.cpp $CRPYT_HOME/src/DaStorMD5.cpp -Isrc

echo "build jni lib..."
mkdir -p gen
g++ -g -fPIC -shared -o $CRPYT_HOME/gen/libDastorRelease.so $CRPYT_HOME/src/DaStorMD5.cpp $CRPYT_HOME/src/DaStorArchive.cpp $CRPYT_HOME/src/DaStorRandom.cpp $CRPYT_HOME/src/com_bigdata_dastor_release_DastorCryptor.cpp $LIBCRYPT -I$CRPYT_HOME/src -I$CRPYT_HOME/include -I$JAVA_HOME/include -I$JAVA_HOME/include/linux

