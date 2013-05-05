#include "com_bigdata_dastor_release_DastorCryptor.h"

#include <map>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h> 
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>

#include "DaStorLicenseMessage.h"
#include "DaStorRandom.h"
#include "DaStorArchive.h"
#include "DaStorMD5.h"
#include "rsa.h"
#include "osrng.h"
#include "files.h"
#include "base64.h"
#include "modes.h"

using namespace std;
using namespace CryptoPP;

map<string, DaStorArchiveEntry> classMap;

byte keyData[AES::DEFAULT_KEYLENGTH];

void getLicense(int ip, short port, unsigned char *sig, int loaderType)
{
//    cout << "begin get license." << endl;

    memset(keyData, 0, 16);

    // init udp socket
    int socketFd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketFd < 0)
    {
        printf("socket() error(%s).", strerror(errno));
        exit(-1);
    }
//    printf("socketFd = %d\n", socketFd);

    int nOptVal = 1;
    if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &nOptVal, sizeof(nOptVal)))
    {
        printf("setsockopt() error(%s).\n", strerror(errno));
        exit(-1);
    }

    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(0);
    if (bind(socketFd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0)
    {
        printf("bind() error(%s).\n", strerror(errno));
        exit(-1);
    }

//    printf("senging request...\n");
    // send message
    DASTORE_LICENSE_REQ req;

    srand((int)time(0));

    // version num
    req.versionNum = rand();

    int sigIndex = rand();
    sigIndex %= 1024;

    // type = loaderType*1024 + sigIndex;
    req.type = loaderType*1024 + sigIndex;

    // signature
    for (int i=0; i < sizeof(req.sig)/sizeof(int); i++)
    {
        int random = rand();
        memcpy(req.sig+i*sizeof(int), &random, sizeof(int));
    }

//    cout << "sig Index = " << sigIndex << endl;

    for (int i=0; i<16; i++)
    {
        req.sig[DaStorRandom::getInstance()->randomArray[sigIndex][i]] = sig[i];
    }

    // data;
    for (int i=0; i < sizeof(req.data)/sizeof(int); i++)
    {
        int random = rand();
        memcpy(req.data+i*sizeof(int), &random, sizeof(int));
    }

//    printf("gen random buffer.\n");
    
    // get random index request data
    int randomIndex = 0; 

    for (int i=0; i<16; i++)
    {
        int num = DaStorRandom::getInstance()->randomArray[539][i];
        randomIndex += req.data[num];
    }

    randomIndex %= 1024;
    randomIndex = (randomIndex < 0)?(-randomIndex):randomIndex;

//    printf("request index = %d\n", randomIndex);

    struct sockaddr_in liceneaddr;
    bzero(&liceneaddr, sizeof(servaddr));
    liceneaddr.sin_family = AF_INET;
    liceneaddr.sin_addr.s_addr = ip;
    liceneaddr.sin_port = htons(port);
    
    if (sendto(socketFd, &req, sizeof(DASTORE_LICENSE_REQ), 0, (struct sockaddr*)&liceneaddr, sizeof(liceneaddr)) == -1)
    {
        printf("send req error.\n");
        exit(-1);
    }

    //printf("recving response...\n");
    // receive message
    DASTORE_LICENSE_RESP resp;
    int msgLen = sizeof(DASTORE_LICENSE_RESP);
    socklen_t len = sizeof(liceneaddr);
    int n = recvfrom(socketFd, &resp, msgLen, 0, (struct sockaddr*)&liceneaddr, &len);
    //printf("message length = %d\n",len);
    if (n != msgLen)
    {
        printf("message length error.\n");
        exit(-1);
    }

//    printf("resp.sig_ok = %d\n",resp.sig_ok);
    if (!resp.sig_ok)
    {
        printf("verify classloader error, exit!\n");
        exit(-1);
    }

//    cout << "verify classloader OK!" << endl;

    for (int i=0; i<16; i++)
    {
        keyData[i] = resp.data[DaStorRandom::getInstance()->randomArray[randomIndex][i]];
    }
    
//    printf("load license OK.\n");
}

void computeLoaderMd5(JNIEnv * env, jobject &loader, unsigned char *md5, int *loaderType)
{
    /*
        ClassLoader.class.getProtectionDomain()
            .getCodeSource().getLocation().getFile();
    */
    jclass ClassLoader_Class = env->GetObjectClass(loader);
    jclass Class_Class = env->GetObjectClass(ClassLoader_Class);
    jmethodID GetProtectionDomain_Method = env->GetMethodID( Class_Class, "getProtectionDomain", "()Ljava/security/ProtectionDomain;"); 
    jobject ProtectionDomain_Object = env->CallObjectMethod(ClassLoader_Class, GetProtectionDomain_Method);
    jclass ProtectionDomain_Class = env->GetObjectClass(ProtectionDomain_Object);
    jmethodID GetCodeSource_Method = env->GetMethodID(ProtectionDomain_Class, "getCodeSource", "()Ljava/security/CodeSource;");
    jobject CodeSource_Object = env->CallObjectMethod(ProtectionDomain_Object, GetCodeSource_Method);
    jclass CodeSource_Class = env->GetObjectClass(CodeSource_Object);
    jmethodID GetLocation_Method = env->GetMethodID(CodeSource_Class, "getLocation", "()Ljava/net/URL;");
    jobject URL_Object = env->CallObjectMethod(CodeSource_Object, GetLocation_Method);
    jclass URL_Class = env->GetObjectClass(URL_Object);
    jmethodID GetFile_Method = env->GetMethodID(URL_Class, "getFile", "()Ljava/lang/String;");
    jstring file_path = (jstring)env->CallObjectMethod(URL_Object, GetFile_Method);

    
    const char *st_file_path = 
            (const char *)env->GetStringUTFChars( file_path, JNI_FALSE );
//    cout << "jar file : " << string(st_file_path) << endl;

    /*
        ClassLoader.class.getName();
    */
    jmethodID GetName_Method = env->GetMethodID(Class_Class, "getName", "()Ljava/lang/String;");
    jstring loader_name = (jstring)env->CallObjectMethod(ClassLoader_Class, GetName_Method);

    const char *st_loader_name = 
            (const char *)env->GetStringUTFChars( loader_name, JNI_FALSE );

    string loaderName(st_loader_name);
//    cout << "Name : " << loaderName << endl;
    if (loaderName == string("com.bigdata.dastor.release.DastorClassLoader"))
    {
        *loaderType = CLASSLOADER_TYPE_DASTOR;
    }
    else if (loaderName == string("org.apache.catalina.loader.WebappClassLoader"))
    {
        *loaderType = CLASSLOADER_TYPE_TOMCAT;
    }
    else
    {
        cout << "Error Class Loader! exit!" << endl;
        exit(-1);
    }

    /*
        StringBuffer sb = new StringBuffer();
        String fullEntryName = loader_name.replace(".", "/");
        sb.append(fullEntryName).append(".class");
        String fullEntryName = sb.toString();
    */
    jclass String_Class = env->FindClass("java/lang/String");
    jmethodID Replace_Method = env->GetMethodID(String_Class, "replace", "(CC)Ljava/lang/String;");
    jstring entry_name = (jstring)env->CallObjectMethod(loader_name, Replace_Method,(jchar)'.',(jchar)'/');
    jstring class_constant = env->NewStringUTF(".class");
    jclass StringBuffer_Class = env->FindClass("java/lang/StringBuffer");
    jmethodID StringBuffer_init_Method = env->GetMethodID(StringBuffer_Class, "<init>", "()V");
    jobject StringBuffer_Object = env->NewObject(StringBuffer_Class, StringBuffer_init_Method);
    jmethodID Append_Method = env->GetMethodID(StringBuffer_Class, "append", "(Ljava/lang/String;)Ljava/lang/StringBuffer;");
    jmethodID ToString_Method = env->GetMethodID(StringBuffer_Class, "toString", "()Ljava/lang/String;");
    env->CallObjectMethod(StringBuffer_Object, Append_Method, entry_name);
    env->CallObjectMethod(StringBuffer_Object, Append_Method, class_constant);
    jstring final_entry_name = (jstring)env->CallObjectMethod(StringBuffer_Object, ToString_Method);

    /*
        JarFile jarFile = new JarFile(file_path);
        InputStream input = jarFile.getInputStream(
            jarFile.getJarEntry("final_entry_name"));
        int len = input.available();
        byte buf[] = new byte[4096];
        ...
        int readSize;
        while ( (readSize=input.read(b))>0){
            ...
        }
    */
    jclass JarFile_Class = env->FindClass("java/util/jar/JarFile");
    jmethodID JarFile_init_METHOD = env->GetMethodID(JarFile_Class, "<init>", "(Ljava/lang/String;)V");
    jobject JarFile_Object = env->NewObject(JarFile_Class, JarFile_init_METHOD, file_path);
    jmethodID JarFile_GetJarEntry_Method = env->GetMethodID(JarFile_Class, "getJarEntry", "(Ljava/lang/String;)Ljava/util/jar/JarEntry;");
    jmethodID JarFile_GetInputStream_Method = env->GetMethodID(JarFile_Class, "getInputStream", "(Ljava/util/zip/ZipEntry;)Ljava/io/InputStream;");
    jobject JarEntry_Object = env->CallObjectMethod(JarFile_Object, JarFile_GetJarEntry_Method, final_entry_name);
    jobject JarInputStream_Object = env->CallObjectMethod(JarFile_Object, JarFile_GetInputStream_Method, JarEntry_Object);
    jclass JarInputStream_Class = env->GetObjectClass(JarInputStream_Object);
    jmethodID JarInputStream_Available_Method = env->GetMethodID(JarInputStream_Class, "available", "()I");
    jmethodID JarInputStream_Read_Method = env->GetMethodID(JarInputStream_Class, "read", "([B)I");
    jint len = env->CallIntMethod(JarInputStream_Object, JarInputStream_Available_Method);
    jbyteArray buf = env->NewByteArray((jsize)4096);

    // compute md5 of classloader
    uint8_t t[16];
    memset(t, 0, 16);
    md5_context context;
    md5_init(&context);
    char *loaderData = (char *)malloc(4096);

    jint readSize = 0;
    while ((readSize = env->CallIntMethod(JarInputStream_Object, JarInputStream_Read_Method, buf)) > 0)
    {
        env->GetByteArrayRegion(buf, 0, readSize, (jbyte*)loaderData);
        md5_update(&context, (uint8_t*)loaderData, readSize);
    }

    md5_final(&context, t);
    memcpy(md5, t, 16);
    free(loaderData);
}



/*
 * Class:     com_bigdata_dastor_release_DastorCryptor
 * Method:    init
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_bigdata_dastor_release_DastorCryptor_init
  (JNIEnv * env, jclass arg, jstring file, jstring serverIp, jobject loader)
{
//    cout << "Java_com_dastor_release_DastorCryptor_init" << endl;
    unsigned char md5[16];
    int loaderType;
    computeLoaderMd5(env, loader, md5, &loaderType);

//    char md5String[36];
//    sprintf(md5String, 
//        "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", 
//        md5[0], md5[1], md5[2], md5[3], md5[4], md5[5], md5[6], md5[7], 
//        md5[8], md5[9], md5[10], md5[11], md5[12], md5[13], md5[14], md5[15]);
//    md5String[32] = '\0';
//    printf("out %s\n", md5String);

    const char *st_file = 
            (const char *)env->GetStringUTFChars( file, JNI_FALSE );
    string archive(st_file);

    const char *st_ip = 
            (const char *)env->GetStringUTFChars( serverIp, JNI_FALSE );

//    cout << "loading key" << endl;
    
    int ip = inet_addr(st_ip);
    short port = 12000;

    getLicense(ip, port, md5, loaderType);

//    for (int i=0; i<AES::DEFAULT_KEYLENGTH; i++)
//    {
//        printf("%d\n",keyData[i]);
//    }

//    cout << "finish load key." << endl;

//    cout << "begin read " << archive << endl;
        
    DaStorArchiveReader reader(archive, (byte*)keyData);
    while (reader.hasNext()) 
    {
        DaStorArchiveEntry entry;
        reader.next(entry);

//        cout << entry.name << endl;

        classMap.insert(pair<string, DaStorArchiveEntry>(entry.name, entry) );

    }

    
    env->ReleaseStringUTFChars( file, (const char *)st_file);
    env->ReleaseStringUTFChars( serverIp, (const char *)st_ip);

    return 0;
}

/*
 * Class:     com_bigdata_dastor_release_DastorCryptor
 * Method:    getClassData
 * Signature: (Ljava/lang/String;Ljava/lang/ClassLoader;Ljava/lang/ClassLoader;)Ljava/lang/Class;
 */
JNIEXPORT jclass JNICALL Java_com_bigdata_dastor_release_DastorCryptor_getClassData
  (JNIEnv * env, jclass agr, jstring className, jobject loader1, jobject loader2)
{
    const char *st_className = 
            (const char *)env->GetStringUTFChars( className, JNI_FALSE );
    string m_className(st_className);

    DaStorArchiveEntry entry = classMap[m_className+".class"];
    
    // save crypted file
    FILE *crypted = fopen("/tmp/crypted", "w+");
    fwrite(entry.data, 1, entry.dataLength, crypted);
    fclose(crypted);
    
//    cout << "saved crypted file" << endl;
    
    // recover to recovered file
    
    ECB_Mode< AES >::Decryption d;
    d.SetKey( keyData, sizeof(keyData));
    
    // The StreamTransformationFilter removes
    //  padding as required.
    FileSource s("/tmp/crypted", true, 
        new StreamTransformationFilter( d,
            new FileSink( "/tmp/recovered" )
        ) // StreamTransformationFilter
    ); // FileSource
    
//    cout << "save recovered file" << endl;
    
    struct stat statbuf;
    stat("/tmp/recovered", &statbuf);
    
    char * classData = (char*)malloc(statbuf.st_size);
    FILE *recovered = fopen("/tmp/recovered", "r+");
    fread(classData, 1, statbuf.st_size, recovered);
    fclose(recovered);

    string command = "rm -f /tmp/crypted /tmp/recovered";
    system(command.c_str());
    
//    cout << "load recovered file into mem" << endl;
        
    jbyteArray bArray = env->NewByteArray(statbuf.st_size);
    env->SetByteArrayRegion(bArray, 0, statbuf.st_size, (jbyte*)classData);
    free(classData);

    jclass ClassLoader_Class = env->GetObjectClass(loader1);

    jmethodID DefineClass_Method = env->GetMethodID( ClassLoader_Class, "defineClass", "(Ljava/lang/String;[BII)Ljava/lang/Class;"); 

    if (DefineClass_Method == NULL) {
        cout << "get method error!" << endl;
        return NULL;
    } else {
        //cout << "found the method! " << m_className << endl;
    }

    jobject Class_Return = env->CallObjectMethod(loader1, DefineClass_Method, className, bArray, 0, statbuf.st_size);

    env->ReleaseStringUTFChars( className, (const char *)st_className);

    return (jclass)Class_Return;
}


