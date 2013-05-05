#ifndef _DASTOR_LICENSE_MESSAGE_H_
#define _DASTOR_LICENSE_MESSAGE_H_

#define CLASSLOADER_TYPE_DASTOR                 1
#define CLASSLOADER_TYPE_TOMCAT                 2

typedef struct {
    int versionNum;
    int type;
    unsigned char sig[1024];
    char data[1024];
} DASTORE_LICENSE_REQ;

typedef struct {
    int versionNum;
    int sig_ok;
    char data[1024];
} DASTORE_LICENSE_RESP;

#endif

