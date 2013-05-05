#ifndef _DASTOR_LICENSE_SERVER_H_
#define _DASTOR_LICENSE_SERVER_H_

#include "DaStorLicenseMessage.h"

class DaStorLicenseServer
{
public:
    DaStorLicenseServer(unsigned short port);
    
    void work();

private:
    unsigned short port;
    int socketFd;

    void open();

    void run();

    void procMsg(DASTORE_LICENSE_REQ &req, DASTORE_LICENSE_RESP &resp);

    bool verifySignature(int type, unsigned char * sig);
};
#endif
