#include "DaStorLicenseServer.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>
#include <string.h>

#include "DaStorAppKey.h"
#include "DaStorRandom.h"
#include "DaStorSignature.h"
#include "DaStorLicenseMessage.h"

DaStorLicenseServer::DaStorLicenseServer(unsigned short port)
{
    this->port = port;
}

void DaStorLicenseServer::work()
{
    open();
    run();
}

void DaStorLicenseServer::open()
{
    socketFd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketFd < 0)
    {
        printf("in DaStorLicenseServer::open(), socket() error(%s).", strerror(errno));
        exit(-1);
    }
//    printf("create socket OK.\n");

    int nOptVal = 1;
    if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &nOptVal, sizeof(nOptVal)))
    {
        printf("in DaStorLicenseServer::open(), setsockopt() error(%s).\n", strerror(errno));
        exit(-1);
    }
//    printf("set sockopt OK.\n");

    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);
    if (bind(socketFd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0)
    {
        printf("in DaStorLicenseServer::open(), bind() error(%s).\n", strerror(errno));
        exit(-1);
    }
//    printf("bind port OK.\n");
}

void DaStorLicenseServer::run()
{
    int n;
    socklen_t len;
    struct sockaddr_in cliaddr;
    DASTORE_LICENSE_REQ msg;
    int msgLen = sizeof(DASTORE_LICENSE_REQ);
    printf("begin listening...\n");
    for ( ; ; )
    {
        len = sizeof(cliaddr);
        n = recvfrom(socketFd, &msg, msgLen, 0, (struct sockaddr*)&cliaddr, &len);
//        printf("recv message, length = %d\n", n);
        if (n != -1)
        {
            if (n == msgLen)
            {
                DASTORE_LICENSE_RESP resp;
                procMsg(msg, resp);
//                memcpy(resp.key, DaStorAppKey::getInstance()->getKey(), sizeof(resp.key));
                if (sendto(socketFd, &resp, sizeof(DASTORE_LICENSE_RESP), 0, (struct sockaddr*)&cliaddr, sizeof(cliaddr)) == -1)
                {
                    printf("in DaStorLicenseServer::run(),  send resp error.\n");
                }
                else
                {
                    printf("in DaStorLicenseServer::run(),  send resp ok.\n");
                }
            }
            else
            {
                printf("in DaStorLicenseServer::run(),  msgLen is bad.\n");
            }
        }
        else
        {
            printf("in DaStorLicenseServer::run(),  recv message error.\n" );
            continue;
        }
    }
}

void DaStorLicenseServer::procMsg(DASTORE_LICENSE_REQ &req, DASTORE_LICENSE_RESP &resp)
{
    // parse type
    int classLoader = req.type / 1024;
    int sigIndex = req.type % 1024;
//    printf("classLoader = %d\n", classLoader);
//    printf("sigIndex = %d\n", sigIndex);

    unsigned char sig[16];
    for (int i=0; i<16; i++)
    {
        sig[i] = req.sig[DaStorRandom::getInstance()->randomArray[sigIndex][i]];
    }
//    printf("sig = %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n",
//        sig[0],sig[1],sig[2],sig[3],sig[4],sig[5],sig[6],sig[7],
//        sig[8],sig[9],sig[10],sig[11],sig[12],sig[13],sig[14],sig[15]);
  
    if (verifySignature(classLoader, sig))
        resp.sig_ok = 1;
    else
        resp.sig_ok = 0;

//    printf("verify sig = %d\n", resp.sig_ok);
    if (!resp.sig_ok)
        return;

//    printf("continue set key...\n");
    
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

    srand((int)time(0));
    
    for (int i=0; i < sizeof(resp.data)/sizeof(int); i++)
    {
        int random = rand();
        memcpy(resp.data+i*sizeof(int), &random, sizeof(int));
    }

    for (int i=0; i<16; i++)
    {
        resp.data[DaStorRandom::getInstance()->randomArray[randomIndex][i]] = DaStorAppKey::getInstance()->getKey()[i];
//        printf("copy %d to num %d.\n", *(DaStorAppKey::getInstance()->getKey()+i), DaStorRandom::getInstance()->randomArray[randomIndex][i]);
    }

}

bool DaStorLicenseServer::verifySignature(int type, unsigned char * sig)
{
    if (type == CLASSLOADER_TYPE_DASTOR)
    {
        bool success = true;
        for (int i=0; i<16; i++)
        {
            if (sig[i] != DaStorSignature::getInstance()->sig_dastor[i])
            {
                printf("error dastor loader.\n");
                success = false;
                break;
            }
        }
        return success;
    }
    else if (type == CLASSLOADER_TYPE_TOMCAT)
    {
        bool success = true;
        for (int i=0; i<16; i++)
        {
            if (sig[i] != DaStorSignature::getInstance()->sig_tomcat[i])
            {
                printf("error dastor loader.\n");
                success = false;
                break;
            }
        }
        return success;
    }
    else
    {
        printf("unknown class loader...\n");
        return false;
    }
}

int main(int argc, char** argv)
{
    DaStorLicenseServer server(12000);
    server.work();
}
