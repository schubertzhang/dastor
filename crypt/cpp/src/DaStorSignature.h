#ifndef _DASTOR_SIGNATURE_H_
#define _DASTOR_SIGNATURE_H_

#include <stdio.h>

class DaStorSignature
{
public:
    static DaStorSignature * getInstance()
    {
        if (NULL == instance)
        {
            instance = new DaStorSignature();
        }
        return instance;
    }
    unsigned char sig_dastor[16];
    unsigned char sig_tomcat[16];
private:
    DaStorSignature()
    {
        init();
    }
    void init();
    static DaStorSignature * instance;
};

#endif

