#ifndef _DASTOR_APP_KEY_H_
#define _DASTOR_APP_KEY_H_

#include <string>
#include "aes.h"

using namespace std;
using namespace CryptoPP;

class DaStorAppKey
{
public:
    static DaStorAppKey * getInstance()
    {
        if (NULL == instance)
        {
            instance = new DaStorAppKey();
        }
        return instance;
    }

    byte* getKey()
    {
        return dastor_app_key;
    }

private:
    DaStorAppKey()
    {
        init();
    }
    void init();
    byte dastor_app_key[AES::DEFAULT_KEYLENGTH];
    static DaStorAppKey * instance;
};

#endif

