#ifndef _DASTOR_CRYPT_H_
#define _DASTOR_CRYPT_H_

#include <string>
#include "rsa.h"
#include "aes.h"

using namespace std;
using namespace CryptoPP;

class DaStorCrypt
{
public:
	DaStorCrypt(string srcDir, string dstDir, string key);

	void work();

private:
	string srcDir;
	string dstDir;
	string key;
	byte   keyData[AES::DEFAULT_KEYLENGTH];

	void crypt(string srcDir, string dstDir);
};


#endif

