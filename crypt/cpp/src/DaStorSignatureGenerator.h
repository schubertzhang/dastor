#ifndef _DASTOR_SIGNATURE_GENERATOR_H_
#define _DASTOR_SIGNATURE_GENERATOR_H_

#include <string>

using namespace std;

class DaStorSignatureGenerator
{
public:
    DaStorSignatureGenerator(string dastorFile, string tomcatFile, string cppFile);
    void work();
private:
    void loadSignature(string file, unsigned char * sig);
    string dastorFile;
    string tomcatFile;
    string cppFile;
};

#endif


