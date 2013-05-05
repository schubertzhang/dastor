#include "DaStorCrypt.h"

#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h> 
#include <stdlib.h>
#include <time.h>

#include "rsa.h"
#include "osrng.h"
#include "files.h"
#include "base64.h"
#include "modes.h"

using namespace std;
using namespace CryptoPP;

DaStorCrypt::DaStorCrypt(string srcDir, string dstDir, string key)
{
    this->srcDir = srcDir;
    this->dstDir = dstDir;
    this->key = key;
    FILE *fp = fopen(key.c_str(), "r+");
    fread(keyData, 1, sizeof(keyData), fp);
    fclose(fp);
}

void DaStorCrypt::work()
{
    crypt(srcDir, dstDir);
}

void DaStorCrypt::crypt(string srcDir, string dstDir)
{
    cout << "open srcDir " << srcDir << endl;
    DIR* dir=opendir(srcDir.c_str());
    
    struct dirent* entry;
    while((entry=readdir(dir)))
    {
        if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0)
            continue;
        string fullname = srcDir+"/"+entry->d_name;
        struct stat statbuf;
        stat(fullname.c_str(), &statbuf);
        if (S_ISDIR(statbuf.st_mode))
        {
            cout << fullname << endl;
            string command="mkdir -p "+dstDir+"/"+entry->d_name;
            system(command.c_str());
            cout << command << endl;
            crypt(fullname, dstDir+"/"+entry->d_name);
        }
        else
        {
            cout << fullname << endl;

            string dstFile = dstDir+"/"+entry->d_name;

            ECB_Mode< AES >::Encryption e;
            // CBC Mode does not use an IV
            e.SetKey( keyData, sizeof(keyData) );
            
            FileSource( fullname.c_str(), true, 
                new StreamTransformationFilter( e,
                    new FileSink( dstFile.c_str() )
                ) // StreamTransformationFilter      
            ); // FileSource

            cout << "crypt " << fullname << " to " << dstFile << endl;
        }
    }
    closedir(dir);
}

int main(char* argc, char** argv)
{
    string src=argv[1]; 
    string dst=argv[2];
    string publicKey=argv[3];
    DaStorCrypt crypt(src, dst, publicKey);
    crypt.work();
}


