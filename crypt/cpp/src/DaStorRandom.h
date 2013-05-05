#ifndef _DASTOR_RANDOM_H_
#define _DASTOR_RANDOM_H_

class DaStorRandom
{
public:
    static DaStorRandom * getInstance();
    unsigned short randomArray[1024][16];
private:
    DaStorRandom();
    static DaStorRandom * instance;
};

#endif
