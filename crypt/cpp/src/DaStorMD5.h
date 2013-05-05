#ifndef _DASTOR_MD5_H_
#define _DASTOR_MD5_H_

#include <sys/types.h> 

//typedef int int32_t;
typedef unsigned int uint32_t;

//typedef short int16_t;
typedef unsigned short uint16_t;

//typedef char int8_t;
typedef unsigned char uint8_t;


// compute MD4

typedef struct {
    unsigned state[4];
    unsigned count[2];
    uint8_t buffer[64];
} md4_context;

void md4_init(md4_context *context);
void md4_update(md4_context *context, uint8_t *input, uint32_t inputLen);
void md4_final(md4_context *context, uint8_t *digest);


// compute MD4

typedef struct {
    uint32_t state[4];
    uint32_t count[2];
    uint8_t buffer[64];
} md5_context;

void md5_init(md5_context *context);
void md5_update(md5_context *context, uint8_t *input, uint32_t inputLen);
void md5_final(md5_context *context, uint8_t *digest);

#endif
