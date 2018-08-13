#pragma once

#include <stdint.h>
// Interface for a generic key value database

namespace keyvaluedatabase
{

#if defined(_MSC_VER)
#define KVD_ABI __cdecl
#else
#define KVD_ABI
#endif

typedef void (KVD_ABI *KVD_existsCallback)(bool exists, void* userPtr);

class KeyValueDatabase
{
public:

	static KeyValueDatabase *create(void);

    // To be thread safe, this returns a freshly allocated *copy* of the record.
    // User must call 'releaseGetMem' when they are done with it.
    virtual void *get(const char *key, uint32_t &dataLen) = 0;
    virtual void releaseGetMem(void *mem) = 0;

    virtual bool exists(const char *key,void *userPointer,KVD_existsCallback callback) = 0;
    virtual bool isList(const char *key) = 0;
    virtual void set(const char *key, const void *data, uint32_t dataLen) = 0;

    // append to an existing or new record; returns size of the list or -1 if unable to do a push
    virtual int32_t push(const char *key, const void *data, uint32_t dataLen) = 0;

    virtual int32_t increment(const char *key,int32_t value) = 0;
    virtual bool isInteger(const char *key) = 0;

    // not use fully implemented
    virtual void watch(const char *key) = 0;

    virtual void unwatch(const char *key) = 0;

	virtual void release(void) = 0;

protected:
	virtual ~KeyValueDatabase(void)
	{
	}
};

}
