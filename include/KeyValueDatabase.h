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

typedef void (KVD_ABI *KVD_standardCallback)(bool ok, void* userPtr);
typedef void (KVD_ABI *KVD_returnCodeCallback)(int32_t ok, void* userPtr);
typedef void (KVD_ABI *KVD_dataCallback)(void* userPtr,const void *data,uint32_t dataLen);

class KeyValueDatabase
{
public:
    enum Provider
    {
        IN_MEMORY,      // simple in memory database for performance testing
        REDIS,          // Use redis as the keyvalue provider
    };

	static KeyValueDatabase *create(Provider p);

    virtual void select(uint32_t index, void *userPointer, KVD_standardCallback callback) = 0;

    // Give up a timeslice to the database system
    virtual void pump(void) = 0;

    virtual void get(const char *key,void *userPointer, KVD_dataCallback callback) = 0;

    virtual void del(const char *key, void *userPointer,KVD_returnCodeCallback callback) = 0;

    virtual void exists(const char *key,void *userPointer, KVD_returnCodeCallback callback) = 0;
    virtual void set(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_standardCallback callback) = 0;

    // append to an existing or new record; returns size of the list or -1 if unable to do a push
    virtual void push(const char *key, const void *data, uint32_t dataLen,void *userPointer, KVD_returnCodeCallback callback) = 0;

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
