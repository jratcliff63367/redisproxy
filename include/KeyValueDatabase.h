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
typedef void (KVD_ABI *KVD_returnCodeCallback)(bool commandOk,int32_t returnCode, void* userPtr);
typedef void (KVD_ABI *KVD_dataCallback)(void* userPtr,const void *data,uint32_t dataLen);
// A 'nullptr' for 'key' means the scan operation is complete!
typedef void (KVD_ABI *KVD_scanCallback)(void *userPtr, const char *key,uint32_t scanIndex);

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

    // Returns a list of keys which match this wildcard.  If 'match' is null, then it returns
    // all keys in the database
    virtual void scan(uint32_t scanIndex,uint32_t maxScan,const char *match,void *userPtr, KVD_scanCallback callback) = 0;

    virtual void get(const char *key,void *userPointer, KVD_dataCallback callback) = 0;

    virtual void del(const char *key, void *userPointer,KVD_returnCodeCallback callback) = 0;

    virtual void exists(const char *key,void *userPointer, KVD_returnCodeCallback callback) = 0;

    virtual void set(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_standardCallback callback) = 0;
    virtual void setnx(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_returnCodeCallback callback) = 0;

    // append to an existing or new record; returns size of the list or -1 if unable to do a push
    virtual void push(const char *key, const void *data, uint32_t dataLen,void *userPointer, KVD_returnCodeCallback callback) = 0;

    virtual void increment(const char *key,int32_t value,void *userPointer,KVD_returnCodeCallback callback) = 0;


    // not use fully implemented
    virtual void watch(uint32_t keyCount,const char **keys,void *userData,KVD_standardCallback callback) = 0;

    virtual void unwatch(void *userData, KVD_standardCallback callback) = 0;

	virtual void release(void) = 0;

protected:
	virtual ~KeyValueDatabase(void)
	{
	}
};

}
