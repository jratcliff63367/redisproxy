#pragma once

// This class pretends to implement the Redis API
// It doesn't really, except in the simplest terms.
// This is entirely educational.

// Forward reference the keyvalue database
namespace keyvaluedatabase
{
    class KeyValueDatabase;
}

namespace redisproxy
{

class RedisProxy
{
public:
    class Callback
    {
    public:
        virtual void receiveRedisMessage(const char *msg) = 0;
    };
    // Provides the *shared* keyvalue database that all connections talk to
	static RedisProxy *create(keyvaluedatabase::KeyValueDatabase *database);

	virtual bool fromClient(const char *message) = 0;

	virtual void getToClient(Callback *c) = 0;

	virtual void release(void) = 0;
protected:
	virtual ~RedisProxy(void)
	{
	}
};


}
