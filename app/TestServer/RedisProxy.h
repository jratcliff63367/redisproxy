#pragma once

// This class pretends to implement the Redis API
// It doesn't really, except in the simplest terms.
// This is entirely educational.

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
	static RedisProxy *create(void);

	virtual bool fromClient(const char *message) = 0;

	virtual void getToClient(Callback *c) = 0;

	virtual void release(void) = 0;
protected:
	virtual ~RedisProxy(void)
	{
	}
};


}
