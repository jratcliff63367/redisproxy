#include "RedisProxy.h"
#include "MemoryStream.h"
#include "StringId.h"			// Helper class to made a string to an id/enum and back very quickly
#include "SimpleBuffer.h"
#include "RedisCommandStream.h"
#include "KeyValueDatabase.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define USE_LOG_FILE 0

#define MAX_COMMAND_STRING (1024*4) // 4k
#define MAX_TOTAL_MEMORY (1024*1024)*1024	// 1gb

namespace redisproxy
{

    class RedisProxyImpl : public RedisProxy
    {
    public:
        RedisProxyImpl(keyvaluedatabase::KeyValueDatabase *database) : mDatabase(database)
        {
            static uint32_t gCount = 0;
            gCount++;
            mInstanceId = gCount;
            printf("RedisProxy[%d]\n", mInstanceId);
            mResponseBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
            mMultiBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
            mCommandStream = rediscommandstream::RedisCommandStream::create();
#if USE_LOG_FILE
            static uint32_t gLogCount = 0;
            gLogCount++;
            char scratch[512];
            snprintf(scratch, 512, "f:\\logfile%d.txt", gLogCount);
            mLogFile = fopen(scratch, "wb");
#endif
        }

        virtual ~RedisProxyImpl(void)
        {
            if (mResponseBuffer)
            {
                mResponseBuffer->release();
            }
            if (mMultiBuffer)
            {
                mMultiBuffer->release();
            }
            if (mCommandStream)
            {
                mCommandStream->release();
            }
#if USE_LOG_FILE
            if (mLogFile)
            {
                fclose(mLogFile);
            }
#endif
        }

        void redisPush(uint32_t argc)
        {
            if (argc == 2)
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                if (key )
                {
                    if (!mDatabase->exists(key) || mDatabase->isList(key))
                    {
                        const char *value = mCommandStream->getAttribute(1, atr, dataLen);
                        int32_t listCount = mDatabase->push(key, value, dataLen);
                        if (listCount >= 0)
                        {
                            char scratch[512];
                            snprintf(scratch, 512, ":%d", listCount);
                            addResponse(scratch);
                        }
                        else
                        {
                            addResponse("(error) WRONGTYPE Operation against a key holding the wrong kind of value");
                        }
                    }
                    else
                    {
                        addResponse("(error) WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                }
            }
            else
            {
                badArgs("rpush");
            }
        }

        void multi(uint32_t argc)
        {
            if (mIsMulti)
            {
                addResponse("(error) ERR MULTI calls can not be nested");
            }
            else
            {
                mIsMulti = true;
                addResponse("+OK");
            }
        }

        void exec(uint32_t argc)
        {
            if (mIsMulti)
            {
                // no extra response needed..
            }
            else
            {
                addResponse("Received: -ERR EXEC without MULTI");
            }
        }

        void unwatch(uint32_t argc)
        {
            if (argc == 0)
            {
                mDatabase->unwatch(nullptr);
                addResponse("+OK");
            }
            else
            {
                badArgs("unwatch");
            }
        }

        void watch(uint32_t argc)
        {
            if (argc >= 1)
            {
                for (uint32_t i = 0; i < argc; i++)
                {
                    rediscommandstream::RedisAttribute atr;
                    uint32_t dataLen;
                    const char *key = mCommandStream->getAttribute(i, atr, dataLen);
                    if (key)
                    {
                        mDatabase->watch(key);
                    }
                }
            }
            addResponse("+OK");
        }

        void setnx(uint32_t argc)
        {
            if (argc == 2)
            {
                if (mDatabase)
                {
                    rediscommandstream::RedisAttribute atr;
                    uint32_t dataLen;
                    const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                    const char *data = mCommandStream->getAttribute(1, atr, dataLen);
                    if (key && data && mDatabase)
                    {
                        if (mDatabase->exists(key))
                        {
                            addResponse(":0");
                        }
                        else
                        {
                            mDatabase->set(key, data, dataLen);
                            addResponse(":1");
                        }
                    }
                }
            }
            else
            {
                badArgs("set");
            }
        }

        bool processMessage(const char *message)
        {
            bool ret = false;

            if (mCommandStream)
            {
                uint32_t argc;
                rediscommandstream::RedisCommand command = mCommandStream->addStream(message, argc);
                switch (command)
                {
                case rediscommandstream::RedisCommand::EXEC:
                    exec(argc);
                    break;
                case rediscommandstream::RedisCommand::SETNX:
                    setnx(argc);
                    break;
                case rediscommandstream::RedisCommand::MULTI:
                    multi(argc);
                    break;
                case rediscommandstream::RedisCommand::WATCH:
                    watch(argc);
                    break;
                case rediscommandstream::RedisCommand::UNWATCH:
                    unwatch(argc);
                    break;
                case rediscommandstream::RedisCommand::RPUSH:
                    redisPush(argc);
                    break;
                case rediscommandstream::RedisCommand::INCR:
                    incrementBy(argc, 1, false);
                    break;
                case rediscommandstream::RedisCommand::DECR:
                    incrementBy(argc, 1, true);
                    break;
                case rediscommandstream::RedisCommand::INCRBY:
                    incrementBy(argc, false);
                    break;
                case rediscommandstream::RedisCommand::DECRBY:
                    incrementBy(argc, true);
                    break;
                case rediscommandstream::RedisCommand::NONE:
                    break;
                case rediscommandstream::RedisCommand::PING:
                    processPing(argc);
                    break;
                case rediscommandstream::RedisCommand::SELECT:
                    select(argc);
                    break;
                case rediscommandstream::RedisCommand::SET:
                    set(argc);
                    break;
                case rediscommandstream::RedisCommand::EXISTS:
                    exists(argc);
                    break;
                case rediscommandstream::RedisCommand::GET:
                    get(argc);
                    break;
                default:
                    assert(0); // command not yet implemented!
                    break;
                }
                if (command != rediscommandstream::RedisCommand::NONE)
                {
                    mCommandStream->resetAttributes();
                }
            }


            return ret;
        }

        virtual bool fromClient(const char *message) override final
        {
            bool ret = false;

#if USE_LOG_FILE
            fprintf(mLogFile, "%s\r\n", message);
            fflush(mLogFile);
#endif
            printf("Receiving: %s\n", message);
            if (mIsMulti)
            {
                addMulti(message); // add this message to the multi buffer
                uint32_t argc;
                rediscommandstream::RedisCommand command = mCommandStream->addStream(message, argc);
                switch (command)
                {
                    case rediscommandstream::RedisCommand::MULTI:
                        multi(argc);
                        mCommandStream->resetAttributes();
                        ret = true;
                        break;
                    case rediscommandstream::RedisCommand::EXEC:
                        mCommandStream->resetAttributes();
                        processMulti(); // process all of the commands we accumulated
                        ret = true;
                        break;
                    default:
                        if (command != rediscommandstream::RedisCommand::NONE)
                        {
                            mMultiCommandCount++;
                            addResponse("+QUEUED"); // respond that the message has been queued
                            mCommandStream->resetAttributes();
                            ret = true;
                        }
                        break;
                }
            }
            else
            {
                ret = processMessage(message);
            }


            return ret;
        }

        void badArgs(const char *cmd)
        {
            char scratch[512];
            snprintf(scratch, 512, "-ERR wrong number of arguments for '%s' command", cmd);
            addResponse(scratch);
        }

        void get(uint32_t argc)
        {
            if (argc == 0)
            {
                badArgs("get");
            }
            else
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                if (key && mDatabase)
                {
                    void *mem = mDatabase->get(key, dataLen);
                    if (mem)
                    {
                        char scratch[512];
                        snprintf(scratch, 512, "$%d", dataLen);
                        addResponse(scratch);
                        char *temp = (char *)malloc(dataLen + 1);
                        memcpy(temp, mem, dataLen);
                        temp[dataLen] = 0;
                        addResponse(temp);
                        free(temp);
                        mDatabase->releaseGetMem(mem);
                    }
                    else
                    {
                        addResponse("$-1");
                    }
                }
                else
                {
                    addResponse("$-1");
                }
            }
        }

        void exists(uint32_t argc)
        {
            if (argc == 0)
            {
                badArgs("exists");
            }
            else
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                bool found = false;
                if (key && mDatabase)
                {
                    found = mDatabase->exists(key);
                }
                if (found)
                {
                    addResponse(":1");
                }
                else
                {
                    addResponse(":0");
                }
            }
        }

        void set(uint32_t argc)
        {
            if (argc == 2)
            {
                if (mDatabase)
                {

                    rediscommandstream::RedisAttribute atr;
                    uint32_t dataLen;
                    const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                    const char *data = mCommandStream->getAttribute(1, atr, dataLen);
                    if (key && data && mDatabase )
                    {
                        mDatabase->set(key, data, dataLen);
                    }
                }
                addResponse("+OK");
            }
            else
            {
                badArgs("set");
            }
        }

        bool isInteger(const char *str)
        {
            bool ret = false;
            char c = *str;
            if ((c >= '0' && c <= '9') || c == '+' || c == '-')
            {
                ret = true;
            }
            return ret;
        }

        void incrementBy(const char *key,int32_t dv,bool isNegative)
        {
            if (key)
            {
                if ( !mDatabase->exists(key) || mDatabase->isInteger(key))
                {
                    if (isNegative)
                    {
                        dv *= -1;
                    }
                    int32_t v = mDatabase->increment(key, dv);
                    char scratch[512];
                    snprintf(scratch, 512, ":%d", v);
                    addResponse(scratch);
                }
                else
                {
                    addResponse("(error) ERR value is not an integer or out of range");
                }
            }
        }

        void incrementBy(uint32_t argc,int32_t dv,bool isNegative)
        {
            if (argc == 1)
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                incrementBy(key, dv, isNegative);
            }
            else
            {
                badArgs(isNegative ? "decr" : "incr");
            }
        }


        void incrementBy(uint32_t argc,bool isNegative)
        {
            if (argc == 2)
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *key = mCommandStream->getAttribute(0, atr, dataLen);
                const char *value = mCommandStream->getAttribute(1, atr, dataLen);
                if (isInteger(value))
                {
                    int32_t dv = atoi(value);
                    incrementBy(key, dv, isNegative);
                }
                else
                {
                    addResponse("(error) ERR value is not an integer or out of range");
                }
            }
            else
            {
                badArgs(isNegative ? "decrby" : "incrby");
            }
        }

        void select(uint32_t argc)
        {
            if (argc == 1)
            {
                rediscommandstream::RedisAttribute atr;
                uint32_t dataLen;
                const char *sel = mCommandStream->getAttribute(0, atr, dataLen);
                if (sel)
                {
                    addResponse("+OK");
                }
            }
            else
            {
                badArgs("select");
            }
        }

        void processPing(uint32_t argc)
        {
            if (argc == 0)
            {
                addResponse("+PONG");
            }
            else
            {
                assert(0); // not yet implemented
            }
        }

        virtual void getToClient(Callback *c) override final
        {
            uint32_t streamLen;
            const uint8_t *scan = mResponseBuffer->getData(streamLen);	// Get the raw stream of responses
            if (streamLen)
            {
                const uint8_t *eof = &scan[streamLen];	// This is the end of the data stream
                while (scan < eof)
                {
                    const uint32_t *header = (const uint32_t *)scan;	// Get the header
                    uint32_t stringLen = header[0];				// Get the length of the API string (JSON response)
                    if (stringLen == 0)
                    {
                        c->receiveRedisMessage(""); // empty string
                    }
                    else
                    {
                        const char *msg = (const char *)&header[1];
                        c->receiveRedisMessage(msg);
                    }
                    scan += (stringLen + 1 + sizeof(uint32_t));	// Advance to the next response
                }
                mResponseBuffer->clear();	// Zero out the response buffer now that we have processed all responses
            }
        }

        virtual void release(void) override final
        {
            delete this;
        }

        // Initialize the memory stream for the response
        inline void initMemoryStream(memorystream::MemoryStream &output)
        {
            uint8_t *writeBuffer = mResponseBuffer->confirmCapacity(MAX_COMMAND_STRING); // make sure there enough room in the response buffer for both the JSON portion and the binary data blob
            assert(writeBuffer);
            if (!writeBuffer) return;
            output.init(writeBuffer, MAX_COMMAND_STRING);	// Initialize the stream class for write access it should *never* overflow
            output.write(uint32_t(0)); // length of message ASCII message
        }

        void addResponse(const char *str)
        {
            uint32_t slen = uint32_t(strlen(str));
            uint8_t *writeBuffer = mResponseBuffer->confirmCapacity(MAX_COMMAND_STRING); // make sure there enough room in the response buffer for both the JSON portion and the binary data blob
            assert(writeBuffer);
            if (!writeBuffer) return;
            uint32_t *header = (uint32_t *)writeBuffer;
            header[0] = slen;
            if (slen)
            {
                memcpy(&header[1], str, slen + 1);
            }
            mResponseBuffer->addBuffer(nullptr, slen + 1 + sizeof(uint32_t));
        }

        void addMulti(const char *str)
        {
            uint32_t slen = uint32_t(strlen(str));
            uint8_t *writeBuffer = mMultiBuffer->confirmCapacity(MAX_COMMAND_STRING); // make sure there enough room in the response buffer for both the JSON portion and the binary data blob
            assert(writeBuffer);
            if (!writeBuffer) return;
            uint32_t *header = (uint32_t *)writeBuffer;
            header[0] = slen;
            if (slen)
            {
                memcpy(&header[1], str, slen + 1);
            }
            mMultiBuffer->addBuffer(nullptr, slen + 1 + sizeof(uint32_t));
        }

        void processMulti(void)
        {
            char scratch[512];
            snprintf(scratch, 512, "*%d", mMultiCommandCount);
            addResponse(scratch);
            uint32_t streamLen;
            const uint8_t *scan = mMultiBuffer->getData(streamLen);	// Get the raw stream of responses
            if (streamLen)
            {
                const uint8_t *eof = &scan[streamLen];	// This is the end of the data stream
                while (scan < eof)
                {
                    const uint32_t *header = (const uint32_t *)scan;	// Get the header
                    uint32_t stringLen = header[0];				// Get the length of the API string (JSON response)
                    if (stringLen == 0)
                    {
                    }
                    else
                    {
                        const char *msg = (const char *)&header[1];
                        processMessage(msg);
                    }
                    scan += (stringLen + 1 + sizeof(uint32_t));	// Advance to the next response
                }
                mMultiBuffer->clear();	// Zero out the response buffer now that we have processed all responses
                mMultiCommandCount = 0;
                mIsMulti = false;
            }
        }


        bool                                    mIsMulti{ false };
        uint32_t                                mInstanceId{ 0 };
        simplebuffer::SimpleBuffer	            *mResponseBuffer{ nullptr };// Where pending responses are stored
        rediscommandstream::RedisCommandStream  *mCommandStream{ nullptr };
        keyvaluedatabase::KeyValueDatabase      *mDatabase{ nullptr };
        uint32_t                                mMultiCommandCount{ 0 };
        simplebuffer::SimpleBuffer              *mMultiBuffer{ nullptr };
#if USE_LOG_FILE
        FILE                                    *mLogFile{ nullptr };
#endif
    };

RedisProxy *RedisProxy::create(keyvaluedatabase::KeyValueDatabase *database)
{
    auto ret = new RedisProxyImpl(database);
    return static_cast<RedisProxy *>(ret);
}


}