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
            mCommandStream = rediscommandstream::RedisCommandStream::create();
        }

        virtual ~RedisProxyImpl(void)
        {
            if (mResponseBuffer)
            {
                mResponseBuffer->release();
            }
            if (mCommandStream)
            {
                mCommandStream->release();
            }
        }

        virtual bool fromClient(const char *message) override final
        {
            bool ret = false;

            printf("Receiving: %s\n", message);
            if (mCommandStream)
            {
                uint32_t argc;
                rediscommandstream::RedisCommand command = mCommandStream->addStream(message, argc);
                switch (command)
                {
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

#if 0 // TODO TODO
        virtual int32_t ParseLine(uint32_t lineno, uint32_t argc, const char **argv)  // return TRUE to continue parsing, return FALSE to abort parsing process
        {
            int32_t ret = 0;

            if (argc)
            {
                const char *cmd = argv[0];
                Keyword key = Keyword(mKeywordTable.string2Id(cmd));
                switch (key)
                {
                case Keyword::SET:
                    addResponse("+OK");
                    break;
                case Keyword::SELECT:
                    addResponse("+OK");
#if 0
                    if (argc == 2)
                    {
                        addResponse("+OK");
                    }
                    else
                    {
                        addResponse("Received: -ERR wrong number of arguments for 'select' command");
                    }
#endif
                    break;
                case Keyword::PING:
                    if (argc == 1)
                    {
                        addResponse("+PONG");
                    }
                    else if (argc == 2)
                    {
                        const char *response = argv[1];
                        uint32_t slen = uint32_t(strlen(response));
                        char scratch[512];
                        snprintf(scratch, 512, "$%d", slen);
                        addResponse(scratch);
                        addResponse(response);
                    }
                    else
                    {
                        addResponse("-ERR wrong number of arguments for 'ping' command");
                    }
                    break;
                }

            }

            return ret;
        }
#endif


        uint32_t                                mInstanceId{ 0 };
        simplebuffer::SimpleBuffer	            *mResponseBuffer{ nullptr };// Where pending responses are stored
        rediscommandstream::RedisCommandStream  *mCommandStream{ nullptr };
        keyvaluedatabase::KeyValueDatabase      *mDatabase{ nullptr };
    };

RedisProxy *RedisProxy::create(keyvaluedatabase::KeyValueDatabase *database)
{
    auto ret = new RedisProxyImpl(database);
    return static_cast<RedisProxy *>(ret);
}


}