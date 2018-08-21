#include "RedisProxy.h"
#include "socketchat.h"
#include "SimpleBuffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <vector>
#include <string>
// intercepts all messages to and from Redis so we can log them
#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define REDIS_PORT_NUMBER 6379    // Redis port number
#define MAX_COMMAND_STRING (1024*4) // 4k
#define MAX_TOTAL_MEMORY (1024*1024)*1024	// 1gb


namespace redisproxy
{

    static FILE *gClientCommands = nullptr; 


    typedef std::vector< std::string > StringVector;

    class RedisProxyMonitor : public RedisProxy, socketchat::SocketChatCallback
    {
    public:
        RedisProxyMonitor(void)
        {
#if 1
            if (gClientCommands == nullptr)
            {
                gClientCommands = fopen("f:\\clientcommands.txt", "wb");
            }
            static uint32_t gCount = 0;
            gCount++;
            char scratch[512];
            snprintf(scratch, 512, "f:\\redismonitor%d.txt", gCount);
            mFph = fopen(scratch, "wb");
#endif
            mSocketChat = socketchat::SocketChat::create("localhost", REDIS_PORT_NUMBER);

            mResponseBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
        }

        virtual ~RedisProxyMonitor(void)
        {
            delete mSocketChat;
            if (mResponseBuffer)
            {
                mResponseBuffer->release();
            }
            if (mFph)
            {
                fclose(mFph);
            }
        }

        virtual bool fromClient(const char *message) override final
        {
            bool ret = true;


            if (gClientCommands)
            {
                if (*message == '*')
                {
                    fprintf(gClientCommands, "\r\n");
                }
                fprintf(gClientCommands, "%s\r\n", message);
                fflush(gClientCommands);
            }

            if (mFph)
            {
                fprintf(mFph, "[Client]%s\r\n", message);
                fflush(mFph);
            }
            if (mSocketChat)
            {
                mSocketChat->sendText(message);
            }

            return ret;
        }

        virtual bool fromClient(const void *data, uint32_t dataLen) override final
        {
            bool ret = true;

            if (gClientCommands)
            {
                const uint8_t *scan = (const uint8_t *)data;
                for (uint32_t i = 0; i < dataLen; i++)
                {
                    uint8_t c = scan[i];
                    if (c >= 32 && c <= 127)
                    {
                        fprintf(gClientCommands, "%c", c);
                    }
                    else
                    {
                        fprintf(gClientCommands, "$%02X", c);
                    }
                }
                fprintf(gClientCommands, "\r\n");
                fflush(gClientCommands);
            }


            if (mFph)
            {
                fprintf(mFph, "[Client]");
                const uint8_t *scan = (const uint8_t *)data;
                for (uint32_t i = 0; i < dataLen; i++)
                {
                    uint8_t c = scan[i];
                    if (c >= 32 && c <= 127)
                    {
                        fprintf(mFph, "%c", c);
                    }
                    else
                    {
                        fprintf(mFph, "$%02X", c);
                    }
                }
                fprintf(mFph, "\r\n");
                fflush(mFph);
            }

            if (mSocketChat)
            {
                mSocketChat->sendBinary(data, dataLen);
            }

            return ret;
        }

        virtual void getToClient(Callback *c) override final
        {
            if (!mSocketChat) return;
            mSocketChat->poll(this, 0);
            uint32_t streamLen;
            const uint8_t *scan = mResponseBuffer->getData(streamLen);	// Get the raw stream of responses
            if (streamLen)
            {
                const uint8_t *eof = &scan[streamLen];	// This is the end of the data stream
                while (scan < eof)
                {
                    const uint32_t *header = (const uint32_t *)scan;	// Get the header
                    uint32_t stringLen = header[0];				// Get the length of the API string (JSON response)
                    const uint8_t *msg = (const uint8_t *)&header[1];
                    c->receiveRedisMessage(msg, stringLen);
                    scan += (stringLen + sizeof(uint32_t));	// Advance to the next response
                }
                mResponseBuffer->clear();	// Zero out the response buffer now that we have processed all responses
            }

        }

        virtual void release(void) override final
        {
            delete this;
        }

        virtual void receiveMessage(const char *data) override final
        {
            if (mFph)
            {
                fprintf(mFph, "[Server]%s\r\n", data);
                fflush(mFph);
            }
            uint32_t slen = uint32_t(strlen(data));
            uint8_t *dest = mResponseBuffer->confirmCapacity(slen + sizeof(slen));
            uint32_t *dlen = (uint32_t *)dest;
            *dlen = slen;
            memcpy(dest + 4, data, slen);
            mResponseBuffer->addBuffer(nullptr, slen + sizeof(slen));
        }

        // if the data stream has non ASCII data in it
        virtual void receiveBinaryMessage(const void *data, uint32_t dataLen) override final
        {
            uint8_t *dest = mResponseBuffer->confirmCapacity(dataLen + sizeof(dataLen));
            uint32_t *dlen = (uint32_t *)dest;
            *dlen = dataLen;
            memcpy(dest + 4, data, dataLen);
            mResponseBuffer->addBuffer(nullptr, dataLen + sizeof(dataLen));
            if (mFph)
            {
                fprintf(mFph, "[Server]");
                const uint8_t *scan = (const uint8_t *)data;
                for (uint32_t i = 0; i < dataLen; i++)
                {
                    uint8_t c = scan[i];
                    if (c >= 32 && c < 127)
                    {
                        fprintf(mFph, "%c", c);
                    }
                    else
                    {
                        fprintf(mFph, "$%02X", c);
                    }
                }
                fprintf(mFph, "\r\n");
                fflush(mFph);
            }
        }

        FILE                                    *mFph{ nullptr };
        socketchat::SocketChat                  *mSocketChat{ nullptr };
        simplebuffer::SimpleBuffer              *mResponseBuffer{ nullptr };
    };

RedisProxy *RedisProxy::createMonitor(void)
{
    auto ret = new RedisProxyMonitor;
    return static_cast<RedisProxy *>(ret);
}


}