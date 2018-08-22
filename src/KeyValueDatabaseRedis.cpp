// Implementation of the KeyValueDatabase class that actually just talks to redis
#include "KeyValueDatabase.h"
#include "socketchat.h"
#include "RedisCommandStream.h"
#include "SimpleBuffer.h"
#include "MemoryStream.h"
#include "Wildcard.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <queue>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define REDIS_PORT_NUMBER 6379    // Redis port number
//#define REDIS_PORT_NUMBER 3010    // Redis port number; using the monitor intercept server

#define MAX_COMMAND_STRING (1024*4) // 4k
#define MAX_TOTAL_MEMORY (1024*1024)*1024	// 1gb
#define MAX_PENDING_COMMAND_COUNT 256


namespace keyvaluedatabase
{
    enum class RedisCommand : uint32_t
    {
        NONE,
        SELECT,
        SET,
        SETNX,
        EXISTS,
        DEL,
        GET,
        WATCH,
        UNWATCH,
        INCREMENT,
        SCAN,
    };

    class PendingRedisCommand
    {
    public:
        RedisCommand    mCommand;
        void            *mUserPointer;
        void            *mCallback;
    };

    class KeyValueDatabaseRedis : public KeyValueDatabase, socketchat::SocketChatCallback
    {
    public:
        KeyValueDatabaseRedis(void)
        {
            mSocketChat = socketchat::SocketChat::create("localhost", REDIS_PORT_NUMBER);
            mRedisSendBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
            mCommandStream = rediscommandstream::RedisCommandStream::create();
        }

        virtual ~KeyValueDatabaseRedis(void)
        {
            delete mSocketChat;
            if (mCommandStream)
            {
                mCommandStream->release();
            }
            if (mRedisSendBuffer)
            {
                mRedisSendBuffer->release();
            }
        }

        // Give up a timeslice to the database system
        virtual void pump(void) override final
        {
            if (mSocketChat)
            {
                mSocketChat->poll(this, 0);
                sendResponses();
            }
        }

        virtual void sendResponses(void)
        {
            uint32_t streamLen;
            const uint8_t *scan = mRedisSendBuffer->getData(streamLen);	// Get the raw stream of responses
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
                        mSocketChat->sendText(msg);
                    }
                    scan += (stringLen + 1 + sizeof(uint32_t));	// Advance to the next response
                }
                mRedisSendBuffer->clear();	// Zero out the response buffer now that we have processed all responses
            }
        }

        virtual void get(const char *key, void *userPointer, KVD_dataCallback callback) override final
        {
            assert(callback); // not implemented yet

            mSocketChat->sendText("*2");

            mSocketChat->sendText("$3");
            mSocketChat->sendText("GET");
            initMemoryStream();
            uint32_t dlen = uint32_t(strlen(key));
            mOutput << "$";
            mOutput << dlen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText(key);

            addPendingResponse(RedisCommand::GET, callback, userPointer);
        }


        virtual void del(const char *key, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            assert(callback); // not implemented yet

            mSocketChat->sendText("*2");

            mSocketChat->sendText("$3");
            mSocketChat->sendText("DEL");
            initMemoryStream();
            uint32_t dlen = uint32_t(strlen(key));
            mOutput << "$";
            mOutput << dlen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText(key);

            addPendingResponse(RedisCommand::DEL, callback, userPointer);

        }


        virtual void exists(const char *key, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            assert(callback); // not implemented yet

            mSocketChat->sendText("*2");

            mSocketChat->sendText("$6");
            mSocketChat->sendText("EXISTS");

            initMemoryStream();
            uint32_t dlen = uint32_t(strlen(key));
            mOutput << "$";
            mOutput << dlen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText(key);

            addPendingResponse(RedisCommand::EXISTS, callback, userPointer);

        }



        // Initialize the memory stream for the response
        inline void initMemoryStream(void)
        {
            mOutput.init(mScratchBuffer, MAX_COMMAND_STRING);
        }

        virtual void select (uint32_t index, void *userPointer, KVD_standardCallback callback) override final
        {
            assert(callback);
            // Send the 'SELECT' command to the Redis server and push the response we are waiting for...
            initMemoryStream();
            mOutput << "SELECT ";
            mOutput << index;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);
            addPendingResponse(RedisCommand::SELECT, callback, userPointer);
        }

        virtual void set(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_standardCallback callback) override final
        {
            assert(callback); // not implemented yet

            mSocketChat->sendText("*3");

            mSocketChat->sendText("$3");
            mSocketChat->sendText("SET");

            initMemoryStream();
            uint32_t dlen = uint32_t(strlen(key));
            mOutput << "$";
            mOutput << dlen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText(key);

            initMemoryStream();
            mOutput << "$";
            mOutput << dataLen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendBinary(data, dataLen);


            addPendingResponse(RedisCommand::SET, callback, userPointer);
        }

        virtual void setnx(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            assert(callback); // not implemented yet

            mSocketChat->sendText("*3");

            mSocketChat->sendText("$5");
            mSocketChat->sendText("SETNX");

            initMemoryStream();
            uint32_t dlen = uint32_t(strlen(key));
            mOutput << "$";
            mOutput << dlen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText(key);

            initMemoryStream();
            mOutput << "$";
            mOutput << dataLen;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendBinary(data, dataLen);


            addPendingResponse(RedisCommand::SETNX, callback, userPointer);
        }


        // append to an existing or new record; returns size of the list or -1 if unable to do a push
        virtual void push(const char *key, const void *data, uint32_t dataLen, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            assert(0); // not implemented yet
        }

        virtual void increment(const char *key, int32_t v, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            char scratch[512];
            if (v >= 0)
            {
                snprintf(scratch,512, "incrby \"%s\" %d", key, v);
            }
            else
            {
                snprintf(scratch, 512, "decrby \"%s\" %d", key, -v);
            }
            mSocketChat->sendText(scratch);
            addPendingResponse(RedisCommand::INCREMENT, callback, userPointer);
        }

        // not use fully implemented
        virtual void watch(uint32_t keyCount, const char **keys, void *userData, KVD_standardCallback callback) override final
        {
            initMemoryStream();
            mOutput << "*";
            mOutput << keyCount + 1;
            mOutput << char(0);
            mSocketChat->sendText((const char *)mScratchBuffer);

            mSocketChat->sendText("$5");
            mSocketChat->sendText("WATCH");

            for (uint32_t i = 0; i < keyCount; i++)
            {
                const char *key = keys[i];
                initMemoryStream();
                uint32_t dlen = uint32_t(strlen(key));
                mOutput << "$";
                mOutput << dlen;
                mOutput << char(0);
                mSocketChat->sendText((const char *)mScratchBuffer);
                mSocketChat->sendText(key);
            }

            addPendingResponse(RedisCommand::WATCH, callback, userData);
        }

        virtual void unwatch(void *userData, KVD_standardCallback callback) override final
        {
            mSocketChat->sendText("unwatch");
            addPendingResponse(RedisCommand::UNWATCH, callback, userData);
        }

        virtual void release(void) override final
        {
            delete this;
        }

        bool isValid(void) const
        {
            return mSocketChat ? true : false;
        }

        void addRedisSend(const char *str)
        {
            uint32_t slen = uint32_t(strlen(str));
            uint8_t *writeBuffer = mRedisSendBuffer->confirmCapacity(MAX_COMMAND_STRING); // make sure there enough room in the response buffer for both the JSON portion and the binary data blob
            assert(writeBuffer);
            if (!writeBuffer) return;
            uint32_t *header = (uint32_t *)writeBuffer;
            header[0] = slen;
            if (slen)
            {
                memcpy(&header[1], str, slen + 1);
            }
            mRedisSendBuffer->addBuffer(nullptr, slen + 1 + sizeof(uint32_t));
        }

        virtual void receiveBinaryMessage(const void *data, uint32_t dataLen) override final
        {
            assert(0); // not yet implemented
        }

        // When we get a message from the Redis server; we parse it and if it's a complete command
        // we handle the response logic
        virtual void receiveMessage(const char *data) override final
        {
            uint32_t argc;
            rediscommandstream::RedisCommand command = mCommandStream->addStream(data, argc);
            switch (command)
            {
                case rediscommandstream::RedisCommand::ERR:
                    processERR();
                    break;
                case rediscommandstream::RedisCommand::OK:
                    processOK();
                    break;
                case rediscommandstream::RedisCommand::NONE:
                    break;
                case rediscommandstream::RedisCommand::RETURN_CODE:
                    processReturnCode();
                    break;
                case rediscommandstream::RedisCommand::RETURN_DATA:
                    processReturnData();
                    break;
                default:
                    assert(0); // not yet implemented!
                    break;
            }
            if (command != rediscommandstream::RedisCommand::NONE)
            {
                mCommandStream->resetAttributes();
            }
        }

        void processReturnData(void)
        {
            if (mPendingRedisCommands.empty())
            {
                assert(0); // got a response we were not expecting!
                return;
            }
            PendingRedisCommand prc = mPendingRedisCommands.front();
            mPendingRedisCommands.pop();
            switch (prc.mCommand)
            {
            case RedisCommand::GET:
            {
                KVD_dataCallback callback = (KVD_dataCallback)prc.mCallback;
                uint32_t dataLen;
                const char *c = mCommandStream->getCommandString(dataLen);
                (*callback)(prc.mUserPointer, c, dataLen);
            }
            break;
            case RedisCommand::SCAN:
                // Time to return the scan results!
                {
                    KVD_scanCallback callback = (KVD_scanCallback)prc.mCallback;
                    uint32_t dataLen;
                    const char *c = mCommandStream->getCommandString(dataLen);
                    uint32_t index = 0;
                    if (c)
                    {
                        index = atoi(c);
                    }
                    uint32_t attributeCount = mCommandStream->getAttributeCount();
                    for (uint32_t i = 0; i < attributeCount; i++)
                    {
                        rediscommandstream::RedisAttribute atr;
                        const char *result = mCommandStream->getAttribute(i, atr, dataLen);
                        if (result)
                        {
                            (*callback)(prc.mUserPointer, result, 0);
                        }
                    }
                    (*callback)(prc.mUserPointer, nullptr, index);
                }
                break;
            default:
                assert(0); // not implemented yet
                break;
            }
        }

        void processReturnCode(void)
        {
            if (mPendingRedisCommands.empty())
            {
                assert(0); // got a response we were not expecting!
                return;
            }
            PendingRedisCommand prc = mPendingRedisCommands.front();
            mPendingRedisCommands.pop();
            switch (prc.mCommand)
            {
            case RedisCommand::EXISTS:
            case RedisCommand::DEL:
            case RedisCommand::INCREMENT:
            case RedisCommand::SETNX:
            {
                KVD_returnCodeCallback callback = (KVD_returnCodeCallback)prc.mCallback;
                uint32_t dataLen;
                const char *c = mCommandStream->getCommandString(dataLen);
                int32_t value = c ? atoi(c) : -1;
                (*callback)(true,value, prc.mUserPointer);
            }
            break;
            default:
                assert(0); // not implemented yet
                break;
            }
        }

        void processOK(void)
        {
            if (mPendingRedisCommands.empty())
            {
                assert(0); // got a response we were not expecting!
                return;
            }
            PendingRedisCommand prc = mPendingRedisCommands.front();
            mPendingRedisCommands.pop();
            switch (prc.mCommand)
            {
                case RedisCommand::SELECT:
                case RedisCommand::SET:
                case RedisCommand::WATCH:
                case RedisCommand::UNWATCH:
                    {
                    KVD_standardCallback callback = (KVD_standardCallback)prc.mCallback;
                    (*callback)(true, prc.mUserPointer);
                    }
                    break;
                default:
                    assert(0); // not implemented yet
                    break;
            }
        }

        void processERR(void)
        {
            if (mPendingRedisCommands.empty())
            {
                assert(0); // got a response we were not expecting!
                return;
            }
            PendingRedisCommand prc = mPendingRedisCommands.front();
            mPendingRedisCommands.pop();
            switch (prc.mCommand)
            {
            case RedisCommand::SELECT:
            case RedisCommand::SET:
            {
                KVD_standardCallback callback = (KVD_standardCallback)prc.mCallback;
                (*callback)(false, prc.mUserPointer);
            }
            break;
            case RedisCommand::INCREMENT:
            {
                KVD_returnCodeCallback callback = (KVD_returnCodeCallback)prc.mCallback;
                (*callback)(false, 0, prc.mUserPointer);
            }
                break;
            default:
                assert(0); // not implemented yet
                break;
            }
        }


        void addPendingResponse(RedisCommand rc,void *callback,void *userPtr)
        {
            PendingRedisCommand prc;
            prc.mCommand = rc;
            prc.mCallback = callback;
            prc.mUserPointer = userPtr;
            mPendingRedisCommands.push(prc);
        }

        // Returns a list of keys which match this wildcard.  If 'match' is null, then it returns
        // all keys in the database
        virtual void scan(uint32_t cursorPosition,uint32_t maxScan,const char *_match,void *userPtr,KVD_scanCallback callback) override final
        {
            char scratch[512];
            char count[512];
            char match[512];

            const char *countStr = "";
            const char *matchStr = "";
            if (maxScan > 0)
            {
                snprintf(count, 512, " count %d", maxScan);
                countStr = count;
            }
            if (_match)
            {
                snprintf(match, 512, " match \"%s\"", _match);
                matchStr = match;
            }
            snprintf(scratch, 512, "scan %d %s%s", cursorPosition, countStr, matchStr);
            mSocketChat->sendText(scratch);
            addPendingResponse(RedisCommand::SCAN, callback, userPtr);
        }


        simplebuffer::SimpleBuffer	            *mRedisSendBuffer{ nullptr };// Where pending responses are stored
        rediscommandstream::RedisCommandStream  *mCommandStream{ nullptr };
        socketchat::SocketChat                  *mSocketChat{ nullptr };
        memorystream::MemoryStream              mOutput;
        uint8_t                                 mScratchBuffer[MAX_COMMAND_STRING];
        std::queue< PendingRedisCommand >       mPendingRedisCommands;
    };

    KeyValueDatabase *createKeyValueDatabaseRedis(void)
    {
        auto ret = new KeyValueDatabaseRedis;
        if (!ret->isValid())
        {
            delete ret;
            ret = nullptr;
        }
        return static_cast<KeyValueDatabase *>(ret);
    }

}