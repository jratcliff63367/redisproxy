#include "RedisProxy.h"
#include "MemoryStream.h"
#include "StringId.h"			// Helper class to made a string to an id/enum and back very quickly
#include "SimpleBuffer.h"
#include "InParser.h"
#include <assert.h>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define MAX_COMMAND_STRING (1024*4) // 4k
#define MAX_TOTAL_MEMORY (1024*1024)*1024	// 1gb

namespace redisproxy
{

enum class Keyword : uint32_t
{
    NONE,
    PING,
    PONG,
    SELECT,
    SET,
};

// A simple struct associating an ASCII string with a unique enumerated keyword
struct KeywordString
{
    KeywordString(void) { };
    KeywordString(const char *str, Keyword kw) : mString(str), mKeyword(kw)
    {
    }
    const char *mString{ nullptr };
    Keyword     mKeyword{ Keyword::NONE };
};

// Table which maps keyword enum values to their corresponding ASCII string
static KeywordString gKeywordString[] =
{
    { "PING", Keyword::PING },
    { "PONG", Keyword::PONG },
    { "SELECT", Keyword::SELECT },
    { "SET", Keyword::SET },
};

class RedisProxyImpl : public RedisProxy, IN_PARSER::InPlaceParserInterface
{
public:
    RedisProxyImpl(void)
    {
        static uint32_t gCount = 0;
        gCount++;
        mInstanceId = gCount;
        printf("RedisProxy[%d]\n", mInstanceId);
        // Initialize the string ID table
        for (auto &i : gKeywordString)
        {
            bool ok = mKeywordTable.addStringId(i.mString, uint32_t(i.mKeyword));
            if (!ok)
            {
                assert(0);
            }
        }
        // Set up the ASCII parser hard separator symbols
        mIPP.DefaultSymbols();
        mResponseBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
    }

    virtual ~RedisProxyImpl(void)
    {
        if (mResponseBuffer)
        {
            mResponseBuffer->release();
        }
    }

    virtual bool fromClient(const char *message) override final
    {
        bool ret = false;

        printf("FROM-CLIENT(%d): %s\n",mInstanceId, message);

        mIPP.Parse(message, this);

        return ret;
    }

    virtual void getToClient(Callback *c) override final
    {
        uint32_t streamLen;
        const uint8_t *scan = mResponseBuffer->getData(streamLen);	// Get the raw stream of responses
        if (streamLen )
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
                scan += (stringLen+1 + sizeof(uint32_t));	// Advance to the next response
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


    uint32_t                    mInstanceId{ 0 };
    simplebuffer::SimpleBuffer	*mResponseBuffer{ nullptr };// Where pending responses are stored
    IN_PARSER::InPlaceParser    mIPP;						// The InPlaceParser class
    stringid::StringId          mKeywordTable;				// The keyword table lookup
};

RedisProxy *RedisProxy::create(void)
{
    auto ret = new RedisProxyImpl;
    return static_cast<RedisProxy *>(ret);
}


}