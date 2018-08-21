#include "RedisCommandStream.h"
#include "StringId.h"			// Helper class to made a string to an id/enum and back very quickly
#include "SimpleBuffer.h"
#include <assert.h>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define MAX_COMMAND_STRING (1024*4) // 4k
#define MAX_TOTAL_MEMORY (1024*1024)*1024	// 1gb

#define MAX_ARGS 32

namespace rediscommandstream
{

    class RedisArgument
    {
    public:
        uint8_t         *mData{ nullptr };                  // Pointer to argument data
        uint32_t        mDataLen{ 0 };                      // Length of argument
        RedisCommand    mCommand{ RedisCommand::NONE };     // If it's a command, the command
        RedisAttribute  mAttribute{ RedisAttribute::NONE }; // If it's an attribute, then the attribute
    };

    // A simple struct associating an ASCII string with a unique enumerated keyword
    struct CommandString
    {
        CommandString(void) { };
        CommandString(const char *str, RedisCommand kw) : mString(str), mCommand(kw)
        {
        }
        const char *mString{ nullptr };
        RedisCommand     mCommand{ RedisCommand::NONE };
    };

    // Table which maps keyword enum values to their corresponding ASCII string
    static CommandString gCommandString[] =
    {
        { "APPEND"                        ,RedisCommand::APPEND},
        { "AUTH"                          ,RedisCommand::AUTH},
        { "BGREWRITEAOF"                  ,RedisCommand::BGREWRITEAOF},
        { "BGSAVE"                        ,RedisCommand::BGSAVE},
        { "BITCOUNT"                      ,RedisCommand::BITCOUNT},
        { "BITFIELD"                      ,RedisCommand::BITFIELD},
        { "BITOP"                         ,RedisCommand::BITOP},
        { "BITPOS"                        ,RedisCommand::BITPOS},
        { "BLPOP"                         ,RedisCommand::BLPOP},
        { "BRPOP"                         ,RedisCommand::BRPOP},
        { "BRPOPLPUSH"                    ,RedisCommand::BRPOPLPUSH},
        { "BZPOPMIN"                      ,RedisCommand::BZPOPMIN},
        { "BZPOPMAX"                      ,RedisCommand::BZPOPMAX},
        { "CLIENT"                        ,RedisCommand::CLIENT},
        { "CLUSTER"                       ,RedisCommand::CLUSTER},
        { "COMMAND"                       ,RedisCommand::COMMAND},
        { "CONFIG"                        ,RedisCommand::CONFIG},
        { "DBSIZE"                        ,RedisCommand::DBSIZE},
        { "DEBUG"                         ,RedisCommand::REDIS_DEBUG},
        { "DECR"                          ,RedisCommand::DECR},
        { "DECRBY"                        ,RedisCommand::DECRBY},
        { "DEL"                           ,RedisCommand::DEL},
        { "DISCARD"                       ,RedisCommand::DISCARD},
        { "DUMP"                          ,RedisCommand::DUMP},
        { "ECHO"                          ,RedisCommand::ECHO},
        { "EVAL"                          ,RedisCommand::EVAL},
        { "EVALSHA"                       ,RedisCommand::EVALSHA},
        { "EXEC"                          ,RedisCommand::EXEC},
        { "EXISTS"                        ,RedisCommand::EXISTS},
        { "EXPIRE"                        ,RedisCommand::EXPIRE},
        { "EXPIREAT"                      ,RedisCommand::EXPIREAT},
        { "FLUSHALL"                      ,RedisCommand::FLUSHALL},
        { "FLUSHDB"                       ,RedisCommand::FLUSHDB},
        { "GEOADD"                        ,RedisCommand::GEOADD},
        { "GEOHASH"                       ,RedisCommand::GEOHASH},
        { "GEOPOS"                        ,RedisCommand::GEOPOS},
        { "GEODIST"                       ,RedisCommand::GEODIST},
        { "GEORADIUS"                     ,RedisCommand::GEORADIUS},
        { "GEORADIUSBYMEMBER"             ,RedisCommand::GEORADIUSBYMEMBER},
        { "GET"                           ,RedisCommand::GET},
        { "GETBIT"                        ,RedisCommand::GETBIT},
        { "GETRANGE"                      ,RedisCommand::GETRANGE},
        { "GETSET"                        ,RedisCommand::GETSET},
        { "HDEL"                          ,RedisCommand::HDEL},
        { "HEXISTS"                       ,RedisCommand::HEXISTS},
        { "HGET"                          ,RedisCommand::HGET},
        { "HGETALL"                       ,RedisCommand::HGETALL},
        { "HINCRBY"                       ,RedisCommand::HINCRBY},
        { "HINCRBYFLOAT"                  ,RedisCommand::HINCRBYFLOAT},
        { "HKEYS"                         ,RedisCommand::HKEYS},
        { "HLEN"                          ,RedisCommand::HLEN},
        { "HMGET"                         ,RedisCommand::HMGET},
        { "HMSET"                         ,RedisCommand::HMSET},
        { "HSET"                          ,RedisCommand::HSET},
        { "HSETNX"                        ,RedisCommand::HSETNX},
        { "HSTRLEN"                       ,RedisCommand::HSTRLEN},
        { "HVALS"                         ,RedisCommand::HVALS},
        { "INCR"                          ,RedisCommand::INCR},
        { "INCRBY"                        ,RedisCommand::INCRBY},
        { "INCRBYFLOAT"                   ,RedisCommand::INCRBYFLOAT},
        { "INFO"                          ,RedisCommand::INFO},
        { "KEYS"                          ,RedisCommand::KEYS},
        { "LASTSAVE"                      ,RedisCommand::LASTSAVE},
        { "LINDEX"                        ,RedisCommand::LINDEX},
        { "LINSERT"                       ,RedisCommand::LINSERT},
        { "LLEN"                          ,RedisCommand::LLEN},
        { "LPOP"                          ,RedisCommand::LPOP},
        { "LPUSH"                         ,RedisCommand::LPUSH},
        { "LPUSHX"                        ,RedisCommand::LPUSHX},
        { "LRANGE"                        ,RedisCommand::LRANGE},
        { "LREM"                          ,RedisCommand::LREM},
        { "LSET"                          ,RedisCommand::LSET},
        { "LTRIM"                         ,RedisCommand::LTRIM},
        { "MEMORY"                        ,RedisCommand::MEMORY},
        { "MGET"                          ,RedisCommand::MGET},
        { "MIGRATE"                       ,RedisCommand::MIGRATE},
        { "MONITOR"                       ,RedisCommand::MONITOR},
        { "MOVE"                          ,RedisCommand::MOVE},
        { "MSET"                          ,RedisCommand::MSET},
        { "MSETNX"                        ,RedisCommand::MSETNX},
        { "MULTI"                         ,RedisCommand::MULTI},
        { "OBJECT"                        ,RedisCommand::OBJECT},
        { "PERSIST"                       ,RedisCommand::PERSIST},
        { "PEXPIRE"                       ,RedisCommand::PEXPIRE},
        { "PEXPIREAT"                     ,RedisCommand::PEXPIREAT},
        { "PFADD"                         ,RedisCommand::PFADD},
        { "PFCOUNT"                       ,RedisCommand::PFCOUNT},
        { "PFMERGE"                       ,RedisCommand::PFMERGE},
        { "PING"                          ,RedisCommand::PING},
        { "PSETEX"                        ,RedisCommand::PSETEX},
        { "PSUBSCRIBE"                    ,RedisCommand::PSUBSCRIBE},
        { "PUBSUB"                        ,RedisCommand::PUBSUB},
        { "PTTL"                          ,RedisCommand::PTTL},
        { "PUBLISH"                       ,RedisCommand::PUBLISH},
        { "PUNSUBSCRIBE"                  ,RedisCommand::PUNSUBSCRIBE},
        { "QUIT"                          ,RedisCommand::QUIT},
        { "RANDOMKEY"                     ,RedisCommand::RANDOMKEY},
        { "READONLY"                      ,RedisCommand::READONLY},
        { "READWRITE"                     ,RedisCommand::READWRITE},
        { "RENAME"                        ,RedisCommand::RENAME},
        { "RENAMENX"                      ,RedisCommand::RENAMENX},
        { "RESTORE"                       ,RedisCommand::RESTORE},
        { "ROLE"                          ,RedisCommand::ROLE},
        { "RPOP"                          ,RedisCommand::RPOP},
        { "RPOPLPUSH"                     ,RedisCommand::RPOPLPUSH},
        { "RPUSH"                         ,RedisCommand::RPUSH},
        { "RPUSHX"                        ,RedisCommand::RPUSHX},
        { "SADD"                          ,RedisCommand::SADD},
        { "SAVE"                          ,RedisCommand::SAVE},
        { "SCARD"                         ,RedisCommand::SCARD},
        { "SCRIPT"                        ,RedisCommand::SCRIPT},
        { "SDIFF"                         ,RedisCommand::SDIFF},
        { "SDIFFSTORE"                    ,RedisCommand::SDIFFSTORE},
        { "SELECT"                        ,RedisCommand::SELECT},
        { "SET"                           ,RedisCommand::SET},
        { "SETBIT"                        ,RedisCommand::SETBIT},
        { "SETEX"                         ,RedisCommand::SETEX},
        { "SETNX"                         ,RedisCommand::SETNX},
        { "SETRANGE"                      ,RedisCommand::SETRANGE},
        { "SHUTDOWN"                      ,RedisCommand::SHUTDOWN},
        { "SINTER"                        ,RedisCommand::SINTER},
        { "SINTERSTORE"                   ,RedisCommand::SINTERSTORE},
        { "SISMEMBER"                     ,RedisCommand::SISMEMBER},
        { "SLAVEOF"                       ,RedisCommand::SLAVEOF},
        { "SLOWLOG"                       ,RedisCommand::SLOWLOG},
        { "SMEMBERS"                      ,RedisCommand::SMEMBERS},
        { "SMOVE"                         ,RedisCommand::SMOVE},
        { "SORT"                          ,RedisCommand::SORT},
        { "SPOP"                          ,RedisCommand::SPOP},
        { "SRANDMEMBER"                   ,RedisCommand::SRANDMEMBER},
        { "SREM"                          ,RedisCommand::SREM},
        { "STRLEN"                        ,RedisCommand::STRLEN},
        { "SUBSCRIBE"                     ,RedisCommand::SUBSCRIBE},
        { "SUNION"                        ,RedisCommand::SUNION},
        { "SUNIONSTORE"                   ,RedisCommand::SUNIONSTORE},
        { "SWAPDB"                        ,RedisCommand::SWAPDB},
        { "SYNC"                          ,RedisCommand::SYNC},
        { "TIME"                          ,RedisCommand::TIME},
        { "TOUCH"                         ,RedisCommand::TOUCH},
        { "TTL"                           ,RedisCommand::TTL},
        { "TYPE"                          ,RedisCommand::TYPE},
        { "UNSUBSCRIBE"                   ,RedisCommand::UNSUBSCRIBE},
        { "UNLINK"                        ,RedisCommand::UNLINK},
        { "UNWATCH"                       ,RedisCommand::UNWATCH},
        { "WAIT"                          ,RedisCommand::WAIT},
        { "WATCH"                         ,RedisCommand::WATCH},
        { "ZADD"                          ,RedisCommand::ZADD},
        { "ZCARD"                         ,RedisCommand::ZCARD},
        { "ZCOUNT"                        ,RedisCommand::ZCOUNT},
        { "ZINCRBY"                       ,RedisCommand::ZINCRBY},
        { "ZINTERSTORE"                   ,RedisCommand::ZINTERSTORE},
        { "ZLEXCOUNT"                     ,RedisCommand::ZLEXCOUNT},
        { "ZPOPMAX"                       ,RedisCommand::ZPOPMAX},
        { "ZPOPMIN"                       ,RedisCommand::ZPOPMIN},
        { "ZRANGE"                        ,RedisCommand::ZRANGE},
        { "ZRANGEBYLEX"                   ,RedisCommand::ZRANGEBYLEX},
        { "ZREVRANGEBYLEX"                ,RedisCommand::ZREVRANGEBYLEX},
        { "ZRANGEBYSCORE"                 ,RedisCommand::ZRANGEBYSCORE},
        { "ZRANK"                         ,RedisCommand::ZRANK},
        { "ZREM"                          ,RedisCommand::ZREM},
        { "ZREMRANGEBYLEX"                ,RedisCommand::ZREMRANGEBYLEX},
        { "ZREMRANGEBYRANK"               ,RedisCommand::ZREMRANGEBYRANK},
        { "ZREMRANGEBYSCORE"              ,RedisCommand::ZREMRANGEBYSCORE},
        { "ZREVRANGE"                     ,RedisCommand::ZREVRANGE},
        { "ZREVRANGEBYSCORE"              ,RedisCommand::ZREVRANGEBYSCORE},
        { "ZREVRANK"                      ,RedisCommand::ZREVRANK},
        { "ZSCORE"                        ,RedisCommand::ZSCORE},
        { "ZUNIONSTORE"                   ,RedisCommand::ZUNIONSTORE},
        { "SCAN"                          ,RedisCommand::SCAN},
        { "SSCAN"                         ,RedisCommand::SSCAN},
        { "HSCAN"                         ,RedisCommand::HSCAN},
        { "ZSCAN"                         ,RedisCommand::ZSCAN},
        { "XADD"                          ,RedisCommand::XADD},
        { "XRANGE"                        ,RedisCommand::XRANGE},
        { "XREVRANGE"                     ,RedisCommand::XREVRANGE},
        { "XLEN"                          ,RedisCommand::XLEN},
        { "XREAD"                         ,RedisCommand::XREAD},
        { "XREADGROUP"                    ,RedisCommand::XREADGROUP},
        { "XPENDING"                      ,RedisCommand::XPENDING},
        // Responses
        { "+OK"                           ,RedisCommand::OK },
    };

    // A simple struct associating an ASCII string with a unique enumerated keyword
    struct AttributeString
    {
        AttributeString(void) { };
        AttributeString(const char *str, RedisAttribute kw) : mString(str), mAttribute(kw)
        {
        }
        const char *mString{ nullptr };
        RedisAttribute     mAttribute{ RedisAttribute::NONE };
    };

    // Table which maps keyword enum values to their corresponding ASCII string
    static AttributeString gAttributeString[] =
    {
        { "KILL",                           RedisAttribute::KILL },
        { "ID",                             RedisAttribute::ID },
        { "TYPE",                           RedisAttribute::TYPE },
        { "NOMRAL",                         RedisAttribute::NOMRAL },
        { "MASTER",                         RedisAttribute::MASTER },
        { "SLAFE",                          RedisAttribute::SLAFE },
        { "PUBSUB",                         RedisAttribute::PUBSUB },
        { "ADDR",                           RedisAttribute::ADDR },
        { "SKIPME",                         RedisAttribute::SKIPME },
        { "LIST",                           RedisAttribute::LIST },
        { "GETNAME",                        RedisAttribute::GETNAME },
        { "PAUSE",                          RedisAttribute::PAUSE },
        { "REPLY",                          RedisAttribute::REPLY },
        { "SETNAME",                        RedisAttribute::SETNAME },
        { "ADDSLOTS",                       RedisAttribute::ADDSLOTS },
        { "COUNT-FAILURE-REPORTS",          RedisAttribute::COUNT_FAILURE_REPORTS },
        { "COUNTKEYSINSLOT",                RedisAttribute::COUNTKEYSINSLOT },
        { "DELSLOTS",                       RedisAttribute::DELSLOTS },
        { "FAILOVER",                       RedisAttribute::FAILOVER },
        { "FORGET",                         RedisAttribute::FORGET },
        { "GETKEYSINSLOT",                  RedisAttribute::GETKEYSINSLOT },
        { "INFO",                           RedisAttribute::INFO },
        { "MEET",                           RedisAttribute::MEET },
        { "NODES",                          RedisAttribute::NODES },
        { "REPLICATE",                      RedisAttribute::REPLICATE },
        { "RESET",                          RedisAttribute::RESET },
        { "HARD",                           RedisAttribute::HARD },
        { "SOFT",                           RedisAttribute::SOFT },
        { "SAVECONFIG",                     RedisAttribute::SAVECONFIG },
        { "SET_CONFIG_EPOCH",               RedisAttribute::SET_CONFIG_EPOCH },
        { "SETSLOT",                        RedisAttribute::SETSLOT },
        { "SLAVES",                         RedisAttribute::SLAVES },
        { "SLOTS",                          RedisAttribute::SLOTS },
        { "COUNT",                          RedisAttribute::COUNT },
        { "GETKEYS",                        RedisAttribute::GETKEYS },
        { "GET",                            RedisAttribute::GET },
        { "REWRITE",                        RedisAttribute::REWRITE },
        { "SET",                            RedisAttribute::SET },
        { "RESETSTATE",                     RedisAttribute::RESETSTATE },
        { "OBJECT",                         RedisAttribute::OBJECT },
        { "SEGFAULT",                       RedisAttribute::SEGFAULT },
        { "ASYNC",                          RedisAttribute::ASYNC },
        { "BEFORE",                         RedisAttribute::BEFORE },
        { "AFTER",                          RedisAttribute::AFTER },
        { "DOCTOR",                         RedisAttribute::DOCTOR },
        { "HELP",                           RedisAttribute::HELP },
        { "MALLOC_STATS",                   RedisAttribute::MALLOC_STATS },
        { "PURGE",                          RedisAttribute::PURGE },
        { "STATS",                          RedisAttribute::STATS },
        { "USAGE",                          RedisAttribute::USAGE },
        { "COPY",                           RedisAttribute::COPY },
        { "REPLACE",                        RedisAttribute::REPLACE },
        { "SAMPLES",                        RedisAttribute::SAMPLES },
        { "DEBUG",                          RedisAttribute::REDIS_DEBUG },
        { "YES",                            RedisAttribute::YES },
        { "SYNC",                           RedisAttribute::SYNC },
        { "NO",                             RedisAttribute::NO },
        { "SHA1",                           RedisAttribute::SHA1 },
        { "EXISTS",                         RedisAttribute::EXISTS },
        { "FLUSH",                          RedisAttribute::FLUSH },
        { "LOAD",                           RedisAttribute::LOAD },
        { "NOSAVE",                         RedisAttribute::NOSAVE },
        { "SAVE",                           RedisAttribute::SAVE },
        { "BY",                             RedisAttribute::BY },
        { "LIMIT",                          RedisAttribute::LIMIT },
        { "ASC",                            RedisAttribute::ASC },
        { "DESC",                           RedisAttribute::DESC },
        { "ALPHA",                          RedisAttribute::ALPHA },
        { "STORE",                          RedisAttribute::STORE },
        { "WITHSCORES",                     RedisAttribute::WITHSCORES },
        { "MATCH",                          RedisAttribute::MATCH },
        { "BLOCK",                          RedisAttribute::BLOCK },
        { "GROUP",                          RedisAttribute::GROUP },
        { "STREAMS",                        RedisAttribute::STREAMS },

    };

class RedisCommandStreamImpl : public RedisCommandStream
{
public:
    RedisCommandStreamImpl(void)
    {
        // Initialize the command lookup table
        for (auto &i : gCommandString)
        {
            bool ok = mCommandTable.addStringId(i.mString, uint32_t(i.mCommand));
            if (!ok)
            {
                assert(0);
            }
        }
        // Initialize the attribute lookup table
        for (auto &i : gAttributeString)
        {
            bool ok = mAttributeTable.addStringId(i.mString, uint32_t(i.mAttribute));
            if (!ok)
            {
                assert(0);
            }
        }
        mCommandBuffer = simplebuffer::SimpleBuffer::create(MAX_COMMAND_STRING, MAX_TOTAL_MEMORY);
    }

    virtual ~RedisCommandStreamImpl(void)
    {
        if (mCommandBuffer)
        {
            mCommandBuffer->release();
        }
    }

    inline bool isWhitespace(char c)
    {
        return c == 32 || c == 9; // if it's a space or a tab, we consider it to be whitespae
    }

    const char *skipWhitespace(const char *cmd)
    {
        while (isWhitespace(*cmd))
        {
            cmd++;
        }
        return cmd;
    }

    // Add incoming data.  If we have a complete command, then it will return the command value
    // it will also set 'argc' to the number of attributes found with this command
    virtual RedisCommand addStream(const char *cmd, uint32_t &argc) override final
    {
        RedisCommand ret = RedisCommand::NONE;

        if (*cmd == '*')
        {
            // specifies expected number of arguments total...
            mExpectedArgumentCount = atoi(cmd+1); // expected number of arguments
            mArgumentCount = 0;
            if (mExpectedArgumentCount == 0)
            {
                printf("Invalid argument count! (%s)\r\n", cmd);
                assert(0);
            }
            else if (mExpectedArgumentCount > MAX_ARGS)
            {
                printf("Exceeded maximum expected argument count (%d) found (%s)\n", MAX_ARGS, cmd);
                assert(0);
            }
        }
        else if (*cmd == '$')
        {
            mExpectedArgumentLength = atoi(cmd+1);    // length we expect the argument to be...
            if (mExpectedArgumentCount == 0)
            {
                mExpectedArgumentCount = 1;
                mArguments[0].mCommand = RedisCommand::RETURN_DATA;
            }
        }
        else if (*cmd == '-')
        {
            ret = mArguments[0].mCommand = RedisCommand::ERR;
            argc = 0;
            uint32_t len = uint32_t(strlen(cmd));
            uint8_t *dest = mCommandBuffer->confirmCapacity(len + 1); // make sure there is room to store it!
            memcpy(dest, cmd, len + 1); // copy the argument with zero byte terminator
            mArguments[0].mData = dest;
            mArgumentCount = 1;
        }
        else if (*cmd == '+')
        {
            ret = mArguments[0].mCommand = getCommand(cmd);
            argc = 0;
            mArgumentCount = 1;
        }
        else if (*cmd == ':')
        {
            ret = mArguments[0].mCommand = RedisCommand::RETURN_CODE;
            argc = 0;
            uint32_t len = uint32_t(strlen(cmd+1));
            uint8_t *dest = mCommandBuffer->confirmCapacity(len + 1); // make sure there is room to store it!
            memcpy(dest, cmd+1, len + 1); // copy the argument with zero byte terminator
            mArguments[0].mData = dest;
            mArgumentCount = 1;
        }
        else
        {
            // if it doesn't begin with either an '*' or a '$' then it's an argument to process.
            uint32_t len = uint32_t(strlen(cmd));
            if (mExpectedArgumentLength)
            {
                if (mExpectedArgumentLength != len)
                {
                    printf("Unexpected!  Length of the argument was not what we expected!\r\n");
                }
                else
                {
                    if (mArgumentCount < mExpectedArgumentCount)
                    {
                        RedisArgument &arg = mArguments[mArgumentCount];
                        uint8_t *dest = mCommandBuffer->confirmCapacity(len + 1); // make sure there is room to store it!
                        // Todo..copy with escape char logic!
                        memcpy(dest, cmd, len + 1); // copy the argument with zero byte terminator
                        mCommandBuffer->addBuffer(nullptr, len + 1);
                        arg.mData = dest;
                        if (mArgumentCount == 0)
                        {
                            if (mArguments[0].mCommand == RedisCommand::RETURN_DATA)
                            {

                            }
                            else
                            {
                                arg.mCommand = getCommand(cmd);
                            }
                        }
                        else
                        {
                            arg.mAttribute = getAttribute(cmd);
                            if (arg.mAttribute == RedisAttribute::NONE)
                            {
                                arg.mAttribute = RedisAttribute::ASCIIZ;
                            }
                        }
                        arg.mDataLen = len;
                        mArgumentCount++;
                        if (mArgumentCount == mExpectedArgumentCount)
                        {
                            ret = mArguments[0].mCommand;
                            argc = mArgumentCount-1; // 
                        }
                    }
                }
            }
            else
            {
                // Ok.. we need to parse the input into a series of arguments...
                while (cmd && *cmd )
                {
                    cmd = skipWhitespace(cmd);
                    if (*cmd == 34) // if it is a quoted string..
                    {
                        assert(0); // not yet supported...
                    }
                    else
                    {
                        const char *eos = cmd + 1;
                        // Advance to either EOS or next whitespace
                        while (!isWhitespace(*eos) && *eos )
                        {
                            eos++;
                        }
                        uint32_t slen = uint32_t(eos - cmd); // length of the string we are adding...
                        uint8_t *dest = mCommandBuffer->confirmCapacity(slen + 1);
                        memcpy(dest, cmd, slen);
                        mCommandBuffer->addBuffer(nullptr, len + 1);
                        dest[slen] = 0;
                        RedisArgument &arg = mArguments[mArgumentCount];
                        arg.mData = dest;
                        if (mArgumentCount == 0)
                        {
                            arg.mCommand = getCommand((const char *)dest);
                        }
                        else
                        {
                            arg.mAttribute = getAttribute((const char *)dest);
                            if (arg.mAttribute == RedisAttribute::NONE)
                            {
                                arg.mAttribute = RedisAttribute::ASCIIZ;
                            }
                        }
                        arg.mDataLen = slen;
                        mArgumentCount++;
                        cmd = eos;
                    }
                }
                if (mArgumentCount)
                {
                    ret = mArguments[0].mCommand;
                    argc = mArgumentCount - 1; // 
                }
            }
        }

        return ret;
    }

    // Returns a pointer to a specific argument, null of it doesn't exist.
    // 'atr' is the type of attribute this is, string, binary data, or a specific known attribute type.
    // 'dataLen' is the length in bytes of this attribute.  Strings will always be zero byte terminated for
    // convenience, but binary blobs you must pay attention to the data length.
    // Note, these values are only valid up until the time you call 'addStream' again
    virtual const char * getAttribute(uint32_t index, RedisAttribute &atr, uint32_t &dataLen) override final
    {
        const char *ret = nullptr;
        atr = RedisAttribute::NONE;
        dataLen = 0;

        index++;
        if (index < mArgumentCount)
        {
            RedisArgument &arg = mArguments[index];
            ret = (const char *)arg.mData;
            dataLen = arg.mDataLen;
            atr = arg.mAttribute;
        }

        return ret;
    }


    virtual void release(void) override final
    {
        delete this;
    }

    // Convert this string into a command, 'NONE' if unknown
    virtual RedisCommand getCommand(const char *c) const override final
    {
        return RedisCommand(mCommandTable.string2Id(c));
    }

    // Convert this string into an attribute, 'NONE' if unknown
    virtual RedisAttribute getAttribute(const char *c) const override final
    {
        return RedisAttribute(mAttributeTable.string2Id(c));
    }

    // Return the ASCII string for this command
    virtual const char * getCommand(RedisCommand r) const override final
    {
        return mCommandTable.id2String(uint32_t(r));
    }

    // Return the ASCII string for this attribute
    virtual const char * getAttribute(RedisAttribute r) const override final
    {
        return mAttributeTable.id2String(uint32_t(r));
    }

    // Semaphore indicating that all of the attributes have been processed and we can reset back to initial state
    virtual void resetAttributes(void) override final
    {
        mArguments[0].mCommand = RedisCommand::NONE;
        mExpectedArgumentCount = 0;
        mArgumentCount = 0;
        mExpectedArgumentLength = 0;
    }

    virtual const char *getCommandString(uint32_t &dataLen) override final
    {
        const char *ret = nullptr;
        dataLen = 0;
        if (mArgumentCount)
        {
            dataLen = mArguments[0].mDataLen;
            ret = (const char *)mArguments[0].mData;
        }
        return ret;
    }


    uint32_t                    mExpectedArgumentCount{ 0 };    // Expected number of arguments
    uint32_t                    mArgumentCount{ 0 };            // current argument counter
    uint32_t                    mExpectedArgumentLength{ 0 };
    RedisArgument               mArguments[MAX_ARGS];           // arguments we found

    simplebuffer::SimpleBuffer	*mCommandBuffer{ nullptr };// Where pending responses are stored
    stringid::StringId          mCommandTable;				// The command table lookup
    stringid::StringId          mAttributeTable;			// The attribute table lookup
};

RedisCommandStream *RedisCommandStream::create(void)
{
    auto ret = new RedisCommandStreamImpl;
    return static_cast<RedisCommandStream *>(ret);
}


}
