#include "KeyValueDatabase.h"
#include <mutex>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unordered_map>

namespace keyvaluedatabase
{

    class Value
    {
    public:
        Value(const void *data, uint32_t dlen)
        {
            mData = malloc(dlen);
            mDataLen = dlen;
            memcpy(mData, data, dlen);
        }

        ~Value(void)
        {
            delete mData;
        }

        bool isInteger(void) const
        {
            bool ret = false;

            if (mDataLen && mData)
            {
                const char *cptr = (const char *)mData;
                char c = *cptr;
                if ((c >= '0' && c <= '9') || c == '+' || c == '-')
                {
                    ret = true;
                }
            }
            return ret;
        }

        int32_t getInteger(void) const
        {
            int32_t ret = 0;

            if (mData && mDataLen < 511)
            {
                char scratch[512];
                memcpy(scratch, mData, mDataLen);
                scratch[mDataLen] = 0;
                ret = atoi(scratch);
            }

            return ret;
        }

        void newData(const void *data, uint32_t dlen)
        {
            free(mData);
            mData = malloc(dlen);
            memcpy(mData, data, dlen);
            mDataLen = dlen;
        }

        uint32_t    mDataLen{ 0 };
        void        *mData{ nullptr };
    };

    typedef std::unordered_map< std::string, Value * > KeyValueMap;

    class KeyValueDatabaseImpl : public KeyValueDatabase
    {
    public:
        KeyValueDatabaseImpl(void)
        {

        }

        virtual ~KeyValueDatabaseImpl(void)
        {
            for (auto &i : mDatabase)
            {
                Value *v = i.second;
                delete v;
            }
        }

        virtual bool exists(const char *_key) override final
        {
            bool ret = false;

            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found != mDatabase.end())
            {
                ret = true;
            }
            unlock();

            return ret;
        }

        virtual void *get(const char *_key, uint32_t &dataLen) override final
        {
            void *ret = nullptr;
            dataLen = 0;

            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found != mDatabase.end())
            {
                Value *v = found->second;
                ret = malloc(v->mDataLen);
                memcpy(ret, v->mData, v->mDataLen);
                dataLen = v->mDataLen;
            }
            unlock();

            return ret;
        }

        virtual void releaseGetMem(void *mem) override final
        {
            free(mem);
        }

        virtual void set(const char *_key, const void *data, uint32_t dataLen) override final
        {
            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found == mDatabase.end())
            {
                Value *v = new Value(data, dataLen);
                mDatabase[key] = v;
            }
            else
            {
                Value *v = found->second;
                delete v;
                v = new Value(data, dataLen);
                mDatabase[key] = v;
            }
            unlock();
        }

        virtual void release(void) override final
        {
            delete this;
        }

        void lock(void)
        {
            mMutex.lock();
        }

        void unlock(void)
        {
            mMutex.unlock();
        }

        virtual bool isInteger(const char *key) override final
        {
            bool ret = false;

            const auto &found = mDatabase.find(std::string(key));
            if (found != mDatabase.end())
            {
                ret = found->second->isInteger();
            }

            return ret;
        }

        virtual int32_t increment(const char *key,int32_t v) override final
        {
            int32_t ret = 0;

            lock();

            const auto &found = mDatabase.find(std::string(key));
            if (found == mDatabase.end())
            {
                char scratch[512];
                snprintf(scratch, 512, "%d", v);
                Value *vstore = new Value(scratch, uint32_t(strlen(scratch)));
                mDatabase[std::string(key)] = vstore;
                ret = v;
            }
            else
            {
                Value *vv = found->second;
                if (vv->isInteger())
                {
                    int32_t cv = vv->getInteger();
                    ret = cv + v;
                    char scratch[512];
                    snprintf(scratch, 512, "%d", ret);
                    vv->newData(scratch, uint32_t(strlen(scratch)));
                }
            }
            unlock();

            return ret;
        }

        std::mutex      mMutex;
        KeyValueMap mDatabase;
    };

KeyValueDatabase *KeyValueDatabase::create(void)
{
    auto ret = new KeyValueDatabaseImpl;
    return static_cast<KeyValueDatabase *>(ret);
}


}

