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

        std::mutex      mMutex;
        KeyValueMap mDatabase;
    };

KeyValueDatabase *KeyValueDatabase::create(void)
{
    auto ret = new KeyValueDatabaseImpl;
    return static_cast<KeyValueDatabase *>(ret);
}


}

