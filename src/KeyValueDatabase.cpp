#include "KeyValueDatabase.h"
#include <mutex>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unordered_map>
#include <assert.h>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

namespace keyvaluedatabase
{

    class DataBlock
    {
    public:
        DataBlock * mNext{ nullptr };
        uint32_t    mDataLen{ 0 };
        void        *mData{ nullptr };
    };


    class Value
    {
    public:
        Value(const void *data, uint32_t dlen,bool isList)
        {
            mIsList = isList;
            getDataBlock(data, dlen);
        }

        ~Value(void)
        {
            releaseDataBlocks();
        }

        bool isInteger(void) const
        {
            bool ret = false;

            if (mRoot)
            {
                const char *cptr = (const char *)mRoot->mData;
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

            if (mRoot && mRoot->mDataLen < 511)
            {
                char scratch[512];
                memcpy(scratch, mRoot->mData, mRoot->mDataLen);
                scratch[mRoot->mDataLen] = 0;
                ret = atoi(scratch);
            }

            return ret;
        }

        void newData(const void *data, uint32_t dlen)
        {
            releaseDataBlocks();
            getDataBlock(data, dlen);
        }

        uint32_t push(const void *data, uint32_t dlen)
        {
            getDataBlock(data, dlen);
            return mBlockCount;
        }

        void releaseDataBlocks(void)
        {
            DataBlock *db = mRoot;
            while (db)
            {
                DataBlock *next = db->mNext;
                free(db);
                db = next;
            }
            mRoot = nullptr;
            mBlockCount = 0;
        }

        DataBlock *getDataBlock(const void *data, uint32_t dataLen)
        {
            DataBlock *db = (DataBlock *)malloc(sizeof(DataBlock) + dataLen);
            new (db) DataBlock;
            db->mData = db + 1;
            db->mDataLen = dataLen;
            db->mNext = nullptr;
            memcpy(db->mData, data, dataLen);
            if (mRoot)
            {
                DataBlock *previous = mRoot;
                while (previous && previous->mNext )
                {
                    previous = previous->mNext;
                }
                previous->mNext = db;
            }
            else
            {
                mRoot = db;
            }
            mBlockCount++;
            return db;
        }

        bool isList(void) const
        {
            return mIsList;
        }

        uint32_t getBlockCount(void) const
        {
            return mBlockCount;
        }

        bool        mIsList{ false };
        uint32_t    mBlockCount{ 0 };
        DataBlock   *mRoot{ nullptr };
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


        virtual void del(const char *_key, void *userPointer,KVD_returnCodeCallback callback) override final
        {
            bool ret = false;
            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found != mDatabase.end())
            {
                Value *v = (*found).second;
                delete v;
                mDatabase.erase(found);
                ret = true;
            }
            unlock();
            if (callback)
            {
                (*callback)(ret ? 1 : 0, userPointer);
            }
        }

        virtual void exists(const char *_key,void *userPointer, KVD_returnCodeCallback callback) override final
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
            if (callback)
            {
                (*callback)(ret ? 1 : 0, userPointer);
            }
        }

        virtual void get(const char *_key,void *userPointer, KVD_dataCallback callback) override final
        {
            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found != mDatabase.end())
            {
                Value *v = found->second;
                if (v->mRoot)
                {
                    (*callback)(userPointer, v->mRoot->mData, v->mRoot->mDataLen);
                }
                else
                {
                    (*callback)(userPointer, nullptr, 0);
                }
            }
            else
            {
                (*callback)(userPointer, nullptr, 0);
            }
            unlock();
        }

        // append to an existing or new record; returns length of the list
        virtual void push(const char *_key, const void *data, uint32_t dataLen, void *userPointer, KVD_returnCodeCallback callback) override final
        {
            int32_t ret = -1;
            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found == mDatabase.end())
            {
                Value *v = new Value(data, dataLen,true);
                mDatabase[key] = v;
                ret = 1;
            }
            else
            {
                Value *v = found->second;
                if (v->mIsList)
                {
                    ret = v->push(data, dataLen);
                    mDatabase[key] = v;
                }
            }
            unlock();

            (*callback)(ret, userPointer);

        }

        virtual void set(const char *_key, const void *data, uint32_t dataLen, void *userPointer, KVD_standardCallback callback) override final
        {
            lock();
            std::string key(_key);
            const auto &found = mDatabase.find(key);
            if (found == mDatabase.end())
            {
                Value *v = new Value(data, dataLen,false);
                mDatabase[key] = v;
            }
            else
            {
                Value *v = found->second;
                v->newData(data, dataLen);
            }
            unlock();
            (*callback)(true, userPointer);
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
                Value *vstore = new Value(scratch, uint32_t(strlen(scratch)),false);
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

        bool isList(const char *key)
        {
            bool ret = false;

            lock();

            const auto &found = mDatabase.find(std::string(key));
            if (found != mDatabase.end())
            {
                Value *v = found->second;
                ret = v->mIsList;
            }

            unlock();

            return ret;
        }

        // not use fully implemented
        virtual void watch(const char *key) override final
        {
            // TODO
        }

        virtual void unwatch(const char *key) override final
        {
            // TODO
        }

        // Give up a timeslice to the database system
        virtual void pump(void) override final
        {

        }

        virtual void select(uint32_t index, void *userPointer, KVD_standardCallback callback) override final
        {
            assert(callback);
            if (!callback) return;
            if (index == 0)
            {
                (*callback)(true, userPointer);
            }
            else
            {
                (*callback)(false, userPointer);
            }
        }


        std::mutex      mMutex;
        KeyValueMap mDatabase;
    };

KeyValueDatabase *createKeyValueDatabaseRedis(void);

KeyValueDatabase *KeyValueDatabase::create(Provider p)
{
    if (p == IN_MEMORY)
    {
        auto ret = new KeyValueDatabaseImpl;
        return static_cast<KeyValueDatabase *>(ret);
    }
    return createKeyValueDatabaseRedis();
}


}

