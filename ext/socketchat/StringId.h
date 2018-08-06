#pragma once

#include <unordered_map>

namespace stringid
{

typedef std::unordered_map< uint32_t, uint32_t > String2Id;
typedef std::unordered_map< uint32_t, const char * > Id2String;

inline uint32_t computeStringHash(const char* _string)
{
    // "DJB" string hash
    const uint8_t* string = (const uint8_t*)_string;
    uint32_t h = 5381;
    for (const uint8_t* ptr = string; *ptr; ptr++)
    {
        uint32_t c = *ptr;
        h = ((h << 5) + h) ^ c;
    }
    return h;
}

inline uint32_t computeStringHashCaseInsensitve(const char* _string)
{
    // "DJB" string hash
    const uint8_t* string = (const uint8_t*)_string;
    uint32_t h = 5381;
    for (const uint8_t* ptr = string; *ptr; ptr++)
    {
        uint32_t c = *ptr;
        if (c >= 'A' && c <= 'Z')
        {
            c += 32;    // lower case
        }
        h = ((h << 5) + h) ^ c;
    }
    return h;
}

// Simple helper class which will do a string lookup for an integer
// reasonably fast.
// does not support hash collisions!
class StringId
{
public:
    // Add a string to id lookup; 'str' is assumed to be persistent if you 
    // want to do a reverse lookup from ID back to original string!
    // id cannot be zero!
    bool addStringId(const char *str, uint32_t id)
    {
        bool ret = false;

        uint32_t hash = computeStringHashCaseInsensitve(str);
        auto const &found = mString2Id.find(hash);
        if (found == mString2Id.end())
        {
            mString2Id[hash] = id;
            mId2String[id] = str;
            ret = true;
        }
        else
        {
            // We have a hash collision! Either the same string twice or, on an extremely rare
            // case an actual duplicate hash.
        }

        return ret;
    }

    // Look up this string to it's corresponding ID
    uint32_t string2Id(const char *str) const
    {
        uint32_t ret = 0;
        uint32_t hash = computeStringHashCaseInsensitve(str);
        auto const &found = mString2Id.find(hash);
        if (found != mString2Id.end())
        {
            ret = (*found).second;
        }
        return ret;
    }

    // Get the string associated with this id
    const char *id2String(uint32_t id) const
    {
        const char *ret = nullptr;

        auto const &found = mId2String.find(id);
        if (found != mId2String.end() )
        {
            ret = (*found).second;
        }

        return ret;
    }



private:
    String2Id   mString2Id;
    Id2String   mId2String;
};

}
