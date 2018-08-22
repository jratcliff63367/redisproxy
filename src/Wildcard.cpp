#pragma warning(disable:4996)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <regex>
#include <string>

#include "Wildcard.h"

class RegularExpression;

// Performs a conventional DOS style wildcard compare, with
// ? and * notation, by converting it into a regular expression match.

namespace wildcard
{
        class WildCardImpl : public WildCard
        {
        public:
            WildCardImpl(const char *wild)
            {
                mIsWild = false;
                mWild = wild;

                // std::regex uses the EMScript standard.
                std::string expression = std::string("^");

                uint32_t len = (uint32_t)strlen(wild);

                for (uint32_t i = 0; i < len; i++)
                {
                    switch (wild[i])
                    {
                    case '?':
                        expression += '.'; // regular expression notation.
                        mIsWild = true;
                        break;
                    case '*':
                        expression += std::string(".*");
                        mIsWild = true;
                        break;
                    case '.':
                        expression += '\\';
                        expression += wild[i];
                        break;
                    case ';':
                        expression += '|';
                        mIsWild = true;
                        break;
                    default:
                        expression += wild[i];
                    }
                }

                if (mIsWild)
                {
                    expression += '$';
                    mRegEx = new std::regex(expression.c_str(), std::regex_constants::extended);
                }
                else
                {
                    mRegEx = nullptr;
                }
            }
            ~WildCardImpl(void) final
            {
                delete mRegEx;
            }

            void release(void) final
            {
                delete this;
            }

            bool isWild(void) const final
            {
                return mIsWild;
            } // see if this is a wildcard string.

            bool isMatch(const char *test) const final
            {
                bool ret = false;

                if (test)
                {
                    if (mIsWild)
                    {
                        if(std::regex_match(test, *mRegEx))
                        {
                            ret = true;
                        }
                    }
                    else
                    {
                        if (strcmp(test, mWild.c_str()) == 0)
                        {
                            ret = true;
                        }
                    }
                }
                return ret;
            }

        private:
            std::regex             *mRegEx;
            bool                    mIsWild;
            std::string             mWild;
        };

        WildCard * WildCard::create(const char *str)
        {
            WildCardImpl *ret = new WildCardImpl(str);
            return static_cast<WildCard *>(ret);
        }

} 
