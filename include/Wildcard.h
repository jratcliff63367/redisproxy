#pragma  once

namespace wildcard
{
        class WildCard
        {
        public:
            static WildCard *create(const char *str);
            virtual bool    isWild(void) const = 0;
            virtual bool    isMatch(const char *str) const = 0;

            virtual void release(void) = 0;
        protected:
            virtual ~WildCard(void) = default;
        };


} 