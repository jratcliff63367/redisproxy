#pragma once

#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "itoa_jeaiii.h"

#ifdef _MSC_VER
#ifndef snprintf
#define snprintf _snprintf_s
#endif
#pragma warning(push)
#pragma warning(disable:4100)
#endif

namespace memorystream
{

class MemoryStream
{
public:
	void init(uint8_t *memoryStream, uint32_t maxLen)
	{
		mMemoryStream = memoryStream;
		mMemoryWrite = mMemoryStream;
		mMemoryEOF = mMemoryStream + maxLen;
	}

	// Write this value to the end of the current stream
	template<typename data>
	inline void write(data v)
	{
		assert(mMemoryStream);
		assert(mMemoryWrite);
		assert(mMemoryEOF);
		assert(mMemoryWrite < mMemoryEOF);
		assert((mMemoryWrite + sizeof(v)) < mMemoryEOF);
		data *dest = (data *)mMemoryWrite;
		*dest = v;
		mMemoryWrite += sizeof(v);
	}

	// Write this value using an offset from the start of the stream
	template<typename data>
	inline void writeOffset(data v,uint32_t offset)
	{
		uint8_t *memoryWrite = mMemoryStream + offset;
		assert(mMemoryStream);
		assert(mMemoryEOF);
		assert(memoryWrite < mMemoryEOF);
		assert((memoryWrite + sizeof(v)) < mMemoryEOF);
		data *dest = (data *)memoryWrite;
		*dest = v;
	}

	inline void writeString(const char *str)
	{
		assert(mMemoryStream);
		assert(mMemoryWrite);
		assert(mMemoryEOF);
		assert(mMemoryWrite < mMemoryEOF);
		if (str == nullptr) str = "";
#ifdef _DEBUG
		size_t slen = strlen(str) + 1;
		assert((mMemoryWrite + slen) < mMemoryEOF);
#endif
		const uint8_t *scan = (const uint8_t *)str;
		while (*scan)
		{
			*mMemoryWrite++ = *scan++;
		}
	}

	// All of the stream operators convert the input to ASCII and write it to the stream.
	// If you want to send raw values use the 'write' command instead
	inline MemoryStream& operator<<(bool v)
	{
		writeString(v ? "1" : "0");
		return *this;
	}

	inline MemoryStream& operator<<(char c)
	{
		write(c); // just write this individual char as is
		return *this;
	}

	inline MemoryStream& operator<<(uint8_t v)
	{
		char scratch[512];
		u32toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(int8_t v)
	{
		char scratch[512];
		i32toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}


	inline MemoryStream& operator<<(const char *c)
	{
		writeString(c);
		return *this;
	}

	inline MemoryStream& operator<<(int64_t v)
	{
		char scratch[512];
		i64toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(uint64_t v)
	{
		char scratch[512];
		u64toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(double v)
	{
		char scratch[512];
		snprintf(scratch, 512, "%f", v);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(float v)
	{
		char scratch[512];
		snprintf(scratch, 512, "%f", double(v));
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(uint32_t v)
	{
		char scratch[512];
		u32toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(int32_t v)
	{
		char scratch[512];
		i32toa_jeaiii(v,scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(uint16_t v)
	{
		char scratch[512];
		u32toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline MemoryStream& operator<<(int16_t v)
	{
		char scratch[512];
		i32toa_jeaiii(v, scratch);
		writeString(scratch);
		return *this;
	}

	inline uint32_t size(void) const
	{
		return uint32_t(mMemoryWrite - mMemoryStream);
	}

	inline uint32_t available(void) const
	{
		return uint32_t(mMemoryEOF - mMemoryStream);
	}

	inline uint8_t *getMemoryStream(void) const
	{
		return mMemoryStream;
	}

	inline uint8_t *getMemoryEOF(void) const
	{
		return mMemoryEOF;
	}

	inline uint8_t *getMemoryWrite(uint32_t writeLen) const
	{
		assert(&mMemoryWrite[writeLen] < mMemoryEOF);
		return mMemoryWrite;
	}

private:
	uint8_t		*mMemoryStream{ nullptr };
	uint8_t		*mMemoryEOF{ nullptr };
	uint8_t		*mMemoryWrite{ nullptr };
};

}

#ifdef _MSC_VER
#pragma warning(pop)
#endif