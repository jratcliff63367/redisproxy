/*
 * Copyright 2009-2011 NVIDIA Corporation.  All rights reserved.
 *
 * NOTICE TO USER:
 *
 * This source code is subject to NVIDIA ownership rights under U.S. and
 * international Copyright laws.  Users and possessors of this source code
 * are hereby granted a nonexclusive, royalty-free license to use this code
 * in individual and commercial software.
 *
 * NVIDIA MAKES NO REPRESENTATION ABOUT THE SUITABILITY OF THIS SOURCE
 * CODE FOR ANY PURPOSE.  IT IS PROVIDED "AS IS" WITHOUT EXPRESS OR
 * IMPLIED WARRANTY OF ANY KIND.  NVIDIA DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOURCE CODE, INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY, NONINFRINGEMENT, AND FITNESS FOR A PARTICULAR PURPOSE.
 * IN NO EVENT SHALL NVIDIA BE LIABLE FOR ANY SPECIAL, INDIRECT, INCIDENTAL,
 * OR CONSEQUENTIAL DAMAGES, OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS,  WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
 * OR OTHER TORTIOUS ACTION,  ARISING OUT OF OR IN CONNECTION WITH THE USE
 * OR PERFORMANCE OF THIS SOURCE CODE.
 *
 * U.S. Government End Users.   This source code is a "commercial item" as
 * that term is defined at  48 C.F.R. 2.101 (OCT 1995), consisting  of
 * "commercial computer  software"  and "commercial computer software
 * documentation" as such terms are  used in 48 C.F.R. 12.212 (SEPT 1995)
 * and is provided to the U.S. Government only as a commercial end item.
 * Consistent with 48 C.F.R.12.212 and 48 C.F.R. 227.7202-1 through
 * 227.7202-4 (JUNE 1995), all U.S. Government End Users acquire the
 * source code with only those rights set forth herein.
 *
 * Any use of this source code in individual and commercial software must
 * include, in the user documentation and internal comments to the code,
 * the above Disclaimer and U.S. Government End Users Notice.
 */

#ifndef INPARSER_H
#define INPARSER_H

#define MAXARGS 512

#include <stdint.h>

/** @file inparser.h
 * @brief        Parse ASCII text, in place, very quickly.
 *
 * This class provides for high speed in-place (destructive) parsing of an ASCII text file.
 * This class will either load an ASCII text file from disk, or can be constructed with a pointer to
 * a piece of ASCII text in memory.  It can only be called once, and the contents are destroyed.
 * To speed the process of parsing, it simply builds pointers to the original ascii data and replaces the
 * separators with a zero byte to indicate end of string.  It performs callbacks to parse each line, in argc/argv format,
 * offering the option to cancel the parsing process at any time.
 *
 *
 * By default the only valid separator is whitespace.  It will not treat commas or any other symbol as a separator.
 * You can specify any character to be  a 'hard' separator, such as an '=' for example and that will come back as a
 * distinct argument string.
 *
 * To use the parser simply inherit the pure virtual base class 'InPlaceParserInterface'.  Define the method 'ParseLine'.
 * When you invoke the Parse method on the InPlaceParser class, you will get an ARGC - ARGV style callback for each line
 * in the source file.  If you return 'false' at any time, it will abort parsing.  The entire thing is stack based, so you
 * can recursively call multiple parser instances.
 *
 * It is important to note.  Since this parser is 'in place' it writes 'zero bytes' (EOS marker) on top of the whitespace.
 * While it can handle text in quotes, it does not handle escape sequences.  This is a limitation which could be resolved.
 * There is a hard coded maximum limit of 512 arguments per line.
 *
 * Here is the full example usage:
 *
 *  InPlaceParser ipp("parse_me.txt");
 *
 *    ipp.Parse(this);
 *
 *  That's it, and you will receive an ARGC - ARGV callback for every line in the file.
 *
 *  If you want to parse some text in memory of your own. (It *MUST* be terminated by a zero byte, and lines seperated by carriage return
 *  or line-feed.  You will receive an assertion if it does not.  If you specify the source data than *you* are responsible for that memory
 *  and must de-allocate it yourself.  If the data was loaded from a file on disk, then it is automatically de-allocated by the InPlaceParser.
 *
 *  You can also construct the InPlaceParser without passing any data, so you can simply pass it a line of data at a time yourself.  The
 *  line of data should be zero-byte terminated.
*/


namespace IN_PARSER
{
class InPlaceParserInterface
{
public:

	virtual int32_t ParseLine(uint32_t lineno,uint32_t argc,const char **argv) =0;  // return TRUE to continue parsing, return FALSE to abort parsing process

	virtual bool preParseLine(uint32_t /* lineno */,const char * /* line */)  
	{ 
		return false; 
	} // optional chance to pre-parse the line as raw data.  If you return 'true' the line will be skipped assuming you snarfed it.
protected:
	virtual ~InPlaceParserInterface(void) {}
};

enum SeparatorType
{
	ST_DATA,        // is data
	ST_HARD,        // is a hard separator
	ST_SOFT,        // is a soft separator
	ST_EOS,          // is a comment symbol, and everything past this character should be ignored
	ST_LINE_FEED
};

class InPlaceParser
{
public:
	InPlaceParser(void)
	{
		Init();
	}

	InPlaceParser(char *data,int32_t len)
	{
		Init();
		SetSourceData(data,len);
	}

	InPlaceParser(const char *fname)
	{
		Init();
		SetFile(fname);
	}

	~InPlaceParser(void);

	void Init(void)
	{
		mQuoteChar = 34;
		mData = 0;
		mLen  = 0;
		mMyAlloc = false;
		for (int32_t i=0; i<256; i++)
		{
			mHard[i] = ST_DATA;
			mHardString[i*2] = (char)i;
			mHardString[i*2+1] = 0;
		}
		mHard[0]  = ST_EOS;
		mHard[32] = ST_SOFT;
		mHard[9]  = ST_SOFT;
		mHard[13] = ST_LINE_FEED;
		mHard[10] = ST_LINE_FEED;
	}

	void SetFile(const char *fname);

	void SetSourceData(char *data,uint32_t len)
	{
		mData = data;
		mLen  = len;
		mMyAlloc = false;
	}

	int32_t  Parse(const char *str,InPlaceParserInterface *callback); // returns true if entire file was parsed, false if it aborted for some reason
	int32_t  Parse(InPlaceParserInterface *callback); // returns true if entire file was parsed, false if it aborted for some reason

	int32_t ProcessLine(int32_t lineno,char *line,InPlaceParserInterface *callback);

	const char ** GetArglist(char *source,int32_t &count); // convert source string into an arg list, this is a destructive parse.

	void SetHardSeparator(char c) // add a hard separator
	{
		mHard[(unsigned char)c] = ST_HARD;
	}

	void SetHard(char c) // add a hard separator
	{
		mHard[(unsigned char)c] = ST_HARD;
	}

	void SetSoft(char c) // add a hard separator
	{
		mHard[(unsigned char)c] = ST_SOFT;
	}


	void SetCommentSymbol(char c) // comment character, treated as 'end of string'
	{
		mHard[(unsigned char)c] = ST_EOS;
	}

	void ClearHardSeparator(char c)
	{
		mHard[(unsigned char)c] = ST_DATA;
	}


	void DefaultSymbols(void); // set up default symbols for hard separator and comment symbol of the '#' character.

	inline bool EOS(char c)
	{
		if ( mHard[(unsigned char)c] == ST_EOS )
		{
			return true;
		}
		return false;
	}

	void SetQuoteChar(char c)
	{
		mQuoteChar = c;
	}

	bool HasData( void ) const
	{
		return ( mData != 0 );
	}

  void setLineFeed(char c)
  {
    mHard[(unsigned char)c] = ST_LINE_FEED;
  }

  bool isLineFeed(char c)
  {
    if ( mHard[(unsigned char)c] == ST_LINE_FEED ) return true;
    return false;
  }

  bool IsHardSeparator(char c)
  {
	  return mHard[(unsigned char)c] == ST_HARD;
  }

  void setMyAlloc(bool state)
  {
	  mMyAlloc = state;
  }

private:

	inline char * AddHard(int32_t &argc,const char **argv,char *foo);
	inline bool   IsHard(char c);
	inline char * SkipSpaces(char *foo);
	inline bool   IsWhiteSpace(char c);
	inline bool   IsNonSeparator(char c); // non separator,neither hard nor soft

	bool			mMyAlloc=false; // whether or not *I* allocated the buffer and am responsible for deleting it.
    char			*mData{ nullptr };  // ascii data to parse.
    uint32_t		mLen{ 0 };   // length of data
	SeparatorType	mHard[256];
	char			mHardString[256*2];
	char			mQuoteChar;
	const char		*mArgv[MAXARGS];
    char            *mStringBuffer{ nullptr };
    uint32_t         mStringLen{ 0 };
};

}

#endif
