
#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#include "socketchat.h"
#include "InputLine.h"
#include "wplatform.h"
#include "InParser.h"
#include "Timer.h"

#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

#define PORT_NUMBER 6379    // Redis port number

typedef std::vector< std::string > StringVector;

class ReceiveData : public socketchat::SocketChatCallback
{
public:
	virtual void receiveMessage(const char *message) override final
	{
        printf("Received: %s\r\n", message);
	}
};

class SendFile : public IN_PARSER::InPlaceParserInterface
{
public:
    SendFile(const char *fname, socketchat::SocketChat *ws) : mSocketChat(ws)
    {
        IN_PARSER::InPlaceParser ipp;
        ipp.SetFile(fname);
        ipp.Parse(this);
    }


    virtual int32_t ParseLine(uint32_t lineno, uint32_t argc, const char **argv)  override final
    {
        return 0;
    }

    virtual bool preParseLine(uint32_t /* lineno */, const char * line) override final
    {
        mStrings.push_back(std::string(line));
        return true;
    }

    bool run(void)
    {
        bool ret = false;
        if (mIndex < mStrings.size())
        {
            const char *str = mStrings[mIndex].c_str();
            printf("Sending: %s\r\n", str);
            mSocketChat->sendText(str);
            timer::Timer t;
            while (t.peekElapsedSeconds() < 0.1)
            {
                wplatform::sleepNano(1000);
            }
            mIndex++;
            ret = true;
        }
        return ret;
    }


    uint32_t                mIndex{ 0 };
    StringVector            mStrings;

    socketchat::SocketChat *mSocketChat{ nullptr };
};

int main(int argc,const char **argv)
{
    uint32_t portNumber = PORT_NUMBER;
	const char *host = "localhost";
	if (argc == 2)
	{
		host = argv[1];
	}
	{
		socketchat::socketStartup();
        socketchat::SocketChat *ws = socketchat::SocketChat::create(host,portNumber);
		if (ws)
		{
			printf("Type: 'bye' or 'quit' or 'exit' to close the client out.\r\n");
			inputline::InputLine *inputLine = inputline::InputLine::create();
			ReceiveData rd;
			bool keepRunning = true;
            SendFile *sf = nullptr;
			while (keepRunning)
			{
                if (sf)
                {
                    bool running = sf->run();
                    if (!running)
                    {
                        printf("Finished running script\r\n");
                        delete sf;
                        sf = nullptr;
                    }
                }
                else
                {
                    const char *data = inputLine->getInputLine();
                    if (data)
                    {
                        if (strcmp(data, "bye") == 0 || strcmp(data, "exit") == 0 || strcmp(data, "quit") == 0)
                        {
                            keepRunning = false;
                        }
                        else if (strcmp(data, "file1") == 0)
                        {
                            sf = new SendFile("f:\\logfile1.txt", ws);
                        }
                        else if (strcmp(data, "file2") == 0)
                        {
                            sf = new SendFile("f:\\logfile2.txt", ws);
                        }
                        else if (strcmp(data, "file3") == 0)
                        {
                            sf = new SendFile("f:\\logfile3.txt", ws);
                        }
                        else if (strcmp(data, "file4") == 0)
                        {
                            sf = new SendFile("f:\\logfile4.txt", ws);
                        }
                        else
                        {
                            ws->sendText(data);
                        }
                    }
                }
				ws->poll(&rd, 1); // poll the socket connection
			}
			delete ws;
		}
		socketchat::socketShutdown();
	}
}
