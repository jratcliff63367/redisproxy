
#include "socketchat.h"
#include "wsocket.h"
#include "InputLine.h"
#include "RedisProxy.h"
#include "KeyValueDatabase.h"
#include "InParser.h"
#include "Timer.h"
#include "wplatform.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include <vector>

#ifdef _MSC_VER
#pragma warning(disable:4100)
#endif

#define USE_MONITOR 1

keyvaluedatabase::KeyValueDatabase::Provider gProvider = keyvaluedatabase::KeyValueDatabase::Provider::REDIS;

typedef std::vector< std::string > StringVector;

//#define PORT_NUMBER 6379    // Redis port number
#define PORT_NUMBER 3010    // test port number

using socketchat::SocketChat;


class SendFile : public IN_PARSER::InPlaceParserInterface, public redisproxy::RedisProxy::Callback
{
public:
    SendFile(void)
    {
#if !USE_MONITOR
        mDatabase = keyvaluedatabase::KeyValueDatabase::create(gProvider);
        mRedisProxy = redisproxy::RedisProxy::create(mDatabase);
#else
        mRedisProxy = redisproxy::RedisProxy::createMonitor();
#endif
    }

    virtual ~SendFile(void)
    {
        if (mDatabase)
        {
            mDatabase->release();
        }
        if (mRedisProxy)
        {
            mRedisProxy->release();
        }
    }

    void sendFile(const char *fname)
    {
        IN_PARSER::InPlaceParser ipp;
        ipp.SetFile(fname);
        ipp.Parse(this);
    }

    void sendCommand(const char *cmd)
    {
        mStrings.push_back(std::string(cmd));
    }

    virtual int32_t ParseLine(uint32_t lineno, uint32_t argc, const char **argv)  override final
    {
        return 0;
    }

    virtual bool preParseLine(uint32_t /* lineno */, const char * line) override final
    {
#if 0
        char scratch[512];
        snprintf(scratch, 512, "del \"%s\"", line);
        mStrings.push_back(std::string(scratch));
#else
        mStrings.push_back(std::string(line));
#endif
        return true;
    }

    bool run(void)
    {
        if (mIndex < mStrings.size())
        {
            const char *str = mStrings[mIndex].c_str();
            printf("Sending: %s\r\n", str);
            mRedisProxy->fromClient(str);
            timer::Timer t;
            while (t.peekElapsedSeconds() < 0.1)
            {
                wplatform::sleepNano(1000);
                mRedisProxy->getToClient(this);
            }
            mIndex++;
        }
        mRedisProxy->getToClient(this);
        return true;
    }

    virtual void receiveRedisMessage(const char *msg) override final
    {
#if 0
        static uint32_t gCount = 0;
        gCount++;
        if ( (gCount%1000) == 0 )
        {
            printf("Received: %d message from Redis\r\n", gCount);
        }
#else
        printf("FromRedis:%s\r\n", msg);
#endif
    }

    virtual void receiveRedisMessage(const void *msg, uint32_t dataLen)
    {
#if 0
        static uint32_t gCount = 0;
        gCount++;
        if ((gCount % 1000) == 0)
        {
            printf("Received: %d message from Redis\r\n", gCount);
        }
#elise
        printf("FromRedis:");
        const uint8_t *scan = (const uint8_t *)msg;
        for (uint32_t i = 0; i < dataLen; i++)
        {
            uint8_t c = scan[i];
            if (c >= 32 && c <= 127)
            {
                printf("%c", c);
            }
            else
            {
                printf("$%02X", c);
            }
        }
        printf("\r\n");
#endif
    }

    uint32_t                            mIndex{ 0 };
    StringVector                        mStrings;
    keyvaluedatabase::KeyValueDatabase  *mDatabase{ nullptr };
    redisproxy::RedisProxy              *mRedisProxy{ nullptr };
};

class ClientConnection : public socketchat::SocketChatCallback, public redisproxy::RedisProxy::Callback
{
public:
	ClientConnection(wsocket::Wsocket *client,uint32_t id,keyvaluedatabase::KeyValueDatabase *dataBase) : mId(id), mDatabase(dataBase)
	{
		mClient = socketchat::SocketChat::create(client);
#if !USE_MONITOR
        mRedisProxy = redisproxy::RedisProxy::create(mDatabase);
#else
        mRedisProxy = redisproxy::RedisProxy::createMonitor();
#endif
	}

	virtual ~ClientConnection(void)
	{
		delete mClient;
        if (mRedisProxy)
        {
            mRedisProxy->release();
        }
	}

	uint32_t getId(void) const
	{
		return mId;
	}

    virtual void receiveRedisMessage(const char *msg) override final
    {
        printf("Sending: %s\r\n", msg);
        mClient->sendText(msg);
    }

    virtual void receiveRedisMessage(const void *data, uint32_t dataLen) override final
    {
        printf("Sending:");
        const uint8_t *scan = (const uint8_t *)data;
        for (uint32_t i = 0; i < dataLen; i++)
        {
            uint8_t c = scan[i];
            if (c >= 32 && c <= 127)
            {
                printf("%c", c);
            }
            else
            {
                printf("$%02X", c);
            }
        }
        printf("\r\n");
        mClient->sendBinary(data, dataLen);
    }

    void pump(void)
	{
		if (mClient)
		{
            mRedisProxy->getToClient(this);
			mClient->poll(this, 1);
		}
	}

	void sendText(const char *str)
	{
		if (mClient)
		{
			mClient->sendText(str);
		}
	}

	virtual void receiveMessage(const char *message) override final
	{
        mRedisProxy->fromClient(message);
	}


    virtual void receiveBinaryMessage(const void *data, uint32_t dataLen) override final
    {
        mRedisProxy->fromClient(data, dataLen);
    }

	bool isConnected(void)
	{
		bool ret = false;

		if (mClient && mClient->getReadyState() != socketchat::SocketChat::CLOSED)
		{
			ret = true;
		}

		return ret;
	}

	socketchat::SocketChat	            *mClient{ nullptr };
	uint32_t				            mId{ 0 };
    redisproxy::RedisProxy              *mRedisProxy{ nullptr };
    keyvaluedatabase::KeyValueDatabase  *mDatabase{ nullptr };
};

typedef std::vector< ClientConnection * > ClientConnectionVector;

class SimpleServer : public redisproxy::RedisProxy::Callback
{
public:
	SimpleServer(void)
	{
		mServerSocket = wsocket::Wsocket::create(SOCKET_SERVER, PORT_NUMBER);
		mInputLine = inputline::InputLine::create();
        mDatabase = keyvaluedatabase::KeyValueDatabase::create(gProvider);
#if USE_MONITOR
        mRedisProxy = redisproxy::RedisProxy::createMonitor();
#else
        mRedisProxy = redisproxy::RedisProxy::create(mDatabase);
#endif
		printf("Redis proxy server started.\r\n");
		printf("Type 'bye', 'quit', or 'exit' to stop the server.\r\n");
		printf("Type anything else to send as a broadcast message to all current client connections.\r\n");
	}

	~SimpleServer(void)
	{
        if (mRedisProxy)
        {
            mRedisProxy->release();
        }
		if (mInputLine)
		{
			mInputLine->release();
		}
		for (auto &i : mClients)
		{
			delete i;
		}
		if (mServerSocket)
		{
			mServerSocket->release();
		}
        if (mDatabase)
        {
            mDatabase->release();
        }
	}

	void run(void)
	{
		bool exit = false;

        SendFile *sf = nullptr;
		while (!exit)
		{
			if (mServerSocket)
			{
				wsocket::Wsocket *clientSocket = mServerSocket->pollServer();

				if (clientSocket)
				{
					uint32_t index = uint32_t(mClients.size()) + 1;
					ClientConnection *cc = new ClientConnection(clientSocket, index,mDatabase);
					printf("New client connection (%d) established.\r\n", index);
					mClients.push_back(cc);
				}
			}

			if (mInputLine)
			{
				const char *str = mInputLine->getInputLine();
				if (str)
				{
					if (strcmp(str, "bye") == 0 ||
						strcmp(str,"quit") == 0 ||
						strcmp(str,"exit") == 0 )
					{
						exit = true;
					}
                    else if (strcmp(str, "kill") == 0)
                    {
                        if (sf == nullptr)
                        {
                            sf = new SendFile;
                        }
                        sf->sendFile("f:\\kill.txt");
                    }
                    else if (strcmp(str, "test") == 0)
                    {
                        if (sf == nullptr)
                        {
                            sf = new SendFile;
                        }
                        sf->sendFile("f:\\test.txt");
                    }
                    else if (strcmp(str, "file1") == 0)
                    {
                        if (sf == nullptr)
                        {
                            sf = new SendFile;
                        }
                        sf->sendFile("f:\\logfile1.txt");
                    }
                    else
                    {
                        mRedisProxy->fromClient(str);
                    }
				}
			}

            if (sf)
            {
                sf->run();
            }

            mRedisProxy->getToClient(this);

			// See if any clients have dropped connection
			bool killed = true;
			while (killed)
			{
				killed = false;
				for (ClientConnectionVector::iterator i = mClients.begin(); i != mClients.end(); ++i)
				{
					if (!(*i)->isConnected() )
					{
						killed = true;
						ClientConnection *cc = (*i);
						printf("Lost connection to client: %d\r\n", cc->getId());
						delete cc;
						mClients.erase(i);
						break;
					}
				}
			}

			// For each active client connection..
			// we see if that client has received a new message
			// If we have received a message from a client, then we echo that message back to
			// all currently connected clients
			for (auto &i : mClients)
			{
                i->pump();
			}
		}
        delete sf;
	}

    virtual void receiveRedisMessage(const char *msg) override final
    {
        printf("FromRedis:%s\r\n", msg);
    }

    virtual void receiveRedisMessage(const void *data, uint32_t dataLen) override final
    {
        printf("FromRedis:");
        const uint8_t *scan = (const uint8_t *)data;
        for (uint32_t i = 0; i < dataLen; i++)
        {
            uint8_t c = scan[i];
            if (c >= 32 && c <= 127)
            {
                printf("%c", c);
            }
            else
            {
                printf("$%02X", c);
            }
        }
        printf("\r\n");
    }

    redisproxy::RedisProxy              *mRedisProxy{ nullptr };
	wsocket::Wsocket		            *mServerSocket{ nullptr };
	inputline::InputLine	            *mInputLine{ nullptr };
	ClientConnectionVector	            mClients;
    keyvaluedatabase::KeyValueDatabase  *mDatabase{ nullptr };
};


int main()
{
	socketchat::socketStartup();
	// Run the simple server
	{
		SimpleServer ss;
		ss.run();
	}

	socketchat::socketShutdown();

	return 0;
}
