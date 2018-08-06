
#ifdef _MSC_VER
#endif

#include "socketchat.h"
#include "wsocket.h"
#include "InputLine.h"
#include "RedisProxy.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include <vector>

#define PORT_NUMBER 6379    // Redis port number
//#define PORT_NUMBER 3009    // test port number

using socketchat::SocketChat;

class ClientConnection : public socketchat::SocketChatCallback, public redisproxy::RedisProxy::Callback
{
public:
	ClientConnection(wsocket::Wsocket *client,uint32_t id) : mId(id)
	{
		mClient = socketchat::SocketChat::create(client);
        mRedisProxy = redisproxy::RedisProxy::create();
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
        mClient->sendText(msg);
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

	bool isConnected(void)
	{
		bool ret = false;

		if (mClient && mClient->getReadyState() != socketchat::SocketChat::CLOSED)
		{
			ret = true;
		}

		return ret;
	}

	socketchat::SocketChat	*mClient{ nullptr };
	uint32_t				mId{ 0 };
    redisproxy::RedisProxy  *mRedisProxy{ nullptr };
};

typedef std::vector< ClientConnection * > ClientConnectionVector;

class SimpleServer
{
public:
	SimpleServer(void)
	{
		mServerSocket = wsocket::Wsocket::create(SOCKET_SERVER, PORT_NUMBER);
		mInputLine = inputline::InputLine::create();
		printf("Simple Websockets chat server started.\r\n");
		printf("Type 'bye', 'quit', or 'exit' to stop the server.\r\n");
		printf("Type anything else to send as a broadcast message to all current client connections.\r\n");
	}

	~SimpleServer(void)
	{
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
	}

	void run(void)
	{
		bool exit = false;

		while (!exit)
		{
			if (mServerSocket)
			{
				wsocket::Wsocket *clientSocket = mServerSocket->pollServer();
				if (clientSocket)
				{
					uint32_t index = uint32_t(mClients.size()) + 1;
					ClientConnection *cc = new ClientConnection(clientSocket, index);
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
				}
			}

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
	}

	wsocket::Wsocket		*mServerSocket{ nullptr };
	inputline::InputLine	*mInputLine{ nullptr };
	ClientConnectionVector	mClients;
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
