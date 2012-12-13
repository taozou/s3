#include "s3conn.h"
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <cstdio>
#include <cstring>

//#include "sysutils.h"

using namespace webstor;

int Size;
int maxSize;
int ConnectionCount = 1;
int numAsyncMan = 4;
int keylow = 0;
int keyhigh = 0;

static const int KB = 1024;
static const int MB = KB * 1024;


static const char bucketName[100] = "scanspeed";

unsigned char **buf;
//[ maxConnectionCount ][ maxSize ];
AsyncMan *asyncMans;

S3Connection ** cons;

static std::string getKey( int i, int objectMB )
{
    std::stringstream tmp;
    tmp << i << "/" << objectMB << "mb" ;
    return tmp.str();
}

void resetBuffer(int i, int key, int size)
{
   unsigned char x = key % 256;
   for( int j = 1; j < size; ++j )
   {
       buf[i][j] = ( unsigned char )( rand() % 256 );
       x = x ^ buf[i][j];
   }
   buf[i][0] = x;
}

void print(char i)
{
    fprintf(stderr, "%c", i);
}

int main( int argc, char **argv )
{   
    ConnectionCount = 1;
    
    srand(0);

    std::vector<int> objectMBs;

    for (int i = 1; i < argc; ++i)
    {
        if (!strcmp(argv[i], "-s"))
        {
            objectMBs.push_back( atoi(argv[++i]) );
        }

        if (!strcmp(argv[i], "-c"))
        {
            ConnectionCount = atoi(argv[++i]);
        }
        
        if (!strcmp(argv[i], "-a"))
        {
            numAsyncMan = atoi(argv[++i]);
        }

        if (!strcmp(argv[i], "-ki"))
        {
            keylow = atoi(argv[++i]);
        }

        if (!strcmp(argv[i], "-kh"))
        {
            keyhigh = atoi(argv[++i]);
        }
    }

    if (objectMBs.size() == 0)
    {
        printf("s3put [-s size(MB)]+ [-c ConnectionCount(1)] [-a numAsyncMan(4)] [-ki keylow(0)] [-kh keyhigh(0)]\n");
        return 1;
    }
    else
    {
        maxSize = 0;
        for (int i = 0; i < objectMBs.size(); ++i)
            if (objectMBs[i] > maxSize)
                maxSize = objectMBs[i];
        maxSize = maxSize * MB;
    }

    S3Config config = {};

    if( !( config.accKey = getenv( "AWS_ACCESS_KEY" ) ) ||
        !( config.secKey = getenv( "AWS_SECRET_KEY" ) )  )
    {
        std::cout << "no AWS_XXXX is set. ";
        return 1;
    }

    asyncMans = new AsyncMan[numAsyncMan];
    buf = new unsigned char*[ConnectionCount];
    
    
    cons = new S3Connection*[ConnectionCount];
    for ( int i = 0; i < ConnectionCount; ++i )
    {
        cons[i] = new S3Connection(config);
        buf[i] = new unsigned char[maxSize];
    }

    //put
    int* job = new int[ConnectionCount];
    
    for (int s = 0; s < objectMBs.size(); ++s )
    {
        int objectMB = objectMBs[s];
        int objectSize = objectMB * MB;
        std::cout << "start " << objectMB << "MB" << std::endl;

        for ( int i = 0; i < ConnectionCount; ++i )
        {
            resetBuffer( i, i, objectSize);
            cons[i]->pendPut( &asyncMans[i % numAsyncMan],
                    bucketName, getKey(i, objectMB).c_str(), buf[i], objectSize);
            job[i] = i;
        }

        for ( int i = keylow; i < keyhigh; ++i)
        {
            int k = S3Connection::waitAny( cons, ConnectionCount, i % ConnectionCount);

            try
            {
                cons[k]->completePut();
            }
            catch ( ... ) {
                std::cout << "fail, retry" << i << "\n";
                cons[ job[k] ]->pendPut( &asyncMans[job[k] % numAsyncMan],
                    bucketName, getKey(job[k], objectMB).c_str(), buf[k], objectSize);
                continue;
            }    
            
            resetBuffer( k, i, objectSize);
            job[k] = i;

            try
            {
                cons[k]->pendPut( &asyncMans[i % numAsyncMan],
                bucketName, getKey(i, objectMB).c_str(), buf[k], objectSize);
            }
            catch ( ... ) {
                std::cout << "retry" << i << "\n";
                cons[k]->pendPut( &asyncMans[i % numAsyncMan],
                    bucketName, getKey(i, objectMB).c_str(), buf[k], objectSize);
            }

            if ( (i % 100) == 0)
                print('.');
        }

        for ( int i = 0; i < ConnectionCount; ++i )
            cons[i]->completePut();
        std::cout << std::endl << "done" << std::endl;
    }

    delete job;
    for ( int i = 0; i < ConnectionCount; ++i )
    {
        delete cons[i];
    }
    delete cons;
    return 0;
}
