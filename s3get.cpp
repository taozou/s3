#include "s3conn.h"
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <cstdio>
#include "sysutils.h"
#include <cstring>
#include <mpi.h>

using namespace webstor;
using namespace webstor::internal;

static const int KB = 1024;
static const int MB = KB * 1024;
static const int maxSize = 16 * MB;
static const char bucketName[100] = "scanspeed";

unsigned char** buf;
AsyncMan* asyncMans;

S3Connection ** cons;

int keylow = 0;
int keyhigh = 0;
bool readAll = false;

static std::string getKey( int i, int objectMB )
{
    std::stringstream tmp;
    tmp << i << "/" << objectMB << "mb" ;
    //tmp << objectMB << "mb/" << (offset + i);
    return tmp.str();
}

void print(char i)
{
    fprintf(stderr, "%c", i);
}

int main( int argc, char **argv )
{
    MPI::Init(argc, argv);
    int rank = MPI::COMM_WORLD.Get_rank();
    int size = MPI::COMM_WORLD.Get_size();

    std::vector<int> objectMBs, connectionsCounts, asyncManCounts;

    for (int i = 1; i < argc; ++i)
    {
        if (!strcmp(argv[i], "-s"))
        {
            objectMBs.push_back( atoi(argv[++i]) );
        }
        else if (!strcmp(argv[i], "-c"))
        {
            connectionsCounts.push_back( atoi(argv[++i]) );
        }
        else if (!strcmp(argv[i], "-a"))
        {
            asyncManCounts.push_back( atoi(argv[++i]) );
        }
        else if (!strcmp(argv[i], "-ki"))
        {
            keylow = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "-kh"))
        {
            keyhigh = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "-all"))
        {
            readAll = true;
        }
    }
    
    if ( !objectMBs.size() )
    {
         printf("s3get -s size(MB)]+ [-c ConnectionCount(1)] [-a numAsyncMan(4)] [-ki keylow(0)] [-kh keyhigh(0)]\n");
         MPI::Finalize();
         return 1;
    }
    
    if ( !connectionsCounts.size() )
    {
        connectionsCounts.push_back( 1 ); 
    }
    
    if ( !asyncManCounts.size() )
    {
        asyncManCounts.push_back( 4 );
    }
    
    int totalKey = keyhigh - keylow + 1;
    if (!readAll)
    {
        totalKey /= size;
        keylow += totalKey * rank;
        keyhigh = keylow + totalKey;
    }
    
    int maxc = 0, maxs = 0, maxa= 0 ;
    
    for (int i = 0; i < connectionsCounts.size(); ++i)
        if (connectionsCounts[i] > maxc)
            maxc = connectionsCounts[i];
    for (int i = 0; i < objectMBs.size(); ++i)
        if (objectMBs[i] > maxs)
            maxs = objectMBs[i];
    for (int i = 0; i < asyncManCounts.size(); ++i)
        if (asyncManCounts[i] > maxa)
            maxa = asyncManCounts[i];
     
    asyncMans = new AsyncMan[maxa];
    buf = new unsigned char*[maxc];
    cons = new S3Connection*[maxc];
    
    S3Config config = {};

    if( !( config.accKey = getenv( "AWS_ACCESS_KEY" ) ) ||
        !( config.secKey = getenv( "AWS_SECRET_KEY" ) )  )
    {
        std::cout << "no AWS_XXXX is set. ";
        return 1;
    }

    for ( int i = 0; i < maxc; ++i )
    {
        cons[i] = new S3Connection(config);
        buf[i] = new unsigned char[ maxs * MB ];
    }

    //get
    for (unsigned int a = 0; a < asyncManCounts.size(); ++a)
    {
        int numAsyncMan = asyncManCounts[a];
        if (rank == 0) std::cout << "asyncMan " << numAsyncMan << std::endl;
        
        for (unsigned int s = 0; s < objectMBs.size(); ++s )
        {
            int objectMB = objectMBs[s];
            int objectSize = objectMB * MB;
            
            if (rank == 0) std::cout << "start " << objectMB << "MB" << std::endl;

            Stopwatch stopwatch;
            for (unsigned int c = 0; c < connectionsCounts.size(); ++c)
            {
                int connectionCount = connectionsCounts[c];
                if (rank == 0) std::cout << connectionCount << " connection(s): \n";

                stopwatch.start();
                for ( int i = 0; i < connectionCount && i < totalKey; ++i )
                {
                    cons[i]->pendGet( &asyncMans[i % numAsyncMan],
                            bucketName, getKey(keylow + i, objectMB).c_str(), buf[i], objectSize);
                }

                for ( int i = connectionCount; i < totalKey; ++i)
                {
                    int k = S3Connection::waitAny( cons, connectionCount, i % connectionCount);

                    S3GetResponse response;

                    try
                    {
                        cons[k]->completeGet();
                    }
                    catch ( ... ) {
                        std::cout << "get fail\n";
                    }

                    //int p = 0;
                    //for (int j = 0; j < objectSize; ++j)
                    //    p = p ^ buf[k][j];
                    //printf("%u\n", p);
                    
                    cons[k]->pendGet( &asyncMans[i % numAsyncMan],
                        bucketName, getKey(keylow + i, objectMB).c_str(), buf[k], objectSize);

                    //if ( !(i % (totalKey / 10)) )
                    //    print('.');
                }
                for ( int i = 0; i < connectionCount; ++i )
                {
                    cons[i]->completeGet();
                    //int p = 0;
                    //for (int j = 0; j < objectSize; ++j)
                    //    p = p ^ buf[i][j];
                    //printf("%u\n", p);
                }
                double bandwidth = 1000.0 * objectMB * totalKey/ stopwatch.elapsed();
                std::cout << rank << ": " << bandwidth << "MiB/s\n";
            }
        }
    }

    MPI::Finalize();
    for ( int i = 0; i < maxc; ++i )
    {
        delete cons[i];
    }
    delete cons;

    return 0;
}
