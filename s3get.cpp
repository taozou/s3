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
static const int maxConnectionCount = 128;
static const int maxNumAsyncMan = 64;
static const char bucketName[100] = "scanspeed";

char* buf[ maxConnectionCount ];
AsyncMan asyncMans[ maxNumAsyncMan ];

S3Connection ** cons;

static int objectMB ;
static int totalKey;
static int offset;

static std::string getKey( int i )
{
    std::stringstream tmp;
    tmp << (offset + i) << "/" << objectMB << "mb" ;
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

    totalKey = atoi(argv[1]) / size;
    offset = totalKey * rank;

    for (int i = 0; i < maxConnectionCount; ++i)
    buf[i] = new char[ maxSize ];
    srand(0);

    std::vector<int> objectMBs, connectionsCounts, asyncManCounts;


    for (int i = 2; i < argc; ++i)
    {
        if (!strcmp(argv[i], "-s"))
        {
            objectMBs.push_back( atoi(argv[++i]) );
        }

        if (!strcmp(argv[i], "-c"))
        {
            connectionsCounts.push_back( atoi(argv[++i]) );
        }

        if (!strcmp(argv[i], "-a"))
        {
            asyncManCounts.push_back( atoi(argv[++i]) );
        }
    }

    if (!objectMBs.size())
    {
        objectMBs.push_back(1);
        objectMBs.push_back(4);
        objectMBs.push_back(16);
    }

    if (!connectionsCounts.size())
    {
        connectionsCounts.push_back(1);
        connectionsCounts.push_back(4);
        connectionsCounts.push_back(16);
        connectionsCounts.push_back(32);
    }

    if (!asyncManCounts.size())
    {
        asyncManCounts.push_back(1);
    }

    S3Config config = {};

    if( !( config.accKey = getenv( "AWS_ACCESS_KEY" ) ) ||
        !( config.secKey = getenv( "AWS_SECRET_KEY" ) )  )
    {
        std::cout << "no AWS_XXXX is set. ";
        return 1;
    }

    cons = new S3Connection*[maxConnectionCount];
    for ( int i = 0; i < maxConnectionCount; ++i )
    {
        cons[i] = new S3Connection(config);
    }

    //get
    for (unsigned int a = 0; a < asyncManCounts.size(); ++a)
    {
        int numAsyncMan = asyncManCounts[a];
        if (rank == 0) std::cout << "asyncMan " << numAsyncMan << std::endl;
        
        for (unsigned int s = 0; s < objectMBs.size(); ++s )
        {
            objectMB = objectMBs[s];
            if (rank == 0) std::cout << "start " << objectMB << "MB" << std::endl;

            Stopwatch stopwatch;
            for (unsigned int c = 0; c < connectionsCounts.size(); ++c)
            {
                int connectionCount = connectionsCounts[c];
                if (rank == 0) std::cout << connectionCount << " connection(s): ";

                stopwatch.start();
                for ( int i = 0; i < connectionCount; ++i )
                {
                    cons[i]->pendGet( &asyncMans[i % numAsyncMan],
                            bucketName, getKey(i).c_str(), buf[i], objectMB * MB);
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

                    /*unsigned char p = 0;
                    for (int i = 0; i < maxSize; ++i)
                        p = p ^ buf[k][i];
                    printf("%d\n", p);
                    */
                    cons[k]->pendGet( &asyncMans[i % numAsyncMan],
                        bucketName, getKey(i).c_str(), buf[k], objectMB * MB);

                    if ( !(i % (totalKey / 10)) )
                        print('.');
                }
                for ( int i = 0; i < connectionCount; ++i )
                    cons[i]->completeGet();
                double bandwidth = 1000.0 * objectMB * totalKey/ stopwatch.elapsed();
                std::cout << rank << ": " << bandwidth << "MiB/s\n";
            }
        }
    }

    MPI::Finalize();
    for ( int i = 0; i < maxConnectionCount; ++i )
    {
        delete cons[i];
    }
    delete cons;

    return 0;
}
