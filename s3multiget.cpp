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
static const char bucketName[100] = "scanspeed";

unsigned char** buf;
AsyncMan* asyncMans;

S3Connection ** cons;

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

    int objectMB = 0; 
    int connectionCount = 1;
    int asyncManCount = 4;
    int key = 0;


    for (int i = 1; i < argc; ++i)
    {
        if (!strcmp(argv[i], "-s"))
        {
            objectMB = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "-c"))
        {
            connectionCount = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "-a"))
        {
            asyncManCount = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "-key"))
        {
            key= atoi(argv[++i]);
        }
    }
    
    if ( !objectMB)
    {
         printf("s3get -s size(MB)]+ [-c ConnectionCount(1)] [-a numAsyncMan(4)] [-key(0)]\n");
         MPI::Finalize();
         return 1;
    }
    
    asyncMans = new AsyncMan[asyncManCount];
    buf = new unsigned char*[connectionCount];
    cons = new S3Connection*[connectionCount];
    
    S3Config config = {};

    if( !( config.accKey = getenv( "AWS_ACCESS_KEY" ) ) ||
        !( config.secKey = getenv( "AWS_SECRET_KEY" ) )  )
    {
        std::cout << "no AWS_XXXX is set. ";
        return 1;
    }

    int unitSize = objectMB * MB / size / connectionCount;
    int base = objectMB * MB / size * rank;
    //fprintf(stderr, "%d %d %d\n", rank, base, unitSize);
    for ( int i = 0; i < connectionCount; ++i )
    {
        cons[i] = new S3Connection(config);
        buf[i] = new unsigned char[ unitSize ];
        //buf[i] = new unsigned char[ objectMB * MB ];
        memset(buf[i], 0, unitSize);
    }

    //get
    Stopwatch stopwatch;
    if (rank == 0) std::cout << connectionCount << " connection(s): \n";

    
    
    stopwatch.start();
    for ( int i = 0; i < connectionCount; ++i )
    {
        //fprintf(stderr, "size %d; offset %d\n", unitSize, unitSize * i + base);
        cons[i]->pendGet( &asyncMans[i % asyncManCount],
                bucketName, getKey(key, objectMB).c_str(), buf[i], unitSize, unitSize * i + base);
    }

    for ( int i = 0; i < connectionCount; ++i )
        cons[i]->completeGet();

    double bandwidth = 1000.0 * objectMB / size / stopwatch.elapsed();
    std::cerr << rank << ": " << bandwidth << "MiB/s\n";

    /*
    int p = 0;    
    for (int c = 0; c < connectionCount; ++c)
    {

       for (int i = 0; i < unitSize; ++i)
       {
            p = p ^ buf[c][i];
       }
    }
    printf("xor: %u\n", p);

    bool same = true;
    for (int i = 0; i < unitSize; ++i)
    {
        for (int c = 1; c < connectionCount; ++c)
        {
            if (buf[0][i] != buf[c][i])
            {
                same = false;
                break;
            }
        }
        if (!same) break;
    }
    fprintf(stderr, "%d\n", same);
    if (same)
    {
        for (int i = 0; i < 10; ++i)
            fprintf(stderr, "%d ", buf[0][i]);
        fprintf(stderr, "\n");
    }*/

    
    MPI::Finalize();
    for ( int i = 0; i < connectionCount; ++i )
    {
        delete cons[i];
    }
    delete cons;

    return 0;
}
