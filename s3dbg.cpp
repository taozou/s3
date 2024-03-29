//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2011-2012, OblakSoft LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 
//
// Author: Maxim Mazeev <mazeev@hotmail.com>

//////////////////////////////////////////////////////////////////////////////
// Debug-only unit-test.
//////////////////////////////////////////////////////////////////////////////

#include "s3conn.h"
#include "sysutils.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>

using namespace webstor;
using namespace webstor::internal;

#ifdef DEBUG

static const size_t MB = 1024 * 1024;

#define DBG_RUN_UNIT_TEST( fn ) dbgRunUnitTest( fn, #fn )

static void
dbgRunUnitTest( void ( &fnTest )(), const char *name )
{
    std::cout << "Running " << name << "...";
    std::cout.flush();

    try
    {
        fnTest();
        std::cout << " done." << std::endl;
    }
    catch( ... )
    {
        std::cout << " failed." << std::endl;
        throw;
    }
}


static void
dbgAssertS3Object( const S3Object &actual, const S3Object &expected ) 
{
    dbgAssert( !strcmp( actual.key.c_str(), expected.key.c_str() ) );
    dbgAssert( !strcmp( actual.etag.c_str(), expected.etag.c_str() ) );
    dbgAssert( actual.size == expected.size );
    dbgAssert( actual.isDir == expected.isDir );
}

static void
dbgAssertS3Objects( const S3Object *actual, const S3Object *expected, size_t size ) 
{
    for( size_t i = 0; i < size; ++i )
    {
        dbgAssertS3Object( actual[ i ], expected[ i ] );
    }
}

static void
dbgAssertS3MultipartUpload( const S3MultipartUpload &actual, 
    const S3MultipartUpload &expected ) 
{
    dbgAssert( !strcmp( actual.key.c_str(), expected.key.c_str() ) );
    dbgAssert( !strcmp( actual.uploadId.c_str(), expected.uploadId.c_str() ) );
    dbgAssert( actual.isDir == expected.isDir );
}

static void
dbgAssertS3MultipartUploads( const S3MultipartUpload *actual, 
    const S3MultipartUpload *expected, size_t size ) 
{
    for( size_t i = 0; i < size; ++i )
    {
        dbgAssertS3MultipartUpload( actual[ i ], expected[ i ] );
    }
}

void
dbgTestS3Connection()
{
    // Check env. variables, if they are not set, skip the rest of the test.

    S3Config config = {};
    const char *bucketName = NULL;

    // Mandatory variables.

    if( !( config.accKey = getenv( "AWS_ACCESS_KEY" ) ) ||
        !( config.secKey = getenv( "AWS_SECRET_KEY" ) ) ||
        !( bucketName = getenv( "AWS_BUCKET_NAME" ) ) )
    {
        std::cout << "skip Amazon/Walrus test because no AWS_XXXX is set. ";
        return;
    }

    // Optional variables.

    config.host = getenv( "AWS_HOST" );

    if( config.host && *config.host && !strstr( config.host, "amazonaws.com" ) )
    {    
        config.isWalrus = true;
    }
    
    config.proxy = getenv( "AWS_PROXY" );

    // Instantiate S3 connection.

    S3Connection con( config );
    AsyncMan asyncMan;

    // Test data.

    const unsigned char expected[] = { 'F', 'O', 'O', 'b', 'a', 'r' };
    const unsigned char expectedOne = 0xf1;
    size_t expectedSize = dimensionOf( expected );
    const char *commonPrefix = "tmp/";
    const char *key = "tmp/folder1/test.dat";
    const char *emptyKey = "tmp/folder2/empty.dat";
    const char *weirdKey = !config.isWalrus ?
        "tmp/folder2/ ~!@#$%^&*()_+.<>?:'\\;.~ ,\"{}[]-=" :
        "tmp/folder2/!@#$%^&*()_+.<>?:'\\;.,\"{}[]-=";   // Walrus doesn't round-trip some characters (e.g. '~' or space)

    // Clean up to make sure there are no leftovers from the previous failed run.

    con.delAll( bucketName, commonPrefix );

    if( !config.isWalrus )
    {
        con.abortAllMultipartUploads( bucketName, commonPrefix );
    }

    // Verify bucket operations.

    // con.createBucket( "oblaksofttest314159" );
    // con.delBucket( "oblaksofttest314159" );

    std::vector< S3Bucket > buckets;
    con.listAllBuckets( &buckets );
    dbgAssert( buckets.size() > 0 );
    
    size_t expectedBucketIndex = 0;
    for( ; expectedBucketIndex < buckets.size() && 
        buckets[ expectedBucketIndex ].name != bucketName; ++expectedBucketIndex ) {}
    dbgAssert( expectedBucketIndex < buckets.size() );
    
    // Verify put.

    S3PutResponse putResponse;
    S3PutResponse putResponseEmpty;
    S3PutResponse putResponseWeird;
    S3Connection con2( config );

    con.put( bucketName, key, expected, expectedSize, 
        false /* publicReadAcl */, false /* srvEncrypt */, "text/plain", &putResponse );
    con.pendPut( &asyncMan, bucketName, emptyKey, expected, 0 /* empty */, 
        true /* publicReadAcl */, false /* srvEncrypt */ );
    con2.pendPut( &asyncMan, bucketName, weirdKey, &expectedOne, 1, 
        false /* publicReadAcl */, true /* srvEncrypt */ );
    con2.completePut( &putResponseWeird );
    con.completePut( &putResponseEmpty );

    // Verify get.

    {
        int bufferSizes[] = { 16, 6, 2, 1, 0 };

        for( int i = 0; i < dimensionOf( bufferSizes ); ++i )
        {
            size_t actualSize = bufferSizes[ i ];
            unsigned char actual[ 16 ];
            dbgAssert( dimensionOf( actual ) >= actualSize );
        
            S3GetResponse getResponse;
            con.get( bucketName, key, actual, actualSize, &getResponse );

            dbgAssert( getResponse.loadedContentLength == std::min( expectedSize, actualSize ) );
            dbgAssert( !memcmp( actual, expected, getResponse.loadedContentLength ) );
            dbgAssert( getResponse.isTruncated == ( actualSize < expectedSize ) );
            dbgAssert( !strcmp( getResponse.etag.c_str(), putResponse.etag.c_str() ) );
        }

        // Empty object.

        unsigned char undefined = 0xde;
        unsigned char actual = undefined;

        S3GetResponse getResponse;
        con.get( bucketName, emptyKey, &actual, 1, &getResponse );
        dbgAssert( getResponse.loadedContentLength == 0 );
        dbgAssert( !getResponse.isTruncated );
        dbgAssert( actual == undefined );
        dbgAssert( !strcmp( getResponse.etag.c_str(), putResponseEmpty.etag.c_str() ) );

        // A weird key.

        con.get( bucketName, weirdKey, &actual, 1, &getResponse );
        dbgAssert( getResponse.loadedContentLength == 1 );
        dbgAssert( !getResponse.isTruncated );
        dbgAssert( actual == expectedOne );
        dbgAssert( !strcmp( getResponse.etag.c_str(), putResponseWeird.etag.c_str() ) );

        // A missing key.

        con.get( bucketName, "missing key", &actual, 1, &getResponse );
        dbgAssert( getResponse.loadedContentLength == -1 );
        dbgAssert( !getResponse.isTruncated );

        // Async cancel.

        con.pendGet( &asyncMan, bucketName, weirdKey, &actual, 1 );
        dbgAssert( con.isAsyncPending() );
        taskSleep( 100 );
        dbgAssert( con.isAsyncPending() );
        con.cancelAsync( );
        dbgAssert( !con.isAsyncPending() );

        // Async.

        con.pendGet( &asyncMan, bucketName, weirdKey, &actual, 1 );
        dbgAssert( con.isAsyncPending() );
        con.completeGet( &getResponse );
        dbgAssert( !con.isAsyncPending() );
        dbgAssert( getResponse.loadedContentLength == 1 );
        dbgAssert( !getResponse.isTruncated );
        dbgAssert( actual == expectedOne );
        dbgAssert( !strcmp( getResponse.etag.c_str(), putResponseWeird.etag.c_str() ) );
    }

    // Verify listObjects.

    {
        S3ListObjectsResponse listBucketResponse;
        std::vector< S3Object > objects;
        objects.reserve( 8 );

        // Enum all objects.

        con.listObjects( bucketName, NULL /* prefix */, NULL /* marker */, 
            NULL /* delimiter */, 0 /* maxKeys */,
            &objects, &listBucketResponse );
        dbgAssert( objects.size() > 0 );

        // Enum objects with the same prefix.

        objects.clear();
        con.listObjects( bucketName, commonPrefix, NULL /* marker */, 
            NULL /* delimiter */, 0 /* maxKeys */,
            &objects, &listBucketResponse );

        S3Object expectedObjects[] = 
        {
            S3Object( key, "", putResponse.etag.c_str(), expectedSize, false ),
            S3Object( weirdKey, "", putResponseWeird.etag.c_str(), 1, false ),
            S3Object( emptyKey, "", putResponseEmpty.etag.c_str(), 0, false )
        };
        
        dbgAssert( !listBucketResponse.isTruncated );
        dbgAssert( objects.size() == dimensionOf( expectedObjects ) );
        dbgAssertS3Objects( objects.data(), expectedObjects, dimensionOf( expectedObjects ) );

        // Paging (page 1).

        const char *initialMarkers[] = { " ", NULL, "" }; 

        for( int i = 0; i < dimensionOf( initialMarkers ); ++i )
        {
            objects.clear();
            con.listObjects( bucketName, commonPrefix, 
                initialMarkers[ i ], NULL /* delimiter */, 1 /* maxKeys */,
                &objects, &listBucketResponse );
            dbgAssert( listBucketResponse.isTruncated );
            dbgAssert( !strcmp( listBucketResponse.nextMarker.c_str(),
                objects[ 0 ].key.c_str() ) );
            dbgAssert( objects.size() == 1 );
            dbgAssertS3Object( objects[ 0 ], expectedObjects[ 0 ] );
        }

        // Paging (page 2).

        objects.clear();
        con.listObjects( bucketName, commonPrefix, 
            expectedObjects[ 0 ].key.c_str() /* marker */, NULL /* delimiter */, 
            dimensionOf( expectedObjects ) - 1 /* maxKeys */, &objects, &listBucketResponse );
        dbgAssert( !listBucketResponse.isTruncated );
        dbgAssert( objects.size() == dimensionOf( expectedObjects ) - 1 );
        dbgAssertS3Objects( objects.data(), &expectedObjects[ 1 ], 
            dimensionOf( expectedObjects ) - 1 );

        // Common prefixes.

        objects.clear(); 
        listBucketResponse.nextMarker = "";

        do
        {
            // Note: paging through directory names requires server to return valid nextMarker,
            // this is not supported by walrus.
            
            con.listObjects( bucketName, commonPrefix, listBucketResponse.nextMarker.c_str(),
                "/" /* delimiter */, 
                config.isWalrus ? 0 : 1 /* maxKeys */, &objects, &listBucketResponse );
        } while( listBucketResponse.isTruncated );

        S3Object expectedDirs[] = 
        {
            S3Object( "tmp/folder1/", "", "", -1, true ),
            S3Object( "tmp/folder2/", "", "", -1, true  ),
        };

        dbgAssert( !listBucketResponse.isTruncated );
        dbgAssert( objects.size() == dimensionOf( expectedDirs ) );
        dbgAssertS3Objects( objects.data(), expectedDirs, dimensionOf( expectedDirs ) );
    }

    // Verify delete.

    {
        S3DelResponse delResponse;
        con.del( bucketName, key, &delResponse );
        con.del( bucketName, emptyKey, &delResponse );
        con.pendDel( &asyncMan, bucketName, weirdKey );
        con.completeDel();
        con.del( bucketName, "missing key" );

        S3ListObjectsResponse listBucketResponse;
        std::vector< S3Object > objects;
        con.listObjects( bucketName, commonPrefix, NULL /* marker */, NULL /* delimiter */, 
            0 /* maxKeys */, &objects, &listBucketResponse );

        dbgAssert( objects.size() == 0 );
    }

    // Verify multipart upload.

    if( !config.isWalrus )
    {
        S3InitiateMultipartUploadResponse initMultipartResponse;
        S3InitiateMultipartUploadResponse initMultipartWeirdKeyResponse;
        S3InitiateMultipartUploadResponse initMultipartEmptyKeyResponse;
        con.initiateMultipartUpload( bucketName, key, false, false, "x-foo/x-bar", &initMultipartResponse );
        con.initiateMultipartUpload( bucketName, weirdKey, false, false, NULL, &initMultipartWeirdKeyResponse );
        con.initiateMultipartUpload( bucketName, emptyKey, false, false, NULL, &initMultipartEmptyKeyResponse );

        S3ListMultipartUploadsResponse listMultipartResponse;
        std::vector< S3MultipartUpload > uploads;

        // Enum all uploads.

        con.listMultipartUploads( bucketName, NULL /*prefix */, NULL /* keyMarker */,
            NULL /* uploadMarker */, NULL /* delimiter */, 0 /* maxUploads */, 
            &uploads, &listMultipartResponse );
        dbgAssert( uploads.size() > 0 );

        // Enum uploads with the same prefix.

        uploads.clear();
        con.listMultipartUploads( bucketName, commonPrefix, NULL /* keyMarker */, 
            NULL /* uploadMarker */, NULL /* delimiter */, 0 /* maxKeys */,
            &uploads, &listMultipartResponse );

        S3MultipartUpload expectedUploads[] = 
        {
            S3MultipartUpload( key, initMultipartResponse.uploadId.c_str(), false ),
            S3MultipartUpload( weirdKey, initMultipartWeirdKeyResponse.uploadId.c_str(), false ),
            S3MultipartUpload( emptyKey, initMultipartEmptyKeyResponse.uploadId.c_str(), false )
        };

        dbgAssert( !listMultipartResponse.isTruncated );
        dbgAssert( uploads.size() == dimensionOf( expectedUploads ) );
        dbgAssertS3MultipartUploads( uploads.data(), expectedUploads, 
            dimensionOf( expectedUploads ) );

        // Paging (page 1).

        uploads.clear();
        con.listMultipartUploads( bucketName, commonPrefix, "" /* keyMarker */, NULL /* uploadMarker */,
            NULL /* delimiter */, 1 /* maxUploads */,
            &uploads, &listMultipartResponse );

        dbgAssert( listMultipartResponse.isTruncated );
        dbgAssert( !strcmp( listMultipartResponse.nextKeyMarker.c_str(), 
            uploads[ 0 ].key.c_str() ) );
        dbgAssert( !strcmp( listMultipartResponse.nextUploadIdMarker.c_str(), 
            uploads[ 0 ].uploadId.c_str() ) );
        dbgAssert( uploads.size() == 1 );
        dbgAssertS3MultipartUpload( uploads[ 0 ], expectedUploads[ 0 ] );

        // Paging (page 2).

        uploads.clear();
        con.listMultipartUploads( bucketName, commonPrefix, 
            expectedUploads[ 0 ].key.c_str(), expectedUploads[ 0 ].uploadId.c_str(),
            NULL /* delimiter */, 
            dimensionOf( expectedUploads ) - 1 /* maxUploads */, &uploads, &listMultipartResponse );
        dbgAssert( !listMultipartResponse.isTruncated );
        dbgAssert( uploads.size() == dimensionOf( expectedUploads ) - 1 );
        dbgAssertS3MultipartUploads( uploads.data(), &expectedUploads[ 1 ], 
            dimensionOf( expectedUploads ) - 1 );

        // Common prefixes.

        uploads.clear(); 
        con.listMultipartUploads( bucketName, commonPrefix, "" /* keyMarker */, NULL /* uploadMarker */, 
            "/" /* delimiter */, 
            0 /* maxKeys */, &uploads, &listMultipartResponse );

        S3MultipartUpload expectedDirs[] = 
        {
            S3MultipartUpload( "tmp/folder1/", "", true ),
            S3MultipartUpload( "tmp/folder2/", "", true ),
        };

        dbgAssert( !listMultipartResponse.isTruncated );
        dbgAssert( uploads.size() == dimensionOf( expectedDirs ) );
        dbgAssertS3MultipartUploads( uploads.data(), expectedDirs, dimensionOf( expectedDirs ) );

        // putPart (5MB + 1 byte).

        size_t partSizes[] = { 5 * MB, 1 };
        S3PutResponse putPartResponses[ dimensionOf( partSizes ) ];
        int partNumber = 0;
        int seq = 0;
        size_t totalSize = 0;

        for( int i = 0; i < dimensionOf( partSizes ); ++i )
        {
            size_t partSize = partSizes[ i ];
            std::vector< unsigned char> data( partSize );

            for( int j = 0; j < partSize; ++j )
            {
                data[ j ] = ( seq++ ) % 256;
            }

            con.putPart( bucketName, key, initMultipartResponse.uploadId.c_str(), partNumber + 1/* starts with 1 */, 
                data.data(), data.size(), &putPartResponses[ partNumber ] );

            partNumber++;
            totalSize += partSize;
        }

        // Complete multipart upload.

        S3CompleteMultipartUploadResponse completeMultipartResponse;
        con.completeMultipartUpload( bucketName, key, initMultipartResponse.uploadId.c_str(), 
            putPartResponses, partNumber, &completeMultipartResponse );

        // putPart (1 byte).

        S3PutResponse putPartWeirdKeyResponse;
        con.putPart( bucketName, weirdKey, initMultipartWeirdKeyResponse.uploadId.c_str(), 1/* starts with 1 */, 
            &expectedOne, 1, &putPartWeirdKeyResponse );
        S3CompleteMultipartUploadResponse completeMultipartWeirdKeyResponse;
        con.completeMultipartUpload( bucketName, weirdKey, initMultipartWeirdKeyResponse.uploadId.c_str(), 
            &putPartWeirdKeyResponse, 1, &completeMultipartWeirdKeyResponse );

        // Enum all objects.

        S3ListObjectsResponse listBucketResponse;
        std::vector< S3Object > objects;
        objects.reserve( 8 );
        con.listObjects( bucketName, commonPrefix, NULL /* marker */, NULL /* delimiter */, 0 /* maxKeys */,
            &objects, &listBucketResponse );

        S3Object expectedObject[] = 
        { 
            S3Object( key, "", completeMultipartResponse.etag.c_str(), totalSize, false /* isDir */  ),
            S3Object( weirdKey, "", completeMultipartWeirdKeyResponse.etag.c_str(), 1, false /* isDir */  )
        };

        dbgAssert( objects.size() == dimensionOf( expectedObject ) );
        dbgAssert( !listBucketResponse.isTruncated );
        dbgAssertS3Objects( objects.data(), expectedObject, dimensionOf( expectedObject ) );

        // Enum multipart uploads.

        uploads.clear();
        con.listMultipartUploads( bucketName, commonPrefix, NULL /* keyMarker */, 
            NULL /* uploadMarker */, NULL /* delimiter */, 0 /* maxKeys */,
            &uploads, &listMultipartResponse );
        dbgAssert( !listMultipartResponse.isTruncated );
        dbgAssert( uploads.size() == 1 );
        dbgAssertS3MultipartUploads( uploads.data(), &expectedUploads[ 2 ], 1 );

        // Abort multipart uploads.

        S3DelResponse abortMultipartUploadResponse;
        con.abortMultipartUpload( bucketName, emptyKey, uploads[ 0 ].uploadId.c_str(), 
            &abortMultipartUploadResponse );

        // Enum multipart uploads.

        uploads.clear();
        con.listMultipartUploads( bucketName, commonPrefix, NULL /* keyMarker */, 
            NULL /* uploadMarker */, NULL /* delimiter */, 0 /* maxKeys */,
            &uploads, &listMultipartResponse );
        dbgAssert( !listMultipartResponse.isTruncated );
        dbgAssert( uploads.size() == 0 );
    }

    else
    {
        size_t blobSize = 5 * MB + 1;
        std::vector< unsigned char> data( blobSize );

        for( int i = 0; i < blobSize; ++i )
        {
            data[ i ] = i % 256;
        }

        con.put( bucketName, key, data.data(), data.size() );
    }

    // Verify timeout.

    {
        size_t blobSize = 5 * MB + 1;
        std::vector< unsigned char> data( blobSize );

        con.setTimeout( 30 );
        std::string exceptionMsg;

        try
        {
            // It should take more than 30 msec to load 5MB blob,
            // so we should timeout.

            con.get( bucketName, key, data.data(), data.size() );  
        }
        catch( const std::exception &e )
        {
            exceptionMsg = e.what();
        }

        dbgAssert( !exceptionMsg.empty() );
        dbgAssert( strstr( exceptionMsg.c_str(), "timed out" ) ); 

        exceptionMsg.clear();  

        try
        {
            con.pendGet( &asyncMan, bucketName, key, data.data(), data.size() );  
            taskSleep( 1000 );
            con.completeGet();
        }
        catch( const std::exception &e )
        {
            exceptionMsg = e.what();
        }

        dbgAssert( !exceptionMsg.empty() );
        dbgAssert( strstr( exceptionMsg.c_str(), "timed out" ) ); 

        con.setTimeout( 0 ); // reset to default.
    }

    // Clean up.

    con.delAll( bucketName, commonPrefix );

    if( !config.isWalrus )
    {
        con.abortAllMultipartUploads( bucketName, commonPrefix );
    }
}

#else
#define DBG_RUN_UNIT_TEST( fn ) 
#endif // DEBUG

int
main( int argc, char **argv )
{
#ifdef DEBUG
    if( argc > 1 && *argv[ 1 ] == 'd' )
        dbgBreak_;
#endif

    try
    {
        DBG_RUN_UNIT_TEST( dbgTestS3Connection );
    }
    catch( const std::exception &e )
    {
        std::cout << std::endl << e.what() << std::endl;
    }
    catch( const char *s )
    {
        std::cout << std::endl << s << std::endl;
    }
    catch( ... )
    {
        std::cout << std::endl << "Unknown error" << std::endl;
    }
}
