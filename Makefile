# Copyright (c) 2011-2012, OblakSoft LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
#
# Author: Artem Livshits <artem.livshits@gmail.com>


# Defines that can be used to customize build flavor.

DEFINES=

#DEFINES+=-DDEBUG  # use this to enable debug-only functionality
#DEFINES+=-DPERF   # use this to enable perf testing / tracing

# Include paths.  The webstor library depends on the following
# libraries:
#
# * curl
# * libxml2
# * openssl
#
# If you have custom installations of those, modify your include
# paths correspondingly.

INCLUDES=-I/usr/include/libxml2 

# Library search paths.  Similar to include paths.

LIBRARIES=

### RULES ###

CXXFLAGS+=$(DEFINES) $(INCLUDES) $(LIBRARIES) -Wno-enum-compare
LOADLIBES+=-lcurl -lssl -lxml2 -O3
CC=mpic++

.PHONY: all
all: s3dbg s3put s3get s3multiget
#s3perf s3test 

s3get: s3get.cpp
	$(CC) $(CXXFLAGS) s3get.cpp webstor.a  $(LOADLIBES) -o s3get
	
s3multiget: s3multiget.cpp
	$(CC) $(CXXFLAGS) s3multiget.cpp webstor.a  $(LOADLIBES) -o s3multiget

.PHONY: clean
clean:
	rm -f s3dbg s3put s3get s3multiget webstor.a
#s3perf s3test 

s3dbg: webstor.a

#s3perf: webstor.a

#s3test: webstor.a

s3put: webstor.a
	
s3multiget: webstor.a

s3get: webstor.a

webstor.a: webstor.a(asyncurl.o s3conn.o sysutils.o)
