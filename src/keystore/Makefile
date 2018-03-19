# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.


SGX_SDK ?= /opt/intel/sgxsdk

ifeq ($(shell getconf LONG_BIT), 32)
	SGX_ARCH := x86
else ifeq ($(findstring -m32, $(CXXFLAGS)), -m32)
	SGX_ARCH := x86
endif

ifeq ($(SGX_ARCH), x86)
	SGX_COMMON_CFLAGS := -m32
	SGX_LIB_DIR := $(SGX_SDK)/lib
else
	SGX_COMMON_CFLAGS := -m64
	SGX_LIB_DIR := $(SGX_SDK)/lib64
endif

ifeq ($(SGX_DEBUG), 1)
        SGX_COMMON_CFLAGS += -O0 -g
else
        SGX_COMMON_CFLAGS += -O2
endif

App_Cpp_Files := key_store.cpp
App_Include_Paths := -I$(TRUCE_SDK)  -I$(SGX_SDK)/include -I$(TRUCE_SDK)/IAS_report_verifier

App_C_Flags := -fPIC -Wno-attributes $(App_Include_Paths)

App_Cpp_Flags := $(App_C_Flags) -std=c++11

App_Link_Flags := -L$(TRUCE_SDK) -ltruce_client -L$(SGX_LIB_DIR) -lsgx_tcrypto -lcurl -lcrypto


App_Cpp_Objects := $(App_Cpp_Files:.cpp=.o)

App_Name := key_store


./%.o: ./%.cpp
	@$(CXX) $(App_Cpp_Flags) -c $< -o $@
	@echo "CXX  <=  $<"

$(App_Name): $(App_Cpp_Objects)
	@$(CXX) $^ -o $@ $(App_Link_Flags)
	@echo "LINK =>  $@"


clean:
	@rm -f $(App_Name) $(App_Cpp_Objects) 
