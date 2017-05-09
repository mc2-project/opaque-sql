######## SGX SDK Settings ########

SGX_SDK ?= /opt/intel/sgxsdk
SGX_MODE ?= SIM
SGX_ARCH ?= x64

ifeq ($(shell getconf LONG_BIT), 32)
	SGX_ARCH := x86
else ifeq ($(findstring -m32, $(CXXFLAGS)), -m32)
	SGX_ARCH := x86
endif

ifeq ($(SGX_ARCH), x86)
	SGX_COMMON_CFLAGS := -m32
	SGX_LIBRARY_PATH := $(SGX_SDK)/lib
	SGX_ENCLAVE_SIGNER := $(SGX_SDK)/bin/x86/sgx_sign
	SGX_EDGER8R := $(SGX_SDK)/bin/x86/sgx_edger8r
else
	SGX_COMMON_CFLAGS := -m64
	SGX_LIBRARY_PATH := $(SGX_SDK)/lib64
	SGX_ENCLAVE_SIGNER := $(SGX_SDK)/bin/x64/sgx_sign
	SGX_EDGER8R := $(SGX_SDK)/bin/x64/sgx_edger8r
endif

ifeq ($(SGX_DEBUG), 1)
ifeq ($(SGX_PRERELEASE), 1)
$(error Cannot set SGX_DEBUG and SGX_PRERELEASE at the same time!!)
endif
endif

ifeq ($(SGX_DEBUG), 1)
        SGX_COMMON_CFLAGS += -O0 -g -DDEBUG
else
        SGX_COMMON_CFLAGS += -O2
endif

ifeq ($(SGX_PERF), 1)
		SGX_COMMON_CFLAGS += -DPERF
endif

######## App Settings ########

ifneq ($(SGX_MODE), HW)
Urts_Library_Name := sgx_urts_sim
else
Urts_Library_Name := sgx_urts
endif	

App_Cpp_Files := App/App.cpp $(wildcard App/TrustedLibrary/*.cpp)
App_Include_Paths := \
	-IInclude -IApp -ICommon -IServiceProvider \
	-I../../target/flatbuffers/flatbuffers-1.4.0/include \
	-I../../target/scala-2.11/src_managed/flatbuffers/gen-cpp \
	-I$(SGX_SDK)/include

App_C_Flags := $(SGX_COMMON_CFLAGS) -fPIC -Wno-attributes $(App_Include_Paths)

# Three configuration modes - Debug, prerelease, release
#   Debug - Macro DEBUG enabled.
#   Prerelease - Macro NDEBUG and EDEBUG enabled.
#   Release - Macro NDEBUG enabled.
ifeq ($(SGX_DEBUG), 1)
        App_C_Flags += -UNDEBUG -UEDEBUG
else ifeq ($(SGX_PRERELEASE), 1)
        App_C_Flags += -DNDEBUG -DEDEBUG -UDEBUG
else
        App_C_Flags += -DNDEBUG -UEDEBUG -UDEBUG
endif

App_Cpp_Flags := $(App_C_Flags) -std=c++11 -Wall -Wextra -Werror
App_Link_Flags := $(SGX_COMMON_CFLAGS) -L$(SGX_LIBRARY_PATH) -l$(Urts_Library_Name) -lsgx_ukey_exchange -lpthread

ifneq ($(SGX_MODE), HW)
	App_Link_Flags += -lsgx_uae_service_sim
else
	App_Link_Flags += -lsgx_uae_service
endif

App_Cpp_Objects := $(App_Cpp_Files:.cpp=.o)

App_Name := app


######## Enclave Settings ########

ifneq ($(SGX_MODE), HW)
	Trts_Library_Name := sgx_trts_sim
	Service_Library_Name := sgx_tservice_sim
	Crypto_Library_Name := sgx_tcrypto
else
	Trts_Library_Name := sgx_trts
	Service_Library_Name := sgx_tservice
	Crypto_Library_Name := sgx_tcrypto
endif

Enclave_Cpp_Files := \
	Enclave/Aggregate.cpp \
	Enclave/Crypto.cpp \
	Enclave/Enclave.cpp \
	Enclave/Filter.cpp \
	Enclave/Flatbuffers.cpp \
	Enclave/Join.cpp \
	Enclave/Project.cpp \
	Enclave/Sort.cpp \
	Enclave/isv_enclave.cpp \
	Enclave/key.cpp \
	Enclave/sgxaes.cpp \
	Enclave/util.cpp
Enclave_Include_Paths := \
	-IInclude -IEnclave -ICommon \
	-I../../target/flatbuffers/flatbuffers-1.4.0/include \
	-I../../target/scala-2.11/src_managed/flatbuffers/gen-cpp \
	-I$(SGX_SDK)/include/stlport \
	-I$(SGX_SDK)/include \
	-I$(SGX_SDK)/include/tlibc \

Enclave_C_Flags := $(SGX_COMMON_CFLAGS) -nostdinc -fvisibility=hidden -fpie -fstack-protector $(Enclave_Include_Paths)
Enclave_Cpp_Flags := $(Enclave_C_Flags) -std=c++11 -nostdinc++ -Wall -Wextra -Werror
Enclave_Link_Flags := $(SGX_COMMON_CFLAGS) -Wl,--no-undefined \
	-nostdlib -nodefaultlibs -nostartfiles \
	-L$(SGX_LIBRARY_PATH) \
	-Wl,--whole-archive -l$(Trts_Library_Name) -Wl,--no-whole-archive \
	-Wl,--start-group \
		-lsgx_tstdc \
		-lsgx_tstdcxx \
		-lsgx_tkey_exchange \
		-l$(Crypto_Library_Name) \
		-l$(Service_Library_Name) \
	-Wl,--end-group \
	-Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined \
	-Wl,-pie,-eenclave_entry -Wl,--export-dynamic  \
	-Wl,--defsym,__ImageBase=0 \
	-Wl,--version-script=Enclave/Enclave.lds

Enclave_Cpp_Objects := $(Enclave_Cpp_Files:.cpp=.o)

App_SO_Name := libSGXEnclave.so
Enclave_Name := enclave.so
Signed_Enclave_Name := enclave.signed.so
Enclave_Config_File := Enclave/Enclave.config.xml
SP_SO_Name := libservice_provider.so

ifeq ($(SGX_MODE), HW)
ifneq ($(SGX_DEBUG), 1)
ifneq ($(SGX_PRERELEASE), 1)
Build_Mode = HW_RELEASE
endif
endif
endif

######## Service Provider/Master Setting ########

ServiceProvider_Cpp_Files := ServiceProvider/Main.cpp ServiceProvider/ecp.cpp ServiceProvider/service_provider.cpp ServiceProvider/ias_ra.cpp ServiceProvider/sp_crypto.cpp
ServiceProvider_Include_Paths := -I$(SGX_SDK)/include -ICommon -IInclude -IServiceProvider

ServiceProvider_C_Flags := $(SGX_COMMON_CFLAGS) -fPIC -Wno-attributes $(ServiceProvider_Include_Paths)
ServiceProvider_Cpp_Flags := $(ServiceProvider_C_Flags) -std=c++11 -Wall -Wextra -Werror
ServiceProvider_Link_Flags :=  $(SGX_COMMON_CFLAGS) -L$(SGX_LIBRARY_PATH) \
	-lcrypto -lssl

ServiceProvider_Cpp_Objects := $(ServiceProvider_Cpp_Files:.cpp=.o)

ServiceProvider_Name := main

.PHONY: all run

ifeq ($(Build_Mode), HW_RELEASE)
all: $(SP_SO_Name) $(App_Name) $(Enclave_Name) $(Signed_Enclave_Name) $(App_SO_Name)
	@echo "The project has been built in release hardware mode."
	@echo "Please sign the $(Enclave_Name) first with your signing key before you run the $(App_Name) to launch and access the enclave."
	@echo "To sign the enclave use the command:"
	@echo "   $(SGX_ENCLAVE_SIGNER) sign -key <your key> -enclave $(Enclave_Name) -out <$(Signed_Enclave_Name)> -config $(Enclave_Config_File)"
	@echo "You can also sign the enclave using an external signing tool. See User's Guide for more details."
	@echo "To build the project in simulation mode set SGX_MODE=SIM. To build the project in prerelease mode set SGX_PRERELEASE=1 and SGX_MODE=HW."
else
all: $(SP_SO_Name)  $(App_Name) $(Signed_Enclave_Name) $(App_SO_Name)
endif

SPexec: $(ServiceProvider_Name)

run: all
ifneq ($(Build_Mode), HW_RELEASE)
	@echo "RUN  =>  $(App_Name) [$(SGX_MODE)|$(SGX_ARCH), OK]"
	@$(CURDIR)/$(App_Name)
endif

######## App Objects ########

App/Enclave_u.c: $(SGX_EDGER8R) Enclave/Enclave.edl
	@echo "GEN  =>  $@"
	@cd App && $(SGX_EDGER8R) --untrusted ../Enclave/Enclave.edl --search-path ../Enclave --search-path $(SGX_SDK)/include

App/Enclave_u.o: App/Enclave_u.c
	@echo "CC   <=  $<"
	@$(CC) $(App_C_Flags) -c $< -o $@

App/%.o: App/%.cpp
	@echo "CXX  <=  $<"
	@$(CXX) $(App_Cpp_Flags) -c $< -o $@

$(App_Name): App/Enclave_u.o $(App_Cpp_Objects)
	@echo "LINK =>  $@"
	@$(CXX) $^ -o $@ $(App_Link_Flags)

$(App_SO_Name): App/Enclave_u.o $(App_Cpp_Objects)
	@echo "LINK =>  $@"
	@$(CXX) $^ $(App_Link_Flags) -shared -o $@

######## Enclave Objects ########

Enclave/Enclave_t.c: $(SGX_EDGER8R) Enclave/Enclave.edl
	@echo "GEN  =>  $@"
	@cd Enclave && $(SGX_EDGER8R) --trusted ../Enclave/Enclave.edl --search-path ../Enclave --search-path $(SGX_SDK)/include

Enclave/Enclave_t.o: Enclave/Enclave_t.c
	@echo "CC   <=  $<"
	@$(CC) $(Enclave_C_Flags) -c $< -o $@

Enclave/%.o: Enclave/%.cpp
	@echo "CXX  <=  $<"
	@$(CXX) $(Enclave_Cpp_Flags) -c $< -o $@

Enclave/key.cpp: $(ServiceProvider_Name)
	@echo "SP   =>  $@"
	./$(ServiceProvider_Name) $@

$(Enclave_Name): Enclave/Enclave_t.o Enclave/sgxaes_asm.o $(Enclave_Cpp_Objects)
	@echo "LINK =>  $@"
	@$(CXX) $^ -o $@  $(Enclave_Link_Flags)

Enclave/Enclave_private.pem:
	@cp $(SGX_SDK)/SampleCode/SampleEnclave/Enclave/Enclave_private.pem Enclave/Enclave_private.pem

$(Signed_Enclave_Name): Enclave/Enclave_private.pem $(Enclave_Name)
	@echo "SIGN =>  $@"
	@$(SGX_ENCLAVE_SIGNER) sign -key $< -enclave $(Enclave_Name) -out $@ -config $(Enclave_Config_File)

######## Service Provider/Master Objects ########

ServiceProvider/%.o: ServiceProvider/%.cpp
	@echo "CXX  <=  $<"
	@$(CXX) $(ServiceProvider_Cpp_Flags) -c $< -o $@

$(ServiceProvider_Name): $(ServiceProvider_Cpp_Objects)
	@echo "LINK =>  $@"
	@$(CXX) $^ -o $@ $(ServiceProvider_Link_Flags)

$(SP_SO_Name): $(ServiceProvider_Cpp_Objects)
	@echo "LINK =>  $@"
	@$(CXX) $^ $(ServiceProvider_Link_Flags) -shared -o $@

.PHONY: clean

clean:
	@rm -f \
		$(App_SO_Name) $(App_Name) \
		$(Enclave_Name) $(Signed_Enclave_Name) \
		$(App_Cpp_Objects) App/Enclave_u.* \
		$(Enclave_Cpp_Objects) Enclave/Enclave_t.* \
		$(ServiceProvider_Cpp_Objects) $(SP_SO_Name) $(ServiceProvider_Name)
