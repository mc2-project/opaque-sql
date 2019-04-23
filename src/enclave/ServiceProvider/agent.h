/*

Copyright 2018 Intel Corporation

This software and the related documents are Intel copyrighted materials,
and your use of them is governed by the express license under which they
were provided to you (License). Unless the License provides otherwise,
you may not use, modify, copy, publish, distribute, disclose or transmit
this software or the related documents without Intel's prior written
permission.

This software and the related documents are provided as is, with no
express or implied warranties, other than those that are expressly stated
in the License.

*/

#ifndef __AGENT_H
#define __AGENT_H

#ifdef _WIN32
# define DEFAULT_CA_BUNDLE DEFAULT_CA_BUNDLE_WIN32
#else
# define DEFAULT_CA_BUNDLE DEFAULT_CA_BUNDLE_LINUX
#endif

#include "httpparser/response.h"
#include "iasrequest.h"

using namespace httpparser;

using namespace std;

#include <string>

class IAS_Connection;

class Agent {
protected:
	IAS_Connection *conn;

public:
	Agent(IAS_Connection *conn_in) { conn= conn_in; }
	virtual ~Agent() { };

	virtual int initialize() { return 1; };
	virtual int request(string const &url, string const &postdata,
		Response &response) = 0;
};


#endif

