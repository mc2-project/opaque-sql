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

#ifndef __AGENT__WGET__H
#define __AGENT__WGET__H

#include "httpparser/response.h"
#include "iasrequest.h"
#include "agent.h"

using namespace httpparser;
using namespace std;

#include <string>

class Agent;
class IAS_Request;

class AgentWget : protected Agent
{
public:
	static string name;

	AgentWget(IAS_Connection *conn) : Agent(conn) {};
	int request(string const &url, string const &post, Response &response);
};

#endif
