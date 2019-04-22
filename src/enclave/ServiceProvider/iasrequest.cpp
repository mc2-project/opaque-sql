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


#ifndef _WIN32
#include "config.h"
#endif
#include <string.h>
#include <stdio.h>
#include <openssl/rand.h>
#include <openssl/x509.h>
#include "crypto.h"
#include "common.h"
#include "agent.h"
#ifdef _WIN32
# define AGENT_LIBCURL
#else
# define AGENT_WGET
#endif
#ifdef AGENT_WGET
# include "agent_wget.h"
#endif
#ifdef AGENT_LIBCURL
# include "agent_curl.h"
#endif
#include "iasrequest.h"
#include "logfile.h"
#include "httpparser/response.h"
#include "base64.h"
#include "hexutil.h"
#include "settings.h"

using namespace std;
using namespace httpparser;

#include <string>
#include <exception>

extern "C" {
	extern char verbose;
	extern char debug;
};

static string ias_servers[2]= {
    IAS_SERVER_DEVELOPMENT_HOST,
    IAS_SERVER_PRODUCTION_HOST
};

static string url_decode(string str);

void ias_list_agents (FILE *fp)
{
	fprintf(fp, "Available user agents:\n");
#ifdef AGENT_LIBCURL
	fprintf(fp, "%s\n", AgentCurl::name.c_str());
#endif
#ifdef AGENT_WGET
	fprintf(fp, "%s\n", AgentWget::name.c_str());
#endif
}

IAS_Connection::IAS_Connection(int server_idx, uint32_t flags)
{
	c_server= ias_servers[server_idx];
	c_cert_type= "PEM";
	c_flags= flags;
	c_pwlen= 0;
	c_key_passwd= NULL;
	c_xor= NULL;
	c_server_port= IAS_PORT;
	c_proxy_mode= IAS_PROXY_AUTO;
	c_agent= NULL;
	c_agent_name= "";
}

IAS_Connection::~IAS_Connection()
{
	if ( c_key_passwd != NULL ) delete[] c_key_passwd;
	if ( c_xor != NULL ) delete[] c_xor;
}

int IAS_Connection::agent(const char *agent_name)
{
#ifdef AGENT_LIBCURL
	if ( AgentCurl::name == agent_name ) {
		c_agent_name= agent_name;
		return 1;
	}
#endif
#ifdef AGENT_WGET
	if ( AgentWget::name == agent_name ) {
		c_agent_name= agent_name;
		return 1;
	}
#endif

	return 0;
}

int IAS_Connection::proxy(const char *server, uint16_t port)
{
	int rv= 1;
	try {
		c_proxy_server= server;
	}
	catch (...) {
		rv= 0;
	}
	c_proxy_port= port;

	c_proxy_mode = IAS_PROXY_FORCE;

	return rv;
}

string IAS_Connection::proxy_url()
{
	string proxy_url;

	if ( c_proxy_server == "" ) return "";

	proxy_url= "http://" + c_proxy_server;

	if ( c_proxy_port != 80 ) {
		proxy_url+= ":";
		proxy_url+= to_string(c_proxy_port);
	}

	return proxy_url;
}

int IAS_Connection::client_cert(const char *file, const char *certtype)
{
	int rv= 1;
	try {
		c_cert_file= file;
		if ( certtype != NULL ) c_cert_type= certtype;
	}
	catch (...) {
		rv= 0;
	}
	return rv;
}

int IAS_Connection::client_key(const char *file, const char *passwd)
{
	size_t i;

	try {
		c_key_file= file;
	}
	catch (...) {
		return 0;
	}

	if ( passwd != NULL ) {
		c_pwlen= strlen(passwd);
		try {
			c_key_passwd= new unsigned char[c_pwlen];
			c_xor= new unsigned char[c_pwlen];
		}
		catch (...) { 
			if ( c_key_passwd != NULL ) delete[] c_key_passwd;
			return 0;
		}

		RAND_bytes(c_xor, (int) c_pwlen);
		for (i= 0; i< c_pwlen; ++i) c_key_passwd[i]=
			(unsigned char) passwd[i]^c_xor[i];
	}

	if ( debug ) {
		eprintf("+++ Password:           %s\n", hexstring(passwd, c_pwlen));
		eprintf("+++ One-time pad:       %s\n", hexstring(c_xor, c_pwlen));
		eprintf("+++ Encrypted password: %s\n", hexstring(c_key_passwd,
			c_pwlen));
	}
	return 1;
}

int IAS_Connection::client_key_passwd(char **passwd, size_t *pwlen)
{
	size_t i;
	char *ch;

	*pwlen= c_pwlen;

	if ( c_pwlen == 0 ) {
		*passwd= NULL;
		return 1;
	}

	try {
		*passwd= new char[c_pwlen+1];
	}
	catch (...) {
		return 0;
	}

	for (i= 0, ch= *passwd; i< c_pwlen; ++i, ++ch) 
		 *ch= (char) (c_key_passwd[i] ^ c_xor[i]);
	*ch= 0;

	return 1;
}

string IAS_Connection::base_url()
{
	string url= "https://" + c_server;

	if ( c_server_port != 443 ) {
		url+= ":";
		url+= to_string(c_server_port);
	}

	url+= "/attestation/sgx/v";

	return url;
}

// Reuse the existing agent or get a new one.

Agent *IAS_Connection::agent()
{
	if ( c_agent == NULL ) return this->new_agent();
	return c_agent;
}

// Get a new agent (and discard the old one if there was one)

Agent *IAS_Connection::new_agent()
{
	Agent *newagent= NULL;

	// If we've requested a specific agent, use that one

	if ( c_agent_name.length() ) {
#ifdef AGENT_LIBCURL
		if ( c_agent_name == AgentCurl::name ) {
			try {
				newagent= (Agent *) new AgentCurl(this);
			}
			catch (...) {
				return NULL;
			}
		}
#endif		
#ifdef AGENT_WGET
		if ( c_agent_name == AgentWget::name ) {
			try {
				newagent= (Agent *) new AgentWget(this);
			}
			catch (...) {
				return NULL;
			}
		}
#endif
	} else {
		// Otherwise, take the first available using this hardcoded
		// order of preference.

#ifdef AGENT_LIBCURL
		if ( debug ) eprintf("+++ Trying agent_curl\n");
		try {
			newagent= (Agent *) new AgentCurl(this);
		}
		catch (...) { newagent= NULL; }
#endif
#ifdef AGENT_WGET
		if ( newagent == NULL ) {
			if ( debug ) eprintf("+++ Trying agent_wget\n");
			try {
				newagent= (Agent *) new AgentWget(this);
			}
			catch (...) { newagent= NULL; }
		}
#endif
	}

	if ( newagent == NULL ) return NULL;

	if ( newagent->initialize() == 0 ) {
		delete newagent;
		return NULL;
	}

	c_agent= newagent;
	return c_agent;
}

IAS_Request::IAS_Request(IAS_Connection *conn, uint16_t version)
{
	r_conn= conn;
	r_api_version= version;
}

IAS_Request::~IAS_Request()
{
}

ias_error_t IAS_Request::sigrl(uint32_t gid, string &sigrl)
{
	Response response;
	char sgid[9];
	string url= r_conn->base_url();
	Agent *agent= r_conn->new_agent();

	snprintf(sgid, 9, "%08x", gid);

	url+= to_string(r_api_version);
	url+= "/sigrl/";
	url+= sgid;

	if ( verbose ) {
		edividerWithText("IAS sigrl HTTP Request");
		eprintf("HTTP GET %s\n", url.c_str());
		edivider();
	}

	if ( agent->request(url, "", response) ) {
		if ( verbose ) {
			edividerWithText("IAS sigrl HTTP Response");
			eputs(response.inspect().c_str());
			edivider();
		}

		if ( response.statusCode == IAS_OK ) {
			sigrl= response.content_string();
		} 
	} else {
		eprintf("Could not query IAS\n");
		return IAS_QUERY_FAILED;
	}

	return response.statusCode;
}

ias_error_t IAS_Request::report(map<string,string> &payload, string &content,
	vector<string> &messages)
{
	Response response;
	map<string,string>::iterator imap;
	string url= r_conn->base_url();
	string certchain;
	string body= "{\n";
	size_t cstart, cend, count, i;
	vector<X509 *> certvec;
	X509 **certar;
	X509 *sign_cert;
	STACK_OF(X509) *stack;
	string sigstr, header;
	size_t sigsz;
	ias_error_t status;
	int rv;
	unsigned char *sig= NULL;
	EVP_PKEY *pkey= NULL;
	Agent *agent= r_conn->new_agent();
	
	try {
		for (imap= payload.begin(); imap!= payload.end(); ++imap) {
			if ( imap != payload.begin() ) {
				body.append(",\n");
			}
			body.append("\"");
			body.append(imap->first);
			body.append("\":\"");
			body.append(imap->second);
			body.append("\"");
		}
		body.append("\n}");

		url+= to_string(r_api_version);
		url+= "/report";
	}
	catch (...) {
		return IAS_QUERY_FAILED;
	}

	if ( verbose ) {
		edividerWithText("IAS report HTTP Request");
		eprintf("HTTP POST %s\n", url.c_str());
		edivider();
	}

	if ( agent->request(url, body, response) ) {
		if ( verbose ) {
			edividerWithText("IAS report HTTP Response");
			eputs(response.inspect().c_str());
			edivider();
		}
	} else {
		eprintf("Could not query IAS\n");
		return IAS_QUERY_FAILED;
	}

	if ( response.statusCode != IAS_OK ) return response.statusCode;

	/*
	 * The response body has the attestation report. The headers have
	 * a signature of the report, and the public signing certificate.
	 * We need to:
	 *
	 * 1) Verify the certificate chain, to ensure it's issued by the
	 *    Intel CA (passed with the -A option).
	 *
	 * 2) Extract the public key from the signing cert, and verify
	 *    the signature.
	 */

	// Get the certificate chain from the headers 

	certchain= response.headers_as_string("X-IASReport-Signing-Certificate");
	if ( certchain == "" ) {
		eprintf("Header X-IASReport-Signing-Certificate not found\n");
		return IAS_BAD_CERTIFICATE;
	}

	// URL decode
	try {
		certchain= url_decode(certchain);
	}
	catch (...) {
		eprintf("invalid URL encoding in header X-IASReport-Signing-Certificate\n");
		return IAS_BAD_CERTIFICATE;
	}

	// Build the cert stack. Find the positions in the string where we
	// have a BEGIN block.

	cstart= cend= 0;
	while (cend != string::npos ) {
		X509 *cert;
		size_t len;

		cend= certchain.find("-----BEGIN", cstart+1);
		len= ( (cend == string::npos) ? certchain.length() : cend )-cstart;

		if ( verbose ) {
			edividerWithText("Certficate");
			eputs(certchain.substr(cstart, len).c_str());
			eprintf("\n");
			edivider();
		}

		if ( ! cert_load(&cert, certchain.substr(cstart, len).c_str()) ) {
			crypto_perror("cert_load");
			return IAS_BAD_CERTIFICATE;
		}

		certvec.push_back(cert);
		cstart= cend;
	}

	count= certvec.size();
	if ( debug ) eprintf( "+++ Found %lu certificates in chain\n", count);

	certar= (X509**) malloc(sizeof(X509 *)*(count+1));
	if ( certar == 0 ) {
		perror("malloc");
		return IAS_INTERNAL_ERROR;
	}
	for (i= 0; i< count; ++i) certar[i]= certvec[i];
	certar[count]= NULL;

	// Create a STACK_OF(X509) stack from our certs

	stack= cert_stack_build(certar);
	if ( stack == NULL ) {
		crypto_perror("cert_stack_build");
		return IAS_INTERNAL_ERROR;
	}

	// Now verify the signing certificate

	rv= cert_verify(this->conn()->cert_store(), stack);

	if ( ! rv ) {
		crypto_perror("cert_stack_build");
		eprintf("certificate verification failure\n");
		status= IAS_BAD_CERTIFICATE;
		goto cleanup;
	} else {
		if ( debug ) eprintf("+++ certificate chain verified\n", rv);
	}

	// The signing cert is valid, so extract and verify the signature

	sigstr= response.headers_as_string("X-IASReport-Signature");
	if ( sigstr == "" ) {
		eprintf("Header X-IASReport-Signature not found\n");
		status= IAS_BAD_SIGNATURE;
		goto cleanup;
	}

	sig= (unsigned char *) base64_decode(sigstr.c_str(), &sigsz);
	if ( sig == NULL ) {
		eprintf("Could not decode signature\n");
		status= IAS_BAD_SIGNATURE;
		goto cleanup;
	}

	if ( verbose ) {
		edividerWithText("Report Signature");
		print_hexstring(stderr, sig, sigsz);
		if ( fplog != NULL ) print_hexstring(fplog, sig, sigsz);
		eprintf( "\n");
		edivider();
	}

	sign_cert= certvec[0]; /* The first cert in the list */

	/*
	 * The report body is SHA256 signed with the private key of the
	 * signing cert.  Extract the public key from the certificate and
	 * verify the signature.
	 */

	if ( debug ) eprintf("+++ Extracting public key from signing cert\n");
	pkey= X509_get_pubkey(sign_cert);
	if ( pkey == NULL ) {
		eprintf("Could not extract public key from certificate\n");
		free(sig);
		status= IAS_INTERNAL_ERROR;
		goto cleanup;
	}

	content= response.content_string();

	if ( debug ) {
		eprintf("+++ Verifying signature over report body\n");
		edividerWithText("Report");
		eputs(content.c_str());
		eprintf("\n");
		edivider();
		eprintf("Content-length: %lu bytes\n", response.content_string().length());
		edivider();
	}

	if ( ! sha256_verify((const unsigned char *) content.c_str(),
		content.length(), sig, sigsz, pkey, &rv) ) {

		free(sig);
		crypto_perror("sha256_verify");
		eprintf("Could not validate signature\n");
		status= IAS_BAD_SIGNATURE;
	} else {
		if ( rv ) {
			if ( verbose ) eprintf("+++ Signature verified\n");
			status= IAS_OK;
		} else {
			eprintf("Invalid report signature\n");
			status= IAS_BAD_SIGNATURE;
		}
	}

	/*
	 * Check for advisory headers
	 */

	header= response.headers_as_string("Advisory-URL");
	if ( header.length() ) messages.push_back(header);

	header= response.headers_as_string("Advisory-IDs");
	if ( header.length() ) messages.push_back(header);

cleanup:
	if ( pkey != NULL ) EVP_PKEY_free(pkey);
	cert_stack_free(stack);
	free(certar);
	for (i= 0; i<count; ++i) X509_free(certvec[i]);
	free(sig);

	return status;
}

// A simple URL decoder 

static string url_decode(string str)
{
	string decoded;
	size_t i;
	size_t len= str.length();

	for (i= 0; i< len; ++i) {
		if ( str[i] == '+' ) decoded+= ' ';
		else if ( str[i] == '%' ) {
			char *e= NULL;
			unsigned long int v;

			// Have a % but run out of characters in the string

			if ( i+3 > len ) throw std::length_error("premature end of string");

			v= strtoul(str.substr(i+1, 2).c_str(), &e, 16);

			// Have %hh but hh is not a valid hex code.
			if ( *e ) throw std::out_of_range("invalid encoding");

			decoded+= static_cast<char>(v);
			i+= 2;
		} else decoded+= str[i];
	}

	return decoded;
}

