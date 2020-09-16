/*
 * Copyright (C) Alex Nekipelov (alex@nekipelov.net)
 * License: MIT
 */

// Changes [JM] by John Mechalas <john.p.mechalas@intel.com>

#ifndef HTTPPARSER_RESPONSE_H
#define HTTPPARSER_RESPONSE_H

#include <string>
#include <vector>
#include <algorithm> // Added by JM
#include <sstream>

namespace httpparser
{

struct Response {
    Response()
        : versionMajor(0), versionMinor(0), keepAlive(false), statusCode(0)
    {}
    
    struct HeaderItem
    {
        std::string name;
        std::string value;
    };

    int versionMajor;
    int versionMinor;
    std::vector<HeaderItem> headers;
    std::vector<char> content;
    bool keepAlive;
    
    unsigned int statusCode;
    std::string status;

    std::string inspect() const
    {
        std::stringstream stream;
        stream << "HTTP/" << versionMajor << "." << versionMinor
               << " " << statusCode << " " << status << "\n";

        for(std::vector<Response::HeaderItem>::const_iterator it = headers.begin();
            it != headers.end(); ++it)
        {
            stream << it->name << ": " << it->value << "\n";
        }

        std::string data(content.begin(), content.end());
		// Added "\n" so it prints like its received. - JM
        stream << "\n" << data << "\n";
        return stream.str();
    }

	// content_string() by JM
	std::string content_string() const
	{
        std::stringstream stream;

        std::string data(content.begin(), content.end());
        stream << data;

		return stream.str();
	}

	// headers_as_string() by JM
	std::string headers_as_string(std::string name) const
	{
        std::stringstream stream;
		// Just in case we get more than one header with the same name.
		for(std::vector<Response::HeaderItem>::const_iterator it = headers.begin();
            it != headers.end(); ++it)
		{
			if ( is_equal_ncase(it->name, name) ) stream << it->value << "\n";
		}

		return stream.str();
	}

	// Case-insensitive string comparison - JM
	bool is_equal_ncase(std::string a, std::string b) const
	{
		transform(a.begin(), a.end(), a.begin(), ::toupper);
		transform(b.begin(), b.end(), b.begin(), ::toupper);
		return ( a == b );
	}

};

} // namespace httpparser

#endif // HTTPPARSER_RESPONSE_H

