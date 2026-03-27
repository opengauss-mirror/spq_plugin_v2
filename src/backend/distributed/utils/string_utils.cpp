/*-------------------------------------------------------------------------
 *
 * string_utils.c
 *
 * This file contains functions to perform useful operations on strings.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/relay_utility.h"
#include "distributed/string_utils.h"
#include "parser/scansup.h"

/*
 * ConvertIntToString returns the string version of given integer.
 */
char* ConvertIntToString(int val)
{
    StringInfo str = makeStringInfo();

    appendStringInfo(str, "%d", val);

    return str->data;
}

/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a "\xXX" string
 *
 * Makes a newly allocated copy of the string passed in, which must be
 * '\0'-terminated. In the backend, additional alloc_flags may be provided and
 * will be passed as-is to palloc_extended(); in the frontend, alloc_flags is
 * ignored and the copy is malloc'd.
 *
 * This function exists specifically to deal with filtering out
 * non-ASCII characters in a few places where the client can provide an almost
 * arbitrary string (and it isn't checked to ensure it's a valid username or
 * database name or similar) and we don't want to have control characters or other
 * things ending up in the log file where server admins might end up with a
 * messed up terminal when looking at them.
 *
 * In general, this function should NOT be used- instead, consider how to handle
 * the string without needing to filter out the non-ASCII characters.
 *
 * Ultimately, we'd like to improve the situation to not require replacing all
 * non-ASCII but perform more intelligent filtering which would allow UTF or
 * similar, but it's unclear exactly what we should allow, so stick to ASCII only
 * for now.
 */
char* pg_clean_ascii(const char* str, int alloc_flags)
{
    size_t dstlen;
    char* dst;
    const char* p;
    size_t i = 0;

    /* Worst case, each byte can become four bytes, plus a null terminator. */
    dstlen = strlen(str) * 4 + 1;

#ifdef FRONTEND
    dst = static_cast<char*>(malloc(dstlen));
#else
    dst = static_cast<char*>(palloc_extended(dstlen, alloc_flags));
#endif

    if (!dst)
        return NULL;

    for (p = str; *p != '\0'; p++) {

        /* Only allow clean ASCII chars in the string */
        if (*p < 32 || *p > 126) {
            Assert(i < (dstlen - 3));
            snprintf(&dst[i], dstlen - i, "\\x%02x", (unsigned char)*p);
            i += 4;
        } else {
            Assert(i < dstlen);
            dst[i] = *p;
            i++;
        }
    }

    Assert(i < dstlen);
    dst[i] = '\0';
    return dst;
}

/*
 * SplitGUCList --- parse a string containing identifiers or file names
 *
 * This is used to split the value of a GUC_LIST_QUOTE GUC variable, without
 * presuming whether the elements will be taken as identifiers or file names.
 * We assume the input has already been through flatten_set_variable_args(),
 * so that we need never downcase (if appropriate, that was done already).
 * Nor do we ever truncate, since we don't know the correct max length.
 * We disallow embedded whitespace for simplicity (it shouldn't matter,
 * because any embedded whitespace should have led to double-quoting).
 * Otherwise the API is identical to SplitIdentifierString.
 *
 * XXX it's annoying to have so many copies of this string-splitting logic.
 * However, it's not clear that having one function with a bunch of option
 * flags would be much better.
 *
 * XXX there is a version of this function in src/bin/pg_dump/dumputils.c.
 * Be sure to update that if you have to change this.
 *
 * Inputs:
 *     rawstring: the input string; must be overwritable!      On return, it's
 *                        been modified to contain the separated identifiers.
 *     separator: the separator punctuation expected between identifiers
 *                        (typically '.' or ',').  Whitespace may also appear around
 *                        identifiers.
 * Outputs:
 *     namelist: filled with a palloc'd list of pointers to identifiers within
 *                       rawstring.  Caller should list_free() this even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 */
bool SplitGUCList(char* rawstring, char separator, List** namelist)
{
    char* nextp = rawstring;
    bool done = false;

    *namelist = NIL;

    while (scanner_isspace(*nextp))
        nextp++; /* skip leading whitespace */

    if (*nextp == '\0')
        return true; /* allow empty string */

    /* At the top of the loop, we are at start of a new identifier. */
    do {
        char* curname;
        char* endp;

        if (*nextp == '"') {
            /* Quoted name --- collapse quote-quote pairs */
            curname = nextp + 1;
            for (;;) {
                endp = strchr(nextp + 1, '"');
                if (endp == NULL)
                    return false; /* mismatched quotes */
                if (endp[1] != '"')
                    break; /* found end of quoted name */
                /* Collapse adjacent quotes into one quote, and look again */
                memmove(endp, endp + 1, strlen(endp));
                nextp = endp;
            }
            /* endp now points at the terminating quote */
            nextp = endp + 1;
        } else {
            /* Unquoted name --- extends to separator or whitespace */
            curname = nextp;
            while (*nextp && *nextp != separator && !scanner_isspace(*nextp))
                nextp++;
            endp = nextp;
            if (curname == nextp)
                return false; /* empty unquoted name not allowed */
        }

        while (scanner_isspace(*nextp))
            nextp++; /* skip trailing whitespace */

        if (*nextp == separator) {
            nextp++;
            while (scanner_isspace(*nextp))
                nextp++; /* skip leading whitespace for next */
                         /* we expect another name, so done remains false */
        } else if (*nextp == '\0')
            done = true;
        else
            return false; /* invalid syntax */

        /* Now safe to overwrite separator with a null */
        *endp = '\0';

        /*
         * Finished isolating current name --- add it to list
         */
        *namelist = lappend(*namelist, curname);

        /* Loop back if we didn't reach end of string */
    } while (!done);

    return true;
}

/*
 * IsValidPort checks if the given port number is valid (0-65535).
 */
bool IsValidPort(int port)
{
    return port >= 0 && port <= 65535;
}

/*
 * IsValidIPAddress checks if the given string is a valid IPv4 address.
 * It validates that the IP has exactly 4 octets, each between 0-255,
 * and doesn't contain leading zeros (except for "0" itself).
 */
bool IsValidIPAddress(const char* ip)
{
    if (ip == NULL)
        return false;

    const char* p = ip;
    int octetCount = 0;

    while (*p != '\0' && octetCount < 4) {
        // Skip leading whitespace (shouldn't happen in normal cases)
        while (*p == ' ' || *p == '\t')
            p++;

        // Check for empty octet
        if (*p == '.' || *p == '\0')
            return false;

        // Parse the octet
        const char* start = p;
        int octet = 0;

        // Check for leading zero (except for "0" itself)
        if (*p == '0' && *(p + 1) != '.' && *(p + 1) != '\0')
            return false;

        while (*p >= '0' && *p <= '9') {
            octet = octet * 10 + (*p - '0');
            p++;
        }

        // Validate octet range
        if (octet < 0 || octet > 255)
            return false;

        // Check if we have a valid separator or end
        if (*p == '.') {
            p++;
            octetCount++;
        } else if (*p == '\0') {
            octetCount++;
            break;
        } else {
            // Invalid character
            return false;
        }

        // Check for empty octet after dot
        if (*p == '\0' && octetCount < 4)
            return false;
    }

    // Must have exactly 4 octets
    return octetCount == 4 && *p == '\0';
}

/*
 * IsValidHostname checks if the given string is a valid hostname according to DNS
 * standards. Valid hostnames:
 * - Can contain letters (a-z, A-Z), digits (0-9), hyphens (-), and dots (.)
 * - Cannot start or end with a hyphen or dot
 * - Each label (between dots) must be 1-63 characters long
 * - Total length must not exceed 253 characters
 * - The top-level domain (last label) must contain at least one letter (cannot be all
 * digits)
 * - Labels cannot be all numeric if they represent an IP-like pattern with 4 labels
 */
bool IsValidHostname(const char* hostname)
{
    if (hostname == NULL)
        return false;

    size_t len = strlen(hostname);
    if (len == 0 || len > 253)
        return false;

    // Check if it starts or ends with invalid characters
    if (hostname[0] == '-' || hostname[0] == '.' || hostname[len - 1] == '-' ||
        hostname[len - 1] == '.')
        return false;

    // Split into labels and validate each one
    char* tempHostname = pstrdup(hostname);
    char* token = strtok(tempHostname, ".");
    char* labels[254];  // Maximum possible labels
    int labelCount = 0;

    while (token != NULL && labelCount < 254) {
        size_t tokenLen = strlen(token);
        // Each label must be 1-63 characters
        if (tokenLen == 0 || tokenLen > 63)
            return false;

        // Check valid characters in label
        bool hasLetter = false;
        for (size_t i = 0; i < tokenLen; i++) {
            char c = token[i];
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                hasLetter = true;
            } else if (c >= '0' && c <= '9') {
                // digit is ok
            } else if (c == '-') {
                // hyphen is ok, but not at start or end of label
                if (i == 0 || i == tokenLen - 1)
                    return false;
            } else {
                // Invalid character
                return false;
            }
        }

        labels[labelCount] = pstrdup(token);
        labelCount++;
        token = strtok(NULL, ".");
    }

    pfree(tempHostname);

    if (labelCount == 0)
        return false;

    // Top-level domain (last label) must contain at least one letter
    bool tldHasLetter = false;
    char* tld = labels[labelCount - 1];
    size_t tldLen = strlen(tld);
    for (size_t i = 0; i < tldLen; i++) {
        char c = tld[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            tldHasLetter = true;
            break;
        }
    }

    if (!tldHasLetter) {
        // Free allocated memory
        for (int i = 0; i < labelCount; i++) {
            pfree(labels[i]);
        }
        return false;
    }

    // Special case: if we have exactly 4 labels and all are numeric,
    // treat as IP address attempt
    if (labelCount == 4) {
        bool allNumeric = true;
        for (int i = 0; i < 4; i++) {
            char* label = labels[i];
            size_t labelLen = strlen(label);
            bool labelIsNumeric = true;

            // Check if label contains only digits
            for (size_t j = 0; j < labelLen; j++) {
                if (label[j] < '0' || label[j] > '9') {
                    labelIsNumeric = false;
                    break;
                }
            }

            if (!labelIsNumeric) {
                allNumeric = false;
                break;
            }
        }

        if (allNumeric) {
            // Free allocated memory
            for (int i = 0; i < labelCount; i++) {
                pfree(labels[i]);
            }
            // Validate as IP address
            return IsValidIPAddress(hostname);
        }
    }

    // Free allocated memory
    for (int i = 0; i < labelCount; i++) {
        pfree(labels[i]);
    }

    return true;
}

/*
 * ValidateNodeAddress validates both address (IP or hostname) and port together.
 * Returns true if both are valid, false otherwise.
 * The address can be either a valid IPv4 address or a valid hostname.
 */
bool ValidateNodeAddress(const char* address, int port)
{
    if (!IsValidPort(port))
        return false;

    // Check if it's a valid IP address
    if (IsValidIPAddress(address))
        return true;

    // Check if it's a valid hostname
    if (IsValidHostname(address))
        return true;

    return false;
}

void CheckIPPort(const char* ip, int port)
{
    if (!ValidateNodeAddress(ip, port)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid IP address or port. Port must be 0-65535 and IP "
                               "must be valid IPv4 format without leading zeros")));
    }
}