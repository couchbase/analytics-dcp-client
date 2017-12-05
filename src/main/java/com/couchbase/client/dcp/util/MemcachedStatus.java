/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util;

public class MemcachedStatus {
    private MemcachedStatus() {
    }

    public static String toString(short response) {
        switch (response) {
            case 0x0000:
                return "No error";
            case 0x0001:
                return "Key not found";
            case 0x0002:
                return "Key exists";
            case 0x0003:
                return "Value too large";
            case 0x0004:
                return "Invalid arguments";
            case 0x0005:
                return "Item not stored";
            case 0x0006:
                return "Incr/Decr on a non-numeric value";
            case 0x0007:
                return "The vbucket belongs to another server";
            case 0x0008:
                return "The connection is not connected to a bucket";
            case 0x001f:
                return "The authentication context is stale, please re-authenticate";
            case 0x0020:
                return "Authentication error";
            case 0x0021:
                return "Authentication continue";
            case 0x0022:
                return "The requested value is outside the legal ranges";
            case 0x0023:
                return "Rollback required";
            case 0x0024:
                return "No access";
            case 0x0025:
                return "The node is being initialized";
            case 0x0081:
                return "Unknown command";
            case 0x0082:
                return "Out of memory";
            case 0x0083:
                return "Not supported";
            case 0x0084:
                return "Internal error";
            case 0x0085:
                return "Busy";
            case 0x0086:
                return "Temporary failure";
            case 0x00c0:
                return "(Subdoc) The provided path does not exist in the document";
            case 0x00c1:
                return "(Subdoc) One of path components treats a non-dictionary as a dictionary, or a non-array as an array";
            case 0x00c2:
                return "(Subdoc) The path’s syntax was incorrect";
            case 0x00c3:
                return "(Subdoc) The path provided is too large; either the string is too long, or it contains too many components";
            case 0x00c4:
                return "(Subdoc) The document has too many levels to parse";
            case 0x00c5:
                return "(Subdoc) The value provided will invalidate the JSON if inserted";
            case 0x00c6:
                return "(Subdoc) The existing document is not valid JSON";
            case 0x00c7:
                return "(Subdoc) The existing number is out of the valid range for arithmetic ops";
            case 0x00c8:
                return "(Subdoc) The operation would result in a number outside the valid range";
            case 0x00c9:
                return "(Subdoc) The requested operation requires the path to not already exist, but it exists";
            case 0x00ca:
                return "(Subdoc) Inserting the value would cause the document to be too deep";
            case 0x00cb:
                return "(Subdoc) An invalid combination of commands was specified";
            case 0x00cc:
                return "(Subdoc) Specified key was successfully found, but one or more path operations failed. Examine the individual lookup_result (MULTI_LOOKUP) / mutation_result (MULTI_MUTATION) structures for details";
            case 0x00cd:
                return "(Subdoc) Operation completed successfully on a deleted document";
            default:
                return "Unknown response status";
        }
    }
}
