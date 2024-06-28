/*
 * Copyright 2017-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util;

public class MemcachedStatus {
    public static final short SUCCESS = 0x00;
    public static final short NOT_FOUND = 0x01;
    public static final short KEY_EXISTS = 0x02;
    public static final short VALUE_TOO_LARGE = 0x03;
    public static final short INVALID_ARGUMENTS = 0x04;
    public static final short ITEM_NOT_STORED = 0x05;
    public static final short INC_DEC_NON_NUMERIC = 0x06;
    public static final short NOT_MY_VBUCKET = 0x07;
    public static final short NOT_CONNECTED_TO_BUCKET = 0x08;
    public static final short STALE_AUTHENTICATION = 0x1f;
    public static final short AUTHENTICATION_ERROR = 0x20;
    public static final short AUTHENTICATION_CONTINUE = 0x21;
    public static final short OUT_OF_RANGE = 0x22;
    public static final short ROLLBACK = 0x23;
    public static final short NO_ACCESS = 0x24;
    public static final short NODE_BEING_INITIALIZED = 0x25;
    public static final short UNKNOWN_COMMAND = 0x81;
    public static final short OUT_OF_MEMORY = 0x82;
    public static final short NOT_SUPPORTED = 0x83;
    public static final short INTERNAL_ERROR = 0x84;
    public static final short BUSY = 0x85;
    public static final short TEMP_FAILURE = 0x86;
    public static final short UNKNOWN_COLLECTION = 0x88;
    public static final short MANIFEST_IS_AHEAD = 0x8b;
    public static final short STREAMID_INVALID = 0x8d;
    public static final short SUBDOC_NOT_FOUND = 0xc0;
    public static final short SUBDOC_NOT_COLLECTION = 0xc1;
    public static final short SUBDOC_INCORRECT_SYNTAX = 0xc2;
    public static final short SUBDOC_TOO_LONG_PATH = 0xc3;
    public static final short SUBDOC_TOO_MANY_LEVELS = 0xc4;
    public static final short SUBDOC_INVALID_JSON_MODIFICATION = 0xc5;
    public static final short SUBDOC_INVALID_JSON = 0xc6;
    public static final short SUBDOC_OUT_OF_SUPPORTED_NUMERIC_RANGE = 0xc7;
    public static final short SUBDOC_OPERATION_RESULT_OUT_OF_SUPPORTED_RANGE = 0xc8;
    public static final short SUBDOC_PATH_ALREADY_EXISTS = 0xc9;
    public static final short SUBDOC_OPERATION_RESULT_TOO_DEEP = 0xca;
    public static final short SUBDOC_INVALID_COMMAND_COMBINATION = 0xcb;
    public static final short SUBDOC_PARTIAL_OPERATION_FAILURE = 0xcc;
    public static final short SUBDOC_SUCCESS_ON_DELETED_DOC = 0xcd;

    private MemcachedStatus() {
    }

    public static String toString(short response) {
        return description(response) + " (0x" + Integer.toHexString(response) + ")";
    }

    private static String description(short response) {
        switch (response) {
            case SUCCESS:
                return "No error";
            case NOT_FOUND:
                return "Key not found";
            case KEY_EXISTS:
                return "Key exists";
            case VALUE_TOO_LARGE:
                return "Value too large";
            case INVALID_ARGUMENTS:
                return "Invalid arguments";
            case ITEM_NOT_STORED:
                return "Item not stored";
            case INC_DEC_NON_NUMERIC:
                return "Incr/Decr on a non-numeric value";
            case NOT_MY_VBUCKET:
                return "The vbucket belongs to another server";
            case NOT_CONNECTED_TO_BUCKET:
                return "The connection is not connected to a bucket";
            case STALE_AUTHENTICATION:
                return "The authentication context is stale, please re-authenticate";
            case AUTHENTICATION_ERROR:
                return "Authentication error";
            case AUTHENTICATION_CONTINUE:
                return "Authentication continue";
            case OUT_OF_RANGE:
                return "The requested value is outside the legal ranges";
            case ROLLBACK:
                return "Rollback required";
            case NO_ACCESS:
                return "No access";
            case NODE_BEING_INITIALIZED:
                return "The node is being initialized";
            case UNKNOWN_COMMAND:
                return "Unknown command";
            case OUT_OF_MEMORY:
                return "Out of memory";
            case NOT_SUPPORTED:
                return "Not supported";
            case INTERNAL_ERROR:
                return "Internal error";
            case BUSY:
                return "Busy";
            case TEMP_FAILURE:
                return "Temporary failure";
            case UNKNOWN_COLLECTION:
                return "Unknown collection";
            case MANIFEST_IS_AHEAD:
                return "Manifest is ahead";
            case STREAMID_INVALID:
                return "Invalid stream-ID (or required and not supplied)";
            case SUBDOC_NOT_FOUND:
                return "(Subdoc) The provided path does not exist in the document";
            case SUBDOC_NOT_COLLECTION:
                return "(Subdoc) One of path components treats a non-dictionary as a dictionary, or a non-array as an array";
            case SUBDOC_INCORRECT_SYNTAX:
                return "(Subdoc) The path’s syntax was incorrect";
            case SUBDOC_TOO_LONG_PATH:
                return "(Subdoc) The path provided is too large; either the string is too long, or it contains too many components";
            case SUBDOC_TOO_MANY_LEVELS:
                return "(Subdoc) The document has too many levels to parse";
            case SUBDOC_INVALID_JSON_MODIFICATION:
                return "(Subdoc) The value provided will invalidate the JSON if inserted";
            case SUBDOC_INVALID_JSON:
                return "(Subdoc) The existing document is not valid JSON";
            case SUBDOC_OUT_OF_SUPPORTED_NUMERIC_RANGE:
                return "(Subdoc) The existing number is out of the valid range for arithmetic ops";
            case SUBDOC_OPERATION_RESULT_OUT_OF_SUPPORTED_RANGE:
                return "(Subdoc) The operation would result in a number outside the valid range";
            case SUBDOC_PATH_ALREADY_EXISTS:
                return "(Subdoc) The requested operation requires the path to not already exist, but it exists";
            case SUBDOC_OPERATION_RESULT_TOO_DEEP:
                return "(Subdoc) Inserting the value would cause the document to be too deep";
            case SUBDOC_INVALID_COMMAND_COMBINATION:
                return "(Subdoc) An invalid combination of commands was specified";
            case SUBDOC_PARTIAL_OPERATION_FAILURE:
                return "(Subdoc) Specified key was successfully found, but one or more path operations failed. Examine the individual lookup_result (MULTI_LOOKUP) / mutation_result (MULTI_MUTATION) structures for details";
            case SUBDOC_SUCCESS_ON_DELETED_DOC:
                return "(Subdoc) Operation completed successfully on a deleted document";
            default:
                return "Unknown response status";
        }
    }

    /**
     * indicates if {@link #description(short)} result of the supplied status code is contained within
     * the supplied throwable's {@link Throwable#getMessage()}
     */
    public static boolean messageContains(Throwable th, short status) {
        return th.getMessage() != null && th.getMessage().contains(description(status));
    }
}
