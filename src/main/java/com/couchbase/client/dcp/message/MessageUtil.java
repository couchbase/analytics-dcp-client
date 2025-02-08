/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamState;

public class MessageUtil {

    /**
     * this does not take flex framing into account; consumers should instead use
     * {@link MessageUtil#getHeaderSize(ByteBuf)}
     */
    private static final int HEADER_SIZE = 24;

    public static final byte MAGIC_CONSUMER = (byte) 0x44;
    public static final byte MAGIC_INT = (byte) 0x79;
    public static final byte MAGIC_REQ = (byte) 0x80;
    public static final byte MAGIC_RES = (byte) 0x81;
    public static final byte MAGIC_REQ_FLEX = (byte) 0x08;

    public static final short KEY_LENGTH_OFFSET = 2;
    public static final short EXTRAS_LENGTH_OFFSET = 4;
    public static final short DATA_TYPE_OFFSET = 5;
    public static final short VBUCKET_OFFSET = 6;
    public static final short BODY_LENGTH_OFFSET = 8;
    public static final short OPAQUE_OFFSET = 12;
    public static final short CAS_OFFSET = 16;

    public static final short FLEX_FRAMING_EXTRAS_LENGTH_OFFSET = 2;
    public static final short FLEX_KEY_LENGTH_OFFSET = 3;

    public static final byte DATA_TYPE_JSON = 0x01;
    public static final byte DATA_TYPE_SNAPPY_COMPRESSED = 0x02;
    public static final byte DATA_TYPE_XATTR = 0x04;

    public static final byte VERSION_OPCODE = 0x0b;
    public static final byte HELO_OPCODE = 0x1f;
    public static final byte STAT_OPCODE = 0x10;
    public static final byte SASL_LIST_MECHS_OPCODE = 0x20;
    public static final byte SASL_AUTH_OPCODE = 0x21;
    public static final byte SASL_STEP_OPCODE = 0x22;
    public static final byte GET_ALL_VB_SEQNOS_OPCODE = 0x48;
    public static final byte OPEN_CONNECTION_OPCODE = 0x50;
    public static final byte DCP_ADD_STREAM_OPCODE = 0x51;
    public static final byte DCP_STREAM_CLOSE_OPCODE = 0x52;
    public static final byte DCP_STREAM_REQUEST_OPCODE = 0x53;
    public static final byte DCP_FAILOVER_LOG_OPCODE = 0x54;
    public static final byte DCP_STREAM_END_OPCODE = 0x55;
    public static final byte DCP_SNAPSHOT_MARKER_OPCODE = 0x56;
    public static final byte DCP_MUTATION_OPCODE = 0x57;
    public static final byte DCP_DELETION_OPCODE = 0x58;
    public static final byte DCP_EXPIRATION_OPCODE = 0x59;
    public static final byte DCP_FLUSH_OPCODE = 0x5a;
    public static final byte DCP_SET_VBUCKET_STATE_OPCODE = 0x5b;
    public static final byte DCP_NOOP_OPCODE = 0x5c;
    public static final byte DCP_BUFFER_ACK_OPCODE = 0x5d;
    public static final byte DCP_CONTROL_OPCODE = 0x5e;
    public static final byte DCP_SYSTEM_EVENT_OPCODE = 0x5f;
    public static final byte DCP_SEQNO_ADVANCED_OPCODE = 0x64;
    public static final byte DCP_OSO_SNAPSHOT_MARKER_OPCODE = 0x65;
    public static final byte SELECT_BUCKET_OPCODE = (byte) 0x89;
    public static final byte OBSERVE_SEQNO_OPCODE = (byte) 0x91;
    public static final byte GET_CLUSTER_CONFIG_OPCODE = (byte) 0xb5;
    public static final byte DCP_COLLECTIONS_MANIFEST_OPCODE = (byte) 0xba;
    public static final byte INTERNAL_ROLLBACK_OPCODE = 0x01;

    public static final short REQ_DCP_MUTATION = MAGIC_REQ << 8 | DCP_MUTATION_OPCODE & 0xff;
    public static final short REQ_DCP_DELETION = MAGIC_REQ << 8 | DCP_DELETION_OPCODE & 0xff;
    public static final short REQ_DCP_EXPIRATION = MAGIC_REQ << 8 | DCP_EXPIRATION_OPCODE & 0xff;
    public static final short REQ_STREAM_END = MAGIC_REQ << 8 | DCP_STREAM_END_OPCODE & 0xff;
    public static final short REQ_SEQNO_ADVANCED = MAGIC_REQ << 8 | DCP_SEQNO_ADVANCED_OPCODE & 0xff;
    public static final short REQ_OSO_SNAPSHOT_MARKER = MAGIC_REQ << 8 | DCP_OSO_SNAPSHOT_MARKER_OPCODE & 0xff;
    public static final short REQ_SYSTEM_EVENT = MAGIC_REQ << 8 | DCP_SYSTEM_EVENT_OPCODE & 0xff;
    public static final short REQ_SET_VBUCKET_STATE = MAGIC_REQ << 8 | DCP_SET_VBUCKET_STATE_OPCODE & 0xff;
    public static final short REQ_SNAPSHOT_MARKER = MAGIC_REQ << 8 | DCP_SNAPSHOT_MARKER_OPCODE & 0xff;
    public static final short REQ_DCP_NOOP = MAGIC_REQ << 8 | DCP_NOOP_OPCODE & 0xff;

    public static final short FLEX_REQ_DCP_MUTATION = MAGIC_REQ_FLEX << 8 | DCP_MUTATION_OPCODE & 0xff;
    public static final short FLEX_REQ_DCP_DELETION = MAGIC_REQ_FLEX << 8 | DCP_DELETION_OPCODE & 0xff;
    public static final short FLEX_REQ_DCP_EXPIRATION = MAGIC_REQ_FLEX << 8 | DCP_EXPIRATION_OPCODE & 0xff;
    public static final short FLEX_REQ_STREAM_END = MAGIC_REQ_FLEX << 8 | DCP_STREAM_END_OPCODE & 0xff;
    public static final short FLEX_REQ_SEQNO_ADVANCED = MAGIC_REQ_FLEX << 8 | DCP_SEQNO_ADVANCED_OPCODE & 0xff;
    public static final short FLEX_REQ_OSO_SNAPSHOT_MARKER =
            MAGIC_REQ_FLEX << 8 | DCP_OSO_SNAPSHOT_MARKER_OPCODE & 0xff;
    public static final short FLEX_REQ_SYSTEM_EVENT = MAGIC_REQ_FLEX << 8 | DCP_SYSTEM_EVENT_OPCODE & 0xff;
    public static final short FLEX_REQ_SET_VBUCKET_STATE = MAGIC_REQ_FLEX << 8 | DCP_SET_VBUCKET_STATE_OPCODE & 0xff;
    public static final short FLEX_REQ_SNAPSHOT_MARKER = MAGIC_REQ_FLEX << 8 | DCP_SNAPSHOT_MARKER_OPCODE & 0xff;

    public static final short RES_DCP_COLLECTIONS_MANIFEST = MAGIC_RES << 8 | DCP_COLLECTIONS_MANIFEST_OPCODE & 0xff;
    public static final short RES_STREAM_REQUEST = MAGIC_RES << 8 | DCP_STREAM_REQUEST_OPCODE & 0xff;
    public static final short RES_GET_SEQNOS = MAGIC_RES << 8 | GET_ALL_VB_SEQNOS_OPCODE & 0xff;
    public static final short RES_STREAM_CLOSE = MAGIC_RES << 8 | DCP_STREAM_CLOSE_OPCODE & 0xff;
    public static final short RES_FAILOVER_LOG = MAGIC_RES << 8 | DCP_FAILOVER_LOG_OPCODE & 0xff;
    public static final short RES_STAT = MAGIC_RES << 8 | STAT_OPCODE & 0xff;

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final byte FRAMING_EXTRA_ID_DCP_STREAM_ID = 2;

    /**
     * the stream id to use when the buffer does not contain a flex frame (i.e. pre-7.0 server)
     */
    public static final int NO_FLEX_FRAMING_STREAM_ID = 1;

    public static final int GET_SEQNOS_GLOBAL_COLLECTION_ID = 1;

    private MessageUtil() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Returns true if message can be processed and false if more data is needed.
     */
    public static boolean isComplete(final ByteBuf buffer) {
        int readable = buffer.readableBytes();
        if (readable < HEADER_SIZE) {
            return false;
        }
        return readable >= (HEADER_SIZE + buffer.getInt(BODY_LENGTH_OFFSET));
    }

    public static byte getOpcode(ByteBuf buffer) {
        return buffer.getByte(1);
    }

    public static String humanizeOpcode(final ByteBuf buffer) {
        return humanizeOpcode(getOpcode(buffer));
    }

    public static String humanizeOpcode(final byte opcode) {
        return String.format("%s (0x%02x)", opcodeName(opcode), opcode);
    }

    public static String opcodeName(final byte opcode) {
        switch (opcode) {
            case VERSION_OPCODE:
                return "VERSION";
            case HELO_OPCODE:
                return "HELO";
            case STAT_OPCODE:
                return "STAT";
            case SASL_LIST_MECHS_OPCODE:
                return "SASL_LIST_MECHS";
            case SASL_AUTH_OPCODE:
                return "SASL_AUTH";
            case SASL_STEP_OPCODE:
                return "SASL_STEP";
            case GET_ALL_VB_SEQNOS_OPCODE:
                return "GET_ALL_VB_SEQNOS";
            case OPEN_CONNECTION_OPCODE:
                return "OPEN_CONNECTION";
            case DCP_ADD_STREAM_OPCODE:
                return "DCP_ADD_STREAM";
            case DCP_STREAM_CLOSE_OPCODE:
                return "DCP_STREAM_CLOSE";
            case DCP_STREAM_REQUEST_OPCODE:
                return "DCP_STREAM_REQUEST";
            case DCP_FAILOVER_LOG_OPCODE:
                return "DCP_FAILOVER_LOG";
            case DCP_STREAM_END_OPCODE:
                return "DCP_STREAM_END";
            case DCP_SNAPSHOT_MARKER_OPCODE:
                return "DCP_SNAPSHOT_MARKER";
            case DCP_MUTATION_OPCODE:
                return "DCP_MUTATION";
            case DCP_DELETION_OPCODE:
                return "DCP_DELETION";
            case DCP_EXPIRATION_OPCODE:
                return "DCP_EXPIRATION";
            case DCP_FLUSH_OPCODE:
                return "DCP_FLUSH";
            case DCP_SET_VBUCKET_STATE_OPCODE:
                return "DCP_SET_VBUCKET_STATE";
            case DCP_NOOP_OPCODE:
                return "DCP_NOOP";
            case DCP_BUFFER_ACK_OPCODE:
                return "DCP_BUFFER_ACK";
            case DCP_CONTROL_OPCODE:
                return "DCP_CONTROL";
            case DCP_SYSTEM_EVENT_OPCODE:
                return "DCP_SYSTEM_EVENT";
            case DCP_SEQNO_ADVANCED_OPCODE:
                return "DCP_SEQNO_ADVANCED";
            case DCP_OSO_SNAPSHOT_MARKER_OPCODE:
                return "DCP_OSO_SNAPSHOT_MARKER";
            case SELECT_BUCKET_OPCODE:
                return "SELECT_BUCKET";
            case OBSERVE_SEQNO_OPCODE:
                return "OBSERVE_SEQNO";
            case GET_CLUSTER_CONFIG_OPCODE:
                return "GET_CLUSTER_CONFIG";
            case DCP_COLLECTIONS_MANIFEST_OPCODE:
                return "DCP_COLLECTIONS_MANIFEST";
            default:
                return "<<UNKNOWN OPCODE>>";
        }
    }

    /**
     * Dumps the given ByteBuf in the "wire format".
     *
     * Note that the response is undefined if a buffer with a different
     * content than the KV protocol is passed in.
     *
     * @return the String ready to be printed/logged.
     */
    public static String humanize(final ByteBuf buffer) {
        StringBuilder sb = new StringBuilder();

        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        short keyLength;
        short framingExtrasLength = 0;
        int bodyLength = buffer.getInt(BODY_LENGTH_OFFSET);

        sb.append("Field          (offset) (value)\n-----------------------------------\n");
        sb.append(String.format("Magic          (0)      0x%02x%n", buffer.getByte(0)));
        sb.append(String.format("Opcode         (1)      0x%02x\t%s%n", getOpcode(buffer),
                humanizeOpcode(getOpcode(buffer))));
        if ((buffer.getByte(0) & 0xf0) != 0) {
            keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
            sb.append(String.format("Key Length     (2,3)    0x%04x%n", keyLength));
        } else {
            framingExtrasLength = getFramingExtrasSize(buffer);
            keyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
            sb.append(String.format("Framing Extras (2)      0x%02x%n", framingExtrasLength));
            sb.append(String.format("Key Length     (3)      0x%02x%n", keyLength));
        }
        sb.append(String.format("Extras Length  (4)      0x%02x%n", extrasLength));
        sb.append(String.format("Data Type      (5)      0x%02x%n", buffer.getByte(5)));
        sb.append(String.format("VBucket        (6,7)    0x%04x%n", buffer.getShort(VBUCKET_OFFSET)));
        sb.append(String.format("Total Body     (8-11)   0x%08x%n", bodyLength));
        sb.append(String.format("Opaque         (12-15)  0x%08x%n", buffer.getInt(OPAQUE_OFFSET)));
        sb.append(String.format("CAS            (16-23)  0x%016x%n", buffer.getLong(CAS_OFFSET)));

        if (framingExtrasLength > 0) {
            // TODO: attempt to parse & humanize known framing extras (i.e. STREAM_ID) instead of hex dump
            byte[] chunk = new byte[8];
            for (int framingRemaining = framingExtrasLength; framingRemaining > 0; framingRemaining -= 8) {
                int framingExtrasOffset = framingRemaining - framingExtrasLength;
                int chunkLen = Math.min(8, framingRemaining);
                int startingByte = HEADER_SIZE + framingExtrasOffset;
                int endingByte = HEADER_SIZE + framingExtrasOffset + chunkLen;
                buffer.getBytes(startingByte, chunk, 0, chunkLen);
                sb.append(String.format("Framing Extras (%d-%d)  0x", startingByte, endingByte));
                if (chunkLen == 8) {
                    sb.append(String.format("%016x", buffer.getLong(startingByte)));
                } else {
                    for (int i = 0; i < chunkLen; i++) {
                        sb.append(String.format("%02x", buffer.getByte(startingByte + i)));
                    }
                }
                sb.append('\n');
            }
        }

        if (extrasLength > 0) {
            sb.append("+ Extras with ").append(extrasLength).append(" bytes\n");
        }

        if (keyLength > 0) {
            sb.append("+ Key with ").append(keyLength).append(" bytes\n");
        }

        int contentLength = bodyLength - extrasLength - keyLength;
        if (contentLength > 0) {
            sb.append("+ Content with ").append(contentLength).append(" bytes\n");
        }

        return sb.toString();
    }

    /**
     * Helper method to initialize a request with an opcode.
     */
    public static void initRequest(byte opcode, ByteBuf buffer) {
        buffer.writeByte(MessageUtil.MAGIC_REQ);
        buffer.writeByte(opcode);
        buffer.writeZero(HEADER_SIZE - 2);
    }

    /**
     * Helper method to initialize a response with an opcode.
     */
    public static void initResponse(byte opcode, ByteBuf buffer) {
        buffer.writeByte(MessageUtil.MAGIC_RES);
        buffer.writeByte(opcode);
        buffer.writeZero(HEADER_SIZE - 2);
    }

    public static void setExtras(ByteBuf extras, ByteBuf buffer) {
        byte oldExtrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        byte newExtrasLength = (byte) extras.readableBytes();
        int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
        int newBodyLength = oldBodyLength - oldExtrasLength + newExtrasLength;

        buffer.setByte(EXTRAS_LENGTH_OFFSET, newExtrasLength);
        buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

        final int headerSize = getHeaderSize(buffer);
        buffer.setBytes(headerSize, extras);
        buffer.writerIndex(headerSize + newBodyLength);
    }

    /**
     * Returns the complete header size, including any present flex framing in the message
     * @param buffer the message (may use flex framing)
     * @return the header size (inclusive of flex framing extras, if present)
     */
    public static int getHeaderSize(ByteBuf buffer) {
        return HEADER_SIZE + getFramingExtrasSize(buffer);
    }

    public static short getFramingExtrasSize(ByteBuf buffer) {
        return buffer.getUnsignedByte(FLEX_FRAMING_EXTRAS_LENGTH_OFFSET);
    }

    public static ByteBuf getExtras(ByteBuf buffer) {
        return buffer.slice(getHeaderSize(buffer), buffer.getByte(EXTRAS_LENGTH_OFFSET));
    }

    public static void setVbucket(short vbucket, ByteBuf buffer) {
        buffer.setShort(VBUCKET_OFFSET, vbucket);
    }

    public static short getVbucket(ByteBuf buffer) {
        return buffer.getShort(VBUCKET_OFFSET);
    }

    /**
     * Helper method to set the key, update the key length and the content length.
     */
    public static void setKey(ByteBuf key, ByteBuf buffer) {
        short framingExtrasLength = getFramingExtrasSize(buffer);
        final int extrasOffset = HEADER_SIZE + framingExtrasLength;
        short oldKeyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
        short newKeyLength = (short) key.readableBytes();
        int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        int newBodyLength = oldBodyLength - oldKeyLength + newKeyLength;
        if (oldKeyLength != newKeyLength && oldBodyLength - extrasLength - framingExtrasLength != 0) {
            throw new IllegalStateException("NYI: cannot change key length with body present!");
        }
        buffer.setByte(FLEX_KEY_LENGTH_OFFSET, newKeyLength);
        buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

        buffer.setBytes(extrasOffset + extrasLength, key);
        buffer.writerIndex(extrasOffset + newBodyLength);
    }

    public static ByteBuf getKey(ByteBuf buffer, boolean isCollectionEnabled) {
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        short framingExtrasLength = getFramingExtrasSize(buffer);
        short keyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
        ByteBuf keyWithPrefix = buffer.slice(HEADER_SIZE + framingExtrasLength + extrasLength, keyLength);
        if (!isCollectionEnabled) {
            return keyWithPrefix; //if collection is not enabled, then key does not contain cid prefix
        }
        int cidPrefixLength = lengthLEB128(keyWithPrefix);
        return keyWithPrefix.slice(cidPrefixLength, keyLength - cidPrefixLength);
    }

    public static String getKeyAsString(ByteBuf buffer, boolean isCollectionEnabled) {
        return getKey(buffer, isCollectionEnabled).toString(UTF_8);
    }

    public static int getCid(ByteBuf buffer) {
        return readLEB128(getKey(buffer, false));
    }

    /**
     * Sets the content payload of the buffer, updating the content length as well.
     */
    public static void setContent(ByteBuf content, ByteBuf buffer) {
        short framingExtrasLength = getFramingExtrasSize(buffer);
        short keyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        // The size of the value is total body length - key length - extras length - framing extras
        int bodyLength = framingExtrasLength + extrasLength + keyLength + content.readableBytes();

        buffer.setInt(BODY_LENGTH_OFFSET, bodyLength);
        if (buffer.ensureWritable(content.readableBytes(), false) == 1) {
            buffer.capacity(buffer.capacity() + content.readableBytes() - buffer.writableBytes());
        }
        buffer.setBytes(HEADER_SIZE + framingExtrasLength + extrasLength + keyLength, content);
        buffer.writerIndex(HEADER_SIZE + framingExtrasLength + bodyLength);

        // todo: what if old body with different size is there?
    }

    public static ByteBuf getContent(ByteBuf buffer) {
        short framingExtrasLength = getFramingExtrasSize(buffer);
        short keyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        // The size of the value is total body length - key length - extras length - framing extras
        int contentLength = buffer.getInt(BODY_LENGTH_OFFSET) - keyLength - extrasLength - framingExtrasLength;
        return buffer.slice(HEADER_SIZE + framingExtrasLength + extrasLength + keyLength, contentLength);
    }

    public static String getContentAsString(ByteBuf buffer) {
        return getContent(buffer).toString(UTF_8);
    }

    public static short getStatus(ByteBuf buffer) {
        return buffer.getShort(VBUCKET_OFFSET);
    }

    public static void setOpaque(int opaque, ByteBuf buffer) {
        buffer.setInt(OPAQUE_OFFSET, opaque);
    }

    public static void setOpaqueHi(short flags, ByteBuf buffer) {
        buffer.setShort(OPAQUE_OFFSET, flags);
    }

    public static void setOpaqueLo(short flags, ByteBuf buffer) {
        buffer.setShort(OPAQUE_OFFSET + 2, flags);
    }

    public static int getOpaque(ByteBuf buffer) {
        return buffer.getInt(OPAQUE_OFFSET);
    }

    public static short getOpaqueHi(ByteBuf buffer) {
        return buffer.getShort(OPAQUE_OFFSET);
    }

    public static short getOpaqueLo(ByteBuf buffer) {
        return buffer.getShort(OPAQUE_OFFSET + 2);
    }

    public static void setCas(long cas, ByteBuf buffer) {
        buffer.setLong(CAS_OFFSET, cas);
    }

    public static long getCas(ByteBuf buffer) {
        return buffer.getLong(CAS_OFFSET);
    }

    /**
     * Get the int value encoded using leb128
     */
    public static int readLEB128(ByteBuf bytebuf) {
        int result = 0;
        int shift = 0;
        int hob;
        do {
            byte b = bytebuf.readByte();
            result |= (b & 0x7F) << shift;
            if (shift == 28 && (b & 0xf0) != 0) {
                // we have already seen 28 bits; if anything more than low 4 bits is present we have an overflow
                throw new ArithmeticException("LEB128 value is larger than 32-bits (low 32-bits: 0x"
                        + Integer.toUnsignedString(result, 16) + ")");
            }
            shift += 7;
            hob = b & 0x80; //get highest order bit value
        } while (hob != 0);
        return result;
    }

    /**
     * Get length (in bytes) of the leb128 encoded value
     */
    public static int lengthLEB128(ByteBuf bytebuf) {
        int numGroups = 0;
        int length = 0;
        int hob;
        do {
            byte b = bytebuf.readByte();
            if (numGroups++ == 4 && (b & 0xf0) != 0) {
                // we have already seen 28 bits; if anything more than low 4 bits is present we have an overflow
                throw new ArithmeticException("LEB128 value is larger than 32-bits");
            }
            hob = b & 0x80;
            length++;
        } while (hob != 0);
        return length;
    }

    /**
     * Returns the message content in its original form (possibly compressed).
     * <p>
     * The returned buffer shares its reference count with the given buffer.
     */
    public static ByteBuf getRawContent(ByteBuf buffer) {
        short framingExtrasLength = getFramingExtrasSize(buffer);
        short keyLength = buffer.getUnsignedByte(FLEX_KEY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        int contentLength = buffer.getInt(BODY_LENGTH_OFFSET) - keyLength - extrasLength;
        return buffer.slice(HEADER_SIZE + framingExtrasLength + keyLength + extrasLength, contentLength);
    }

    /**
     * Returns a new array containing the uncompressed content of the given message.
     */
    public static byte[] getContentAsByteArray(ByteBuf buffer) {
        final ByteBuf rawContent = getRawContent(buffer);

        // When OpenConnectionFlags.NO_VALUE is used, the content is always empty.
        // Documents can still be flagged as snappy compressed, so do this check before
        // attempting to decompress the empty content, otherwise snappy decompression fails.
        if (rawContent.readableBytes() == 0) {
            // ByteBufUtil.getBytes(rawContent) would return a new array.
            // Generate less garbage by reusing the same empty array.
            return EMPTY_BYTE_ARRAY;
        }

        return ByteBufUtil.getBytes(rawContent);
    }

    public static int streamId(ByteBuf buffer) {
        return streamId(buffer, NO_FLEX_FRAMING_STREAM_ID);
    }

    // http://src.couchbase.org/source/xref/trunk/kv_engine/docs/BinaryProtocol.md
    //
    // #### Request header with "flexible framing extras"
    //
    //    Some commands may accept extra attributes which may be set in the
    //    flexible framing extras section in the request packet. Such packets
    //    is identified by using a different magic (0x08 intead of 0x80).
    //    If enabled the header looks like:
    //
    //    Byte/     0       |       1       |       2       |       3       |
    //            /              |               |               |               |
    //            |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
    //            +---------------+---------------+---------------+---------------+
    //            0| Magic (0x08)  | Opcode        | Framing extras| Key Length    |
    //            +---------------+---------------+---------------+---------------+
    //            4| Extras length | Data type     | vbucket id                    |
    //            +---------------+---------------+---------------+---------------+
    //            8| Total body length                                             |
    //            +---------------+---------------+---------------+---------------+
    //            12| Opaque                                                        |
    //            +---------------+---------------+---------------+---------------+
    //            16| CAS                                                           |
    //            |                                                               |
    //            +---------------+---------------+---------------+---------------+
    //    Total 24 bytes
    //
    //    Following the header you'd now find the section containing the framing
    //    extras (the size is specified in byte 2). Following the framing extras you'll
    //    find the extras, then the key and finally the value. The size of the value
    //    is total body length - key length - extras length - framing extras.
    //
    //    The framing extras is encoded as a series of variable-length `FrameInfo` objects.
    //
    //        Each `FrameInfo` consists of:
    //
    //            * 4 bits: *Object Identifier*. Encodes first 15 object IDs directly; with the 16th value (15) used
    //                       as an escape to support an additional 256 IDs by combining the value of the next byte:
    //                * `0..14`: Identifier for this element.
    //                * `15`: Escape: ID is 15 + value of next byte.
    //            * 4 bits: *Object Length*. Encodes sizes 0..14 directly; value 15 is
    //                       used to encode sizes above 14 by combining the value of a following
    //                       byte:
    //                * `0..14`: Size in bytes of the element data.
    //            * `15`: Escape: Size is 15 + value of next byte (after any object ID
    //                    escape bytes).
    //            * N Bytes: *Object data*.
    //
    public static int streamId(ByteBuf buffer, int defaultId) {
        short framingExtrasLen = getFramingExtrasSize(buffer);
        if (framingExtrasLen == 0) {
            return defaultId;
        }
        ByteBuf framingExtra = buffer.slice(HEADER_SIZE, framingExtrasLen);
        while (framingExtra.readableBytes() > 0) {
            byte b = framingExtra.readByte();
            switch (b >> 4) {
                case FRAMING_EXTRA_ID_DCP_STREAM_ID:
                    // TODO: remove sanity check
                    if ((b & 0xf) != 2) {
                        throw new IllegalStateException("malformed dcp stream id in framing extras");
                    }
                    return framingExtra.readUnsignedShort();
                default:
                    // ignore unknown framing extra
                    if ((b & 0xf) < 15) {
                        framingExtra.skipBytes(b & 0xf);
                    } else {
                        framingExtra.skipBytes(15 + framingExtra.readByte());
                    }
            }
        }
        throw new IllegalStateException("did not find stream id in framing extras!");
    }

    public static StreamState streamState(ByteBuf buf, DcpChannel channel) {
        return streamState(buf, channel.getSessionState());
    }

    public static StreamState streamState(ByteBuf buf, SessionState sessionState) {
        int streamId = streamId(buf);
        return sessionState.streamState(streamId);
    }
}
