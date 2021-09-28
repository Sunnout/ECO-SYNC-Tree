package crdts.operations;


import io.netty.buffer.ByteBuf;

import java.io.*;

public abstract class Operation {

    protected final String opType;
    protected final String crdtId;
    protected final String crdtType;

    public Operation(String opType, String crdtId, String crdtType) {
        this.opType = opType;
        this.crdtId = crdtId;
        this.crdtType = crdtType;
    }

    public String getOpType() {
        return this.opType;
    }

    public String getCrdtId() {
        return this.crdtId;
    }

    public String getCrdtType() {
        return this.crdtType;
    }

    public static void serialize(Operation operation, ByteBuf out) throws IOException {
        out.writeInt(operation.opType.getBytes().length);
        out.writeBytes(operation.opType.getBytes());
        out.writeInt(operation.crdtId.getBytes().length);
        out.writeBytes(operation.crdtId.getBytes());
        out.writeInt(operation.crdtType.getBytes().length);
        out.writeBytes(operation.crdtType.getBytes());
    }

    public static String opTypeFromByteArray(ByteBuf in) {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        in.resetReaderIndex();
        return new String(string);
    }

    public static String crdtIdFromByteArray(ByteBuf in) {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        in.resetReaderIndex();
        return new String(string);
    }

    public static String crdtTypeFromByteArray(ByteBuf in) {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        in.resetReaderIndex();
        return new String(string);
    }

}
