package crdts.operations;


import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;

public abstract class Operation {

    protected Host sender;
    protected int senderClock;
    protected VectorClock vc;
    protected final String opType;
    protected final String crdtId;
    protected final String crdtType;

    public Operation(Host sender, int senderClock, VectorClock vc, String opType, String crdtId, String crdtType) {
        this.sender = sender;
        this.senderClock = senderClock;
        this.vc = vc;
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

    public VectorClock getVectorClock() {
        return this.vc;
    }

    public void setVectorClock(VectorClock vc) {
        this.vc = vc;
    }

    public Host getSender() {
        return sender;
    }

    public void setSender(Host sender) {
        this.sender = sender;
    }

    public int getSenderClock() {
        return senderClock;
    }

    public void setSenderClock(int senderClock) {
        this.senderClock = senderClock;
    }

    public static void serialize(Operation operation, ByteBuf out) throws IOException {
        out.writeInt(operation.opType.getBytes().length);
        out.writeBytes(operation.opType.getBytes());
        out.writeInt(operation.crdtId.getBytes().length);
        out.writeBytes(operation.crdtId.getBytes());
        out.writeInt(operation.crdtType.getBytes().length);
        out.writeBytes(operation.crdtType.getBytes());
        VectorClock.serializer.serialize(operation.vc, out);
        Host.serializer.serialize(operation.sender, out);
        out.writeInt(operation.senderClock);
    }

    public static String opTypeFromByteArray(ByteBuf in) throws IOException {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        in.resetReaderIndex();
        return new String(string);
    }

    public static String crdtIdFromByteArray(ByteBuf in) throws IOException {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        in.resetReaderIndex();
        return new String(string);
    }

    public static String crdtTypeFromByteArray(ByteBuf in) throws IOException {
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

    public static VectorClock vectorClockFromByteArray(ByteBuf in) throws IOException {
        in.markReaderIndex();
        byte[] string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        string = new byte[in.readInt()];
        in.readBytes(string);
        VectorClock vc = VectorClock.serializer.deserialize(in);
        in.resetReaderIndex();
        return vc;
    }

}
