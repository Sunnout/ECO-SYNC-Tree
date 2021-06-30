package crdts.operations.vc;


import crdts.operations.Operation;
import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public abstract class OperationVC extends Operation {

    protected VectorClock vc;

    public OperationVC(Host sender, int senderClock, String opType, String crdtId, String crdtType, VectorClock vc) {
        super(sender, senderClock, opType, crdtId, crdtType);
        this.vc = vc;
    }

    public VectorClock getVectorClock() {
        return this.vc;
    }

    public void setVectorClock(VectorClock vc) {
        this.vc = vc;
    }

    public static void serialize(OperationVC operation, ByteBuf out) throws IOException {
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
