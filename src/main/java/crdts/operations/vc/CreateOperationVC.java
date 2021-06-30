package crdts.operations.vc;

import crdts.operations.CreateOperation;
import crdts.operations.Operation;
import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;


public class CreateOperationVC extends OperationVC {

    private final String[] dataTypes;

    public CreateOperationVC(Host sender, int senderClock, String opType, String crdtId, String crdtType, String[] dataTypes, VectorClock vc) {
        super(sender, senderClock, opType, crdtId, crdtType, vc);
        this.dataTypes = dataTypes;
    }

    public String[] getDataTypes() {
        return this.dataTypes;
    }

    @Override
    public String toString() {
        return "CreateOperationVC{" +
                "sender=" + sender +
                ", senderClock=" + senderClock +
                ", opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                '}';
    }

    public static MyOpSerializer<CreateOperationVC> serializer = new MyOpSerializer<CreateOperationVC>() {
        @Override
        public void serialize(CreateOperationVC createOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            OperationVC.serialize(createOperation, out);
            int size = createOperation.dataTypes.length;
            out.writeInt(size);
            for(int i = 0; i < size; i++) {
                out.writeInt(createOperation.dataTypes[i].getBytes().length);
                out.writeBytes(createOperation.dataTypes[i].getBytes());
            }
        }

        @Override
        public CreateOperationVC deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            size = in.readInt();
            String[] dataType = new String[size];
            for(int i = 0; i < size; i++) {
                byte[] dataTypeBytes = new byte[in.readInt()];
                in.readBytes(dataTypeBytes);
                dataType[i] = new String(dataTypeBytes);
            }
            return new CreateOperationVC(sender, senderClock, new String(opType), new String(crdtId), new String(crdtType), dataType, vc);
        }
    };

}
