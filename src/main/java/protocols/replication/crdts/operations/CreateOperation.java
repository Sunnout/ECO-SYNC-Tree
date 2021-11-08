package protocols.replication.crdts.operations;

import io.netty.buffer.ByteBuf;
import protocols.replication.crdts.serializers.CRDTOpSerializer;
import protocols.replication.crdts.serializers.MySerializer;

import java.io.IOException;


public class CreateOperation extends Operation {

    private final String[] dataTypes;

    public CreateOperation(String opType, String crdtId, String crdtType, String[] dataTypes) {
        super(opType, crdtId, crdtType);
        this.dataTypes = dataTypes;
    }

    public String[] getDataTypes() {
        return this.dataTypes;
    }

    @Override
    public String toString() {
        return "CreateOperation{" +
                "opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                '}';
    }

    public static CRDTOpSerializer<CreateOperation> serializer = new CRDTOpSerializer<CreateOperation>() {
        @Override
        public void serialize(CreateOperation createOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            Operation.serialize(createOperation, out);
            int size = createOperation.dataTypes.length;
            out.writeInt(size);
            for(int i = 0; i < size; i++) {
                out.writeInt(createOperation.dataTypes[i].getBytes().length);
                out.writeBytes(createOperation.dataTypes[i].getBytes());
            }
        }

        @Override
        public CreateOperation deserialize(MySerializer[] serializers, ByteBuf in) {
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            size = in.readInt();
            String[] dataType = new String[size];
            for(int i = 0; i < size; i++) {
                byte[] dataTypeBytes = new byte[in.readInt()];
                in.readBytes(dataTypeBytes);
                dataType[i] = new String(dataTypeBytes);
            }
            return new CreateOperation(new String(opType), new String(crdtId), new String(crdtType), dataType);
        }
    };

}
