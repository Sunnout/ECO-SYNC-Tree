package serializers;

import io.netty.buffer.ByteBuf;
import protocols.replication.CRDTCommunicationInterface;

import java.io.IOException;

public interface MyCRDTSerializer<T> {

    void serialize(T t, MySerializer[] serializers, ByteBuf out) throws IOException;

    T deserialize(CRDTCommunicationInterface kernel, MySerializer[] serializers, ByteBuf in) throws IOException;
}
