package serializers;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface CRDTOpSerializer<T> {

    void serialize(T t, MySerializer[] serializers, ByteBuf out) throws IOException;

    T deserialize(MySerializer[] serializers, ByteBuf in) throws IOException;
}
