package serializers;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface MySerializer<T> {

    void serialize(T t, ByteBuf out) throws IOException;

    T deserialize(ByteBuf in) throws IOException;
}
