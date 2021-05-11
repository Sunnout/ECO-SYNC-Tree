package protocols.membership.cyclon.components;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class Connection {

    private final Host host;
    private int age;
    
    public Connection(Host host, int age){
        this.host = host;
        this.age = age;
    }

    public Host getHost() {
        return host;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
    
    public static ISerializer<Connection> serializer = new ISerializer<Connection>() {
        public void serialize(Connection con, ByteBuf out) throws IOException {
            Host.serializer.serialize(con.host, out);
            out.writeInt(con.age);
        }

        public Connection deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            int age = in.readInt();
            return new Connection(host, age);
        }
    };

}
