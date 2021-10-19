import datatypes.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.broadcast.plumtree.PlumTree;
import protocols.replication.*;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MySerializer;

import java.io.IOException;
import java.net.InetAddress;

public class TestCRDTSerializers {

    public static void main(String[] args) throws Exception {

//        testCounterCRDTSerializer();
//        testRegisterCRDTSerializer();
//        testSetCRDTSerializer();
//        testMapCRDTSerializer();

    }

    // To work we need to comment kernel.downstream in op counter crdt class
    private static void testCounterCRDTSerializer() throws HandlerRegistrationException, IOException {
        CRDTCommunicationInterface kernel = new ReplicationKernel(PlumTree.PROTOCOL_ID);
        OpCounterCRDT counter = new OpCounterCRDT(kernel, "CRDT1");
        counter.incrementBy(new Host(InetAddress.getByName("127.0.0.1"), 6000),6);
        System.out.println(counter.getCrdtId());
        System.out.println(counter.value());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = IntegerType.serializer;
        OpCounterCRDT.serializer.serialize(counter, serializers, buf);
        OpCounterCRDT newCrdt = (OpCounterCRDT) OpCounterCRDT.serializer.deserialize(kernel, serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.value());
    }

    // To work we need to comment kernel.downstream in lww register crdt class
    private static void testRegisterCRDTSerializer() throws HandlerRegistrationException, IOException {
        CRDTCommunicationInterface kernel = new ReplicationKernel(PlumTree.PROTOCOL_ID);
        LWWRegisterCRDT register = new LWWRegisterCRDT(kernel, "CRDT2");
        register.assign(new Host(InetAddress.getByName("127.0.0.1"), 6000),new StringType("Eu sou uma string!"));
        System.out.println(register.getCrdtId());
        System.out.println(register.value());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = StringType.serializer;
        LWWRegisterCRDT.serializer.serialize(register, serializers, buf);
        LWWRegisterCRDT newCrdt = (LWWRegisterCRDT) LWWRegisterCRDT.serializer.deserialize(kernel, serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.value());
    }

    // To work we need to comment kernel.downstream in add and remove in or set crdt class
    private static void testSetCRDTSerializer() throws HandlerRegistrationException, IOException {
        CRDTCommunicationInterface kernel = new ReplicationKernel(PlumTree.PROTOCOL_ID);
        ORSetCRDT set = new ORSetCRDT(kernel, "CRDT3");
        set.add(new Host(InetAddress.getByName("127.0.0.1"), 6000), new BooleanType(true));
        set.add(new Host(InetAddress.getByName("127.0.0.1"), 6000), new BooleanType(true));
        set.add(new Host(InetAddress.getByName("127.0.0.1"), 6000), new BooleanType(false));
        set.remove(new Host(InetAddress.getByName("127.0.0.1"), 6000), new BooleanType(false));
        set.add(new Host(InetAddress.getByName("127.0.0.1"), 6000), new BooleanType(false));
        System.out.println(set.getCrdtId());
        System.out.println(set.elements());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = BooleanType.serializer;
        ORSetCRDT.serializer.serialize(set, serializers, buf);
        ORSetCRDT newCrdt = (ORSetCRDT) ORSetCRDT.serializer.deserialize(kernel, serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.elements());
    }

    // To work we need to comment kernel.downstream in put and delete in or map crdt class
    private static void testMapCRDTSerializer() throws HandlerRegistrationException, IOException {
        CRDTCommunicationInterface kernel = new ReplicationKernel(PlumTree.PROTOCOL_ID);
        ORMapCRDT map = new ORMapCRDT(kernel, "CRDT4");
        map.put(new Host(InetAddress.getByName("127.0.0.1"), 6000), new ByteType((byte)1), new ShortType((short)3));
        map.put(new Host(InetAddress.getByName("127.0.0.1"), 6000), new ByteType((byte)1), new ShortType((short)7));
        map.delete(new Host(InetAddress.getByName("127.0.0.1"), 6000), new ByteType((byte)1));
        map.put(new Host(InetAddress.getByName("127.0.0.1"), 6000), new ByteType((byte)0), new ShortType((short)47));
        System.out.println("ID: " + map.getCrdtId());
        System.out.println("Keys: " + map.keys());
        System.out.println("Values: " + map.values());
        System.out.println("Key 1: " + map.get(new ByteType((byte)1)));
        System.out.println("Key 0: " + map.get(new ByteType((byte)0)));
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = ByteType.serializer;
        serializers[1] = ShortType.serializer;
        ORMapCRDT.serializer.serialize(map, serializers, buf);
        ORMapCRDT newCrdt = (ORMapCRDT) ORMapCRDT.serializer.deserialize(kernel, serializers, buf);
        System.out.println("ID: " + newCrdt.getCrdtId());
        System.out.println("Keys: " + newCrdt.keys());
        System.out.println("Values: " + newCrdt.values());
        System.out.println("Key 1: " + newCrdt.get( new ByteType((byte)1)));
        System.out.println("Key 0: " + newCrdt.get( new ByteType((byte)0)));
    }

}
