import protocols.replication.crdts.datatypes.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.replication.*;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import protocols.replication.crdts.serializers.MySerializer;

import java.io.IOException;

public class TestCRDTSerializers {

    public static void main(String[] args) throws Exception {

//        testCounterCRDTSerializer();
//        testRegisterCRDTSerializer();
//        testSetCRDTSerializer();
//        testMapCRDTSerializer();

    }

    // To work we need to comment kernel.downstream in op counter crdt class
    private static void testCounterCRDTSerializer() throws HandlerRegistrationException, IOException {
        OpCounterCRDT counter = new OpCounterCRDT("CRDT1");
        counter.incrementByOperation(6);
        System.out.println(counter.getCrdtId());
        System.out.println(counter.value());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = IntegerType.serializer;
        OpCounterCRDT.serializer.serialize(counter, serializers, buf);
        OpCounterCRDT newCrdt = (OpCounterCRDT) OpCounterCRDT.serializer.deserialize(serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.value());
    }

    // To work we need to comment kernel.downstream in lww register crdt class
    private static void testRegisterCRDTSerializer() throws HandlerRegistrationException, IOException {
        LWWRegisterCRDT register = new LWWRegisterCRDT("CRDT2");
        register.assignOperation(new StringType("Eu sou uma string!"));
        System.out.println(register.getCrdtId());
        System.out.println(register.value());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = StringType.serializer;
        LWWRegisterCRDT.serializer.serialize(register, serializers, buf);
        LWWRegisterCRDT newCrdt = (LWWRegisterCRDT) LWWRegisterCRDT.serializer.deserialize(serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.value());
    }

    // To work we need to comment kernel.downstream in add and remove in or set crdt class
    private static void testSetCRDTSerializer() throws HandlerRegistrationException, IOException {
        ORSetCRDT set = new ORSetCRDT("CRDT3");
        set.addOperation(new BooleanType(true));
        set.addOperation(new BooleanType(true));
        set.addOperation(new BooleanType(false));
        set.removeOperation(new BooleanType(false));
        set.addOperation(new BooleanType(false));
        System.out.println(set.getCrdtId());
        System.out.println(set.elements());
        ByteBuf buf = Unpooled.buffer();
        MySerializer[] serializers = new MySerializer[2];
        serializers[0] = BooleanType.serializer;
        ORSetCRDT.serializer.serialize(set, serializers, buf);
        ORSetCRDT newCrdt = (ORSetCRDT) ORSetCRDT.serializer.deserialize(serializers, buf);
        System.out.println(newCrdt.getCrdtId());
        System.out.println(newCrdt.elements());
    }

    // To work we need to comment kernel.downstream in put and delete in or map crdt class
    private static void testMapCRDTSerializer() throws HandlerRegistrationException, IOException {
        ORMapCRDT map = new ORMapCRDT("CRDT4");
        map.putOperation(new ByteType((byte)1), new ShortType((short)3));
        map.putOperation(new ByteType((byte)1), new ShortType((short)7));
        map.deleteOperation(new ByteType((byte)1));
        map.putOperation(new ByteType((byte)0), new ShortType((short)47));
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
        ORMapCRDT newCrdt = (ORMapCRDT) ORMapCRDT.serializer.deserialize(serializers, buf);
        System.out.println("ID: " + newCrdt.getCrdtId());
        System.out.println("Keys: " + newCrdt.keys());
        System.out.println("Values: " + newCrdt.values());
        System.out.println("Key 1: " + newCrdt.get( new ByteType((byte)1)));
        System.out.println("Key 0: " + newCrdt.get( new ByteType((byte)0)));
    }

}
