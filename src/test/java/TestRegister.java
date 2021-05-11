//import protocols.replication.LWWRegisterCRDT;
//import crdts.operations.Operation;
//import crdts.operations.RegisterOperation;
//import datatypes.StringType;
//
//public class TestRegister {
//
//    public static void main(String[] args) throws Exception {
//
//        testAll();
//
//    }
//
//    private static void testAll() {
//        System.out.println("TEST ALL");
//
//        LWWRegisterCRDT r1 = new LWWRegisterCRDT("A");
//        LWWRegisterCRDT r2 = new LWWRegisterCRDT("A");
//
//        System.out.println("Value of r1: " + r1.value());
//        Operation op = r1.assign(new StringType("Hello!"));
//        System.out.println("Value of r1: " + r1.value());
//
//        System.out.println("Value of r2: " + r2.value());
//        r2.upstream(op);
//        System.out.println("Value of r2: " + r2.value());
//
//        System.out.println("Value of r2: " + r2.value());
//        op = r2.assign(new StringType("Goodbye!"));
//        System.out.println(((RegisterOperation)op).getTimestamp());
//        System.out.println("Value of r2: " + r2.value());
//
//        System.out.println("Value of r1: " + r1.value());
//        Operation op1 = r1.assign(new StringType("Hi there!"));
//        System.out.println(((RegisterOperation)op1).getTimestamp());
//        System.out.println("Value of r1: " + r1.value());
//
//        System.out.println("Value of r1: " + r1.value());
//        r1.upstream(op);
//        System.out.println("Value of r1: " + r1.value());
//
//        System.out.println("Value of r2: " + r2.value());
//        r2.upstream(op1);
//        System.out.println("Value of r2: " + r2.value());
//    }
//
//}
