//import protocols.replication.OpCounterCRDT;
//import crdts.operations.CounterOperation;
//import crdts.operations.Operation;
//
//public class TestCounter {
//
//    public static final String C1 = "c1";
//    public static final String C2 = "c2";
//
//    public static void main(String[] args) throws Exception {
//
//        //testIncrement();
//        //testIncrementByFive();
//        //testDecrement();
//        //testDecrementByFive();
//        testAll();
//
//    }
//
//    private static void testIncrement() {
//        System.out.println("TEST INCREMENT");
//
//        OpCounterCRDT c1 = new OpCounterCRDT(C1);
//        OpCounterCRDT c2 = new OpCounterCRDT(C2);
//
//        System.out.println("Value of c1: " + c1.value());
//        Operation op = (CounterOperation) c1.increment();
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//    }
//
//    private static void testIncrementByFive() {
//        System.out.println("TEST INCREMENT BY 5");
//
//        OpCounterCRDT c1 = new OpCounterCRDT(C1);
//        OpCounterCRDT c2 = new OpCounterCRDT(C2);
//
//        System.out.println("Value of c1: " + c1.value());
//        Operation op = (CounterOperation) c1.incrementBy(5);
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//    }
//
//    private static void testDecrement() {
//        System.out.println("TEST DECREMENT");
//
//        OpCounterCRDT c1 = new OpCounterCRDT(C1);
//        OpCounterCRDT c2 = new OpCounterCRDT(C2);
//
//        System.out.println("Value of c1: " + c1.value());
//        Operation op = (CounterOperation) c1.decrement();
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//    }
//
//    private static void testDecrementByFive() {
//        System.out.println("TEST DECREMENT BY 5");
//
//        OpCounterCRDT c1 = new OpCounterCRDT(C1);
//        OpCounterCRDT c2 = new OpCounterCRDT(C2);
//
//        System.out.println("Value of c1: " + c1.value());
//        Operation op = (CounterOperation) c1.decrementBy(5);
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//    }
//
//    private static void testAll() {
//        System.out.println("TEST ALL");
//
//        OpCounterCRDT c1 = new OpCounterCRDT(C1);
//        OpCounterCRDT c2 = new OpCounterCRDT(C2);
//
//        System.out.println("Value of c1: " + c1.value());
//        Operation op = (CounterOperation) c1.decrementBy(5);
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//
//        System.out.println("Value of c1: " + c1.value());
//        op = (CounterOperation) c1.increment();
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        c2.upstream(op);
//        System.out.println("Value of c2: " + c2.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        op = (CounterOperation) c2.decrement();
//        System.out.println("Value of c2: " + c2.value());
//
//        System.out.println("Value of c1: " + c1.value());
//        c1.upstream(op);
//        System.out.println("Value of c1: " + c1.value());
//
//        System.out.println("Value of c2: " + c2.value());
//        op = (CounterOperation) c2.incrementBy(7);
//        System.out.println("Value of c2: " + c2.value());
//
//        System.out.println("Value of c1: " + c1.value());
//        c1.upstream(op);
//        System.out.println("Value of c1: " + c1.value());
//    }
//}
