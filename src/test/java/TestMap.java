//import protocols.replication.ORMapCRDT;
//import crdts.operations.Operation;
//import crdts.operations.MapOperation;
//import crdts.utils.TaggedElement;
//import datatypes.IntegerType;
//import datatypes.StringType;
//
//import java.util.List;
//import java.util.Set;
//import java.util.UUID;
//
//public class TestMap {
//
//    public static void main(String[] args) throws Exception {
//
////        testPutAndDelete();
////        testKeysAndValues();
////        testContains();
////        testGetForConcurrentPutsAndOverwrite();
////        testConcurrentPutAndDelete();
//
//        TaggedElement t1 = new TaggedElement(new IntegerType(1), new UUID(12345678, 87654321));
//        TaggedElement t2 = new TaggedElement(new IntegerType(1), new UUID(12345678, 87654321));
//        System.out.println(t1.equals(t2));
//
//
//    }
//
//    private static void testPutAndDelete() {
//        System.out.println("TEST PUT; DELETE");
//
//        ORMapCRDT m1 = new ORMapCRDT("A");
//        ORMapCRDT m2 = new ORMapCRDT("A");
//
//        Operation op = m1.put(new IntegerType(1), new StringType("Ema"));
//        System.out.println("Map 1 - Value of key 1:");
//        printSet(m1.get(new IntegerType(1)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 1:");
//        printSet(m2.get(new IntegerType(1)));
//
//        op = m2.put(new IntegerType(1), new StringType("Maria"));
//        System.out.println("Map 2 - Value of key 1:");
//        printSet(m2.get(new IntegerType(1)));
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 1:");
//        printSet(m1.get(new IntegerType(1)));
//
//        op = (MapOperation) m2.delete(new IntegerType(1));
//        System.out.println("Map 2 - Value of key 1:");
//        printSet(m2.get(new IntegerType(1)));
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 1:");
//        printSet(m1.get(new IntegerType(1)));
//
//        op = (MapOperation) m2.delete(new IntegerType(2));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//
//        // Getting values in non existing key
//        System.out.println("Map 1 - Value of key 3:");
//        printSet(m1.get(new IntegerType(3)));
//    }
//
//    private static void testKeysAndValues() {
//        System.out.println("TEST KEYS; VALUES");
//
//        ORMapCRDT m1 = new ORMapCRDT("A");
//        ORMapCRDT m2 = new ORMapCRDT("A");
//
//        Operation op = m1.put(new IntegerType(15), new StringType("Ema"));
//        System.out.println("Map 1 - Value of key 15:");
//        printSet(m1.get(new IntegerType(15)));
//        System.out.println("Map 1 - Keys:");
//        printSet(m1.keys());
//        System.out.println("Map 1 - Values:");
//        printList(m1.values());
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 15:");
//        printSet(m2.get(new IntegerType(15)));
//        System.out.println("Map 2 - Keys:");
//        printSet(m2.keys());
//        System.out.println("Map 2 - Values:");
//        printList(m2.values());
//
//        op = (MapOperation) m2.put(new IntegerType(2), new StringType("Maria"));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//        System.out.println("Map 2 - Keys:");
//        printSet(m2.keys());
//        System.out.println("Map 2 - Values:");
//        printList(m2.values());
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//        System.out.println("Map 1 - Keys:");
//        printSet(m1.keys());
//        System.out.println("Map 1 - Values:");
//        printList(m1.values());
//
//        //Delete Ema
//        op = (MapOperation) m2.delete(new IntegerType(15));
//        System.out.println("Map 2 - Value of key 15:");
//        printSet(m2.get(new IntegerType(15)));
//        System.out.println("Map 2 - Keys:");
//        printSet(m2.keys());
//        System.out.println("Map 2 - Values:");
//        printList(m2.values());
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 15:");
//        printSet(m1.get(new IntegerType(15)));
//        System.out.println("Map 1 - Keys:");
//        printSet(m1.keys());
//        System.out.println("Map 1 - Values:");
//        printList(m1.values());
//
//        //Delete All
//        op = (MapOperation) m2.delete(new IntegerType(2));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//        System.out.println("Map 2 - Keys:");
//        printSet(m2.keys());
//        System.out.println("Map 2 - Values:");
//        printList(m2.values());
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//        System.out.println("Map 1 - Keys:");
//        printSet(m1.keys());
//        System.out.println("Map 1 - Values:");
//        printList(m1.values());
//
//    }
//
//    private static void testContains() {
//        System.out.println("TEST CONTAINS");
//
//        ORMapCRDT m1 = new ORMapCRDT("A");
//        ORMapCRDT m2 = new ORMapCRDT("A");
//
//        Operation op = (MapOperation) m1.put(new IntegerType(15), new StringType("Ema"));
//        System.out.println("Map 1 - Value of key 15:");
//        printSet(m1.get(new IntegerType(15)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 15:");
//        printSet(m2.get(new IntegerType(15)));
//
//
//        System.out.println("Map 1 - Contains key 15: " + m1.contains(new IntegerType(15)));
//        System.out.println("Map 2 - Contains key 15: " + m2.contains(new IntegerType(15)));
//        System.out.println("Map 1 - Contains key 0: " + m1.contains(new IntegerType(0)));
//        System.out.println("Map 2 - Contains key 0: " + m2.contains(new IntegerType(0)));
//
//        //Delete key 15
//        op = (MapOperation) m1.delete(new IntegerType(15));
//        System.out.println("Map 1 - Value of key 15:");
//        printSet(m1.get(new IntegerType(15)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 15:");
//        printSet(m2.get(new IntegerType(15)));
//
//        System.out.println("Map 1 - Contains key 15: " + m1.contains(new IntegerType(15)));
//        System.out.println("Map 2 - Contains key 15: " + m2.contains(new IntegerType(15)));
//        System.out.println("Map 1 - Contains key 0: " + m1.contains(new IntegerType(0)));
//        System.out.println("Map 2 - Contains key 0: " + m2.contains(new IntegerType(0)));
//
//    }
//
//    private static void testGetForConcurrentPutsAndOverwrite() {
//        System.out.println("TEST GET CONCURRENT; OVERWRITE");
//
//        ORMapCRDT m1 = new ORMapCRDT("A");
//        ORMapCRDT m2 = new ORMapCRDT("A");
//
//        Operation op = (MapOperation) m1.put(new IntegerType(1), new StringType("Ema"));
//        System.out.println("Map 1 - Value of key 1:");
//        printSet(m1.get(new IntegerType(1)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 1:");
//        printSet(m2.get(new IntegerType(1)));
//
//        //Concurrent Adds
//        op = (MapOperation) m2.put(new IntegerType(2), new StringType("Maria"));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//
//        Operation op1 = (MapOperation) m1.put(new IntegerType(2), new StringType("Joana"));
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//
//        m2.upstream(op1);
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//
//        //Overwrite
//        op = (MapOperation) m2.put(new IntegerType(2), new StringType("Ana"));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//
//        m1.upstream(op);
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//
//    }
//
//    private static void testConcurrentPutAndDelete() {
//        System.out.println("TEST CONCURRENT PUT AND DELETE");
//
//        ORMapCRDT m1 = new ORMapCRDT("A");
//        ORMapCRDT m2 = new ORMapCRDT("A");
//
//        m2.upstream(m1.put(new IntegerType(1), new StringType("Ema")));
//
//        //Concurrent add and remove of key 1 (already exists)
//        Operation op = (MapOperation) m1.put(new IntegerType(1), new StringType("Ema"));
//        System.out.println("Map 1 - Value of key 1:");
//        printSet(m1.get(new IntegerType(1)));
//
//        Operation op1 = (MapOperation) m2.delete(new IntegerType(1));
//        System.out.println("Map 2 - Value of key 1:");
//        printSet(m2.get(new IntegerType(1)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 1 after merge:");
//        printSet(m2.get(new IntegerType(1)));
//
//        m1.upstream(op1);
//        System.out.println("Map 1 - Value of key 1 after merge:");
//        printSet(m1.get(new IntegerType(1)));
//
//        //Concurrent add and remove of key 2 (does not exist)
//        op = (MapOperation) m1.put(new IntegerType(2), new StringType("Joana"));
//        System.out.println("Map 1 - Value of key 2:");
//        printSet(m1.get(new IntegerType(2)));
//
//        op1 = (MapOperation) m2.delete(new IntegerType(2));
//        System.out.println("Map 2 - Value of key 2:");
//        printSet(m2.get(new IntegerType(2)));
//
//        m2.upstream(op);
//        System.out.println("Map 2 - Value of key 2 after merge:");
//        printSet(m2.get(new IntegerType(2)));
//
//        m1.upstream(op1);
//        System.out.println("Map 1 - Value of key 2 after merge:");
//        printSet(m1.get(new IntegerType(2)));
//
//    }
//
//
//        private static void printSet(Set set) {
//        if( set == null)
//            System.out.println("null");
//        else {
//            System.out.printf("{ ");
//            set.forEach(e -> System.out.printf(e + " "));
//            System.out.printf("}\n");
//        }
//    }
//
//    private static void printList(List list) {
//        if(list == null)
//            System.out.println("null");
//        else {
//            System.out.printf("{ ");
//            list.forEach(e -> System.out.printf(e + " "));
//            System.out.printf("}\n");
//        }
//    }
//
//}
