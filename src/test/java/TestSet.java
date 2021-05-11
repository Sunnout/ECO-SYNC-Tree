//import protocols.replication.ORSetCRDT;
//import crdts.operations.Operation;
//import crdts.operations.SetOperation;
//import datatypes.StringType;
//
//import java.util.Set;
//
//public class TestSet {
//
//    public static void main(String[] args) throws Exception {
//
////        testAdd();
////        testRemove();
////        testElementsAndRemoveDupesAndLookup();
////        testConcurrentAddAndRemove();
//
//    }
//
//    private static void testAdd() {
//        System.out.println("TEST ADD");
//
//        ORSetCRDT s1 = new ORSetCRDT("A");
//        ORSetCRDT s2 = new ORSetCRDT("A");
//
//        System.out.println("s1 shoud be empty:");
//        printSet(s1.elements());
//        Operation op = (SetOperation) s1.add(new StringType("A"));
//        System.out.println("s1 should be {A}:");
//        printSet(s1.elements());
//
//        System.out.println("s2 should be empty:");
//        printSet(s2.elements());
//        s2.upstream(op);
//        System.out.println("s2 should be {A}:");
//        printSet(s2.elements());
//    }
//
//    private static void testRemove() {
//        System.out.println("TEST REMOVE");
//
//        ORSetCRDT s1 = new ORSetCRDT("A");
//        ORSetCRDT s2 = new ORSetCRDT("A");
//
//        s2.upstream(s1.add(new StringType("A")));
//        s2.upstream(s1.add(new StringType("B")));
//
//        System.out.println("s1 should be {A,B}:");
//        printSet(s1.elements());
//        System.out.println("s2 should be {A,B}:");
//        printSet(s2.elements());
//
//        Operation op = (SetOperation) s1.remove(new StringType("B"));
//        System.out.println("s1 should be {A}:");
//        printSet(s1.elements());
//
//        s2.upstream(op);
//        System.out.println("s2 should be {A}:");
//        printSet(s2.elements());
//    }
//
//    private static void testElementsAndRemoveDupesAndLookup() {
//        System.out.println("TEST ELEMENTS; REMOVE DUPES; LOOKUP");
//
//        ORSetCRDT s1 = new ORSetCRDT("A");
//        ORSetCRDT s2 = new ORSetCRDT("A");
//
//        s2.upstream(s1.add(new StringType("A")));
//        s2.upstream(s1.add(new StringType("B")));
//        s2.upstream(s1.add(new StringType("C")));
//        s2.upstream(s1.add(new StringType("D")));
//        System.out.println("s1 should be {A,B,C,D}:");
//        printSet(s1.elements());
//        System.out.println("A belongs to set should be true: " + s1.lookup(new StringType("A")));
//        s2.upstream(s1.add(new StringType("A")));
//        System.out.println("s1 should be {A,B,C,D}:");
//        printSet(s1.elements());
//        System.out.println("A belongs to set should be true: " + s1.lookup(new StringType("A")));
//        s2.upstream(s1.add(new StringType("D")));
//        System.out.println("s2 should be {A,B,C,D}:");
//        printSet(s2.elements());
//        System.out.println("D belongs to set should be true: " + s2.lookup(new StringType("D")));
//        s2.upstream(s1.remove(new StringType("A")));
//        System.out.println("s1 should be {B,C,D}:");
//        printSet(s1.elements());
//        System.out.println("A belongs to set should be false: " + s1.lookup(new StringType("A")));
//        s1.upstream(s2.remove(new StringType("D")));
//        System.out.println("s2 should be {B,C}:");
//        printSet(s2.elements());
//        System.out.println("D belongs to set should be false: " + s2.lookup(new StringType("D")));
//
//    }
//
//    private static void testConcurrentAddAndRemove() {
//        System.out.println("TEST CONCURRENT ADD AND REMOVE");
//
//        ORSetCRDT s1 = new ORSetCRDT("A");
//        ORSetCRDT s2 = new ORSetCRDT("A");
//
//        s2.upstream(s1.add(new StringType("A")));
//        s2.upstream(s1.add(new StringType("B")));
//        s2.upstream(s1.add(new StringType("C")));
//
//        //Concurrent add and remove of A (already exists)
//        System.out.println("s1 should be {A,B,C}:");
//        printSet(s1.elements());
//        Operation op = (SetOperation) s1.add(new StringType("A"));
//        System.out.println("s1 should be {A,B,C}:");
//        printSet(s1.elements());
//
//        System.out.println("s2 should be {A,B,C}:");
//        printSet(s2.elements());
//        Operation op1 = (SetOperation) s2.remove(new StringType("A"));
//        System.out.println("s2 should be {B,C}:");
//        printSet(s2.elements());
//        s2.upstream(op);
//        System.out.println("Value of s2 after merge should be {A,B,C}:");
//        printSet(s2.elements());
//
//        System.out.println("s1 should be {A,B,C}:");
//        printSet(s1.elements());
//        s1.upstream(op1);
//        System.out.println("Value of s1 after merge should be {A,B,C}:");
//        printSet(s1.elements());
//
//        //Concurrent add and remove of D (does not exist)
//        System.out.println("s1 should be {A,B,C}:");
//        printSet(s1.elements());
//        op = (SetOperation) s1.add(new StringType("D"));
//        System.out.println("s1 should be {A,B,C,D}:");
//        printSet(s1.elements());
//
//        System.out.println("s2 should be {A,B,C}:");
//        printSet(s2.elements());
//        op1 = (SetOperation) s2.remove(new StringType("D"));
//        System.out.println("s2 should be {A,B,C}:");
//        printSet(s2.elements());
//        s2.upstream(op);
//        System.out.println("Value of s2 after merge should be {A,B,C,D}:");
//        printSet(s2.elements());
//
//        System.out.println("s1 should be {A,B,C,D}:");
//        printSet(s1.elements());
//        s1.upstream(op1);
//        System.out.println("Value of s1 after merge should be {A,B,C,D}:");
//        printSet(s1.elements());
//
//    }
//
//    private static void printSet(Set set) {
//        System.out.printf("{ ");
//        set.forEach(e -> System.out.printf(e + " "));
//        System.out.printf("}\n");
//    }
//
//}
