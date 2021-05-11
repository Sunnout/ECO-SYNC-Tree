import crdts.operations.Operation;
import crdts.operations.SetOperation;
import protocols.replication.utils.SortOpsByHostClock;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

public class TestOpComparator {

    public static void main(String[] args) throws Exception {
        Queue<Operation> queue = new PriorityBlockingQueue<>(10, new SortOpsByHostClock());

        Operation op = new SetOperation(null, 0, null, "SET_ADD", "crdtId", "CRDT_TYPE", null);
        queue.add(op);

        op = new SetOperation(null, 7, null, "SET_ADD", "crdtId", "CRDT_TYPE", null);
        queue.add(op);

        op = new SetOperation(null, 2, null, "SET_ADD", "crdtId", "CRDT_TYPE", null);
        queue.add(op);

        op = new SetOperation(null, 3, null, "SET_ADD", "crdtId", "CRDT_TYPE", null);
        queue.add(op);

        op = new SetOperation(null, 123, null, "SET_ADD", "crdtId", "CRDT_TYPE", null);
        queue.add(op);

        while(!queue.isEmpty()){
            op = queue.poll();
            System.out.println(op.getSenderClock());
        }

    }

}