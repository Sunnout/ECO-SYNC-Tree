package protocols.replication.utils;

import crdts.operations.Operation;

import java.util.Comparator;

public class SortOpsByHostClock implements Comparator<Operation> {

    @Override
    public int compare(Operation o1, Operation o2) {
        int v1 = o1.getSenderClock();
        int v2 = o2.getSenderClock();
        if (v1 < v2)
            return -1;
        else if (v1 > v2)
            return 1;
        else
            return 0;
    }

}
