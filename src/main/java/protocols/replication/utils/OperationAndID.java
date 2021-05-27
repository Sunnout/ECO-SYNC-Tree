package protocols.replication.utils;

import crdts.operations.Operation;

import java.util.UUID;

public class OperationAndID {

    private Operation op;
    private UUID id;

    public OperationAndID(Operation op, UUID id) {
        this.op = op;
        this.id = id;
    }

    public Operation getOp() {
        return op;
    }

    public UUID getId() {
        return id;
    }
}
