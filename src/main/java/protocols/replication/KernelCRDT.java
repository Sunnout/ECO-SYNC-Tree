package protocols.replication;

import protocols.replication.crdts.interfaces.GenericCRDT;
import protocols.replication.crdts.operations.Operation;

interface KernelCRDT extends GenericCRDT {

    void upstream(Operation op);

    void installState(KernelCRDT newCRDT);
    
}
