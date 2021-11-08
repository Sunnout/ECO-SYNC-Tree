package protocols.replication.crdts.interfaces;

import protocols.replication.crdts.datatypes.SerializableType;

public interface RegisterCRDT extends GenericCRDT {

    SerializableType value();

}
