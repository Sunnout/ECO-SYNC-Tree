package protocols.replication.crdts.interfaces;

import protocols.replication.crdts.datatypes.SerializableType;

import java.util.Set;


public interface SetCRDT extends GenericCRDT {

    boolean lookup(SerializableType elem);

    Set<SerializableType> elements();

}
