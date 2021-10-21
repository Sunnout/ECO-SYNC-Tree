package crdts.interfaces;

import datatypes.SerializableType;

import java.util.Set;


public interface SetCRDT extends GenericCRDT {

    boolean lookup(SerializableType elem);

    Set<SerializableType> elements();

}
