package crdts.interfaces;

import datatypes.SerializableType;

public interface RegisterCRDT extends GenericCRDT {

    SerializableType value();

}
