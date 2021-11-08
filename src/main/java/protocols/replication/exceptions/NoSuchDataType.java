package protocols.replication.exceptions;

public class NoSuchDataType extends RuntimeException {

    public NoSuchDataType(String dataType) {
        super(dataType + " does not exist.");
    }

}
