package exceptions;

public class NoSuchCrdtType extends RuntimeException {

    public NoSuchCrdtType(String crdtType) {
        super(crdtType + " does not exist.");
    }

}
