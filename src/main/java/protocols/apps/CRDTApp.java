package protocols.apps;

import java.util.*;

import crdts.interfaces.GenericCRDT;
import crdts.operations.Operation;
import protocols.replication.*;
import protocols.replication.OpCounterCRDT.CounterOpType;
import protocols.replication.LWWRegisterCRDT.RegisterOpType;
import protocols.replication.ORSetCRDT.SetOpType;
import protocols.replication.ORMapCRDT.MapOpType;
import datatypes.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.timers.*;
import protocols.replication.notifications.*;
import protocols.replication.requests.*;
import protocols.replication.utils.OperationAndID;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class CRDTApp extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(CRDTApp.class);
    private static final int TO_MILLIS = 1000;

    //RUN = 0 --> counter; 1 --> register; 2 --> set; 3 --> map; 4 --> 8 registers;
    //5 --> 8 sets; 6 --> 8 maps; 7 --> 1 of each CRDT
    private static final int RUN = 7;

    //True for several periodic ops, false for 1 op per crdt from each app
    private static final boolean PERIODIC_OPS = true;

    private static final String COUNTER = "counter";
    private static final String LWW_REGISTER = "lww_register";
    private static final String OR_SET = "or_set";
    private static final String OR_MAP = "or_map";

    private static final String CRDT0 = "CRDT0";
    private static final String CRDT1 = "CRDT1";
    private static final String CRDT2 = "CRDT2";
    private static final String CRDT3 = "CRDT3";
    private static final String CRDT4 = "CRDT4";
    private static final String CRDT5 = "CRDT5";
    private static final String CRDT6 = "CRDT6";
    private static final String CRDT7 = "CRDT7";
    private static final String CRDT8 = "CRDT8";

    //Protocol information, to register in babel
    public static final String PROTO_NAME = "CRDTApp";
    public static final short PROTO_ID = 300;

    private final short replicationKernelId;
    private final Host self;

    //Time to wait until creating crdts
    private final int createTime;
    //Time to wait until starting to send messages
    private final int startTime;
    //Time to wait until releasing crdts
    private final int releaseTime;
    //Time to run before stopping sending messages
    private final int runTime;
    //Time to wait until shut down
    private final int cooldownTime;

    //Interval between each increment
    private final int ops1Interval;
    //Interval between each decrement
    private final int ops2Interval;

    //Increment(By), decrement(By) and value periodic timers
    private long ops1Timer;
    private long ops2Timer;

    //Map of crdtId to GenericCRDT
    private Map<String, GenericCRDT> myCRDTs;

    private Random rand;

    public CRDTApp(Properties properties, Host self, short replicationKernelId) throws HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.replicationKernelId = replicationKernelId;
        this.self = self;
        this.myCRDTs = new HashMap<>();

        //Read configurations
        this.createTime = Integer.parseInt(properties.getProperty("create_time"));
        this.startTime = Integer.parseInt(properties.getProperty("start_time"));
        this.releaseTime = Integer.parseInt(properties.getProperty("release_time"));
        this.cooldownTime = Integer.parseInt(properties.getProperty("cooldown_time"));
        this.runTime = Integer.parseInt(properties.getProperty("run_time"));
        this.ops1Interval = Integer.parseInt(properties.getProperty("ops1"));
        this.ops2Interval = Integer.parseInt(properties.getProperty("ops2"));
        this.rand = new Random();

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ExecuteOps1Timer.TIMER_ID, this::uponExecuteOps1Timer);
        registerTimerHandler(ExecuteOps2Timer.TIMER_ID, this::uponExecuteOps2Timer);
        registerTimerHandler(SingleOpTimer.TIMER_ID, this::uponExecuteSingleOpTimer);
        registerTimerHandler(CreateCRDTsTimer.TIMER_ID, this::uponCreateCRDTsTimer);
        registerTimerHandler(StartTimer.TIMER_ID, this::uponStartTimer);
        registerTimerHandler(ReleaseCrdtTimer.TIMER_ID, this::uponReleaseCrdtTimer);
        registerTimerHandler(StopTimer.TIMER_ID, this::uponStopTimer);
        registerTimerHandler(PrintValuesTimer.TIMER_ID, this::uponPrintValuesTimer);
        registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ReturnCRDTNotification.NOTIFICATION_ID, this::uponReturnCRDTNotification);
        subscribeNotification(CRDTAlreadyExistsNotification.NOTIFICATION_ID, this::uponCRDTAlreadyExistsNotification);

    }

    @Override
    public void init(Properties props) {
        //Wait before creating crdts
        logger.info("Waiting...");
        setupTimer(new CreateCRDTsTimer(), createTime * TO_MILLIS);
    }

    /* --------------------------------- Methods --------------------------------- */

    private void getCRDTs(int run) {
        if(run == 0) {
            getCRDT(COUNTER, new String[]{"int"}, CRDT0);
        } else if(run == 1) {
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
        } else if(run == 2) {
            getCRDT(OR_SET, new String[]{"int"}, CRDT2);
        } else if(run == 3) {
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT3);
        } else if(run == 4) {
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
            getCRDT(LWW_REGISTER, new String[]{"long"}, CRDT2);
            getCRDT(LWW_REGISTER, new String[]{"short"}, CRDT3);
            getCRDT(LWW_REGISTER, new String[]{"float"}, CRDT4);
            getCRDT(LWW_REGISTER, new String[]{"double"}, CRDT5);
            getCRDT(LWW_REGISTER, new String[]{"string"}, CRDT6);
            getCRDT(LWW_REGISTER, new String[]{"boolean"}, CRDT7);
            getCRDT(LWW_REGISTER, new String[]{"byte"}, CRDT8);
        } else if(run == 5) {
            getCRDT(OR_SET, new String[]{"int"}, CRDT1);
            getCRDT(OR_SET, new String[]{"long"}, CRDT2);
            getCRDT(OR_SET, new String[]{"short"}, CRDT3);
            getCRDT(OR_SET, new String[]{"float"}, CRDT4);
            getCRDT(OR_SET, new String[]{"double"}, CRDT5);
            getCRDT(OR_SET, new String[]{"string"}, CRDT6);
            getCRDT(OR_SET, new String[]{"boolean"}, CRDT7);
            getCRDT(OR_SET, new String[]{"byte"}, CRDT8);
        } else if(run == 6) {
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT1);
            getCRDT(OR_MAP, new String[]{"byte", "short"}, CRDT2);
            getCRDT(OR_MAP, new String[]{"byte", "long"}, CRDT3);
            getCRDT(OR_MAP, new String[]{"byte", "float"}, CRDT4);
            getCRDT(OR_MAP, new String[]{"byte", "double"}, CRDT5);
            getCRDT(OR_MAP, new String[]{"byte", "boolean"}, CRDT6);
            getCRDT(OR_MAP, new String[]{"byte", "string"}, CRDT7);
            getCRDT(OR_MAP, new String[]{"byte", "byte"}, CRDT8);
        } else if(run == 7) {
            getCRDT(COUNTER, new String[]{"int"}, CRDT0);
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
            getCRDT(OR_SET, new String[]{"int"}, CRDT2);
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT3);
        }
    }

    private void executeOp1(int run) {
        if(run == 0) {
            executeCounterOperation(CRDT0, CounterOpType.INCREMENT);
        } else if(run == 1) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
        } else if(run == 2) {
            executeSetOperation(CRDT2, SetOpType.ADD, new IntegerType(rand.nextInt(10)));
        } else if(run == 3) {
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)rand.nextInt(2)), new IntegerType(rand.nextInt(10)));
        } else if(run == 4) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(5));
            executeRegisterOperation(CRDT2, RegisterOpType.ASSIGN, new LongType(5L));
            executeRegisterOperation(CRDT3, RegisterOpType.ASSIGN, new ShortType((short)2));
            executeRegisterOperation(CRDT4, RegisterOpType.ASSIGN, new FloatType(8f));
            executeRegisterOperation(CRDT5, RegisterOpType.ASSIGN, new DoubleType(1.4));
            executeRegisterOperation(CRDT6, RegisterOpType.ASSIGN, new StringType("Olá"));
            executeRegisterOperation(CRDT7, RegisterOpType.ASSIGN, new BooleanType(true));
            executeRegisterOperation(CRDT8, RegisterOpType.ASSIGN, new ByteType((byte)0));
        } else if(run == 5) {
            executeSetOperation(CRDT1, SetOpType.ADD, new IntegerType(1));
            executeSetOperation(CRDT2, SetOpType.ADD, new LongType(5L));
            executeSetOperation(CRDT3, SetOpType.ADD, new ShortType((short)2));
            executeSetOperation(CRDT4, SetOpType.ADD, new FloatType(8f));
            executeSetOperation(CRDT5, SetOpType.ADD, new DoubleType(1.4));
            executeSetOperation(CRDT6, SetOpType.ADD, new StringType("Olá"));
            executeSetOperation(CRDT7, SetOpType.ADD, new BooleanType(true));
            executeSetOperation(CRDT8, SetOpType.ADD, new ByteType((byte)0));
        } else if(run == 6) {
            executeMapOperation(CRDT1, MapOpType.PUT, new ByteType((byte)1), new IntegerType(1));
            executeMapOperation(CRDT2, MapOpType.PUT, new ByteType((byte)1), new ShortType((short)4));
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)1), new LongType(5L));
            executeMapOperation(CRDT4, MapOpType.PUT, new ByteType((byte)1), new FloatType(8f));
            executeMapOperation(CRDT5, MapOpType.PUT, new ByteType((byte)1), new DoubleType(1.4));
            executeMapOperation(CRDT6, MapOpType.PUT, new ByteType((byte)1), new BooleanType(true));
            executeMapOperation(CRDT7, MapOpType.PUT, new ByteType((byte)1), new StringType("Olá, bom dia"));
            executeMapOperation(CRDT8, MapOpType.PUT, new ByteType((byte)1), new ByteType((byte)0));
        } else if(run == 7) {
            executeCounterOperation(CRDT0, CounterOpType.INCREMENT);
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
            executeSetOperation(CRDT2, SetOpType.ADD, new IntegerType(rand.nextInt(10)));
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)rand.nextInt(2)), new IntegerType(rand.nextInt(10)));
        }
    }

    private void executeOp2(int run) {
        if(run == 0) {
            executeCounterOperation(CRDT0, CounterOpType.DECREMENT);
        } else if(run == 1) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
        } else if(run == 2) {
            executeSetOperation(CRDT2, SetOpType.REMOVE, new IntegerType(rand.nextInt(10)));
        } else if(run == 3) {
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)rand.nextInt(2)), null);
        } else if(run == 4) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(7));
            executeRegisterOperation(CRDT2, RegisterOpType.ASSIGN, new LongType(8L));
            executeRegisterOperation(CRDT3, RegisterOpType.ASSIGN, new ShortType((short)4));
            executeRegisterOperation(CRDT4, RegisterOpType.ASSIGN, new FloatType(9f));
            executeRegisterOperation(CRDT5, RegisterOpType.ASSIGN, new DoubleType(1.35));
            executeRegisterOperation(CRDT6, RegisterOpType.ASSIGN, new StringType("Bom dia"));
            executeRegisterOperation(CRDT7, RegisterOpType.ASSIGN, new BooleanType(false));
            executeRegisterOperation(CRDT8, RegisterOpType.ASSIGN, new ByteType((byte)1));
        } else if(run == 5) {
            executeSetOperation(CRDT1, SetOpType.REMOVE, new IntegerType(1));
            executeSetOperation(CRDT2, SetOpType.REMOVE, new LongType(5L));
            executeSetOperation(CRDT3, SetOpType.REMOVE, new ShortType((short)2));
            executeSetOperation(CRDT4, SetOpType.REMOVE, new FloatType(8f));
            executeSetOperation(CRDT5, SetOpType.REMOVE, new DoubleType(1.4));
            executeSetOperation(CRDT6, SetOpType.REMOVE, new StringType("Olá"));
            executeSetOperation(CRDT7, SetOpType.REMOVE, new BooleanType(true));
            executeSetOperation(CRDT8, SetOpType.REMOVE, new ByteType((byte)0));
        } else if(run == 6) {
            executeMapOperation(CRDT1, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT2, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT4, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT5, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT6, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT7, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT8, MapOpType.DELETE, new ByteType((byte)1), null);
        } else if(run == 7) {
            executeCounterOperation(CRDT0, CounterOpType.DECREMENT);
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
            executeSetOperation(CRDT2, SetOpType.REMOVE, new IntegerType(rand.nextInt(10)));
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)rand.nextInt(2)), null);
        }
    }

    private void printFinalValues(int run) {
        logger.info("RESULTS:");
        logger.info("Final vector clock: {}", ReplicationKernel.vectorClock);

        if(run == 0) {
            logger.info("Integer value of {}: {}", CRDT0, getCounterValue(CRDT0));
        } else if(run == 1) {
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
        } else if(run == 2) {
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
        } else if(run == 3) {
            Set<SerializableType> keys = getMapKeys(CRDT3);
            for(SerializableType key : keys) {
                logger.info("{} key {} : {}", CRDT3, key, getMapping(CRDT3, key));
            }
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
        } else if(run == 4) {
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
            logger.info("Long value of {}: {}", CRDT2, getRegisterValue(CRDT2));
            logger.info("Short value of {}: {}", CRDT3, getRegisterValue(CRDT3));
            logger.info("Float value of {}: {}", CRDT4, getRegisterValue(CRDT4));
            logger.info("Double value of {}: {}", CRDT5, getRegisterValue(CRDT5));
            logger.info("String value of {}: {}", CRDT6, getRegisterValue(CRDT6));
            logger.info("Boolean value of {}: {}", CRDT7, getRegisterValue(CRDT7));
            logger.info("Byte value of {}: {}", CRDT8, getRegisterValue(CRDT8));
        } else if(run == 5) {
            logger.info("Value of {}: {}", CRDT1, getSetValue(CRDT1));
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
            logger.info("Value of {}: {}", CRDT3, getSetValue(CRDT3));
            logger.info("Value of {}: {}", CRDT4, getSetValue(CRDT4));
            logger.info("Value of {}: {}", CRDT5, getSetValue(CRDT5));
            logger.info("Value of {}: {}", CRDT6, getSetValue(CRDT6));
            logger.info("Value of {}: {}", CRDT7, getSetValue(CRDT7));
            logger.info("Value of {}: {}", CRDT8, getSetValue(CRDT8));
        } else if(run == 6) {
            logger.info("Keys of {}: {}", CRDT1, getMapKeys(CRDT1));
            logger.info("Values of {}: {}", CRDT1, getMapValues(CRDT1));
            logger.info("Keys of {}: {}", CRDT2, getMapKeys(CRDT2));
            logger.info("Values of {}: {}", CRDT2, getMapValues(CRDT2));
            logger.info("Keys of {}: {}", CRDT3, getMapKeys(CRDT3));
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
            logger.info("Keys of {}: {}", CRDT4, getMapKeys(CRDT4));
            logger.info("Values of {}: {}", CRDT4, getMapValues(CRDT4));
            logger.info("Keys of {}: {}", CRDT5, getMapKeys(CRDT5));
            logger.info("Values of {}: {}", CRDT5, getMapValues(CRDT5));
            logger.info("Keys of {}: {}", CRDT6, getMapKeys(CRDT6));
            logger.info("Values of {}: {}", CRDT6, getMapValues(CRDT6));
            logger.info("Keys of {}: {}", CRDT7, getMapKeys(CRDT7));
            logger.info("Values of {}: {}", CRDT7, getMapValues(CRDT7));
            logger.info("Keys of {}: {}", CRDT8, getMapKeys(CRDT8));
            logger.info("Values of {}: {}", CRDT8, getMapValues(CRDT8));
        } else if(run == 7) {
            logger.info("Integer value of {}: {}", CRDT0, getCounterValue(CRDT0));
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
            Set<SerializableType> keys = getMapKeys(CRDT3);
            for(SerializableType key : keys) {
                logger.info("{} key {} : {}", CRDT3, key, getMapping(CRDT3, key));
            }
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
        }

        logger.info("Number of sent operations: {}", ReplicationKernel.sentOps);
        logger.info("Number of received operations: {}", ReplicationKernel.receivedOps);
        logger.info("Number of executed operations: {}", ReplicationKernel.executedOps);
    }

    private void getCRDT(String crdtType, String[] dataType, String crdtId) {
        //Creating new CRDT by asking the replication kernel for it
        sendRequest(new GetCRDTRequest(UUID.randomUUID(), self, crdtType, dataType, crdtId), replicationKernelId);
    }

    private void releaseCRDT(String crdtId) {
        //Removing local copy of CRDT
        if(myCRDTs.remove(crdtId) != null) {
            sendRequest(new ReleaseCRDTRequest(UUID.randomUUID(), self, crdtId), replicationKernelId);
        } else {
            //No CRDT with crdtId
        }
    }

    private void executeCounterOperation(String crdtId, CounterOpType opType) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof OpCounterCRDT) {
                switch(opType) {
                    case INCREMENT:
                        ((OpCounterCRDT) crdt).increment(self);
                        break;
                    case DECREMENT:
                        ((OpCounterCRDT) crdt).decrement(self);
                        break;
                    default:
                        //No other ops
                        break;
                }

            } else {
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void executeCounterOperation(String crdtId, CounterOpType opType, int value) {
        GenericCRDT crdt = myCRDTs.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof OpCounterCRDT) {
                switch(opType) {
                    case INCREMENT_BY:
                        ((OpCounterCRDT) crdt).incrementBy(self, value);
                        break;
                    case DECREMENT_BY:
                        ((OpCounterCRDT) crdt).decrementBy(self, value);
                        break;
                    default:
                        //No other ops
                        break;
                }

            } else {
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void executeRegisterOperation(String crdtId, RegisterOpType opType, SerializableType value) {
        GenericCRDT crdt = myCRDTs.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof LWWRegisterCRDT) {
                switch(opType) {
                    case ASSIGN:
                        ((LWWRegisterCRDT) crdt).assign(self, value);
                        break;
                    default:
                        //No other ops
                        break;
                }

            } else {
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void executeSetOperation(String crdtId, SetOpType opType, SerializableType value) {
        GenericCRDT crdt = myCRDTs.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof ORSetCRDT) {
                switch(opType) {
                    case ADD:
                        ((ORSetCRDT) crdt).add(self, value);
                        break;
                    case REMOVE:
                        ((ORSetCRDT) crdt).remove(self, value);
                        break;
                    default:
                        //No other ops
                        break;
                }

            } else {
                //CRDT with crdtId is not a orsetsCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void executeMapOperation(String crdtId, MapOpType opType, SerializableType key, SerializableType value) {
        GenericCRDT crdt = myCRDTs.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof ORMapCRDT) {
                switch(opType) {
                    case PUT:
                        ((ORMapCRDT) crdt).put(self, key, value);
                        break;
                    case DELETE:
                        ((ORMapCRDT) crdt).delete(self, key);
                        break;
                    default:
                        //No other ops
                        break;
                }

            } else {
                //CRDT with crdtId is not a orsetsCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private int getCounterValue(String crdtId) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof OpCounterCRDT) {
                return ((OpCounterCRDT) crdt).value();
            } else {
                return 0; //TODO: exception?
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            return 0;
            //No CRDT with crdtId
        }
    }

    private Object getRegisterValue(String crdtId) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof LWWRegisterCRDT) {
                return ((LWWRegisterCRDT) crdt).value();
            } else {
                return null; //TODO: exception?
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            return null;
            //No CRDT with crdtId
        }
    }

    private Object getSetValue(String crdtId) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof ORSetCRDT) {
                return ((ORSetCRDT) crdt).elements();
            } else {
                return null; //TODO: exception?
                //CRDT with crdtId is not a orsetCRDT
            }
        } else {
            return null;
            //No CRDT with crdtId
        }
    }

    private Set<SerializableType> getMapKeys(String crdtId) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof ORMapCRDT) {
                return ((ORMapCRDT) crdt).keys();
            } else {
                return null; //TODO: exception?
                //CRDT with crdtId is not a ormapCRDT
            }
        } else {
            return null;
            //No CRDT with crdtId
        }
    }

    private List<SerializableType> getMapValues(String crdtId) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof ORMapCRDT) {
                return ((ORMapCRDT) crdt).values();
            } else {
                return null; //TODO: exception?
                //CRDT with crdtId is not a ormapCRDT
            }
        } else {
            return null;
            //No CRDT with crdtId
        }
    }

    private Set<SerializableType> getMapping(String crdtId, SerializableType key) {
        GenericCRDT crdt = myCRDTs.get(crdtId);
        if(crdt != null) {
            if(crdt instanceof ORMapCRDT) {
                return ((ORMapCRDT) crdt).get(key);
            } else {
                return null; //TODO: exception?
                //CRDT with crdtId is not a ormapCRDT
            }
        } else {
            return null;
            //No CRDT with crdtId
        }
    }

    private void createCRDTs() {
        //Creating CRDTs
        logger.info("Creating crdts...");
        getCRDTs(RUN);
        startOperations();
//        setupTimer(new StartTimer(), prepareTime * TO_MILLIS);
    }

    private void startOperations() {
        logger.info("Starting operations...");

        if(PERIODIC_OPS) {
            ops1Timer = setupPeriodicTimer(new ExecuteOps1Timer(), 0, ops1Interval);
            ops2Timer = setupPeriodicTimer(new ExecuteOps2Timer(), 0, ops2Interval);
        } else {
            setupTimer(new SingleOpTimer(), ops1Interval);
        }

        //Release CRDTs
//        setupTimer(new ReleaseCrdtTimer(), releaseTime * TO_MILLIS);

        //And setup single timers
        setupTimer(new StopTimer(), runTime * TO_MILLIS);
    }

    /* --------------------------------- Timers --------------------------------- */

    private void uponCreateCRDTsTimer(CreateCRDTsTimer timer, long timerId) {
        createCRDTs();
    }

    private void uponStartTimer(StartTimer startTimer, long timerId) {
        startOperations();
    }

    private void uponReleaseCrdtTimer(ReleaseCrdtTimer releaseCrdtTimer, long timerId) {
        releaseCRDT(CRDT0);
    }

    private void uponExecuteOps1Timer(ExecuteOps1Timer incTimer, long timerId) {
        executeOp1(RUN);
    }

    private void uponExecuteOps2Timer(ExecuteOps2Timer decTimer, long timerId) {
        executeOp2(RUN);
    }

    private void uponExecuteSingleOpTimer(SingleOpTimer timer, long timerId) {
        executeOp1(RUN);
    }

    private void uponStopTimer(StopTimer stopTimer, long timerId) {
        logger.info("Stopping broadcasts");

        //Stop executing operations
        this.cancelTimer(ops1Timer);
        this.cancelTimer(ops2Timer);

        setupTimer(new PrintValuesTimer(), cooldownTime * TO_MILLIS);
//        setupTimer(new ExitTimer(), cooldownTime * TO_MILLIS);
    }

    private void uponPrintValuesTimer(PrintValuesTimer printValuesTimer, long timerId) {
        printFinalValues(RUN);
        setupTimer(new ExitTimer(), cooldownTime * TO_MILLIS);
    }

    private void uponExitTimer(ExitTimer exitTimer, long timerId) {
        logger.info("Exiting...");
        //Shutting down
        System.exit(0);
    }


    /* --------------------------------- Notifications --------------------------------- */

    private void uponReturnCRDTNotification(ReturnCRDTNotification notification, short sourceProto) {
        GenericCRDT crdt = notification.getCrdt();
        String crdtId = crdt.getCrdtId();
        logger.info("CRDT {} was created by {}", crdtId, self);
        //Saving crdt in local map
        myCRDTs.put(crdtId, crdt);
    }

    private void uponCRDTAlreadyExistsNotification(CRDTAlreadyExistsNotification notification, short sourceProto) {
        logger.info("CRDT {} already exists for {}", notification.getCrdtId(), self);
    }

}
