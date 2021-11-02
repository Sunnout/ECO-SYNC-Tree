package protocols.broadcast.periodicpull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.GossipMessage;
import protocols.broadcast.common.messages.SynchronizationMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.broadcast.common.utils.CommunicationCostCalculator;
import protocols.broadcast.common.utils.MultiFileManager;
import protocols.broadcast.common.utils.VectorClock;
import protocols.broadcast.periodicpull.timers.PeriodicPullTimeout;
import protocols.broadcast.plumtree.utils.IncomingSync;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

public class PeriodicPullBroadcast extends CommunicationCostCalculator {
    private static final Logger logger = LogManager.getLogger(PeriodicPullBroadcast.class);

    public final static short PROTOCOL_ID = 490;
    public final static String PROTOCOL_NAME = "BCAST-PeriodicPull";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;
    private final static int SECONDS_TO_MILLIS = 1000;


    private final long reconnectTimeout;
    private final long pullTimeout;

    private long periodicPullTimer;

    private final Set<Host> partialView;
    private final Set<Host> neighbours;
    private final Set<UUID> received;

    private IncomingSync incomingSync;
    private long startTime;

    private final Random rnd;

    public static VectorClock vectorClock; // Local vector clock
    private int seqNumber; // Counter of local operations

    private final MultiFileManager fileManager;

    /*** Stats ***/
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncPull;

    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncPull;
    public static int receivedDupes;



    /*--------------------------------- Initialization ---------------------------------------- */

    public PeriodicPullBroadcast(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        long garbageCollectionTimeout = Long.parseLong(properties.getProperty("garbage_collection_timeout", "15")) * SECONDS_TO_MILLIS;
        long garbageCollectionTTL = Long.parseLong(properties.getProperty("garbage_collection_ttl", "60")) * SECONDS_TO_MILLIS;

        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));
        this.pullTimeout = Long.parseLong(properties.getProperty("pull_timeout", "2000"));

        this.partialView = new HashSet<>();
        this.neighbours = new HashSet<>();
        this.received = new HashSet<>();

        this.incomingSync = new IncomingSync(null, null);
        this.startTime = 0;

        this.rnd = new Random();

        vectorClock = new VectorClock(myself);

        int indexSpacing = Integer.parseInt(properties.getProperty("index_spacing", "100"));
        this.fileManager = new MultiFileManager(garbageCollectionTimeout, garbageCollectionTTL, indexSpacing, myself);

        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to setup channel-specific properties. See the
        // channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("bcast_port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
        // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
        // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
        // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SynchronizationMessage.MSG_ID, SynchronizationMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClockMsg, this::onMessageFailed);
        registerMessageHandler(channelId, SynchronizationMessage.MSG_ID, this::uponReceiveSynchronizationMsg, this::onMessageFailed);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(PeriodicPullTimeout.TIMER_ID, this::uponPeriodicPullTimeout);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        this.periodicPullTimer = setupTimer(new PeriodicPullTimeout(), pullTimeout);
        logger.debug("SETUP timer {} init", this.periodicPullTimer);
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        logger.info("SENT {}", mid);
        handleGossipMessage(new GossipMessage(mid, myself, ++seqNumber, request.getMsg()), myself, false);
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveVectorClockMsg(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedVC++;
        logger.debug("Received {} from {}", msg, from);

        SynchronizationMessage synchronizationMsg = new SynchronizationMessage(msg.getMid(), null,
                this.fileManager.readSyncOpsFromFile(msg.getVectorClock(), vectorClock));
        sendMessage(synchronizationMsg, from, TCPChannel.CONNECTION_IN);
        sentSyncOps++;
        sentSyncPull += synchronizationMsg.getMsgs().size();
        logger.debug("Sent {} to {}", synchronizationMsg, from);
    }

    private void uponReceiveSynchronizationMsg(SynchronizationMessage msg, Host from, short sourceProto, int channelId) {
        receivedSyncOps++;

        UUID mid = msg.getMid();
        IncomingSync hostInfo = new IncomingSync(from, mid);
        if (!hostInfo.equals(incomingSync))
            return;

        logger.debug("Received {} from {}", msg, from);

        try {
            for (byte[] serMsg : msg.getMsgs()) {
                receivedSyncPull++;

                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serMsg));
                GossipMessage gossipMessage = GossipMessage.deserialize(dis);

                if (received.add(gossipMessage.getMid())) {
                    handleGossipMessage(gossipMessage, from, true);
                }  else {
                    logger.info("DUPLICATE from {}", from);
                    receivedDupes++;
                }

            }
            logger.info("Received sync ops. Sync {} ENDED", mid);
        } catch (IOException e) {
            logger.error("Sync message handling error", e);
        }
        long timeout = pullTimeout - (System.currentTimeMillis() - this.startTime);
        if(timeout < 0)
            timeout = 0;
        this.incomingSync = new IncomingSync(null, null);
        this.periodicPullTimer = setupTimer(new PeriodicPullTimeout(), timeout);
        logger.debug("SETUP timer {} uponSyncMsgs", this.periodicPullTimer);
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponReconnectTimeout(ReconnectTimeout timeout, long timerId) {
        Host neighbour = timeout.getHost();
        if(partialView.contains(neighbour)) {
            logger.debug("Reconnecting with {}", neighbour);
            openConnection(neighbour);
        } else {
            logger.debug("Not reconnecting because {} is down", neighbour);
        }
    }

    private void uponPeriodicPullTimeout(PeriodicPullTimeout timeout, long timerId) {
        logger.debug("TIMEOUT {}", this.periodicPullTimer);

        Host h = getRandomNeighbour();
        if(h != null) {
            UUID mid = UUID.randomUUID();
            this.incomingSync = new IncomingSync(h, mid);
            VectorClockMessage msg = new VectorClockMessage(mid, new VectorClock(vectorClock.getClock()));
            sendMessage(msg, h);
            sentVC++;
            logger.debug("Sent {} to {}", msg, h);
            this.startTime = System.currentTimeMillis();
        } else {
            this.periodicPullTimer = setupTimer(new PeriodicPullTimeout(), pullTimeout);
            logger.debug("SETUP timer {} uponTimer", this.periodicPullTimer);
        }
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour)) {
            logger.debug("Added {} to partial view due to up {}", neighbour, partialView);
        } else {
            logger.error("Tried to add {} to partial view but is already there {}", neighbour, partialView);
        }

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.remove(neighbour)) {
            logger.debug("Removed {} from partial view due to down {}", neighbour, partialView);
        }

        if (neighbours.remove(neighbour)) {
            logger.debug("Removed {} from neighbours due to down {}", neighbour, neighbours);
        }

        closeConnection(neighbour);

        if(neighbour.equals(incomingSync.getHost())) {
            this.incomingSync = new IncomingSync(null, null);
            this.periodicPullTimer = setupTimer(new PeriodicPullTimeout(), pullTimeout);
            logger.debug("SETUP timer {} uponNeighDown", this.periodicPullTimer);
        }
    }


    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.trace("Host {} is down, cause: {}", host, event.getCause());

        if (neighbours.remove(host)) {
            logger.debug("Removed {} from neighbours due to plumtree down {}", host, neighbours);
        }

        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    @SuppressWarnings("rawtypes")
    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection to host {} failed, cause: {}", host, event.getCause());
        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);
        if(partialView.contains(neighbour)) {
            if (neighbours.add(neighbour)) {
                logger.debug("Added {} to neighbours {}", neighbour, neighbours);
            }
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void handleGossipMessage(GossipMessage msg,  Host from, boolean fromSync) {
        UUID mid = msg.getMid();
        Host sender = msg.getOriginalSender();
        vectorClock.incrementClock(sender);

        try {
            this.fileManager.writeOperationToFile(msg, vectorClock);
        } catch (IOException e) {
            logger.error("Error when writing operation to file", e);
        }

        logger.debug("Received {} from {}", mid, from);
        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, sender, msg.getContent(), fromSync));
    }

    private Host getRandomNeighbour() {
        if(neighbours.size() > 0) {
            int idx = rnd.nextInt(neighbours.size());
            Host[] hosts = neighbours.toArray(new Host[0]);
            return hosts[idx];
        } else
            return null;
    }
}