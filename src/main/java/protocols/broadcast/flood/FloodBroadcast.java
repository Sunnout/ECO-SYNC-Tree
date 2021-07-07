package protocols.broadcast.flood;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SyncOpsMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.flood.messages.FloodMessage;
import protocols.broadcast.common.notifications.SendVectorClockNotification;
import protocols.broadcast.common.notifications.VectorClockNotification;
import protocols.broadcast.common.requests.SyncOpsRequest;
import protocols.broadcast.common.requests.VectorClockRequest;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class FloodBroadcast extends GenericProtocol  {
    private static final Logger logger = LogManager.getLogger(FloodBroadcast.class);

    public final static short PROTOCOL_ID = 1500;
    public final static String PROTOCOL_NAME = "BCAST-Flood";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;

    private final long reconnectTimeout;

    private final Set<Host> partialView;
    private final Set<Host> neighbours;
    private final Queue<Host> pending;
    private Host currentPending;
    private final Queue<FloodMessage> bufferedOps; //Buffer ops received between sending vc to kernel and sending sync ops (and send them after)
    private boolean buffering; //To know if we are between sending vc to kernel and sending ops to neighbour
    private final Set<UUID> received;

    /*** Stats ***/
    public static int sentFlood;
    public static int sentSendVC;
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncFlood;

    public static int receivedFlood;
    public static int receivedDupesFlood;
    public static int receivedSendVC;
    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncFlood;
    public static int receivedDupesSyncFlood;



    /*--------------------------------- Initialization ---------------------------------------- */

    public FloodBroadcast(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));

        this.partialView = new HashSet<>();
        this.neighbours = new HashSet<>();
        this.pending = new LinkedList<>();
        this.bufferedOps = new LinkedList<>();
        this.buffering = false;
        this.received = new HashSet<>();

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
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(VectorClockRequest.REQUEST_ID, this::uponVectorClock);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOps);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, FloodMessage.MSG_ID, FloodMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, FloodMessage.MSG_ID, this::uponReceiveFlood, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponReceiveSyncOps, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        Host sender = request.getSender();
        byte[] content = request.getMsg();
        logger.debug("Propagating my {} to {}", mid, neighbours);
        FloodMessage msg = new FloodMessage(mid, sender, sourceProto, content);
        logger.info("SENT {}", mid);
        uponReceiveFlood(msg, myself, getProtoId(), -1);
    }

    private void uponVectorClock(VectorClockRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(), request.getVectorClock());
        sendMessage(msg, neighbour, TCPChannel.CONNECTION_IN);
        sentVC++;
        logger.debug("Sent {} to {}", msg, neighbour);
    }

    private void uponSyncOps(SyncOpsRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        if(!neighbour.equals(currentPending))
            return;

        SyncOpsMessage msg = new SyncOpsMessage(request.getMsgId(), request.getIds(), request.getOperations());
        sendMessage(msg, neighbour);
        sentSyncOps++;
        sentSyncFlood += request.getIds().size();
        logger.debug("Sent {} to {}", msg, neighbour);
        handleBufferedOperations(neighbour);
        addPendingToNeighbours();
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveFlood(FloodMessage msg, Host from, short sourceProto, int channelId) {
        receivedFlood++;

        UUID mid = msg.getMid();
        if (received.add(mid)) {
            handleFloodMessage(msg, from, false);
        } else {
            receivedDupesFlood++;
        }
    }

    private void uponReceiveVectorClock(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedVC++;

        logger.debug("Received {} from {}", msg, from);
        if(!from.equals(currentPending))
            return;
        this.buffering = true;
        triggerNotification(new VectorClockNotification(msg.getSender(), msg.getVectorClock()));
    }

    private void uponReceiveSendVectorClock(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedSendVC++;

        logger.debug("Received {} from {}", msg, from);
        triggerNotification(new SendVectorClockNotification(from));
    }

    private void uponReceiveSyncOps(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        receivedSyncOps++;

        logger.debug("Received {} from {}", msg, from);
        Iterator<byte[]> opIt = msg.getOperations().iterator();
        Iterator<byte[]> idIt = msg.getIds().iterator();

        while (opIt.hasNext() && idIt.hasNext()) {
            receivedSyncFlood++;

            byte[] serOp = opIt.next();
            byte[] serId = idIt.next();
            UUID mid = deserializeId(serId);

            if (received.add(mid)) {
                handleFloodMessage(new FloodMessage(mid, from , sourceProto, serOp), from, true);
            } else {
                receivedDupesSyncFlood++;
            }
        }
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

        if (pending.remove(neighbour)) {
            logger.debug("Removed {} from pending due to down {}", neighbour, pending);
        }

        if (neighbour.equals(currentPending)) {
            logger.debug("Removed {} from current pending due to down", neighbour);
            tryNextSync();
        }
        closeConnection(neighbour);
    }


    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.debug("Host {} is down, cause: {}", host, event.getCause());

        if (neighbours.remove(host)) {
            logger.debug("Removed {} from neighbours due to plumtree down {}", host, neighbours);
        }

        if (pending.remove(host)) {
            logger.debug("Removed {} from pending due to plumtree down {}", host, pending);
        }

        if (host.equals(currentPending)) {
            logger.debug("Removed {} from current pending due to plumtree down", host);
            tryNextSync();
        }

        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

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
            logger.debug("Trying sync from neighbour {} up", neighbour);
            startSynchronization(neighbour);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void handleFloodMessage(FloodMessage msg, Host from, boolean fromSync) {
        UUID mid = msg.getMid();
        logger.debug("Received op {} from {}. Is from sync {}", mid, from, fromSync);
        if(buffering)
            this.bufferedOps.add(msg);

        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, msg.getSender(), msg.getContent(), fromSync));
        forwardFloodMessage(msg, from);
    }

    private void forwardFloodMessage(FloodMessage msg, Host from) {
        neighbours.forEach(host -> {
            if (!host.equals(from)) {
                logger.debug("Sent {} to {}", msg, host);
                sendMessage(msg, host);
                sentFlood++;
            }
        });
    }

    private void startSynchronization(Host neighbour) {
        if (currentPending == null) {
            currentPending = neighbour;
            logger.debug("{} is my currentPending start", neighbour);
            requestVectorClock(currentPending);
        } else {
            pending.add(neighbour);
            logger.debug("Added {} to pending {}", neighbour, pending);
        }
    }

    private void addPendingToNeighbours() {
        if (neighbours.add(currentPending)) {
            logger.debug("Added {} to neighbours {} : pending list {}", currentPending, neighbours, pending);
        }

        tryNextSync();
    }

    private void tryNextSync() {
        currentPending = pending.poll();
        if (currentPending != null) {
            logger.debug("{} is my currentPending try", currentPending);
            requestVectorClock(currentPending);
        }
    }

    private void requestVectorClock(Host neighbour) {
        SendVectorClockMessage msg = new SendVectorClockMessage(UUID.randomUUID());
        sendMessage(msg, neighbour);
        sentSendVC++;
        logger.debug("Sent {} to {}", msg, neighbour);
    }

    private void handleBufferedOperations(Host neighbour) {
        this.buffering = false;
        FloodMessage msg;
        while((msg = bufferedOps.poll()) != null) {
            if(!msg.getSender().equals(neighbour)) {
                sendMessage(msg, neighbour);
                sentFlood++;
                logger.debug("Sent buffered {} to {}", msg, neighbour);
            }
        }
    }

    private UUID deserializeId(byte[] msg) {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        return new UUID(buf.readLong(), buf.readLong());
    }


    /*--------------------------------- Metrics ---------------------------------*/

    /**
     * If we passed a value > 0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
     * periodically by the channel. "getInConnections" and "getOutConnections" returns the currently established
     * connection to/from me. "getOldInConnections" and "getOldOutConnections" returns connections that have already
     * been closed.
     */
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics: ");
        long bytesSent = 0;
        long bytesReceived = 0;

        for(ChannelMetrics.ConnectionMetrics c: event.getOutConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldOutConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getInConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldInConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        sb.append(String.format("BytesSent=%s ", bytesSent));
        sb.append(String.format("BytesReceived=%s", bytesReceived));
        logger.info(sb);
    }
}