package protocols.broadcast.plumtree;

import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.plumtree.notifications.NewPendingNeighbourNotification;
import protocols.broadcast.plumtree.notifications.SendVectorClockNotification;
import protocols.broadcast.plumtree.notifications.VectorClockNotification;
import protocols.broadcast.plumtree.notifications.SyncOpsNotification;
import protocols.broadcast.plumtree.requests.AddPendingToEagerRequest;
import protocols.broadcast.plumtree.requests.SendVectorClockRequest;
import protocols.broadcast.plumtree.requests.SyncOpsRequest;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.broadcast.plumtree.requests.MyVectorClockRequest;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.plumtree.timers.IHaveTimeout;
import protocols.broadcast.plumtree.utils.AddressedIHaveMessage;
import protocols.broadcast.plumtree.utils.LazyQueuePolicy;
import protocols.broadcast.plumtree.utils.MessageSource;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

import java.io.IOException;
import java.util.*;

public class PlumTree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "PlumTree";

    private final int space;

    private final long timeout1;
    private final long timeout2;

    private final Host myself;

    private final Set<Host> eager;
    private final Set<Host> lazy;

    private final Queue<Host> pending;
    private Host currentPending;

    private final Map<UUID, Queue<MessageSource>> missing;
    private final Map<UUID, GossipMessage> received;

    private final Queue<UUID> stored;

    private final Map<UUID, Long> onGoingTimers;
    private final Queue<AddressedIHaveMessage> lazyQueue;

    private final LazyQueuePolicy policy;

    private boolean channelReady;

    public PlumTree(Properties properties, Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.space = Integer.parseInt(properties.getProperty("space", "6000"));
        this.stored = new LinkedList<>();

        this.eager = new HashSet<>();
        this.lazy = new HashSet<>();

        this.pending = new LinkedList<>();

        this.missing = new HashMap<>();
        this.received = new HashMap<>();
        this.onGoingTimers = new HashMap<>();
        this.lazyQueue = new LinkedList<>();

        this.policy = HashSet::new;

        this.timeout1 = Long.parseLong(properties.getProperty("timeout1", "1000"));
        this.timeout2 = Long.parseLong(properties.getProperty("timeout2", "500"));

        this.channelReady = false;

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(MyVectorClockRequest.REQUEST_ID, this::uponMyVectorClockRequest);
        registerRequestHandler(SendVectorClockRequest.REQUEST_ID, this::uponSendVectorClockRequest);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOpsRequest);
        registerRequestHandler(AddPendingToEagerRequest.REQUEST_ID, this::uponAddPendingToEagerRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }


    /*--------------------------------- Messages ---------------------------------------- */
    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        UUID mid = msg.getMid();
        if(!received.containsKey(msg.getMid())) {
            logger.info("Propagating {} to {}", mid, eager);
            triggerNotification(new DeliverNotification(msg.getMid(), from, msg.getContent()));
            received.put(mid, msg);
            stored.add(mid);
            if (stored.size() > space) {
                UUID toRemove = stored.poll();
                received.put(toRemove, null);
            }

            Long tid;
            if((tid = onGoingTimers.remove(mid)) != null) {
                cancelTimer(tid);
            }
            eagerPush(msg, msg.getRound() + 1, from);
            lazyPush(msg, msg.getRound() + 1, from);

            startSynchronization(from, true); //TODO: check both doing this

            if(lazy.remove(from)) {
                logger.trace("Removed {} from lazy {}", from, lazy);
                logger.debug("Removed {} from lazy", from);
            }

            optimize(msg, msg.getRound(), from);
        } else {
            if(eager.remove(from)) {
                logger.trace("Removed {} from eager {}", from, eager);
                logger.debug("Removed {} from eager", from);
            } if(lazy.add(from)) {
                logger.trace("Added {} to lazy {}", from, lazy);
                logger.debug("Added {} to lazy", from);
            }
            sendMessage(new PruneMessage(), from);
        }
    }



    private void uponReceivePrune(PruneMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if(eager.remove(from)) {
            logger.trace("Removed {} from eager {}", from, eager);
            logger.debug("Removed {} from eager", from);
        }

        if(lazy.add(from)) {
            logger.trace("Added {} to lazy {}", from, lazy);
            logger.debug("Added {} to lazy", from);
        }
    }

    private void uponReceiveGraft(GraftMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        logger.debug("Received {} from {}", msg, from);

        startSynchronization(from, false);

        if(lazy.remove(from)) {
            logger.trace("Removed {} from lazy {}", from, lazy);
            logger.debug("Removed {} from lazy", from);
        }

        //TODO este if se calhar não é preciso, porque a mensagem vai na sincronizacao
//        if(received.getOrDefault(mid, null) != null) {
//            sendMessage(received.get(mid), from);
//        }
    }

    private void uponReceiveIHave(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        logger.debug("Received {} from {}", msg, from);
        if(!received.containsKey(msg.getMid())) {
            if(!onGoingTimers.containsKey(mid)) {
                long tid = setupTimer(new IHaveTimeout(msg.getMid()), timeout1);
                onGoingTimers.put(mid, tid);
            }
            missing.computeIfAbsent(mid, v-> new LinkedList<>()).add(new MessageSource(from, msg.getRound()));
        }
    }

    private void uponVectorClockMessage(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        triggerNotification(new VectorClockNotification(msg.getSender(), msg.getVectorClock()));
    }

    private void uponSendVectorClockMessage(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        triggerNotification(new SendVectorClockNotification(msg.getSender()));
    }

    private void uponSyncOpsMessage(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        //TODO read msg ids and cancel timers
        triggerNotification(new SyncOpsNotification(from, msg.getOperations()));
    }

    private void uponAddPendingToEagerMessage(AddPendingToEagerMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        if(eager.add(currentPending)) {
            logger.info("Added {} to eager {}", currentPending, eager);
            logger.debug("Added {} to eager", currentPending);
        }

        currentPending = pending.poll();
        if(currentPending != null) {
            logger.info("{} is my currentPending", currentPending);
            sendNewPendingNeighbourNotification(currentPending);
        }
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        if(!received.containsKey(timeout.getMid())) {
            MessageSource msgSrc = missing.get(timeout.getMid()).poll();
            if (msgSrc != null) {
                long tid = setupTimer(timeout, timeout2);
                onGoingTimers.put(timeout.getMid(), tid);
                Host neighbour = msgSrc.peer;

                startSynchronization(neighbour, false);

                if (lazy.remove(neighbour)) {
                    logger.trace("Removed {} from lazy {}", neighbour, lazy);
                    logger.debug("Removed {} from lazy", neighbour);
                }

                sendMessage(new GraftMessage(timeout.getMid(), msgSrc.round), neighbour);
            }
        }
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        if (!channelReady)
            return;

        UUID mid = request.getMsgId();
        GossipMessage msg = new GossipMessage(mid, request.getSender(), 0, request.getMsg());
        logger.info("Propagating my {} to {}", mid, eager);
        eagerPush(msg, 0, myself);
        lazyPush(msg, 0, myself);
        triggerNotification(new DeliverNotification(mid, request.getSender(), msg.getContent()));
        received.put(mid, msg);
        stored.add(mid);
        if (stored.size() > space) {
            UUID toRemove = stored.poll();
            received.put(toRemove, null);
        }
    }

    private void uponMyVectorClockRequest(MyVectorClockRequest request, short sourceProto) {
        if (!channelReady)
            return;

        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(),
                request.getVectorClock());
        sendMessage(msg, request.getTo());
        logger.info("Sent {} to {}", msg, request.getTo());
    }

    private void uponSendVectorClockRequest(SendVectorClockRequest request, short sourceProto) {
        if (!channelReady)
            return;

        SendVectorClockMessage msg = new SendVectorClockMessage(request.getMsgId(), request.getSender());
        sendMessage(msg, request.getTo());
        logger.info("Sent {} to {}", msg, request.getTo());
    }

    private void uponSyncOpsRequest(SyncOpsRequest request, short sourceProto) {
        if (!channelReady)
            return;

        //TODO SyncOpsMessage needs msg ids
        SyncOpsMessage msg = new SyncOpsMessage(request.getOperations());
        sendMessage(msg, request.getTo());
        logger.info("Sent {} to {}", msg, request.getTo());
    }

    private void uponAddPendingToEagerRequest(AddPendingToEagerRequest request, short sourceProto) {
        if (!channelReady)
            return;

        AddPendingToEagerMessage msg = new AddPendingToEagerMessage(UUID.randomUUID(), myself);
        sendMessage(msg, request.getSender());
        logger.info("Sent {} to {}", msg, request.getSender());
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        startSynchronization(notification.getNeighbour(), false);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Neighbour In Down {}", neighbour);
        if(eager.remove(neighbour)) {
            logger.debug("Removed {} from eager {}", neighbour, eager);
            logger.debug("Removed {} from eager", neighbour);
        }

        if(lazy.remove(neighbour)) {
            logger.trace("Removed {} from lazy {}", neighbour, lazy);
            logger.debug("Removed {} from lazy", neighbour);
        }

        if(pending.remove(neighbour)) {
            logger.trace("Removed {} from pending {}", neighbour, lazy);
            logger.debug("Removed {} from pending", neighbour);
        }

        MessageSource msgSrc  = new MessageSource(neighbour, 0);
        for(Queue<MessageSource> iHaves : missing.values()) {
            iHaves.remove(msgSrc);
        }
        closeConnection(neighbour);
    }


    /*--------------------------------- Auxiliary Methods ---------------------------------------- */

    private void startSynchronization(Host neighbour, boolean fromGossip) {
        if(!neighbour.equals(currentPending) && !eager.contains(neighbour) && !pending.contains(neighbour)) {
            if(fromGossip)
                logger.info("Sync with {} was from gossip", neighbour);
            if(currentPending == null) {
                currentPending = neighbour;
                logger.info("{} is my currentPending", neighbour);
                sendNewPendingNeighbourNotification(currentPending);
            } else {
                pending.add(neighbour);
                logger.info("Added {} to pending {}", neighbour, pending);
            }
        }
    }

    private void sendNewPendingNeighbourNotification(Host neighbour) {
        NewPendingNeighbourNotification notification = new NewPendingNeighbourNotification(neighbour);
        triggerNotification(notification);
        logger.info("Sent {} to kernel", notification);
    }

    private void eagerPush(GossipMessage msg, int round, Host from) {
        for(Host peer : eager) {
            if(!peer.equals(from)) {
                sendMessage(msg.setRound(round), peer);
                logger.info("Sent {} received from {} to {}", msg, from, peer);
            }
        }
    }

    private void lazyPush(GossipMessage msg, int round, Host from) {
        for(Host peer : lazy) {
            if(!peer.equals(from)) {
                lazyQueue.add(new AddressedIHaveMessage(new IHaveMessage(msg.getMid(), round), peer));
            }
        }
        dispatch();
    }

    private void dispatch() {
        Set<AddressedIHaveMessage> announcements = policy.apply(lazyQueue);
        for(AddressedIHaveMessage msg : announcements) {
            sendMessage(msg.msg, msg.to);
        }
        lazyQueue.removeAll(announcements);
    }

    private void optimize(GossipMessage msg, int round, Host from) {

    }


    /*--------------------------------- Initialization ---------------------------------------- */

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int channelId = notification.getChannelId();
        registerSharedChannel(channelId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);
        registerMessageSerializer(channelId, AddPendingToEagerMessage.MSG_ID, AddPendingToEagerMessage.serializer);



        try {

            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip);
            registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePrune);
            registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraft);
            registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHave);

            registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponSendVectorClockMessage);
            registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponVectorClockMessage);
            registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponSyncOpsMessage);
            registerMessageHandler(channelId, AddPendingToEagerMessage.MSG_ID, this::uponAddPendingToEagerMessage);

        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        this.channelReady = true;
    }
}
