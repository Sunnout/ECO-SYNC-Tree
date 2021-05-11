package protocols.broadcast.plumtree;

import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.membership.common.notifications.ChannelCreated;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.plumtree.messages.GossipMessage;
import protocols.broadcast.plumtree.messages.GraftMessage;
import protocols.broadcast.plumtree.messages.IHaveMessage;
import protocols.broadcast.plumtree.messages.PruneMessage;
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


        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }


    /*--------------------------------- Messages ---------------------------------------- */
    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received {} from {}", msg, from);
        UUID mid = msg.getMid();
        if(!received.containsKey(msg.getMid())) {
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
            eagerPush(msg, msg.getRound() +1, from);
            lazyPush(msg, msg.getRound() +1, from);

            if(eager.add(from)) {
                logger.trace("Added {} to eager {}", from, eager);
                logger.debug("Added {} to eager", from);
            }
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

    private void eagerPush(GossipMessage msg, int round, Host from) {
        for(Host peer : eager) {
            if(!peer.equals(from)) {
                sendMessage(msg.setRound(round), peer);
                logger.trace("Sent {} to {}", msg, peer);
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
        if(eager.add(from)) {
            logger.trace("Added {} to eager {}", from, eager);
            logger.debug("Added {} to eager", from);
        } if(lazy.remove(from)) {
            logger.trace("Removed {} from lazy {}", from, lazy);
            logger.debug("Removed {} from lazy", from);
        }

        if(received.getOrDefault(mid, null) != null) {
            sendMessage(received.get(mid), from);
        }
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

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        if(!received.containsKey(timeout.getMid())) {
            MessageSource msgSrc = missing.get(timeout.getMid()).poll();
            if (msgSrc != null) {
                long tid = setupTimer(timeout, timeout2);
                onGoingTimers.put(timeout.getMid(), tid);

                if (eager.add(msgSrc.peer)) {
                    logger.trace("Added {} to eager {}", msgSrc.peer, eager);
                    logger.debug("Added {} to eager", msgSrc.peer);
                }
                if (lazy.remove(msgSrc.peer)) {
                    logger.trace("Removed {} from lazy {}", msgSrc.peer, lazy);
                    logger.debug("Removed {} from lazy", msgSrc.peer);
                }

                sendMessage(new GraftMessage(timeout.getMid(), msgSrc.round), msgSrc.peer);
            }
        }
    }


    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        if (!channelReady)
            return;
        UUID mid = request.getMsgId();
        GossipMessage msg = new GossipMessage(mid, request.getSender(), 0, request.getMsg());
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

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        if(eager.add(notification.getNeighbour())) {
            logger.trace("Added {} to eager {}", notification.getNeighbour(), eager);
            logger.debug("Added {} to eager", notification.getNeighbour());
        } else
            logger.trace("Received neigh up but {} is already in eager {}", notification.getNeighbour(), eager);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        if(eager.remove(notification.getNeighbour())) {
            logger.trace("Removed {} from eager {}", notification.getNeighbour(), eager);
            logger.debug("Removed {} from eager", notification.getNeighbour());
        }
        if(lazy.remove(notification.getNeighbour())) {
            logger.trace("Removed {} from lazy {}", notification.getNeighbour(), lazy);
            logger.debug("Removed {} from lazy", notification.getNeighbour());
        }

        MessageSource msgSrc  = new MessageSource(notification.getNeighbour(), 0);
        for(Queue<MessageSource> iHaves : missing.values()) {
            iHaves.remove(msgSrc);
        }
        closeConnection(notification.getNeighbour());
    }

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

        /*---------------------- Register Message Handlers -------------------------- */

        try {

            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip);
            registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePrune);
            registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraft);
            registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHave);

        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        this.channelReady = true;
    }
}
