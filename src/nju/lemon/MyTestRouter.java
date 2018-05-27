package nju.lemon;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;

/**
 * A simple routing solution without test
 * TODO: NEED TEST
 */
public class MyTestRouter {
    private static final int WAIT_PERIOD = 15; //s
    private static final int WORKING_PERIOD = 5;
    private ArrayList<LinkedList> sendQueues;
    private LinkedList<MyPacket> broadCastPackets;

    private int selfId;
    private int maxId;
    private Timer timer;

    private DatagramSocket sendSocket;
    private InetAddress senderIp;
    private int senderPort;

    private final Object lockObj = new Object();

    public MyTestRouter(DatagramSocket socket, InetAddress senderIp, int port, int id, int maxId) {
        this.sendSocket = socket;
        this.senderIp = senderIp;
        this.senderPort = port;
        this.selfId = id;
        this.maxId = maxId;
        sendQueues = new ArrayList<>(3);
        sendQueues.add(new LinkedList<MyPacket>());
        sendQueues.add(new LinkedList<MyPacket>());
        sendQueues.add(new LinkedList<MyPacket>());
        broadCastPackets = new LinkedList<>();
    }

    /**
     * Enqueue packet
     * if packet does not exist, add it
     * if packet exists, remove the old one and add the new one
     * @param packet
     */
    private void enqueuePacket(MyPacket packet){
        int priority = packet.getPrioriry();
        synchronized (lockObj) {
            LinkedList<MyPacket> queue = sendQueues.get(priority);
            if (queue.contains(packet)) {
                int index = queue.indexOf(packet);
                MyPacket packetInQueue = queue.get(index);
                if(packet.isNewerThan(packetInQueue)) {
                    queue.remove(packetInQueue);
                    queue.add(index, packet);
                }
            } else {
                if (packet.getSrcId() != this.selfId) {
                    queue.add(packet);
                }
            }
        }
    }

    /**
     *
     * @param packet
     * @return true if packet is for selfId device
     *         false if packet is for others or time sync
     */
    public boolean handleReceivedPacket(MyPacket packet) {
        if(packet.getPrioriry() == MyPacket.PACKET_TIME_SYNC) {
            startTimeSync();
            return false;
        }
        if(packet.getDestId() != 0) {
            if (packet.getDestId() == this.selfId) return true;
            enqueuePacket(packet);
            return false;
        } else {
            //broadcast
            if(broadCastPackets.contains(packet)) return false;
            broadCastPackets.add(packet);
            enqueuePacket(packet);
            if(broadCastPackets.size() > 16) {
                broadCastPackets.remove(0);
            }
            return true;
        }
    }


    /**
     * wrap the private method
     * @param packet
     */
    public void pendingPacket(MyPacket packet) {
        enqueuePacket(packet);
    }

    /**
     * send task, send packet from the queue with a higher priority
     */
    TimerTask sendTask = new TimerTask() {
        @Override
        public void run() {
            synchronized (lockObj) {
                if (!sendQueues.get(MyPacket.PACKET_PRIORITY_HIGH).isEmpty()) {
                    if (sendPacket((MyPacket) sendQueues.get(MyPacket.PACKET_PRIORITY_HIGH).get(0))) {
                        sendQueues.get(MyPacket.PACKET_PRIORITY_HIGH).remove(0);
                    }
                } else if (!sendQueues.get(MyPacket.PACKET_PRIORITY_NORMAL).isEmpty()) {
                    if (sendPacket((MyPacket) sendQueues.get(MyPacket.PACKET_PRIORITY_NORMAL).get(0))) {
                        sendQueues.get(MyPacket.PACKET_PRIORITY_NORMAL).remove(0);
                    }
                } else if (!sendQueues.get(MyPacket.PACKET_PRIORITY_LOW).isEmpty()) {
                    if (sendPacket((MyPacket) sendQueues.get(MyPacket.PACKET_PRIORITY_LOW).get(0))) {
                        sendQueues.get(MyPacket.PACKET_PRIORITY_LOW).remove(0);
                    }
                } else {
                    System.out.println("Send queue of ID" + selfId + " is empty");
                }
            }
        }
    };

    /**
     * use the socket send the packet
     * @param packet
     * @return
     */
    private boolean sendPacket(MyPacket packet) {
        try {
            DatagramPacket datagramPacket = new DatagramPacket(packet.getBytes(), packet.getLength(), senderIp, senderPort);
            sendSocket.send(datagramPacket);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * raise the time sync command
     * @param packet
     */
    public void startTimeSync(MyPacket packet) {
        if(sendPacket(packet)){
            startTimeSync();
        }
    }


    /**
     * handle the time sync command
     */
    private void startTimeSync() {
        timer = new Timer();
        long delay = 1000 * (WAIT_PERIOD + (this.selfId - 1) * WORKING_PERIOD);
        long period = WORKING_PERIOD * maxId * 1000;
        timer.schedule(sendTask, delay, period);
    }

    void stop() {
        if(timer != null)
            timer.cancel();
    }

}
