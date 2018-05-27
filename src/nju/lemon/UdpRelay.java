package nju.lemon;

import java.io.IOException;
import java.net.*;

public class UdpRelay {
    private static final String DEFAULT_IP = "127.0.0.1";    //Use localhost only for now
    private static final int RECEIVE_BUFFER_LENGTH = 2048;

    private DatagramSocket sendSocket;
    private DatagramSocket receiveSocket;
    private InetAddress sendIp;
    private InetAddress receiveIp;
    private PacketReceivedListener receivedListener;
    private boolean isRunning = false;
    private int sendPort, receivePort;
    private Runnable receiveWorker;
    private Thread receiveThread;

    private int selfId;
    private int maxId;

    private MyTestRouter router;

    public UdpRelay(int sendPort, int receivePort) {
        this(sendPort, receivePort, null);
    }

    public UdpRelay(int sendPort, int receivePort, PacketReceivedListener listener) {
        this.sendPort = sendPort;
        this.receivePort = receivePort;
        receivedListener = listener;
        try {
            sendIp = InetAddress.getByName(DEFAULT_IP);
            receiveIp = InetAddress.getByName(DEFAULT_IP);
        } catch (UnknownHostException e) {
            //This never happens
            e.printStackTrace();
        }
    }

    private void init() {
        try {
            sendSocket = new DatagramSocket();
            receiveSocket = new DatagramSocket(receivePort, receiveIp);
        } catch (SocketException e) {
            e.printStackTrace();
            System.exit(1);
        }
        receiveWorker = new Runnable() {
            @Override
            public void run() {
                while (isRunning) {
                    try {
                        byte[] receiveBuffer = new byte[RECEIVE_BUFFER_LENGTH];
                        DatagramPacket packet = new DatagramPacket(receiveBuffer, RECEIVE_BUFFER_LENGTH);
                        receiveSocket.receive(packet);
                        String receivedMessage = new String(packet.getData(), packet.getOffset(), packet.getLength());
                        //Send up to routing;
                        //waiting for result;
                        //upload
                        MyPacket receivedPacket = MyPacket.fromBytes(receivedMessage.getBytes());
                        boolean upload = router.handleReceivedPacket(receivedPacket);
                        if(upload) {
                            if (receivedListener == null) {
                                System.out.println("Packet received: " + receivedMessage + ", but receive listener is null.");
                            } else {
                                receivedListener.onReceive(receivedMessage);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        isRunning = true;

        router = new MyTestRouter(sendSocket, sendIp, sendPort, selfId, maxId);
    }

    public UdpRelay setMaxId(int maxId) {
        this.maxId = maxId;
        return this;
    }

    public UdpRelay setSelfId(int selfId) {
        this.selfId = selfId;
        return this;
    }

    public UdpRelay setSendIp(String ip) {
        try {
            this.sendIp = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            System.err.println("Unknown host name(send ip), use default ip 127.0.0.1");
        }
        return this;
    }

    public UdpRelay setReceiveIp(String ip) {
        try {
            this.receiveIp = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            System.err.println("Unknown host name(receive ip), use default ip 127.0.0.1");
        }
        return this;
    }

    public void send(String msg) {
        if (sendSocket == null) return;
        try {
            DatagramPacket packet = new DatagramPacket(msg.getBytes(), msg.length(), sendIp, sendPort);
            sendSocket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void send(MyPacket packet) {
        if(sendSocket == null) return;
        router.pendingPacket(packet);
    }

    public void resetPacketReceiveListener(PacketReceivedListener listener) {
        stop();
        this.receivedListener = listener;
        start();
    }

    public void start() {
        if(this.selfId == 0 || this.maxId == 0) {
            System.out.println("Please set id first.");
        }
        init();
        if (receiveSocket == null) return;
        receiveThread = new Thread(receiveWorker);
        receiveThread.start();
    }

    public void stop() {
        try {
            isRunning = false;
            receiveThread.interrupt();
            sendSocket.close();
            receiveSocket.close();
            if(router != null) {
                router.stop();
            }
        } catch (Exception e) {
            //Now do Nothing
            e.printStackTrace();
        } finally {
            isRunning = false;
            receiveThread = null;
            sendSocket = null;
            receiveSocket = null;
        }
    }

    public interface PacketReceivedListener {
        void onReceive(String msg);
    }

}
