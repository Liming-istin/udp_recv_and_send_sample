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
        }
        receiveWorker = () -> {
            while (isRunning) {
                try {
                    byte[] receiveBuffer = new byte[RECEIVE_BUFFER_LENGTH];
                    DatagramPacket packet = new DatagramPacket(receiveBuffer, RECEIVE_BUFFER_LENGTH);
                    receiveSocket.receive(packet);
                    String receivedMessage = new String(packet.getData(), packet.getOffset(), packet.getLength());
                    if (receivedListener == null) {
                        System.out.println("Packet received: " + receivedMessage + ", but receive listener is null.");
                    } else {
                        receivedListener.onReceive(receivedMessage);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        isRunning = true;
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

    public void resetPacketReceiveListener(PacketReceivedListener listener) {
        stop();
        this.receivedListener = listener;
        start();
    }

    public void start() {
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

    interface PacketReceivedListener {
        void onReceive(String msg);
    }

}
