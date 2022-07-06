package io.github.balazskreith.hamok.emulators.purgatory;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastChat {
    static final String DEFAULT_GROUP = "239.1.2.3";
    static final int DEFAULT_PORT = 1234;
    static final int DEFAULT_TTL = 1;

    InetAddress group;
    int port;
    int ttl;

    public MulticastChat (InetAddress group, int port, int ttl) {
        this.group = group;
        this.port = port;
        this.ttl = ttl;
        initAWT ();
    }
    Frame frame;
    TextArea area;
    TextField field;
    public void initAWT () {
        frame = new Frame
                ("MulticastChat [" + group.getHostAddress () + ":" + port + "]");
        frame.addWindowListener (new WindowAdapter () {
            public void windowOpened (WindowEvent event) {
                field.requestFocus ();
            }
            public void windowClosing (WindowEvent event) {
                try {
                    stop ();
                } catch (IOException ignored) {
                }
            }
        });
        area = new TextArea ("", 12, 24, TextArea.SCROLLBARS_VERTICAL_ONLY);
        area.setEditable (false);
        frame.add (area, "Center");
        field = new TextField ("");
        field.addActionListener (new ActionListener () {
            public void actionPerformed (ActionEvent event) {
                netSend (event.getActionCommand ());
                field.selectAll ();
            }
        });
        frame.add (field, "South");
        frame.pack ();
    }

    public void start () throws IOException {
        netStart ();
        frame.setVisible (true);
    }
    public void stop () throws IOException {
        netStop ();
        frame.setVisible (false);
    }
    MulticastSocket socket;
    BufferedReader in;
    OutputStreamWriter out;
    Thread listener;
    void netStart () throws IOException {
        socket = new MulticastSocket (port);
        socket.setTimeToLive (ttl);
        socket.joinGroup (group);
        in = new BufferedReader
                (new InputStreamReader (new DatagramInputStream (socket), "UTF8"));
        out =
                new OutputStreamWriter (new DatagramOutputStream (socket, group, port), "UTF8");
        listener = new Thread () {
            public void run () {
                netReceive ();
            }
        };
        listener.start ();
    }
    void netStop () throws IOException {
        listener.interrupt ();
        listener = null;
        socket.leaveGroup (group);
        socket.close ();
    }
    void netSend (String message) {
        try {
            out.write (message + "\n");
            out.flush ();
        } catch (IOException ex) {
            ex.printStackTrace ();
        }
    }
    void netReceive () {
        try {
            Thread myself = Thread.currentThread ();
            while (listener == myself) {
                String message = in.readLine ();
                area.append (message + "\n");
            }
        } catch (IOException ex) {
            area.append ("- listener stopped");
            ex.printStackTrace ();
        }
    }
//    public static void main (String[] args) throws IOException {
//        if ((args.length > 3) || ((args.length > 0) && args[1].endsWith ("help"))) {
//            System.out.println
//                    ("Syntax: MulticastChat [<group:" + DEFAULT_GROUP +
//                            "> [<port:" + DEFAULT_PORT + ">] [<ttl:" + DEFAULT_TTL + ">]]");
//            System.exit (0);
//        }
//        String groupStr = (args.length > 0) ? args[0] : DEFAULT_GROUP;
//        InetAddress group = InetAddress.getByName (groupStr);
//        int port = (args.length > 1) ? Integer.parseInt (args[1]) : DEFAULT_PORT;
//        int ttl = (args.length > 2) ? Integer.parseInt (args[2]) : DEFAULT_TTL;
//        MulticastChat chat = new MulticastChat (group, port, ttl);
//        chat.start ();
//    }

    public class DatagramInputStream extends InputStream {
        byte[] buffer;
        int index, count;
        DatagramSocket socket;
        DatagramPacket packet;

        public DatagramInputStream (DatagramSocket socket) {
            this.socket = socket;
            buffer = new byte[65508];
            packet = new DatagramPacket (buffer, 0);
        }
        public synchronized int read () throws IOException {
            while (index >= count)
                receive ();
            return (int) buffer[index ++];
        }
        public synchronized int read (byte[] data, int offset, int length)
                throws IOException {
            if (length <= 0)
                return 0;
            while (index >= count)
                receive ();
            if (count - index < length)
                length = count - index;
            System.arraycopy (buffer, index, data, offset, length);
            index += length;
            return length;
        }
        public synchronized long skip (long amount) throws IOException {
            if (amount <= 0)
                return 0;
            while (index >= count)
                receive ();
            if (count - index < amount)
                amount = count - index;
            index += amount;
            return amount;
        }
        public synchronized int available () throws IOException {
            return count - index;
        }
        void receive () throws IOException {
            packet.setLength (buffer.length);
            socket.receive (packet);
            index = 0;
            count = packet.getLength ();
        }
    }

    public class DatagramOutputStream extends ByteArrayOutputStream {
        DatagramSocket socket;
        DatagramPacket packet;

        public DatagramOutputStream
                (DatagramSocket socket, InetAddress address, int port) {
            this (socket, address, port, 512);
        }
        public DatagramOutputStream
                (DatagramSocket socket, InetAddress address, int port, int initialSize) {
            super (initialSize);
            this.socket = socket;
            packet = new DatagramPacket (buf, 0, address, port);
        }
        public synchronized void flush () throws IOException {
            if (count >= 65508)
                throw new IOException ("Packet overflow (" + count + ") bytes");
            packet.setData (buf, 0, count);
            socket.send (packet);
            reset ();
        }
    }
}
