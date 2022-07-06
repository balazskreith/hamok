package io.github.balazskreith.hamok.transports;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Enumeration;

public final class DefaultConfigs {

    public static final int DEFAULT_MULTICAST_PORT = 59382;
    public static final int DEFAULT_UNICAST_PORT = 59381;
    public static final int DATAGRAM_PACKET_HEADER_LENGTH = 1 + 8 + 4;
    public static final int DATAGRAM_PACKET_BUFFER_MAX_LENGTH = 1200;
    public static final int DEFAULT_SOCKET_RECEIVING_TIMEOUT = 500;

    private DefaultConfigs() {

    }

    static NetworkInterface chooseDefaultInterface() {
        Enumeration<NetworkInterface> nifs;

        try {
            nifs = NetworkInterface.getNetworkInterfaces();
        } catch (IOException ignore) {
            // unable to enumerate network interfaces
            return null;
        }

        NetworkInterface preferred = null;
        NetworkInterface dual = null;
        NetworkInterface nonLinkLocal = null;
        NetworkInterface ppp = null;
        NetworkInterface loopback = null;

        while (nifs.hasMoreElements()) {
            NetworkInterface ni = nifs.nextElement();
            try {
                if (!ni.isUp() || !ni.supportsMulticast())
                    continue;

                boolean ip4 = false, ip6 = false, isNonLinkLocal = false;
                PrivilegedAction<Enumeration<InetAddress>> pa = ni::getInetAddresses;
                @SuppressWarnings("removal")
                Enumeration<InetAddress> addrs = AccessController.doPrivileged(pa);
                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();
                    if (!addr.isAnyLocalAddress()) {
                        if (addr instanceof Inet4Address) {
                            ip4 = true;
                        } else if (addr instanceof Inet6Address) {
                            ip6 = true;
                        }
                        if (!addr.isLinkLocalAddress()) {
                            isNonLinkLocal = true;
                        }
                    }
                }

                boolean isLoopback = ni.isLoopback();
                boolean isPPP = ni.isPointToPoint();
                if (!isLoopback && !isPPP) {
                    // found an interface that is not the loopback or a
                    // point-to-point interface
                    if (preferred == null) {
                        preferred = ni;
                    }
                    if (ip4 && ip6) {
                        if (isNonLinkLocal) return ni;
                        if (dual == null) dual = ni;
                    }
                    if (nonLinkLocal == null) {
                        if (isNonLinkLocal) nonLinkLocal = ni;
                    }
                }
                if (ppp == null && isPPP)
                    ppp = ni;
                if (loopback == null && isLoopback)
                    loopback = ni;

            } catch (IOException skip) { }
        }

        if (dual != null) {
            return dual;
        } else if (nonLinkLocal != null) {
            return nonLinkLocal;
        } else if (preferred != null) {
            return preferred;
        } else {
            return (ppp != null) ? ppp : loopback;
        }
    }
}
