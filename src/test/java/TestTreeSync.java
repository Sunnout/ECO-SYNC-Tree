import protocols.broadcast.synctree.utils.OutgoingSync;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

public class TestTreeSync {

    public static void main(String[] args) throws Exception {
        OutgoingSync os1 = new OutgoingSync(new Host(InetAddress.getByName("10.10.0.10"),6000));
        OutgoingSync os2 = new OutgoingSync(new Host(InetAddress.getByName("10.10.0.10"),6000));

        System.out.println(os1.equals(os2));

        Set<OutgoingSync> set = new HashSet<>();
        set.add(os1);
        System.out.println(set.contains(os2));
        set.remove(os1);
        System.out.println(set.contains(os2));
        System.out.println(set.contains(os2));
    }
}
