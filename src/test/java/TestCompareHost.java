
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;


public class TestCompareHost {

    private static final String IP_PREFIX = "10.10.0.";

    public static void main(String[] args) throws Exception {

        Host[] orderedHosts = new Host[200];
        for(int i = 10; i < 210; i++) {
            orderedHosts[i-10] = new Host(InetAddress.getByName(IP_PREFIX + i), 6000);
        }

        int n = orderedHosts.length;
        for (int i = 0; i < n-1; i++)
            for (int j = 0; j < n-i-1; j++)
                if (orderedHosts[j].compareTo(orderedHosts[j+1]) > 0) {
                    // swap arr[j+1] and arr[j]
                    Host temp = orderedHosts[j];
                    orderedHosts[j] = orderedHosts[j+1];
                    orderedHosts[j+1] = temp;
                }


        for (int i = 0; i < n; ++i) {
            System.out.print(orderedHosts[i] + " ");
            System.out.println();
        }
    }
}
