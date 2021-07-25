import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class FindNameClient {
    public static void main (String[] args) {
        try {
            FindName proxy = RPC.getProxy(FindName.class,1L, new InetSocketAddress("127.0.0.1", 12345), new Configuration());
            String name = proxy.findName(20210735010137L);
            System.out.println("Your name is " + name);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
