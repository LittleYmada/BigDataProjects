import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class FindNameServer {
    public static void main(String [] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(12345);

        builder.setProtocol(FindName.class);
        builder.setInstance(new FindNameImpl());

        RPC.Server server = builder.build();
        server.start();
    }
}
