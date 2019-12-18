/*
Schema:
  store      TEXT
  item       TEXT
  quantity   INT
  order_time BIGINT

CREATE TABLE simpleapp.data(store TEXT, item TEXT, quantity INT, order_time BIGINT, PRIMARY KEY ((store), order_time));
 */

package hessian.example;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;

public class SimpleApp {
    private String username = null;
    private String password = null;
    private String host = null;
    private String datacenter = null;
    private String creds = null;
    private int iterations = 100;
    private int sleepms = 100;

    private CqlSession session = null;
    private PreparedStatement preparedStatement = null;
    private String keyspace = "simpleapp";
    private String table = "data";

    public static void main(String args[]) throws InterruptedException {
        SimpleApp simpleApp = new SimpleApp();
        boolean success = simpleApp.run(args);
        if (success) {
            System.exit(0);
        } else {
            usage();
            System.exit(-1);
        }
    }

    public static void usage() {
        StringBuilder sb = new StringBuilder("Usage: \n");
        sb.append("  -u <username>\n");
        sb.append("  -p <password>\n");
        sb.append("  -c <creds.zip path>\n");
        sb.append("  -h <hostname/ip>\n");
        sb.append("  -d <datacenter>\n");
        //sb.append("  -i <num_iterations>\n");
        //sb.append("  -s <sleep ms>\n");
        System.err.println(sb.toString());
    }

    public boolean error(String msg) {
        System.err.println(msg);
        return false;
    }

    public boolean run(String args[]) throws InterruptedException {
        if (!parseArgs(args))
            return false;
        if (!setup())
            return false;

        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < iterations; i++) {
            String store = "Store " + random.nextInt(1000);
            String item = "Item " + (random.nextInt(1000) + 2000);
            Integer quantity = random.nextInt(100);
            Long order_time = System.currentTimeMillis();
            BoundStatement boundStatement = preparedStatement.bind(store, item, quantity, order_time);
            session.execute(boundStatement);
            System.out.print("\rInserted " + (i + 1) + " records");

            TimeUnit.MILLISECONDS.sleep(sleepms);
        }
        System.out.print("\nDone\n");

        return true;
    }

    public boolean parseArgs(String args[]) {
        if (0 != args.length % 2) {
            return error("Must supply even number of arguments");
        }
        Map<String, String> amap = new HashMap<String,String>();
        for (int i = 0; i < args.length; i+=2)
            amap.put(args[i], args[i+1]);

        String tkey;
        if (null != (tkey = amap.remove("-u")))  username = tkey;
        if (null != (tkey = amap.remove("-p")))  password = tkey;
        if (null != (tkey = amap.remove("-c")))  creds = tkey;
        if (null != (tkey = amap.remove("-h")))  host = tkey;
        if (null != (tkey = amap.remove("-d")))  datacenter = tkey;
        if (null != (tkey = amap.remove("-i")))  iterations = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-s")))  sleepms = Integer.parseInt(tkey);

        // VALIDATE
        if (null != creds) {
            File tfile = new File(creds);
            if (!tfile.isFile())
                return error("Creds must be a file");
            if (null == username)
                return error("If you supply the creds file you must supply a username");
            if (null == password)
                return error("If you supply the creds file you must supply a password");
        }
        else {
            if (null == host) {
                return error("If you do not supply the creds file, you must specify a hostname/IP");
            }
            if (null == datacenter) {
                return error("If you specify the host, you must specify a datacenter");
            }
            if ((null == username) && (null != password))
                return error("If you supply the password you must supply the username");
        }

        return true;
    }

    public boolean setup() {
        CqlSessionBuilder builder = CqlSession.builder();
        if (null != creds) {
            builder = builder.withCloudSecureConnectBundle(Paths.get(creds))
                    .withAuthCredentials(username, password)
                    .withKeyspace(keyspace);
        }
        else {
            builder = builder.addContactPoint(InetSocketAddress.createUnresolved(host, 9042))
                    .withLocalDatacenter(datacenter);
            if ((null != username) && (null != password)) {
                builder = builder.withAuthCredentials(username, password);
            }
        }
        session = builder.build();

        preparedStatement = session.prepare("INSERT INTO " + keyspace + "." + table + "(store, item, quantity, order_time) VALUES (?, ?, ?, ?)");

        return true;
    }
}
