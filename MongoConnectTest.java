import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

/**
 * Standalone MongoDB connectivity test.
 * Tests SSL + Kerberos (GSSAPI) connection to MongoDB independently of Kafka Connect.
 *
 * Usage: See RUN_INSTRUCTIONS.txt
 */
public class MongoConnectTest {

    // ── EDIT THESE TWO VALUES ──────────────────────────────────────────────────
    static final String TRUSTSTORE_PATH   = "C:/Users/G01516271/kafka-connect-test/mongo-truststore.jks";
    static final String TRUSTSTORE_PASS   = "changeit";
    // ── Everything else is read from the context doc — change only if needed ──

    static final String CONNECTION_URI =
        "mongodb://G01516271@glodsr02z02401.intranet.barcapint.com:27181/" +
        "?authMechanism=GSSAPI&authSource=$external&tls=true";

    static final String DATABASE = "odsCorporate";

    public static void main(String[] args) {

        System.out.println("===========================================");
        System.out.println("  MongoDB SSL + Kerberos Connectivity Test");
        System.out.println("===========================================");
        System.out.println();

        // Step 1 — Print the truststore being used
        String truststore = System.getProperty("javax.net.ssl.trustStore");
        String tsFromArgs  = (args.length > 0) ? args[0] : null;
        String tsPass      = (args.length > 1) ? args[1] : TRUSTSTORE_PASS;

        if (tsFromArgs != null) {
            System.setProperty("javax.net.ssl.trustStore",         tsFromArgs);
            System.setProperty("javax.net.ssl.trustStorePassword", tsPass);
            System.out.println("[INFO] Using truststore from argument : " + tsFromArgs);
        } else {
            System.setProperty("javax.net.ssl.trustStore",         TRUSTSTORE_PATH);
            System.setProperty("javax.net.ssl.trustStorePassword", TRUSTSTORE_PASS);
            System.out.println("[INFO] Using truststore from hardcode : " + TRUSTSTORE_PATH);
        }

        // Step 2 — Check Kerberos JAAS config is set
        String jaas = System.getProperty("java.security.auth.login.config");
        if (jaas == null || jaas.isEmpty()) {
            System.out.println("[WARN] java.security.auth.login.config is NOT set.");
            System.out.println("       Make sure you run with:");
            System.out.println("       -Djava.security.auth.login.config=C:/Users/G01516271/kafka-connect-test/jaas.conf");
        } else {
            System.out.println("[INFO] JAAS config found : " + jaas);
        }

        System.out.println("[INFO] Connecting to : " + CONNECTION_URI);
        System.out.println();

        // Step 3 — Attempt connection
        try (MongoClient client = MongoClients.create(CONNECTION_URI)) {

            System.out.println("[INFO] MongoClient created. Sending ping...");

            Document result = client
                .getDatabase(DATABASE)
                .runCommand(new Document("ping", 1));

            System.out.println();
            System.out.println("===========================================");
            System.out.println("  SUCCESS — MongoDB responded to ping!");
            System.out.println("  Response: " + result.toJson());
            System.out.println("===========================================");

        } catch (Exception e) {
            System.out.println();
            System.out.println("===========================================");
            System.out.println("  FAILED — Exception during connection:");
            System.out.println("===========================================");
            System.out.println("  Type   : " + e.getClass().getName());
            System.out.println("  Message: " + e.getMessage());
            System.out.println();
            System.out.println("  Full stack trace:");
            e.printStackTrace(System.out);
            System.out.println();
            System.out.println("===========================================");
            System.out.println("  Likely causes:");
            System.out.println("  SSLHandshakeException  → truststore problem (wrong certs or wrong file)");
            System.out.println("  GSSException           → Kerberos/JAAS problem (check klist & jaas.conf)");
            System.out.println("  MongoTimeoutException  → network/host problem (check hostname & port)");
            System.out.println("  MongoSecurityException → auth rejected (check principal in keytab)");
            System.out.println("===========================================");
        }
    }
}
