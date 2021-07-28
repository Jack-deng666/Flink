import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MyTest {

        public static void main(String[] args) throws InterruptedException, ExecutionException {
            Properties pro = new Properties();
            pro.put("bootstrap.servers", "10.0.0.40:9092");
            ListTopicsResult result = KafkaAdminClient.create(pro).listTopics();
            KafkaFuture<Set<String>> set = result.names();
            System.out.println(set.get());
        }

    }
