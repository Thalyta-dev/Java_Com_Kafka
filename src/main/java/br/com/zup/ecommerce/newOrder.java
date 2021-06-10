package br.com.zup.ecommerce;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class newOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String,String>(properties());
        var key = UUID.randomUUID().toString();

        var value =key + "22,54,97";

        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER",key,value);
        Callback callback = (recordMetadata, e) ->{
            if(e != null){

                e.printStackTrace();
                return;
            }

            System.out.println("Sucesso enviado " + recordMetadata.topic() + " Particao " + recordMetadata.partition() + " offiset " + recordMetadata.offset() );
        };

        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, value);

        var email = "Olá, sou o novo email";
        producer.send(record,callback).get();
        producer.send(emailRecord,callback).get();

    }


    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;

    }
}
