package org.myconcept;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

    private static final String TASK_QUEUE_NAME = "test";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);

            String message = String.join(" ", argv);

            for(int i =0; i<20; i++) {
                String toSendMessage, theMessage;
                theMessage = message + String.valueOf(i);

                if(i%2 == 0)
                    toSendMessage = "$" + theMessage;
                else
                    toSendMessage = "#" + theMessage;

                channel.basicPublish("", TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        toSendMessage.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + toSendMessage + "'");
            }
        }
    }

}
