package org.myconcept;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    private static final String TASK_QUEUE_NAME = "test";

    public static void main(String[] argv) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();

        ExecutorService executor = Executors.newFixedThreadPool(5);

        Runnable task = () -> {
            try {
                final Channel channel = connection.createChannel();

                channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
                System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

                channel.basicQos(1);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");

                    System.out.println(" [x] Received '" + message + "'");
                    boolean return_value = false;
                    try {
                        return_value = doWork(message);
                    } finally {
                        System.out.println(" [" + message + "[ Done");
                        if(return_value)
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        else if(delivery.getEnvelope().isRedeliver())
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);  // Do not requeue if redelivered
                        else {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);   // Requeue on one failure
                        }
                    }

                };
                channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> {
                });
            }
            catch (Exception e) {

            }
            finally {

            }
        };

        for(int i=0; i<5; i++) {
            executor.submit(task);
        }
    }

    private static boolean doWork(String task) {

        // Save to DB
        // AND
        // Send To Zelle

        /*
        for (char ch : task.toCharArray()) {
            if (ch == '$') {
                try {
                    System.out.println("Working on " + task);
                    Thread.sleep(2000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    return true;
                }
            }
            if(ch == '#') {
                try {
                    System.out.println("Working on " + task);
                    Thread.sleep(100);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    return true;
                }
            }
        }
         */

        return task.chars().mapToObj(c->(char)c)
                .anyMatch(c-> c == '$');

    }
}

