package com.example.jmslistener;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Hashtable;

public class JmsListener {

    public static void main(String[] args) throws Exception {
        // Konfigurasi JNDI ke WebLogic
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
        env.put(Context.PROVIDER_URL, "t3://172.18.1.93:7001");
        env.put(Context.SECURITY_PRINCIPAL, "weblogic");
        env.put(Context.SECURITY_CREDENTIALS, "@Sadapaingan1");

        Context ctx = new InitialContext(env);

        // Lookup queue dan connection factory
        QueueConnectionFactory factory = (QueueConnectionFactory) ctx.lookup("jms/TestConnectionFactory");
        Queue listenQueue = (Queue) ctx.lookup("jms/SubmitServiceOrderPOCQueue");
        Queue sendQueue = (Queue) ctx.lookup("jms/ReceiveServiceOrderPOCQueue");

        QueueConnection connection = factory.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        // Listener
        QueueReceiver receiver = session.createReceiver(listenQueue);
        receiver.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    String body = ((TextMessage) message).getText();
                    
                    System.out.println("\n==============================================");
                    System.out.println("[INFO] Received JMS Message:");
                    System.out.println(body);
                    try {
                        // Parse JSON
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode rootNode = mapper.readTree(body);

                        // Ambil nilai externalId
                        String externalId = rootNode
                                .path("broadbandOrder")
                                .path("Header")
                                .path("externalId")
                                .asText();

                        System.out.println("[INFO] Extracted externalId: " + externalId);
                        System.out.println("[INFO] Starting to send provisioning status updates...");

                        // Kirim 3 message ke queue lain
                        String[] statuses = { "PROV_START", "PROV_ISSUED", "PROV_COMPLETED" };

                        for (String status : statuses) {
                            try {
                                TextMessage outMsg = session.createTextMessage();
                                outMsg.setJMSCorrelationID(message.getJMSCorrelationID());
                                outMsg.setText(
                                        "{\"dataWO\":" + body + ", \"status\":\"" + status + "\"}");

                                QueueSender sender = session.createSender(sendQueue);
                                sender.send(outMsg);

                                System.out.println("[SEND] Sent status: " + status + " | externalId: " + externalId);
                                System.out.println("[WAIT] Sleeping 15 seconds before next message...");
                                Thread.sleep(15000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                System.err.println("[ERROR] Sleep interrupted: " + ie.getMessage());
                            }
                        }

                        System.out.println("[INFO] All provisioning messages sent for externalId: " + externalId);
                    } catch (Exception e) {
                        System.err.println("[ERROR] Failed to parse message or send: " + e.getMessage());
                        e.printStackTrace();
                    }

                    System.out.println("==============================================\n");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        connection.start();
        System.out.println("JMS Listener started. Waiting for messages...");

        // Tambahkan ini agar aplikasi tetap hidup
        synchronized (JmsListener.class) {
            JmsListener.class.wait();
        }

    }
}
