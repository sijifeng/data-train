package com.season.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.api.SecureRpcClientFactory;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClient;
import java.nio.charset.Charset;
import java.util.Properties;
/**
 * Created by jiyc on 2017/6/11.
 */
public class ThriftApp {
    public static void main(String[] args) {
        MySecureRpcClientFacade client = new MySecureRpcClientFacade();
        // Initialize client with the remote Flume agent's host, port
        Properties props = new Properties();
        props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, "thrift");
        props.setProperty("hosts", "h1");
        props.setProperty("hosts.h1", "client.example.org"+":"+ String.valueOf(41414));

        // Initialize client with the kerberos authentication related properties
        props.setProperty("kerberos", "true");
        props.setProperty("client-principal", "flumeclient/client.example.org@EXAMPLE.ORG");
        props.setProperty("client-keytab", "/tmp/flumeclient.keytab");
        props.setProperty("server-principal", "flume/server.example.org@EXAMPLE.ORG");
        client.init(props);

        // Send 10 events to the remote Flume agent. That agent should be
        // configured to listen with an AvroSource.
        String sampleData = "Hello Flume!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }

        client.cleanUp();
    }
}




class MySecureRpcClientFacade {
    private RpcClient client;
    private Properties properties;

    public void init(Properties properties) {
        // Setup the RPC connection
        this.properties = properties;
        // Create the ThriftSecureRpcClient instance by using SecureRpcClientFactory
        this.client = SecureRpcClientFactory.getThriftInstance(properties);
    }

    public void sendDataToFlume(String data) {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            client = SecureRpcClientFactory.getThriftInstance(properties);
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}