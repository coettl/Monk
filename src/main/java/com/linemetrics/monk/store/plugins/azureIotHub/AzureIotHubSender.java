package com.linemetrics.monk.store.plugins.azureIotHub;

import com.google.gson.JsonObject;
import com.microsoft.azure.sdk.iot.device.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class AzureIotHubSender {
    private static IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;

    private String _connectionString;
    private DeviceClient client;

    public AzureIotHubSender(String connectionString) {
        this._connectionString = connectionString;
        this.createDeviceClient();
    }

    public void sendData(List<JsonObject> objects) {
        try {
            client.open();

            int concurrentSend = 10;

            List<JsonObject> objectSlice = objects.subList(0, concurrentSend);
            Object[] lockObjects = new Object[concurrentSend];
            EventCallback[] callbacks = new EventCallback[concurrentSend];

            for (int i = 0; i < concurrentSend; i++) {
                lockObjects[i] = new Object();
                callbacks[i] = new EventCallback();

                Message message = new Message(objectSlice.get(i).toString());
                client.sendEventAsync(message, callbacks[i], lockObjects[i]);
                System.out.println("Sending started!");
            }

            for (int i = 0; i < concurrentSend; i++) {
                synchronized (lockObjects[0]) {
                    lockObjects[0].wait();
                }
            }

            client.closeNow();
            System.out.println("DONE!");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void sendData(JsonObject object) {
        try {
            client.open();
            Message message = new Message(object.toString());
            Object lockobj = new Object();

            // Send the message.
            EventCallback callback = new EventCallback();
            client.sendEventAsync(message, callback, lockobj);

            synchronized (lockobj) {
                lockobj.wait();
            }
            client.closeNow();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void createDeviceClient() {
        try {
            this.client = new DeviceClient(this._connectionString, protocol);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static class EventCallback implements IotHubEventCallback {
        public void execute(IotHubStatusCode status, Object context) {
            System.out.println("IoT Hub responded to message with status: " + status.name());

            if (context != null) {
                synchronized (context) {
                    context.notify();
                }
            }
        }
    }
}
