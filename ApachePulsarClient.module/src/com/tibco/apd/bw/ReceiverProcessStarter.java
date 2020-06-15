package com.tibco.apd.bw;

import java.io.Serializable;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.Message;

import com.tibco.bw.palette.shared.java.JavaProcessStarter;


@SuppressWarnings("rawtypes")
public class ReceiverProcessStarter extends JavaProcessStarter implements
		Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8945943801263890455L;
	private Client client;
	private Consumer<byte[]> consumer;
	private String topicName;
	private String subscriptionName;
	private ReceiveMessage receiveMessage;
	
	public ReceiverProcessStarter() {
		System.out.println("java process starter constructor");
	}
	
	public void InitParams(String topicName, String subscriptionName) {
		System.out.println("Initialising");
		this.topicName = topicName;
		this.subscriptionName = subscriptionName;
	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onShutdown() {
		if(client!=null) {
			client.close();
		}
		
	}

	@Override
	public void onStart() throws Exception {

		System.out.println("On start lifecycle method");
		
		this.client =  (Client) this.getJavaGlobalInstance();
		

		this.consumer = client.getClient().newConsumer()
				.topic(this.topicName)
				.subscriptionType(SubscriptionType.Shared)
				.subscriptionName(this.subscriptionName)
				.subscribe();
		
		System.out.println("Consumer created");
		
		receiveMessage = new ReceiveMessage(this, consumer);
		
		Thread thread = new Thread(receiveMessage);
		thread.start();
		
	}

	@Override
	public void onStop() throws Exception {
		client.close();	
	}
	
	
	public static class ReceiveMessage implements Runnable {

		Consumer<byte[]> consumer;
		
		JavaProcessStarter javaProcessStarter;
		volatile boolean flowControl = false;
		
		public ReceiveMessage(JavaProcessStarter javaProcessStarter, Consumer<byte[]> consumer) {
			this.consumer = consumer;
			this.javaProcessStarter = javaProcessStarter;
		}
		
		
		public void setFlowControlEnabled(boolean flowControl) {
			this.flowControl = flowControl;
		}
		
		@Override
		public void run() {
			
			do {
			if(!flowControl) {
				Message<byte[]> msg;
				try {
					msg = consumer.receive();
					String content = new String(msg.getData());
					javaProcessStarter.onEvent(content);
					consumer.acknowledge(msg);
					
				} catch (PulsarClientException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
						
			} while(true);	
		}
	}
}
