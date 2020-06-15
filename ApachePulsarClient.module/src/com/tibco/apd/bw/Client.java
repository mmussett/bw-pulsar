package com.tibco.apd.bw;


import java.io.Serializable;

import org.apache.pulsar.client.api.*;


public class Client implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2215008955150059823L;
	private PulsarClient client;
	private String serviceUrl;

	public Client(String url) throws PulsarClientException {
		
		this.serviceUrl = url;
		this.client = initClient();
	}

	public String getUrl() {
		return this.serviceUrl;
	}
	
	public PulsarClient getClient() {
		return this.client;
	}
	
	public void close(){
		try {
			this.client.close();
		} catch (PulsarClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private PulsarClient initClient() throws PulsarClientException {
		return client = PulsarClient.builder().serviceUrl(this.serviceUrl).build();
	}
	

	public void Publish(String topic, String message) {

			try {

			Producer<String> stringProducer = client.newProducer(Schema.STRING)
					.topic(topic)
					.create();
			

			stringProducer.send(message);
			stringProducer.close();
            
			
		} catch (PulsarClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

}
