package com.exchange.consumer;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class Consumer {

	/*
	
	@Autowired
    Queue queue;
	@RabbitListener(queues = "#{queue.getName()}")  // Dynamically reading the queue name using SpEL from the "queue" object.
    public void receive(final String message) {
        System.out.println("Listening messages from the queue!!");
        System.out.println("Received the following message from the queue= " + message);
         System.out.println("Message received successfully from the queue.");
    }
	
	@RabbitListener(queues = "${rabbitmq.queuetradehistory}")  // Dynamically reading the queue name using SpEL from the "queue" object.
    public void receivetrade(final String message) {
        System.out.println("Listening messages from the queue!!");
        System.out.println("Received the following message from the queue= " + message);
         System.out.println("Message received successfully from the queue.");
    }
  */  
    
    
}
