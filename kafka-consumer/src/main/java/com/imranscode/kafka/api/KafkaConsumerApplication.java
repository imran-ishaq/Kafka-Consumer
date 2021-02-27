package com.imranscode.kafka.api;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.imranscode.kafka.api.config.EmailServiceConfig;
import com.imranscode.kafka.api.serviceimpl.MailServiceImplementation;
import com.imranscode.services.MailService;

@SpringBootApplication
@RestController
public class KafkaConsumerApplication {
  
@Autowired
 private MailServiceImplementation mail;
	List<String> messages = new ArrayList<>();

	User userFromTopic = null;

	@GetMapping("/consumeStringMessage")
	public List<String> consumeMsg() {
		return messages;
	}

	@GetMapping("/consumeJsonMessage")
	public User consumeJsonMessage() {
		mail.sendSimpleMessage(userFromTopic.getEmail(), "Kafaka message Service", "Hi This is a test Email. \\n\\n Regards,\\n Imran Ishaq");
		
		return userFromTopic;
	}

	@KafkaListener(groupId = "imrans-code1", topics = "practice", containerFactory = "kafkaListenerContainerFactory")
	public List<String> getMsgFromTopic(String data) {
		messages.add(data);
		return messages;
	}

	@KafkaListener(groupId = "imrans-code2", topics = "practice2", containerFactory = "userKafkaListenerContainerFactory")
	public User getJsonMsgFromTopic(User user) {
		userFromTopic = user;
		System.out.println("Email is sent for the User :" +userFromTopic);
		return userFromTopic;
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}
}
