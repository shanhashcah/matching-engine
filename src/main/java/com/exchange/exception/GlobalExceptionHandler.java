package com.exchange.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishResult;
import com.exchange.model.Model;
import com.exchange.model.UserTrade;

import jakarta.servlet.http.HttpServletRequest;

@RestControllerAdvice
public class GlobalExceptionHandler {

	@Autowired
	Environment env;

	@ExceptionHandler(Exception.class)
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	public UserTrade caughtAllException(Exception ex, HttpServletRequest request) {

		UserTrade res = new UserTrade();
		StringWriter errors = new StringWriter();
		PrintWriter printWriter = new PrintWriter(errors);
		ex.printStackTrace(printWriter);
		System.out.println(errors.toString());
		System.out.println(request.getRequestURL());
		String time = readTime();

		if (time == null) {
			
			AmazonSNS snsClient1 = AmazonSNSClientBuilder.standard().withRegion(env.getProperty("regionarn")).build();

			PublishResult result = snsClient1.publish(env.getProperty("arn"), "API Path.."+request.getRequestURL()+"\n"+errors.toString());
			
			
			writeTime(LocalDateTime.now());
		} else {
			
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSSSSSSS");
			LocalDateTime now = LocalDateTime.now();
			long diff = Duration.between(LocalDateTime.parse(time, dateTimeFormatter), now).toHours();
			
			if (diff > 2) {
				

				AmazonSNS snsClient1 = AmazonSNSClientBuilder.standard().withRegion(env.getProperty("regionarn")).build();

				PublishResult result = snsClient1.publish(env.getProperty("arn"), "API Path.."+request.getRequestURL()+"\n"+errors.toString());
                   

				writeTime(LocalDateTime.now());
			}
		}

		res.setMessage("Error.." + ex.getMessage());
		res.setStatuscode("0");
		return res;
	}

	public String readTime() {
		synchronized (this) {

			Path path = Paths.get(env.getProperty("filepath") + "time.txt");
			List<String> now = null;
			try {
				now = Files.readAllLines(path);
				if (now.isEmpty()) {
					return null;
				} else {
					return now.get(0);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}

	public void writeTime(LocalDateTime time) {

		synchronized (this) {

			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSSSSSSS");
			try {
				Files.write(Paths.get(env.getProperty("filepath") + "time.txt"),
						dateTimeFormatter.format(time).getBytes());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

	}
}
