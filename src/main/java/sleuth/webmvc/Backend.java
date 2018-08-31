package sleuth.webmvc;

import java.util.Date;
import javax.jms.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;

@EnableAutoConfiguration
@EnableJms
@Import(JmsTracingConfiguration.class)
public class Backend implements JmsListenerConfigurer {

  @JmsListener(destination = "backend")
  public void onMessage() {
    System.err.println(new Date().toString());
  }

  @Override public void configureJmsListeners(JmsListenerEndpointRegistrar registrar) {
    SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
    endpoint.setId("simple");
    endpoint.setDestination("backend");
    endpoint.setMessageListener(m -> onMessage());
    registrar.registerEndpoint(endpoint);
  }

  @Bean public Topic topic() {
    return new ActiveMQTopic("backend");
  }

  public static void main(String[] args) {
    SpringApplication.run(Backend.class,
        "--spring.application.name=backend",
        "--server.port=9000"
    );
  }
}
