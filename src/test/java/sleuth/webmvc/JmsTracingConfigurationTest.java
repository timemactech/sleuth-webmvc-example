package sleuth.webmvc;

import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import org.apache.activemq.ra.ActiveMQActivationSpec;
import org.apache.activemq.ra.ActiveMQResourceAdapter;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.activemq.ActiveMQAutoConfiguration;
import org.springframework.boot.jms.XAConnectionFactoryWrapper;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jca.support.ResourceAdapterFactoryBean;
import org.springframework.jca.work.SimpleTaskWorkManager;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.endpoint.JmsMessageEndpointManager;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

// inspired by org.springframework.boot.autoconfigure.jms.JmsAutoConfigurationTests
public class JmsTracingConfigurationTest {
  final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(
          ActiveMQAutoConfiguration.class,
          JmsAutoConfiguration.class,
          TestTracingConfiguration.class,
          JmsTracingConfiguration.class
      ));

  @Test public void tracesConnectionFactory() {
    contextRunner.run(JmsTracingConfigurationTest::checkConnection);
  }

  @Test public void tracesXAConnectionFactories() {
    contextRunner.withUserConfiguration(XAConfiguration.class).run(ctx -> {
      checkConnection(ctx);
      checkXAConnection(ctx);
    });
  }

  @AutoConfigureBefore(ActiveMQAutoConfiguration.class)
  static class XAConfiguration {
    @Bean XAConnectionFactoryWrapper xaConnectionFactoryWrapper() {
      return connectionFactory -> (ConnectionFactory) connectionFactory;
    }
  }

  @Test public void tracesListener_jmsMessageListener() {
    contextRunner.withUserConfiguration(SimpleJmsListenerConfiguration.class).run(ctx -> {
      ctx.getBean(JmsTemplate.class).convertAndSend("myQueue", "foo");

      Callable<Span> takeSpan = ctx.getBean("takeSpan", Callable.class);
      List<Span> trace = Arrays.asList(takeSpan.call(), takeSpan.call(), takeSpan.call());

      assertThat(trace).allSatisfy(s -> assertThat(s.traceId()).isEqualTo(trace.get(0).traceId()));
      assertThat(trace).extracting(Span::name).containsExactly("send", "receive", "on-message");
    });
  }

  @Configuration
  @EnableJms
  static class SimpleJmsListenerConfiguration implements JmsListenerConfigurer {
    @Autowired CurrentTraceContext current;

    @Override public void configureJmsListeners(JmsListenerEndpointRegistrar registrar) {
      SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
      endpoint.setId("myCustomEndpointId");
      endpoint.setDestination("myQueue");
      endpoint.setMessageListener(simpleMessageListener(current));
      registrar.registerEndpoint(endpoint);
    }

    @Bean MessageListener simpleMessageListener(CurrentTraceContext current) {
      return message -> {
        // Didn't restart the trace
        assertThat(current.get()).extracting(TraceContext::parentIdAsLong).isNotEqualTo(0L);
      };
    }
  }

  @Test public void tracesListener_annotationMessageListener() {
    contextRunner.withUserConfiguration(AnnotationJmsListenerConfiguration.class).run(ctx -> {
      ctx.getBean(JmsTemplate.class).convertAndSend("myQueue", "foo");

      Callable<Span> takeSpan = ctx.getBean("takeSpan", Callable.class);
      List<Span> trace = Arrays.asList(takeSpan.call(), takeSpan.call(), takeSpan.call());

      assertThat(trace).allSatisfy(s -> assertThat(s.traceId()).isEqualTo(trace.get(0).traceId()));
      assertThat(trace).extracting(Span::name).containsExactly("send", "receive", "on-message");
    });
  }

  @Configuration
  @EnableJms
  static class AnnotationJmsListenerConfiguration {
    @Autowired CurrentTraceContext current;

    @JmsListener(destination = "myQueue")
    public void onMessage() {
      assertThat(current.get()).extracting(TraceContext::parentIdAsLong).isNotEqualTo(0L);
    }
  }

  @Test public void tracesListener_jcaMessageListener() {
    contextRunner.withUserConfiguration(JcaJmsListenerConfiguration.class).run(ctx -> {
      ctx.getBean(JmsTemplate.class).convertAndSend("myQueue", "foo");

      Callable<Span> takeSpan = ctx.getBean("takeSpan", Callable.class);
      List<Span> trace = Arrays.asList(takeSpan.call(), takeSpan.call(), takeSpan.call());

      assertThat(trace).allSatisfy(s -> assertThat(s.traceId()).isEqualTo(trace.get(0).traceId()));
      assertThat(trace).extracting(Span::name).containsExactly("send", "receive", "on-message");
    });
  }

  @Configuration
  static class JcaJmsListenerConfiguration {
    @Autowired CurrentTraceContext current;

    @Bean ResourceAdapterFactoryBean resourceAdapter() {
      ResourceAdapterFactoryBean resourceAdapter = new ResourceAdapterFactoryBean();
      ActiveMQResourceAdapter real = new ActiveMQResourceAdapter();
      real.setServerUrl("vm://localhost?broker.persistent=false");
      resourceAdapter.setResourceAdapter(real);
      resourceAdapter.setWorkManager(new SimpleTaskWorkManager());
      return resourceAdapter;
    }

    @Bean MessageListener simpleMessageListener(CurrentTraceContext current) {
      return message -> {
        // Didn't restart the trace
        assertThat(current.get()).extracting(TraceContext::parentIdAsLong).isNotEqualTo(0L);
      };
    }

    @Bean JmsMessageEndpointManager endpointManager(ResourceAdapter resourceAdapter, MessageListener simpleMessageListener) {
      JmsMessageEndpointManager endpointManager = new JmsMessageEndpointManager();
      endpointManager.setResourceAdapter(resourceAdapter);

      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setUseJndi(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination("myQueue");

      endpointManager.setActivationSpec(spec);
      endpointManager.setMessageListener(simpleMessageListener);
      return endpointManager;
    }
  }

  static void checkConnection(AssertableApplicationContext ctx) throws JMSException {
    // Not using try-with-resources as that doesn't exist in JMS 1.1
    Connection con = ctx.getBean(ConnectionFactory.class).createConnection();
    try {
      con.setExceptionListener(exception -> {
      });
      assertThat(con.getExceptionListener().getClass().getName())
          .startsWith("brave.jms.TracingExceptionListener");
    } finally {
      con.close();
    }
  }

  static void checkXAConnection(AssertableApplicationContext ctx) throws JMSException {
    // Not using try-with-resources as that doesn't exist in JMS 1.1
    XAConnection con = ctx.getBean(XAConnectionFactory.class).createXAConnection();
    try {
      con.setExceptionListener(exception -> {
      });
      assertThat(con.getExceptionListener().getClass().getName())
          .startsWith("brave.jms.TracingExceptionListener");
    } finally {
      con.close();
    }
  }
}
