package sleuth.webmvc;

import brave.Tracing;
import brave.jms.JmsTracing;
import brave.propagation.CurrentTraceContext;
import javax.jms.ConnectionFactory;
import javax.jms.MessageListener;
import javax.jms.XAConnectionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.listener.endpoint.JmsMessageEndpointManager;

@Configuration
class JmsTracingConfiguration {

  @Bean JmsTracing jmsTracing(Tracing tracing) {
    return JmsTracing.create(tracing);
  }

  // Setup basic JMS functionality
  @Bean BeanPostProcessor connectionFactoryDecorator(BeanFactory beanFactory) {
    return new BeanPostProcessor() {
      @Override public Object postProcessAfterInitialization(Object bean, String beanName)
          throws BeansException {
        // Wrap the caching connection factories instead of its target, because it catches callbacks
        // such as ExceptionListener. If we don't wrap, cached callbacks like this won't be traced.
        if (bean instanceof CachingConnectionFactory) {
          JmsTracing jmsTracing = getJmsTracing();
          if (jmsTracing == null) return bean; // graceful on failure for any reason.
          return jmsTracing.connectionFactory((CachingConnectionFactory) bean);
        }

        if (bean instanceof JmsMessageEndpointManager) {
          JmsTracing jmsTracing = getJmsTracing();
          if (jmsTracing == null) return bean; // graceful on failure for any reason.

          JmsMessageEndpointManager manager = (JmsMessageEndpointManager) bean;
          MessageListener listener = manager.getMessageListener();
          if (listener != null) {
            // Adds a consumer span as we have no visibility into JCA's implementation of messaging
            manager.setMessageListener(jmsTracing.messageListener(listener, true));
          }
          return bean;
        }

        // We check XA first in case the ConnectionFactory also implements XAConnectionFactory
        if (bean instanceof XAConnectionFactory) {
          JmsTracing jmsTracing = getJmsTracing();
          if (jmsTracing == null) return bean; // graceful on failure for any reason.
          return jmsTracing.xaConnectionFactory((XAConnectionFactory) bean);
        } else if (bean instanceof ConnectionFactory) {
          JmsTracing jmsTracing = getJmsTracing();
          if (jmsTracing == null) return bean; // graceful on failure for any reason.
          return jmsTracing.connectionFactory((ConnectionFactory) bean);
        }
        return bean;
      }

      // cache initialization request to avoid calling getBean for every matched bean
      JmsTracing jmsTracing;

      // Lazy lookup JmsTracing so that the BPP doesn't end up needing to proxy anything.
      JmsTracing getJmsTracing() {
        if (jmsTracing != null) return jmsTracing;
        try {
          return (jmsTracing = beanFactory.getBean(JmsTracing.class));
        } catch (BeansException e) {
          return null;
        }
      }
    };
  }

  /** Choose the tracing endpoint registry */
  @Bean
  TracingJmsListenerEndpointRegistry registry(JmsTracing jmsTracing, CurrentTraceContext current) {
    return new TracingJmsListenerEndpointRegistry(jmsTracing, current);
  }

  /** Setup the tracing endpoint registry */
  @Bean
  public JmsListenerConfigurer configureTracing(TracingJmsListenerEndpointRegistry registry) {
    return registrar -> registrar.setEndpointRegistry(registry);
  }
}
