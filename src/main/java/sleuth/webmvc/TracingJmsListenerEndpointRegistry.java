package sleuth.webmvc;

import brave.jms.JmsTracing;
import brave.propagation.CurrentTraceContext;
import java.lang.reflect.Field;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerEndpoint;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.config.MethodJmsListenerEndpoint;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.jms.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.lang.Nullable;

/** This ensures listeners end up continuing the trace from {@link MessageConsumer#receive()} */
class TracingJmsListenerEndpointRegistry extends JmsListenerEndpointRegistry {
  final JmsTracing jmsTracing;
  final CurrentTraceContext current;
  // Not all state can be copied without using reflection
  final Field messageHandlerMethodFactoryField;
  final Field embeddedValueResolverField;

  TracingJmsListenerEndpointRegistry(JmsTracing jmsTracing, CurrentTraceContext current) {
    this.jmsTracing = jmsTracing;
    this.current = current;
    this.messageHandlerMethodFactoryField = tryField("messageHandlerMethodFactory");
    this.embeddedValueResolverField = tryField("embeddedValueResolver");
  }

  @Override public void registerListenerContainer(
      JmsListenerEndpoint endpoint,
      JmsListenerContainerFactory<?> factory,
      boolean startImmediately
  ) {
    if (endpoint instanceof MethodJmsListenerEndpoint) {
      endpoint = trace((MethodJmsListenerEndpoint) endpoint);
    } else if (endpoint instanceof SimpleJmsListenerEndpoint) {
      endpoint = trace((SimpleJmsListenerEndpoint) endpoint);
    }
    super.registerListenerContainer(endpoint, factory, startImmediately);
  }

  /**
   * This wraps the {@link SimpleJmsListenerEndpoint#getMessageListener()} delegate in a new span.
   */
  SimpleJmsListenerEndpoint trace(SimpleJmsListenerEndpoint source) {
    MessageListener delegate = source.getMessageListener();
    if (delegate == null) return source;
    source.setMessageListener(jmsTracing.messageListener(delegate, false));
    return source;
  }

  /**
   * It would be better to trace by wrapping, but {@link MethodJmsListenerEndpoint#createMessageListener(MessageListenerContainer)},
   * is protected so we can't call it from outside code. In other words, a forwarding pattern can't
   * be used. Instead, we copy state from the input.
   *
   * NOTE: As {@linkplain MethodJmsListenerEndpoint} is neither final, nor effectively final. For
   * this reason we can't ensure copying will get all state. For example, a subtype could hold state
   * we aren't aware of, or change behavior. We can consider checking that input is not a subtype,
   * and most conservatively leaving unknown subtypes untraced.
   */
  MethodJmsListenerEndpoint trace(MethodJmsListenerEndpoint source) {
    // Skip out rather than incompletely copying the source
    if (messageHandlerMethodFactoryField == null || embeddedValueResolverField == null) {
      return source;
    }

    // We want the stock implementation, except we want to wrap the message listener in a new span
    MethodJmsListenerEndpoint dest = new MethodJmsListenerEndpoint() {
      @Override protected MessagingMessageListenerAdapter createMessageListenerInstance() {
        return new TracingMessagingMessageListenerAdapter(jmsTracing, current);
      }
    };

    // set state from AbstractJmsListenerEndpoint
    dest.setId(source.getId());
    dest.setDestination(source.getDestination());
    dest.setSubscription(source.getSubscription());
    dest.setSelector(source.getSelector());
    dest.setConcurrency(source.getConcurrency());

    // set state from MethodJmsListenerEndpoint
    dest.setBean(source.getBean());
    dest.setMethod(source.getMethod());
    dest.setMostSpecificMethod(source.getMostSpecificMethod());

    try {
      dest.setMessageHandlerMethodFactory(get(source, messageHandlerMethodFactoryField));
      dest.setEmbeddedValueResolver(get(source, embeddedValueResolverField));
    } catch (IllegalAccessException e) {
      return source; // skip out rather than incompletely copying the source
    }
    return dest;
  }

  @Nullable static Field tryField(String name) {
    try {
      Field field =
          MethodJmsListenerEndpoint.class.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException e) {
      return null;
    }
  }

  @Nullable static <T> T get(Object object, Field field) throws IllegalAccessException {
    return (T) field.get(object);
  }
}
