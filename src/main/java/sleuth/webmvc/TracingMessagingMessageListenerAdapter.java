package sleuth.webmvc;

import brave.Span;
import brave.jms.JmsTracing;
import brave.propagation.CurrentTraceContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import org.springframework.jms.listener.adapter.MessagingMessageListenerAdapter;

/** This wraps the message listener in a child span */
final class TracingMessagingMessageListenerAdapter extends MessagingMessageListenerAdapter {

  final JmsTracing jmsTracing;
  final CurrentTraceContext current;

  TracingMessagingMessageListenerAdapter(JmsTracing jmsTracing, CurrentTraceContext current) {
    this.jmsTracing = jmsTracing;
    this.current = current;
  }

  @Override public void onMessage(Message message, Session session) throws JMSException {
    Span span = jmsTracing.nextSpan(message).name("on-message").start();
    try (CurrentTraceContext.Scope ws = current.newScope(span.context())) {
      super.onMessage(message, session);
    } catch (JMSException | RuntimeException | Error e) {
      span.error(e);
      throw e;
    } finally {
      span.finish();
    }
  }
}
