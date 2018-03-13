package sleuth.webmvc;

import brave.Span;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import java.lang.reflect.Method;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.sleuth.util.SpanNameUtil;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;

import static org.springframework.util.ReflectionUtils.findMethod;

@EnableAutoConfiguration
@EnableAspectJAutoProxy
@Import(TracingAspect.class)
public class Backend {

  @KafkaListener(topics = "backend")
  public void onMessage(ConsumerRecord<?, ?> message) {
    System.err.println(message);
  }

  public static void main(String[] args) {
    SpringApplication.run(Backend.class,
        "--spring.application.name=backend",
        "--server.port=9000"
    );
  }
}

@Aspect class TracingAspect {
  @Autowired CurrentTraceContext currentTraceContext;
  @Autowired KafkaTracing kafkaTracing;

  // toy example as listeners allow parameters besides record.
  // Real needs to intercept MessagingMessageListenerAdapter.invokeHandler or similar
  @Around("@annotation(listener) && args(record)")
  public Object traceListener(
      ProceedingJoinPoint pjp,
      KafkaListener listener,
      ConsumerRecord<?, ?> record
  ) throws Throwable {
    String name = SpanNameUtil.toLowerHyphen(getMethod(pjp).getName());
    Span span = kafkaTracing.nextSpan(record).name(name).start();

    try (Scope scope = currentTraceContext.newScope(span.context())) {
      return pjp.proceed();
    } catch (RuntimeException | Error e) {
      span.error(e);
      throw e;
    } finally {
      span.finish();
    }
  }

  static Method getMethod(ProceedingJoinPoint pjp) {
    Method method = ((MethodSignature) pjp.getSignature()).getMethod();
    return findMethod(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());
  }
}
