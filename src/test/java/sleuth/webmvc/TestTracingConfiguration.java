package sleuth.webmvc;

import brave.Tracing;
import brave.internal.HexCodec;
import brave.propagation.CurrentTraceContext;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.Annotation;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

@Configuration
class TestTracingConfiguration {
  static final String CONTEXT_LEAK = "context.leak";

  /**
   * When testing servers or asynchronous clients, spans are reported on a worker thread. In order
   * to read them on the main thread, we use a concurrent queue. As some implementations report
   * after a response is sent, we use a blocking queue to prevent race conditions in tests.
   */
  BlockingQueue<Span> spans = new LinkedBlockingQueue<>();

  /** Call this to block until a span was reported */
  @Bean Callable<Span> takeSpan() {
    return () -> {
      Span result = spans.poll(3, TimeUnit.SECONDS);
      assertThat(result)
          .withFailMessage("Span was not reported")
          .isNotNull();
      assertThat(result.annotations())
          .extracting(Annotation::value)
          .doesNotContain(CONTEXT_LEAK);
      return result;
    };
  }

  @Bean CurrentTraceContext currentTraceContext() {
    return ThreadLocalCurrentTraceContext.newBuilder()
        .addScopeDecorator(StrictScopeDecorator.create())
        .build();
  }

  @Bean Tracing tracing(CurrentTraceContext currentTraceContext) {
    return Tracing.newBuilder()
        .spanReporter(s -> {
          // make sure the context was cleared prior to finish.. no leaks!
          TraceContext current = currentTraceContext.get();
          boolean contextLeak = false;
          if (current != null) {
            // add annotation in addition to throwing, in case we are off the main thread
            if (HexCodec.toLowerHex(current.spanId()).equals(s.id())) {
              s = s.toBuilder().addAnnotation(s.timestampAsLong(), CONTEXT_LEAK).build();
              contextLeak = true;
            }
          }
          spans.add(s);
          // throw so that we can see the path to the code that leaked the context
          if (contextLeak) {
            throw new AssertionError(CONTEXT_LEAK + " on " + Thread.currentThread().getName());
          }
        })
        .currentTraceContext(currentTraceContext)
        .build();
  }
}
