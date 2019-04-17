package sleuth.webmvc;

import java.util.Date;

import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

@EnableAutoConfiguration
@EnableDubbo // redundant https://github.com/apache/incubator-dubbo-spring-boot-project/issues/322
@Service(interfaceClass = Api.class)
public class Backend implements Api {

  @Override public String printDate() {
    return new Date().toString();
  }

  public static void main(String[] args) {
    SpringApplication.run(Backend.class,
        "--spring.application.name=backend",
        // These args allow dubbo to start without any web framework
        "--spring.main.web-environment=false",
        "--dubbo.registry.address=N/A",
        "--dubbo.protocol.port=9000"
    );
  }
}
