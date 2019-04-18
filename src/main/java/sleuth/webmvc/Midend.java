package sleuth.webmvc;

import org.apache.dubbo.config.annotation.Reference;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@EnableAutoConfiguration
@RestController
@CrossOrigin // So that javascript can be hosted elsewhere
public class Midend {

    @Reference(url = "dubbo://127.0.0.1:9000")
    Api api;

    @RequestMapping("/api")
    public String callBackend() {
        return api.printDate();
    }

    public static void main(String[] args) {
        SpringApplication.run(Midend.class,
                "--spring.application.name=midend",
                "--server.port=9001"
        );
    }
}
