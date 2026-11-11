package uk.co.sainsburys;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class MessageProcessor {
    public void processMessage(String message) throws RetryableException, IOException {
        log.info("Processing message: {}", message);
    }
}
