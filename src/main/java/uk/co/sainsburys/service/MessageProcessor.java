package uk.co.sainsburys.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import uk.co.sainsburys.exception.RetryableException;

import java.io.IOException;

@Slf4j
@Component
public class MessageProcessor {
    public void processMessage(String message) throws RetryableException, IOException {
        log.info("Processing message: {}", message);
    }
}
