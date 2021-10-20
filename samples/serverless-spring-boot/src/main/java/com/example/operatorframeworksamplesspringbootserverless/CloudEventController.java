package com.example.operatorframeworksamplesspringbootserverless;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CloudEventController {

  private static final Logger log = LoggerFactory.getLogger(CloudEventController.class);

  @PostMapping("/event")
  public void processEvent(@RequestBody String event, @RequestHeader Map<String, String> headers) {
    log.info("Body: {}; Headers: {}", event, headers);
  }

}
