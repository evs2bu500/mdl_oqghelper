package com.pabu5h.evs2.oqghelper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.Setter;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@Setter
@Service
public class OqgHelper {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${oqg.path}")
    private String oqgPath;
    @Value("${oqg.ept.r}")
    private String oqgEptR;
    @Value("${oqg.ept.iu}")
    private String oqgEptIU;
    @Value("${oqg.ept.del}")
    private String oqgEptDel;

//    OqgHelper(RestTemplate template){
//        restTemplate = template;
//    }
    private final Logger logger;

    public OqgHelper(Logger logger) {
        this.logger = logger;
    }

    public List<Map<String, Object>> OqgR(String sqlQuery) throws Exception {
        String rUrl = oqgPath + oqgEptR;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> payloadForJson = new HashMap<>();
        payloadForJson.put("sql", sqlQuery);

        // Create the request entity
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(payloadForJson, headers);

        try {
            // Make the request using RestTemplate
            ResponseEntity<String> response = restTemplate.postForEntity(rUrl, requestEntity, String.class);

            if(response.getStatusCode() == HttpStatus.OK) {
                if (response.getBody() == null){
                    throw new Exception("OQG Query Error: Response body is null");
                }
                if(response.getBody().equals("[]")){
                    return List.of();
                }
                // Convert the response body to the desired object
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(response.getBody(), new TypeReference<List<Map<String, Object>>>(){});
            }else{
                throw new Exception("OQG Query Error: " + response.getStatusCode());
            }
        } catch (Exception e){
            throw new Exception("OQG Query Error: " + e.getMessage());
        }
    }

    public Map<String, Object> OqgIU(String sqlQuery) throws Exception {
        String iuUrl = oqgPath + oqgEptIU;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> payloadForJson = new HashMap<>();
        payloadForJson.put("sql", sqlQuery);

        // Create the request entity
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(payloadForJson, headers);

        try {
            // Make the request using RestTemplate
            ResponseEntity<String> response = restTemplate.postForEntity(iuUrl, requestEntity, String.class);

            String responseBody = response.getBody();
            if(responseBody == null){
                throw new Exception("OQG IU Error: Response body is null");
            }
            int numRowsAffected = Integer.parseInt(responseBody);
            return Map.of("resp", numRowsAffected);

        } catch (Exception e){
            throw new Exception("OQG IU Error: " + e.getMessage());
        }
    }

    public Map<String, Object> OqgD(String sqlQuery) throws Exception {
        String iuUrl = oqgPath + oqgEptDel;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> payloadForJson = new HashMap<>();
        payloadForJson.put("sql", sqlQuery);

        // Create the request entity
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(payloadForJson, headers);

        try {
            // Make the request using RestTemplate
            ResponseEntity<String> response = restTemplate.postForEntity(iuUrl, requestEntity, String.class);

            String responseBody = response.getBody();
            if(responseBody == null){
                throw new Exception("OQG IU Error: Response body is null");
            }
            int numRowsAffected = Integer.parseInt(responseBody);
            return Map.of("resp", numRowsAffected);

        } catch (Exception e){
            throw new Exception("OQG IU Error: " + e.getMessage());
        }
    }
}