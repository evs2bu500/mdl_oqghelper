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

import static org.apache.logging.log4j.LogManager.getLogger;

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

    @Value("${oqg.ept.r2}")
    private String oqgEptR2;
    @Value("${oqg.ept.i2}")
    private String oqgEptI2;

//    OqgHelper(RestTemplate template){
//        restTemplate = template;
//    }
    private final Logger logger = getLogger(OqgHelper.class.getName());

//    public OqgHelper(Logger logger) {
//        this.logger = logger;
//    }

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
                    String msg = "OQG R Error: Response body is null";
                    logger.info(msg);
                    throw new Exception(msg);
                }
                if(response.getBody().equals("[]")){
                    return List.of();
                }
                // Convert the response body to the desired object
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(response.getBody(), new TypeReference<List<Map<String, Object>>>(){});
            }else{
                String msg = "OQG R Error: " + response.getStatusCode();
                logger.info(msg);
                throw new Exception(msg);
            }
        } catch (Exception e){
            String msg = "OQG R Error: " + e.getMessage();
            logger.info(msg);
            throw new Exception(msg);
        }
    }

    public List<Map<String, Object>> OqgR2(String sqlQuery, boolean useReadReplica) throws Exception {
        String rUrl = oqgPath + oqgEptR;
        if(useReadReplica){
            rUrl = oqgPath + oqgEptR2;
        }
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
                    String msg = "OQG R2 Error: Response body is null";
                    logger.info(msg);
                    throw new Exception(msg);
                }
                if(response.getBody().equals("[]")){
                    return List.of();
                }
                // Convert the response body to the desired object
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(response.getBody(), new TypeReference<List<Map<String, Object>>>(){});
            }else{
                String msg = "OQG R2 Error: " + response.getStatusCode();
                logger.info(msg);
                throw new Exception(msg);
            }
        } catch (Exception e){
            String msg = "OQG R2 Error: " + e.getMessage();
            logger.info(msg);
            throw new Exception(msg);
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
                String msg = "OQG IU Error: Response body is null";
                logger.info(msg);
                throw new Exception(msg);
            }
            int numRowsAffected = Integer.parseInt(responseBody);
            return Map.of("resp", numRowsAffected);

        } catch (Exception e){
            String msg = "OQG IU Error: " + e.getMessage();
            logger.info(msg);
            throw new Exception(msg);
        }
    }

    public Map<String, Object> OqgI2(String sqlQuery, boolean getReturnId) throws Exception {
        String iuUrl = oqgPath + oqgEptI2;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> payloadForJson = new HashMap<>();
        payloadForJson.put("sql", sqlQuery);
        payloadForJson.put("get_return_id", getReturnId);

        // Create the request entity
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(payloadForJson, headers);

        try {
            // Make the request using RestTemplate
            ResponseEntity<String> response = restTemplate.postForEntity(iuUrl, requestEntity, String.class);

            String responseBody = response.getBody();
            if(responseBody == null){
                String msg = "OQG I2 Error: Response body is null";
                logger.info(msg);
                throw new Exception(msg);
            }

            if(getReturnId){
                long id = Long.parseLong(responseBody);
                return Map.of("resp", Long.toString(id));
            }else {
                return Map.of("resp", responseBody);
            }
        } catch (Exception e){
            String msg = "OQG I2 Error: " + e.getMessage();
            logger.info(msg);
            throw new Exception(msg);
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
                String msg = "OQG D Error: Response body is null";
                logger.info(msg);
                throw new Exception(msg);
            }
            int numRowsAffected = Integer.parseInt(responseBody);
            return Map.of("resp", numRowsAffected);

        } catch (Exception e){
            String msg = "OQG D Error: " + e.getMessage();
            logger.info(msg);
            throw new Exception(msg);
        }
    }
}