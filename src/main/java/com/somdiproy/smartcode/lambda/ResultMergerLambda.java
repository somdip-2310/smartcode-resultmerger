package com.somdiproy.smartcode.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ResultMergerLambda implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    
    private final DynamoDB dynamoDB;
    private final Table analysisTable;
    private final ObjectMapper objectMapper;
    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE_NAME");
    
    public ResultMergerLambda() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(client);
        this.analysisTable = dynamoDB.getTable(TABLE_NAME);
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        context.getLogger().log("ResultMerger processing: " + event);
        
        try {
            String analysisId = (String) event.get("analysisId");
            List<Map<String, Object>> chunkResults = (List<Map<String, Object>>) event.get("chunkResults");
            
            Map<String, Object> mergedResult = mergeResults(chunkResults);
            mergedResult.put("analysisId", analysisId);
            
            // Save to DynamoDB
            saveAnalysisResult(analysisId, mergedResult);
            
            return mergedResult;
            
        } catch (Exception e) {
            context.getLogger().log("Error merging results: " + e.getMessage());
            throw new RuntimeException("Failed to merge results", e);
        }
    }
    
    private Map<String, Object> mergeResults(List<Map<String, Object>> chunkResults) {
        Map<String, Object> merged = new HashMap<>();
        
        List<Map<String, Object>> allIssues = new ArrayList<>();
        List<Map<String, Object>> allSuggestions = new ArrayList<>();
        double totalScore = 0;
        int validScores = 0;
        
        for (Map<String, Object> chunk : chunkResults) {
            if (chunk.containsKey("issues")) {
                allIssues.addAll((List<Map<String, Object>>) chunk.get("issues"));
            }
            if (chunk.containsKey("suggestions")) {
                allSuggestions.addAll((List<Map<String, Object>>) chunk.get("suggestions"));
            }
            if (chunk.containsKey("overallScore")) {
                totalScore += ((Number) chunk.get("overallScore")).doubleValue();
                validScores++;
            }
        }
        
        merged.put("summary", "Analysis completed across " + chunkResults.size() + " code segments");
        merged.put("overallScore", validScores > 0 ? totalScore / validScores : 0);
        merged.put("issues", allIssues);
        merged.put("suggestions", allSuggestions);
        merged.put("chunkCount", chunkResults.size());
        
        return merged;
    }
    
    private void saveAnalysisResult(String analysisId, Map<String, Object> result) throws Exception {
        Item item = new Item()
            .withPrimaryKey("analysisId", analysisId)
            .withString("status", "COMPLETED")
            .withLong("timestamp", System.currentTimeMillis())
            .withLong("ttl", System.currentTimeMillis() / 1000 + TimeUnit.DAYS.toSeconds(7))
            .withJSON("result", objectMapper.writeValueAsString(result));
        
        analysisTable.putItem(item);
    }
}