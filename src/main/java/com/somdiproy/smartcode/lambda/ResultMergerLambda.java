package com.somdiproy.smartcode.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * ResultMergerLambda - Merges analysis results from multiple code chunks
 * 
 * This Lambda function is responsible for:
 * 1. Aggregating results from parallel chunk analyses
 * 2. Computing overall scores and metrics
 * 3. Consolidating issues and suggestions
 * 4. Saving final results to DynamoDB
 * 
 * @author Somdip Roy
 */
public class ResultMergerLambda implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    
    private final DynamoDB dynamoDB;
    private final Table analysisTable;
    private final ObjectMapper objectMapper;
    private static final String TABLE_NAME = System.getenv().getOrDefault("DYNAMODB_TABLE_NAME", "code-analysis-results");
    private static final int TTL_DAYS = Integer.parseInt(System.getenv().getOrDefault("TTL_DAYS", "7"));
    
    public ResultMergerLambda() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(client);
        this.analysisTable = dynamoDB.getTable(TABLE_NAME);
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        context.getLogger().log("ResultMerger processing event: " + event);
        
        try {
            // Extract parameters
            String analysisId = extractAnalysisId(event);
            List<Map<String, Object>> chunkResults = extractChunkResults(event);
            Map<String, Object> metadata = extractMetadata(event);
            
            context.getLogger().log("Merging " + chunkResults.size() + " chunk results for analysis: " + analysisId);
            
            // Merge all chunk results
            Map<String, Object> mergedResult = mergeChunkResults(chunkResults, null, metadata);
            
            // Add analysis metadata
            mergedResult.put("analysisId", analysisId);
            mergedResult.put("mergedAt", new Date().toString());
            mergedResult.put("totalChunks", chunkResults.size());
            
            // Ensure all required fields exist
            ensureCompleteResult(mergedResult);
            
            // Save to DynamoDB
            saveAnalysisResult(analysisId, mergedResult, context);
            
            // Return the merged result
            return mergedResult;
            
        } catch (Exception e) {
            context.getLogger().log("Error in ResultMerger: " + e.getMessage());
            e.printStackTrace();
            
            // Return error result
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("error", true);
            errorResult.put("errorMessage", e.getMessage());
            errorResult.put("errorType", e.getClass().getSimpleName());
            
            // Try to save error status if we have analysisId
            try {
                String analysisId = extractAnalysisId(event);
                if (analysisId != null) {
                    updateAnalysisStatus(analysisId, "FAILED", "Merge failed: " + e.getMessage(), null, null);
                }
            } catch (Exception saveError) {
                context.getLogger().log("Failed to save error status: " + saveError.getMessage());
            }
            
            throw new RuntimeException("Failed to merge results: " + e.getMessage(), e);
        }
    }
    
    /**
     * Extract analysis ID from various event formats
     */
    private String extractAnalysisId(Map<String, Object> event) {
        // Direct field
        if (event.containsKey("analysisId")) {
            return (String) event.get("analysisId");
        }
        
        // From first chunk result
        List<Map<String, Object>> chunkResults = extractChunkResults(event);
        if (!chunkResults.isEmpty() && chunkResults.get(0).containsKey("analysisId")) {
            return (String) chunkResults.get(0).get("analysisId");
        }
        
        throw new IllegalArgumentException("analysisId not found in event");
    }
    
    /**
     * Extract chunk results from various event formats
     */
    private List<Map<String, Object>> extractChunkResults(Map<String, Object> event) {
        // Direct field
        if (event.containsKey("chunkResults")) {
            return (List<Map<String, Object>>) event.get("chunkResults");
        }
        
        // From array input (Step Functions Map state output)
        if (event.containsKey("Payload") && event.get("Payload") instanceof List) {
            return (List<Map<String, Object>>) event.get("Payload");
        }
        
        // If event itself is an array wrapper
        if (event.size() == 1 && event.values().iterator().next() instanceof List) {
            return (List<Map<String, Object>>) event.values().iterator().next();
        }
        
        throw new IllegalArgumentException("chunkResults not found in event");
    }
    
    /**
     * Extract metadata from event
     */
    private Map<String, Object> extractMetadata(Map<String, Object> event) {
        if (event.containsKey("metadata")) {
            return (Map<String, Object>) event.get("metadata");
        }
        return new HashMap<>();
    }
    
    /**
     * Comprehensive merge of chunk results with all required fields
     */
    private Map<String, Object> mergeChunkResults(List<Map<String, Object>> chunkResults, String originalCode, Map<String, Object> metadata) {
        Map<String, Object> merged = new HashMap<>();
        
        // Initialize aggregators
        double totalScore = 0;
        int validScores = 0;
        List<Map<String, Object>> allIssues = new ArrayList<>();
        List<Map<String, Object>> allSuggestions = new ArrayList<>();
        List<Map<String, Object>> allVulnerabilities = new ArrayList<>();
        List<String> allBottlenecks = new ArrayList<>();
        Set<String> uniqueBottlenecks = new HashSet<>();
        
        // Score aggregators
        double totalSecurityScore = 0;
        int securityScoreCount = 0;
        double totalPerformanceScore = 0;
        int performanceScoreCount = 0;
        double totalMaintainabilityScore = 0;
        int maintainabilityScoreCount = 0;
        double totalReadabilityScore = 0;
        int readabilityScoreCount = 0;
        
        boolean hasSecurityIssues = false;
        int totalLinesOfCode = 0;
        int totalComplexity = 0;
        int complexityCount = 0;
        
        // Issue severity counters
        int criticalIssues = 0;
        int highIssues = 0;
        int mediumIssues = 0;
        int lowIssues = 0;
        
        // Process each chunk result
        for (Map<String, Object> chunk : chunkResults) {
            // Aggregate overall scores
            if (chunk.containsKey("overallScore") && chunk.get("overallScore") != null) {
                totalScore += ((Number) chunk.get("overallScore")).doubleValue();
                validScores++;
            }
            
            // Aggregate issues
            if (chunk.containsKey("issues") && chunk.get("issues") instanceof List) {
                List<Map<String, Object>> issues = (List<Map<String, Object>>) chunk.get("issues");
                for (Map<String, Object> issue : issues) {
                    allIssues.add(issue);
                    // Count by severity
                    String severity = (String) issue.getOrDefault("severity", "MEDIUM");
                    switch (severity.toUpperCase()) {
                        case "CRITICAL": criticalIssues++; break;
                        case "HIGH": highIssues++; break;
                        case "MEDIUM": mediumIssues++; break;
                        case "LOW": lowIssues++; break;
                    }
                }
            }
            
            // Aggregate suggestions
            if (chunk.containsKey("suggestions") && chunk.get("suggestions") instanceof List) {
                allSuggestions.addAll((List<Map<String, Object>>) chunk.get("suggestions"));
            }
            
            // Aggregate security data
            if (chunk.containsKey("security") && chunk.get("security") instanceof Map) {
                Map<String, Object> security = (Map<String, Object>) chunk.get("security");
                if (security.containsKey("securityScore") && security.get("securityScore") != null) {
                    totalSecurityScore += ((Number) security.get("securityScore")).doubleValue();
                    securityScoreCount++;
                }
                if (security.containsKey("vulnerabilities") && security.get("vulnerabilities") instanceof List) {
                    allVulnerabilities.addAll((List<Map<String, Object>>) security.get("vulnerabilities"));
                }
                if (security.containsKey("hasSecurityIssues") && Boolean.TRUE.equals(security.get("hasSecurityIssues"))) {
                    hasSecurityIssues = true;
                }
            }
            
            // Aggregate performance data
            if (chunk.containsKey("performance") && chunk.get("performance") instanceof Map) {
                Map<String, Object> performance = (Map<String, Object>) chunk.get("performance");
                if (performance.containsKey("performanceScore") && performance.get("performanceScore") != null) {
                    totalPerformanceScore += ((Number) performance.get("performanceScore")).doubleValue();
                    performanceScoreCount++;
                }
                if (performance.containsKey("bottlenecks") && performance.get("bottlenecks") instanceof List) {
                    List<Object> bottlenecks = (List<Object>) performance.get("bottlenecks");
                    for (Object bottleneck : bottlenecks) {
                        if (bottleneck instanceof String) {
                            String bottleneckStr = (String) bottleneck;
                            if (uniqueBottlenecks.add(bottleneckStr)) {
                                allBottlenecks.add(bottleneckStr);
                            }
                        }
                    }
                }
            }
            
            // Aggregate quality metrics
            if (chunk.containsKey("quality") && chunk.get("quality") instanceof Map) {
                Map<String, Object> quality = (Map<String, Object>) chunk.get("quality");
                
                if (quality.containsKey("maintainabilityScore") && quality.get("maintainabilityScore") != null) {
                    totalMaintainabilityScore += ((Number) quality.get("maintainabilityScore")).doubleValue();
                    maintainabilityScoreCount++;
                }
                
                if (quality.containsKey("readabilityScore") && quality.get("readabilityScore") != null) {
                    totalReadabilityScore += ((Number) quality.get("readabilityScore")).doubleValue();
                    readabilityScoreCount++;
                }
                
                if (quality.containsKey("linesOfCode") && quality.get("linesOfCode") != null) {
                    totalLinesOfCode += ((Number) quality.get("linesOfCode")).intValue();
                }
                
                if (quality.containsKey("complexityScore") && quality.get("complexityScore") != null) {
                    totalComplexity += ((Number) quality.get("complexityScore")).intValue();
                    complexityCount++;
                }
            }
        }
        
        // Build merged result
        merged.put("summary", generateSummary(chunkResults.size(), allIssues.size(), criticalIssues, highIssues));
        merged.put("overallScore", validScores > 0 ? totalScore / validScores : 5.0);
        merged.put("issues", allIssues);
        merged.put("suggestions", allSuggestions);
        merged.put("chunkCount", chunkResults.size());
        
        // Build security object
        Map<String, Object> mergedSecurity = new HashMap<>();
        mergedSecurity.put("securityScore", securityScoreCount > 0 ? totalSecurityScore / securityScoreCount : 7.0);
        mergedSecurity.put("vulnerabilities", allVulnerabilities);
        mergedSecurity.put("hasSecurityIssues", hasSecurityIssues);
        mergedSecurity.put("criticalIssuesCount", criticalIssues);
        mergedSecurity.put("highIssuesCount", highIssues);
        mergedSecurity.put("mediumIssuesCount", mediumIssues);
        mergedSecurity.put("lowIssuesCount", lowIssues);
        merged.put("security", mergedSecurity);
        
        // Build performance object
        Map<String, Object> mergedPerformance = new HashMap<>();
        mergedPerformance.put("performanceScore", performanceScoreCount > 0 ? totalPerformanceScore / performanceScoreCount : 7.0);
        mergedPerformance.put("bottlenecks", allBottlenecks);
        mergedPerformance.put("complexity", determineComplexity(totalComplexity, complexityCount));
        merged.put("performance", mergedPerformance);
        
        // Build quality metrics
        Map<String, Object> mergedQuality = new HashMap<>();
        mergedQuality.put("maintainabilityScore", maintainabilityScoreCount > 0 ? totalMaintainabilityScore / maintainabilityScoreCount : 7.5);
        mergedQuality.put("readabilityScore", readabilityScoreCount > 0 ? totalReadabilityScore / readabilityScoreCount : 8.0);
        mergedQuality.put("linesOfCode", totalLinesOfCode);
        mergedQuality.put("complexityScore", complexityCount > 0 ? totalComplexity / complexityCount : 5);
        mergedQuality.put("testCoverage", 0.0); // Default as we don't analyze tests
        mergedQuality.put("duplicateLines", 0); // Default
        mergedQuality.put("technicalDebt", determineTechnicalDebt(allIssues.size(), totalLinesOfCode));
        merged.put("quality", mergedQuality);
        
        // Add metadata
        if (metadata != null && !metadata.isEmpty()) {
            merged.put("metadata", metadata);
        } else {
            Map<String, Object> defaultMetadata = new HashMap<>();
            defaultMetadata.put("analysisTimestamp", new Date().toString());
            defaultMetadata.put("mergedByLambda", true);
            merged.put("metadata", defaultMetadata);
        }
        
        return merged;
    }
    
    /**
     * Generate comprehensive summary
     */
    private String generateSummary(int chunkCount, int totalIssues, int criticalIssues, int highIssues) {
        StringBuilder summary = new StringBuilder();
        summary.append("Comprehensive analysis completed");
        
        if (chunkCount > 1) {
            summary.append(" across ").append(chunkCount).append(" code segments");
        }
        
        summary.append(". Found ").append(totalIssues).append(" total issues");
        
        if (criticalIssues > 0 || highIssues > 0) {
            summary.append(" including ");
            if (criticalIssues > 0) {
                summary.append(criticalIssues).append(" critical");
                if (highIssues > 0) summary.append(" and ");
            }
            if (highIssues > 0) {
                summary.append(highIssues).append(" high severity");
            }
            summary.append(" issues that require immediate attention");
        }
        
        summary.append(".");
        return summary.toString();
    }
    
    /**
     * Determine complexity level based on score
     */
    private String determineComplexity(int totalComplexity, int count) {
        if (count == 0) return "Medium";
        
        int avgComplexity = totalComplexity / count;
        if (avgComplexity <= 3) return "Low";
        else if (avgComplexity <= 7) return "Medium";
        else if (avgComplexity <= 10) return "High";
        else return "Very High";
    }
    
    /**
     * Determine technical debt based on issues and code size
     */
    private String determineTechnicalDebt(int issueCount, int linesOfCode) {
        if (linesOfCode == 0) return "Medium";
        
        double issuesPerLine = (double) issueCount / linesOfCode;
        if (issuesPerLine < 0.01) return "Low";
        else if (issuesPerLine < 0.05) return "Medium";
        else if (issuesPerLine < 0.1) return "High";
        else return "Very High";
    }
    
    /**
     * Ensure all required fields exist in result
     */
    private void ensureCompleteResult(Map<String, Object> result) {
        // Ensure top-level fields
        if (!result.containsKey("summary")) {
            result.put("summary", "Analysis completed");
        }
        if (!result.containsKey("overallScore")) {
            result.put("overallScore", 5.0);
        }
        if (!result.containsKey("issues")) {
            result.put("issues", new ArrayList<>());
        }
        if (!result.containsKey("suggestions")) {
            result.put("suggestions", new ArrayList<>());
        }
        
        // Ensure security section
        if (!result.containsKey("security")) {
            Map<String, Object> security = new HashMap<>();
            security.put("securityScore", 7.0);
            security.put("vulnerabilities", new ArrayList<>());
            security.put("hasSecurityIssues", false);
            result.put("security", security);
        }
        
        // Ensure performance section
        if (!result.containsKey("performance")) {
            Map<String, Object> performance = new HashMap<>();
            performance.put("performanceScore", 7.0);
            performance.put("bottlenecks", new ArrayList<>());
            performance.put("complexity", "Medium");
            result.put("performance", performance);
        }
        
        // Ensure quality section
        if (!result.containsKey("quality")) {
            Map<String, Object> quality = new HashMap<>();
            quality.put("maintainabilityScore", 7.5);
            quality.put("readabilityScore", 8.0);
            quality.put("linesOfCode", 0);
            quality.put("complexityScore", 5);
            quality.put("testCoverage", 0.0);
            quality.put("duplicateLines", 0);
            quality.put("technicalDebt", "Medium");
            result.put("quality", quality);
        }
    }
    
    /**
     * Save analysis result to DynamoDB
     */
    private void saveAnalysisResult(String analysisId, Map<String, Object> result, Context context) throws Exception {
        context.getLogger().log("Saving merged result to DynamoDB for analysis: " + analysisId);
        
        try {
            // Serialize result to JSON
            String resultJson = objectMapper.writeValueAsString(result);
            
            // Create DynamoDB item
            Item item = new Item()
                .withPrimaryKey("analysisId", analysisId)
                .withString("status", "COMPLETED")
                .withString("message", "Analysis completed successfully")
                .withLong("timestamp", System.currentTimeMillis())
                .withLong("ttl", System.currentTimeMillis() / 1000 + TimeUnit.DAYS.toSeconds(TTL_DAYS))
                .withString("resultJson", resultJson)
                .withJSON("result", resultJson); // Backward compatibility
            
            // Add metadata attributes
            if (result.containsKey("overallScore")) {
                item.withNumber("overallScore", ((Number) result.get("overallScore")).doubleValue());
            }
            if (result.containsKey("issues")) {
                item.withNumber("issueCount", ((List) result.get("issues")).size());
            }
            
            // Save to DynamoDB
            analysisTable.putItem(item);
            
            context.getLogger().log("Successfully saved merged result to DynamoDB");
            
        } catch (Exception e) {
            context.getLogger().log("Error saving to DynamoDB: " + e.getMessage());
            throw new RuntimeException("Failed to save analysis result", e);
        }
    }
    
    /**
     * Update analysis status (for error cases)
     */
    private void updateAnalysisStatus(String analysisId, String status, String message, Map<String, Object> result, Map<String, Object> metadata) {
        try {
            Item item = new Item()
                .withPrimaryKey("analysisId", analysisId)
                .withString("status", status)
                .withString("message", message != null ? message : "Status updated")
                .withLong("timestamp", System.currentTimeMillis())
                .withLong("ttl", System.currentTimeMillis() / 1000 + TimeUnit.DAYS.toSeconds(TTL_DAYS));
            
            if (result != null) {
                String resultJson = objectMapper.writeValueAsString(result);
                item.withString("resultJson", resultJson);
                item.withJSON("result", resultJson);
            }
            
            analysisTable.putItem(item);
            
        } catch (Exception e) {
            System.err.println("Failed to update DynamoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }
}