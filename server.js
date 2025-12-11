// server.js - Express Server & Routes

const express = require('express');
const cors = require('cors');
const axios = require('axios');
require('dotenv').config();

// Import configuration and services = config.js and services.js
const config = require('./config');
const services = require('./services');

// Initialize Express app
const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Calls a function in config to print configuration details when the server starts.
config.logConfig();


//===================== UNIFIED CHATBOT ENDPOINT ====================
app.post('/api/chat', async (req, res) => {
  try {
    const { question, userId = 'anonymous' } = req.body;
// check point -1 
    if (!question) {
      return res.status(400).json({
        success: false,
        error: 'Question is required'
      });
    }

    console.log(`\n${'='.repeat(70)}`);
    console.log(`🗂️ NEW QUESTION: "${question}"`);
    console.log(`🧑User ID: ${userId}`);
    console.log(`${'='.repeat(70)}\n`);

    const session = services.getOrCreateSession(userId);

    // ======================================= Check point - 2 Check for greetings first =======================================
    if (services.isGreeting(question)) {
      console.log(`👋 Detected greeting, returning friendly response`);
      const greetingResponse = services.getGreetingResponse();
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'greeting',
        result: { answer: greetingResponse }
      });
// Send back the greeting response as JSON and stop processing further.
      return res.json({
        success: true,
        answer: greetingResponse,
        system: 'greeting',
        sessionId: session.sessionId
      });
    }
// =======================================  Check point - 3 System routing =======================================
   
// Determine which system should handle the question
    const targetSystem = services.determineSystem(question);

    // ======================. Route to TXQL  ======================

    if (targetSystem === 'txql') {
      console.log(`🧭 Routing to TXQL system...`);
      
      // ========== STEP 1: Preprocess for TXQL license key ==========
      const txqlPreprocessed = services.preprocessTXQLLicenseKey(question);
      
      let txqlKey = null;
      let queryToProcess = question;
      
      if (txqlPreprocessed.hasTXQLKey) {
        console.log(`✅ TXQL query with custom license key detected`);
        txqlKey = txqlPreprocessed.txqlLicenseKey;
        queryToProcess = txqlPreprocessed.actualQuery;
        console.log(`   📝 Processing query: "${queryToProcess}"`);
        console.log(`   🔑 Using custom key for database selection`);
      } else {
        console.log(`ℹ️  No custom key detected - using default QUERY_EXEC_KEY`);
      }
      
      // ========== STEP 2: Execute TXQL query with optional custom key ==========
      const result = await services.queryTXQL(
        queryToProcess,
        session.txqlSessionId,
        3,        // maxRetries
        60000,    // timeout
        txqlKey   // customKey (null if not provided)
      );

      if (result.success) {
        // Check if this is CommLog data
        const data = result.executionResults?.data || [];
        const isCommLogData = data.length > 0 && (
          data[0].hasOwnProperty('CommlogNum') || 
          data[0].hasOwnProperty('CommDateTime') ||
          data[0].hasOwnProperty('PatNum')
        );
        
        if (isCommLogData) {
          console.log(`📋 Detected CommLog data - returning structured format`);
          const structuredData = services.formatCommLogAsStructuredData(data);
          
          session.conversationHistory.push({
            timestamp: new Date(),
            question: question,
            system: 'txql',
            result: {
              ...result,
              structuredData: structuredData,
              customKeyUsed: txqlPreprocessed.hasTXQLKey,
              licenseKey: txqlKey
            }
          });

          return res.json({
            success: true,
            answer: `Found ${data.length} communication records for this patient`,
            sqlQuery: result.sqlQuery,
            executionResults: result.executionResults,
            structuredData: structuredData,
            customKeyUsed: txqlPreprocessed.hasTXQLKey,
            licenseKey: txqlKey ? txqlKey.substring(0, 40) + '...' : null,
            system: 'txql',
            sessionId: session.sessionId
          });
        }
        
        // For non-CommLog data, return as before
        session.conversationHistory.push({
          timestamp: new Date(),
          question: question,
          system: 'txql',
          result: {
            ...result,
            customKeyUsed: txqlPreprocessed.hasTXQLKey,
            licenseKey: txqlKey
          }
        });

        return res.json({
          success: true,
          answer: result.data,
          sqlQuery: result.sqlQuery,
          executionResults: result.executionResults,
          customKeyUsed: txqlPreprocessed.hasTXQLKey,
          licenseKey: txqlKey ? txqlKey.substring(0, 40) + '...' : null,
          system: 'txql',
          sessionId: session.sessionId
        });
      } else {
        return res.status(500).json({
          success: false,
          error: result.friendlyError,
          customKeyUsed: txqlPreprocessed.hasTXQLKey,
          licenseKey: txqlKey ? txqlKey.substring(0, 40) + '...' : null,
          system: 'txql',
          sessionId: session.sessionId
        });
      }
    }

    // =========================  Route to AI Voice ========================

    
    // Route to AI Voice
if (targetSystem === 'aivoice') {
  console.log(`🟢 Routing to AI Voice system...`);

  // ========== STEP 1: PREPROCESS LICENSE KEY ==========
  const preprocessed = services.preprocessLicenseKeyFromQuestion(question);
  
  // Use the ACTUAL QUERY (without license key prefix) for all processing
  const processQuery = preprocessed.actualQuery;
  const extractedLicenseKey = preprocessed.licenseKey;
  
  console.log(`\n📊 PREPROCESSING RESULT:`);
  console.log(`   Has License Key: ${preprocessed.hasLicenseKey}`);
  if (preprocessed.hasLicenseKey) {
    console.log(`   Extracted Key: ${extractedLicenseKey.substring(0, 40)}...`);
    console.log(`   Processing Query: "${processQuery}"`);
  }

  // Resolve context from conversation history (using processed query)
  const context = services.resolveFromContext(processQuery, session);
  
  // ========== INTELLIGENT ROUTING SYSTEM ==========
  // STEP 2: Classify Intent (using processed query)
  const intent = services.classifyAIVoiceIntent(processQuery);
  
  // STEP 3: Extract Entities (using processed query)
  const entities = services.extractCallEntities(processQuery);
  
  // STEP 4: Check for special handlers
  const isDirectionQuery = config.isCallDirectionQuery(processQuery);
  
  // STEP 5: Determine if we can handle with direct analysis
  const hasExtractedEntities = Object.values(entities).some(v => v !== null);
  const canHandleDirectly = intent && (
    intent === 'filter' || 
    intent === 'count' || 
    intent === 'content' ||
    intent === 'analysis' ||
    hasExtractedEntities
  );
  
  console.log(`   🎯 Can handle directly: ${canHandleDirectly} (intent: ${intent}, has entities: ${hasExtractedEntities})`);
  
  // ========== SPECIAL CASE: LICENSE KEY IN QUESTION ==========
  if (preprocessed.hasLicenseKey) {
    console.log(`\n🔑 LICENSE KEY QUERY DETECTED - Special Handler`);
    console.log(`   Strategy: Fetch with hardcoded key → Filter by extracted key`);
    
    // Get date range
    const startDate = entities.dates?.startDate || 
                     context.dates?.startDate || 
                     config.getCurrentDateInTimezone(config.TIMEZONE);
    const endDate = entities.dates?.endDate || 
                   context.dates?.endDate || 
                   config.getCurrentDateInTimezone(config.TIMEZONE);
    
    console.log(`   📅 Date range: ${startDate} to ${endDate}`);
    console.log(`   🔑 Hardcoded key used for API: ${config.HARDCODED_LICENSE_KEY.substring(0, 20)}...`);
    console.log(`   🔍 Will filter results by: ${extractedLicenseKey.substring(0, 40)}...`);
    
    // STEP 1: Fetch ALL calls using HARDCODED LICENSE KEY
    const callResult = await services.fetchCallDetails(startDate, endDate, false, false);
    
    if (!callResult.success) {
      return res.status(500).json({
        success: false,
        error: callResult.error,
        friendlyError: callResult.friendlyError,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    if (!callResult.rawData || callResult.rawData.length === 0) {
      const answer = `No calls found for the date range ${startDate} to ${endDate}.`;
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: { 
          success: true, 
          answer: answer,
          dateRange: { startDate, endDate },
          extractedLicenseKey: extractedLicenseKey
        }
      });
      
      return res.json({
        success: true,
        answer: answer,
        callHistory: [],
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    console.log(`   ✅ Fetched ${callResult.rawData.length} total calls`);
    
    // STEP 2: Filter by extracted license key
    const licenseKeyResult = services.getCallsForLicenseKey(
      callResult.rawData, 
      extractedLicenseKey
    );
    
    if (!licenseKeyResult.found || licenseKeyResult.count === 0) {
      // No calls found for this license key
      let answer = `❌ **No Calls Found**\n\n`;
      answer += `**License Key:** \`${extractedLicenseKey.substring(0, 40)}...\`\n`;
      answer += `**Date Range:** ${startDate} to ${endDate}\n\n`;
      answer += `**Query:** "${processQuery}"\n\n`;
      
      if (licenseKeyResult.availableLicenseKeys && licenseKeyResult.availableLicenseKeys.length > 0) {
        answer += `**💡 Available License Keys for this date range:**\n\n`;
        licenseKeyResult.availableLicenseKeys.slice(0, 10).forEach((key, idx) => {
          answer += `${idx + 1}. \`${key}\`\n`;
        });
        
        if (licenseKeyResult.availableLicenseKeys.length > 10) {
          answer += `\n*...and ${licenseKeyResult.availableLicenseKeys.length - 10} more*\n`;
        }
      }
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: { 
          success: true, 
          answer: answer,
          dateRange: { startDate, endDate },
          extractedLicenseKey: extractedLicenseKey,
          found: false
        }
      });
      
      return res.json({
        success: true,
        answer: answer,
        callHistory: [],
        metadata: {
          licenseKey: extractedLicenseKey,
          found: false,
          totalCallsFetched: callResult.rawData.length,
          callsMatchingKey: 0
        },
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    // STEP 3: Build response based on intent
    console.log(`   ✅ Found ${licenseKeyResult.count} calls for license key`);
    
    let answer;
    
    if (intent === 'count' || processQuery.toLowerCase().includes('how many')) {
      // COUNT INTENT: Show statistics
      answer = `## 🔑 License Key Query Results\n\n`;
      answer += `**License Key:** \`${extractedLicenseKey.substring(0, 40)}...\`\n`;
      answer += `**Date Range:** ${startDate} to ${endDate}\n`;
      answer += `**Query:** "${processQuery}"\n\n`;
      answer += `---\n\n`;
      answer += `### 📊 Statistics\n\n`;
      answer += `- **Total Calls:** ${licenseKeyResult.count}\n`;
      answer += `- **Total Cost:** $${licenseKeyResult.totalCost.toFixed(2)}\n`;
      answer += `- **Average Duration:** ${licenseKeyResult.avgDuration} seconds\n\n`;
      
      // Add breakdown by call direction if available
      const inbound = licenseKeyResult.calls.filter(c => c.callDirection === 'inbound').length;
      const outbound = licenseKeyResult.calls.filter(c => c.callDirection === 'outbound').length;
      
      if (inbound > 0 || outbound > 0) {
        answer += `### 📞 Call Direction\n\n`;
        answer += `- **Inbound:** ${inbound}\n`;
        answer += `- **Outbound:** ${outbound}\n\n`;
      }
      
    } else if (intent === 'content') {
      // CONTENT INTENT: Show detailed call information
      answer = services.formatCallDetails(licenseKeyResult.calls, processQuery);
      
    } else {
      // FILTER/ANALYSIS INTENT: Show comprehensive analysis
      answer = services.buildComprehensiveCallAnalysis(licenseKeyResult.calls, processQuery);
    }
    
    const result = {
      success: true,
      answer: answer,
      callHistory: licenseKeyResult.calls,
      licenseKeyStats: licenseKeyResult,
      system: 'aivoice',
      dateRange: { startDate, endDate },
      extractedLicenseKey: extractedLicenseKey,
      metadata: {
        intent: intent,
        totalCallsFetched: callResult.rawData.length,
        callsMatchingKey: licenseKeyResult.count
      }
    };
    
    session.conversationHistory.push({
      timestamp: new Date(),
      question: question,
      system: 'aivoice',
      result: result
    });

    return res.json({
      success: true,
      answer: result.answer,
      callHistory: result.callHistory || [],
      licenseKeyStats: result.licenseKeyStats,
      metadata: result.metadata,
      system: 'aivoice',
      sessionId: session.sessionId
    });
  }
  
  // ========== HANDLER 1: Call Direction Queries (NO LICENSE KEY) ==========
  if (isDirectionQuery) {
    console.log(`📞 Detected call direction query (bypassing OpenAI)`);
    
    const startDate = context.dates?.startDate || config.getCurrentDateInTimezone(config.TIMEZONE);
    const endDate = context.dates?.endDate || config.getCurrentDateInTimezone(config.TIMEZONE);
    
    console.log(`   Date range: ${startDate} to ${endDate}`);
    
    const callResult = await services.fetchCallDetails(startDate, endDate, false, false);
    
    if (!callResult.success) {
      return res.status(500).json({
        success: false,
        error: callResult.error,
        friendlyError: callResult.friendlyError,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    if (!callResult.rawData || callResult.rawData.length === 0) {
      const answer = `No calls found for the date range ${startDate} to ${endDate}.`;
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: { 
          success: true, 
          answer: answer,
          dateRange: { startDate, endDate }
        }
      });
      
      return res.json({
        success: true,
        answer: answer,
        callHistory: [],
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    const directionStats = services.countCallsByDirection(callResult.rawData);
    const summary = services.buildCallDirectionSummary(directionStats, { startDate, endDate });
    
    const result = {
      success: true,
      answer: summary,
      callHistory: callResult.data,
      directionStats: directionStats,
      system: 'aivoice',
      dateRange: { startDate, endDate }
    };
    
    session.conversationHistory.push({
      timestamp: new Date(),
      question: question,
      system: 'aivoice',
      result: result
    });

    return res.json({
      success: true,
      answer: result.answer,
      callHistory: result.callHistory || [],
      directionStats: result.directionStats,
      system: 'aivoice',
      sessionId: session.sessionId
    });
  }
  
  // ========== HANDLER 2: Intelligent Entity-Based Analysis ==========
  // ========== HANDLER 2: Intelligent Entity-Based Analysis (NO LICENSE KEY) ==========
  if (canHandleDirectly && !isDirectionQuery) {
    console.log(`🎯 Using intelligent entity-based analysis`);
    
    // Get date range (from entities, context, or current question)
    const startDate = entities.dates?.startDate || 
                     context.dates?.startDate || 
                     config.getCurrentDateInTimezone(config.TIMEZONE);
    const endDate = entities.dates?.endDate || 
                   context.dates?.endDate || 
                   config.getCurrentDateInTimezone(config.TIMEZONE);
    
    console.log(`   📅 Date range: ${startDate} to ${endDate}`);
    
    // Fetch call data (include transcripts if content intent)
    const includeTranscripts = intent === 'content';
    const callResult = await services.fetchCallDetails(startDate, endDate, includeTranscripts, false);
    
    if (!callResult.success) {
      return res.status(500).json({
        success: false,
        error: callResult.error,
        friendlyError: callResult.friendlyError,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    if (!callResult.rawData || callResult.rawData.length === 0) {
      const answer = `No calls found for the date range ${startDate} to ${endDate}.`;
      
      return res.json({
        success: true,
        answer: answer,
        callHistory: [],
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    // Filter using intelligent entity-based system
    const filteredCalls = services.filterCallsByEntities(callResult.rawData, entities);
    
    // Build response based on intent (use processQuery instead of question)
    let analysis;
    if (intent === 'content') {
      // User wants transcripts, summaries, or detailed content
      analysis = services.formatCallDetails(filteredCalls, processQuery);
    } else {
      // User wants analysis, statistics, or filtered lists
      analysis = services.buildComprehensiveCallAnalysis(filteredCalls, processQuery);
    }
    
    const result = {
      success: true,
      answer: analysis,
      callHistory: filteredCalls,
      system: 'aivoice',
      dateRange: { startDate, endDate },
      intent: intent,
      entities: entities,
      filteredCount: filteredCalls.length,
      totalCount: callResult.rawData.length
    };
    
    session.conversationHistory.push({
      timestamp: new Date(),
      question: question,
      system: 'aivoice',
      result: result
    });

    return res.json({
      success: true,
      answer: result.answer,
      callHistory: result.callHistory || [],
      metadata: {
        intent: intent,
        filteredCount: result.filteredCount,
        totalCount: result.totalCount
      },
      system: 'aivoice',
      sessionId: session.sessionId
    });
  }
  
  // ========== HANDLER 3: License Key Queries ==========
  if (isLicenseQuery) {
    console.log(`🔑 Detected license key query (bypassing OpenAI)`);
    
    const startDate = context.dates?.startDate || config.getCurrentDateInTimezone(config.TIMEZONE);
    const endDate = context.dates?.endDate || config.getCurrentDateInTimezone(config.TIMEZONE);
    
    console.log(`   Date range: ${startDate} to ${endDate}`);
    
    const callResult = await services.fetchCallDetails(startDate, endDate, false, false);
    
    if (!callResult.success) {
      return res.status(500).json({
        success: false,
        error: callResult.error,
        friendlyError: callResult.friendlyError,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    if (!callResult.rawData || callResult.rawData.length === 0) {
      const answer = `No calls found for the date range ${startDate} to ${endDate}.`;
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: { 
          success: true, 
          answer: answer,
          dateRange: { startDate, endDate },
          licenseKey: extractedKey
        }
      });
      
      return res.json({
        success: true,
        answer: answer,
        callHistory: [],
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
    
    if (extractedKey) {
      console.log(`   Looking for specific license key: ${extractedKey.substring(0, 20)}...`);
      const licenseKeyResult = services.getCallsForLicenseKey(callResult.rawData, extractedKey);
      
      let answer;
      if (licenseKeyResult.found) {
        answer = `🔑 **License Key Analysis**\n\n`;
        answer += `**License Key:** \`${licenseKeyResult.licenseKey.substring(0, 40)}...\`\n`;
        answer += `**Date Range:** ${startDate} to ${endDate}\n\n`;
        answer += `📊 **Statistics:**\n`;
        answer += `- Total Calls: **${licenseKeyResult.count}**\n`;
        answer += `- Total Cost: **$${licenseKeyResult.totalCost.toFixed(2)}**\n`;
        answer += `- Average Duration: **${licenseKeyResult.avgDuration} seconds**\n`;
      } else {
        answer = `❌ **No Calls Found**\n\n`;
        answer += `**License Key:** \`${extractedKey.substring(0, 40)}...\`\n`;
        answer += `**Date Range:** ${startDate} to ${endDate}\n\n`;
        
        if (licenseKeyResult.availableLicenseKeys && licenseKeyResult.availableLicenseKeys.length > 0) {
          answer += `**💡 Available License Keys for this date range:**\n`;
          licenseKeyResult.availableLicenseKeys.forEach((key, idx) => {
            answer += `${idx + 1}. \`${key}\`\n`;
          });
        }
      }
      
      const result = {
        success: true,
        answer: answer,
        callHistory: licenseKeyResult.calls || [],
        licenseKeyStats: licenseKeyResult,
        system: 'aivoice',
        dateRange: { startDate, endDate },
        licenseKey: extractedKey
      };
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: result
      });

      return res.json({
        success: true,
        answer: result.answer,
        callHistory: result.callHistory || [],
        licenseKeyStats: result.licenseKeyStats,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    } else {
      console.log(`   Generating license key breakdown`);
      const licenseKeySummary = services.buildLicenseKeySummary(callResult.rawData);
      const grouped = services.groupCallsByLicenseKey(callResult.rawData);
      
      const result = {
        success: true,
        answer: licenseKeySummary,
        callHistory: callResult.data,
        licenseKeyBreakdown: grouped,
        system: 'aivoice',
        dateRange: { startDate, endDate }
      };
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'aivoice',
        result: result
      });

      return res.json({
        success: true,
        answer: result.answer,
        callHistory: result.callHistory || [],
        licenseKeyBreakdown: result.licenseKeyBreakdown,
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }
  }
  
  // If we get here, it's a general AI Voice question that needs OpenAI
  // Return a friendly message if OpenAI is not configured
  if (!config.OPENAI_API_KEY) {
    return res.status(500).json({
      success: false,
      error: 'AI Voice system requires OpenAI API key for this type of query',
      friendlyError: 'I can help with specific queries like:\n- "How many calls today?"\n- "Show calls with upsell opportunities"\n- "Which calls had negative sentiment?"\n\nFor general conversational questions, OpenAI API key is required.'
    });
  }
  
  // For other general questions, respond with a helpful message
  return res.json({
    success: true,
    answer: `I can help you analyze call data! Try asking:\n\n📊 **Analytics:**\n- "How many calls today?"\n- "Show calls by license key"\n- "Which calls had upsell opportunities?"\n\n📝 **Content:**\n- "Show transcripts for today"\n- "Give me call summaries"\n- "Show AI review feedback"\n\n🎯 **Filtering:**\n- "Show calls with negative sentiment"\n- "Which calls need follow-up?"\n- "Show unsuccessful appointments"`,
    system: 'aivoice',
    sessionId: session.sessionId
  });
}

  } catch (error) {
    console.error('❌ Error in unified chat:', error);
    res.status(500).json({
      success: false,
      error: 'An unexpected error occurred',
      friendlyError: 'Something went wrong. Please try again.',
      technicalError: error.message
    });
  }
});

// ==================== COMMLOG ANALYSIS ENDPOINT ====================

/**
 * Endpoint: Analyze CommLog data for a patient
 */
app.post('/api/commlog/analyze', async (req, res) => {
  try {
    const { patNum, startDate, endDate } = req.body;
    
    if (!patNum) {
      return res.status(400).json({
        success: false,
        error: 'patNum is required'
      });
    }
    
    // Build SQL query
    let query = `SELECT * FROM commlog WHERE PatNum = ${patNum}`;
    
    if (startDate && endDate) {
      query += ` AND CommDateTime BETWEEN '${startDate}' AND '${endDate}'`;
    }
    
    query += ' ORDER BY CommDateTime DESC LIMIT 1000';
    
    console.log('ðŸ“Š Executing CommLog query:', query);
    
    // Execute query via TXQL
    const session = services.getOrCreateSession('commlog_analysis');
    const result = await services.queryTXQL(query, session.txqlSessionId, 3, 60000);
    
    if (!result.success) {
      return res.status(500).json({
        success: false,
        error: 'Failed to fetch CommLog data',
        details: result.friendlyError
      });
    }
    
    // Parse the execution results
    const records = result.executionResults?.data || [];
    
    if (records.length === 0) {
      return res.json({
        success: true,
        message: 'No communication records found',
        patNum: patNum,
        recordCount: 0
      });
    }
    
    // Analyze the records
    const analysis = services.analyzeCommLog(records);
    const phoneCallDetails = services.extractPhoneCallDetails(records);
    
    return res.json({
      success: true,
      patNum: patNum,
      recordCount: records.length,
      analysis: analysis,
      phoneCalls: phoneCallDetails,
      formattedRecords: analysis.formattedRecords.slice(0, 50), // Clean, readable format
      rawRecords: records.slice(0, 50) // Original data if needed
    });
    
  } catch (error) {
    console.error('âŒ Error in CommLog analysis:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message
    });
  }
});

// ==================== LEGACY ENDPOINTS ====================

// New endpoint for license key analysis
app.post('/api/aivoice/license-keys', async (req, res) => {
  try {
    const { startDate, endDate, licenseKey } = req.body;

    if (!startDate || !endDate) {
      return res.status(400).json({
        success: false,
        error: 'startDate and endDate are required'
      });
    }

    console.log(`\nðŸ”‘ License Key Analysis Request`);
    console.log(`   Date Range: ${startDate} to ${endDate}`);
    if (licenseKey) {
      console.log(`   Specific License Key: ${licenseKey.substring(0, 20)}...`);
    }

    const callResult = await services.fetchCallDetails(startDate, endDate, false, false);

    if (!callResult.success) {
      return res.status(500).json({
        success: false,
        error: callResult.error,
        friendlyError: callResult.friendlyError
      });
    }

    if (!callResult.rawData || callResult.rawData.length === 0) {
      return res.json({
        success: true,
        message: 'No calls found for the specified date range',
        data: []
      });
    }

    if (licenseKey) {
      // Analyze specific license key
      const result = services.getCallsForLicenseKey(callResult.rawData, licenseKey);
      
      return res.json({
        success: result.found,
        message: result.message,
        licenseKey: licenseKey,
        callCount: result.count,
        totalCost: result.totalCost,
        avgDuration: result.avgDuration,
        calls: result.calls
      });
    } else {
      // Return breakdown of all license keys
      const grouped = services.groupCallsByLicenseKey(callResult.rawData);
      const summary = services.buildLicenseKeySummary(callResult.rawData);
      
      return res.json({
        success: true,
        summary: summary,
        breakdown: grouped,
        totalCalls: callResult.rawData.length,
        uniqueLicenseKeys: Object.keys(grouped).length
      });
    }
  } catch (error) {
    console.error('âŒ Error in license key analysis:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message
    });
  }
});

// ==================== LEGACY ENDPOINTS ====================

app.post('/api/txql/chat', async (req, res) => {
  const { question, userId = 'anonymous', maxRetries = 3, timeout = 60000 } = req.body;

  if (!question) {
    return res.status(400).json({
      success: false,
      error: 'Question is required'
    });
  }

  const session = services.getOrCreateSession(userId);
  const result = await services.queryTXQL(question, session.txqlSessionId, maxRetries, timeout);

  if (result.success) {
    res.json({
      success: true,
      answer: result.data,
      sqlQuery: result.sqlQuery,
      executionResults: result.executionResults,
      sessionId: session.sessionId
    });
  } else {
    res.status(500).json({
      success: false,
      error: result.friendlyError,
      sessionId: session.sessionId
    });
  }
});

let callHistory = [];

app.post('/api/ask', async (req, res) => {
  try {
    const { question, messages = [] } = req.body;

    if (!question || !config.OPENAI_API_KEY) {
      return res.status(400).json({ error: 'Invalid request' });
    }

    const prefilledDates = config.detectSingleDateFromQuestion(question);
    const tools = [{
      type: 'function',
      function: {
        name: 'aivoice_get_call_details',
        description: 'Fetches AI Voice call records',
        parameters: {
          type: 'object',
          properties: {
            startDate: { type: 'string' },
            endDate: { type: 'string' },
            includeTranscript: { type: 'boolean', default: false },
            includeAudio: { type: 'boolean', default: false }
          },
          required: ['startDate', 'endDate']
        }
      }
    }];

    const conversationMessages = [
      {
        role: 'system',
        content: 'You are an AI assistant for PatientXpress AI voice call analysis.'
      },
      ...messages.filter(m => m.role !== 'system').map(m => ({
        role: m.role,
        content: m.content || m.text || ''
      })),
      {
        role: 'user',
        content: prefilledDates ? `${question}\n\n(Dates: ${prefilledDates.startDate} to ${prefilledDates.endDate})` : question
      }
    ];

    let aiResponse = await services.callOpenAI(conversationMessages, tools);
    let responseMessage = aiResponse.choices[0].message;

    if (responseMessage.tool_calls) {
      const toolCall = responseMessage.tool_calls[0];
      const functionArgs = JSON.parse(toolCall.function.arguments || '{}');
      const startDate = functionArgs.startDate || prefilledDates?.startDate;
      const endDate = functionArgs.endDate || prefilledDates?.endDate;

      if (startDate && endDate) {
        const result = await services.fetchCallDetails(startDate, endDate, functionArgs.includeTranscript, functionArgs.includeAudio);
        
        conversationMessages.push(responseMessage);
        conversationMessages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          name: 'aivoice_get_call_details',
          content: JSON.stringify({ success: result.success, summary: result.summary, count: result.count })
        });

        aiResponse = await services.callOpenAI(conversationMessages, tools);
        responseMessage = aiResponse.choices[0].message;

        if (result.success) {
          callHistory = result.data;
        }
      }
    }

    res.json({
      answer: responseMessage.content || 'No response',
      callHistory
    });

  } catch (error) {
    console.error('Error in /api/ask:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ==================== UTILITY ENDPOINTS ====================

app.get('/api/txql/session', (req, res) => {
  const { userId = 'anonymous' } = req.query;
  const session = services.getSession(userId);

  if (!session) {
    return res.json({ success: false, message: 'No active session' });
  }

  res.json({
    success: true,
    session: {
      sessionId: session.sessionId,
      createdAt: session.createdAt,
      lastActive: session.lastActive,
      messageCount: session.conversationHistory.length,
      recentMessages: session.conversationHistory.slice(-10)
    }
  });
});

app.delete('/api/txql/session', (req, res) => {
  const { userId = 'anonymous' } = req.body;
  const key = `user_${userId}`;
  
  if (services.activeSessions.has(key)) {
    services.activeSessions.delete(key);
    res.json({ success: true, message: 'Session cleared' });
  } else {
    res.json({ success: false, message: 'No session found' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    activeSessions: services.activeSessions.size,
    systems: {
      aivoice: 'AI Voice Call Analysis',
      txql: 'SQL Database Queries (with execution)',
      greeting: 'Greeting Handler'
    }
  });
});

// Start server
app.listen(config.PORT, async () => {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`ðŸš€ UNIFIED CHATBOT SERVER (WITH SQL EXECUTION + COMMLOG ANALYSIS)`);
  console.log(`${'='.repeat(70)}`);
  console.log(`\nðŸ“¡ Main Endpoint (Auto-routes to correct system):`);
  console.log(`   â†’ POST http://localhost:${config.PORT}/api/chat`);
  console.log(`\nðŸ“ž NEW: CommLog Analysis`);
  console.log(`   â†’ POST http://localhost:${config.PORT}/api/commlog/analyze`);
  console.log(`   Analyze patient communication records (calls, texts, emails)`);
  console.log(`\nðŸ”‘ License Key Analysis`);
  console.log(`   â†’ POST http://localhost:${config.PORT}/api/aivoice/license-keys`);
  console.log(`   Analyze call distribution by license key`);
  console.log(`\nâœ¨ SQL Query Execution`);
  console.log(`   TXQL returns SQL â†’ Executes via query.8px.us â†’ Shows actual results!`);
  console.log(`\nâ° Timezone Configuration:`);
  console.log(`   Current timezone: ${config.TIMEZONE}`);
  console.log(`   Current date: ${config.getCurrentDateInTimezone(config.TIMEZONE)}`);
  console.log(`\n 🧭TXQL System (Database Queries):`);
  console.log(`   Questions: users, tables, orders, California, age, etc.`);
  console.log(`\n 🎙️ AI Voice System (Call Analysis):`);
  console.log(`   Questions: calls, appointments, patients, sentiment, license keys, etc.`);
  console.log(`\n 📋 Legacy Endpoints:`);
  console.log(`   â†’ POST http://localhost:${config.PORT}/api/txql/chat (TXQL only)`);
  console.log(`   â†’ POST http://localhost:${config.PORT}/api/ask (AI Voice only)`);
  console.log(`\nðŸ’š Health: http://localhost:${config.PORT}/api/health`);
  console.log(`\n${'='.repeat(70)}\n`);
  
  if (!config.OPENAI_API_KEY) {
    console.warn('âš ï¸  WARNING: config.OPENAI_API_KEY not set (AI Voice will not work)');
  }
  
  console.log(`ðŸŽ¯ FEATURE: SQL queries are now executed and results displayed!`);
  console.log(`   Auth: ${config.QUERY_EXEC_AUTH}`);
  console.log(`   Key: ${config.QUERY_EXEC_KEY}\n`);
  
  // Test both services on startup
  const txqlOk = await services.testTXQLConnection();
  const aiVoiceOk = await services.testAIVoiceConnection();
  
  console.log(`\n${'='.repeat(70)}`);
  console.log(`ðŸ“Š SERVICE STATUS SUMMARY`);
  console.log(`${'='.repeat(70)}`);
  console.log(`   TXQL Database Service:     ${txqlOk ? 'âœ… ONLINE' : 'âŒ OFFLINE'}`);
  console.log(`   AI Voice Call Service:     ${aiVoiceOk ? 'âœ… ONLINE' : 'âŒ OFFLINE'}`);
  
  if (!txqlOk || !aiVoiceOk) {
    console.log(`\nâš ï¸  WARNING: Some services are unavailable`);
    if (!txqlOk) {
      console.log(`   â€¢ Database queries will not work`);
    }
    if (!aiVoiceOk) {
      console.log(`   â€¢ Call analysis will not work`);
    }
  } else {
    console.log(`\nâœ… All systems operational!`);
  }
  
  console.log(`${'='.repeat(70)}\n`);});