// server.js - Express Server & Routes
// This file sets up the Express server and defines all API endpoints

const express = require('express');
const cors = require('cors');
const axios = require('axios');
require('dotenv').config();

// Import configuration and services
const config = require('./config');
const services = require('./services');

// Initialize Express app
const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Log configuration on startup
config.logConfig();

app.post('/api/chat', async (req, res) => {
  try {
    const { question, userId = 'anonymous' } = req.body;

    if (!question) {
      return res.status(400).json({
        success: false,
        error: 'Question is required'
      });
    }

    console.log(`\n${'='.repeat(70)}`);
    console.log(`ðŸ“© NEW QUESTION: "${question}"`);
    console.log(`ðŸ‘¤ User ID: ${userId}`);
    console.log(`${'='.repeat(70)}\n`);

    const session = services.getOrCreateSession(userId);

    // Check for greetings first
    if (services.isGreeting(question)) {
      console.log(`ðŸ‘‹ Detected greeting, returning friendly response`);
      const greetingResponse = services.getGreetingResponse();
      
      session.conversationHistory.push({
        timestamp: new Date(),
        question: question,
        system: 'greeting',
        result: { answer: greetingResponse }
      });

      return res.json({
        success: true,
        answer: greetingResponse,
        system: 'greeting',
        sessionId: session.sessionId
      });
    }

    // Determine which system should handle the question
    const targetSystem = services.determineSystem(question);

    // Route to TXQL
    if (targetSystem === 'txql') {
      console.log(`ðŸ”µ Routing to TXQL system...`);
      const result = await services.queryTXQL(question, session.txqlSessionId);

      if (result.success) {
        // Check if this is CommLog data
        const data = result.executionResults?.data || [];
        const isCommLogData = data.length > 0 && (
          data[0].hasOwnProperty('CommlogNum') || 
          data[0].hasOwnProperty('CommDateTime') ||
          data[0].hasOwnProperty('PatNum')
        );
        
        if (isCommLogData) {
          console.log(`ðŸ“‹ Detected CommLog data - returning structured format`);
          const structuredData = services.formatCommLogAsStructuredData(data);
          
          session.conversationHistory.push({
            timestamp: new Date(),
            question: question,
            system: 'txql',
            result: {
              ...result,
              structuredData: structuredData
            }
          });

          return res.json({
            success: true,
            answer: `Found ${data.length} communication records for this patient`,
            sqlQuery: result.sqlQuery,
            executionResults: result.executionResults,
            structuredData: structuredData,  // Add structured data for React components
            system: 'txql',
            sessionId: session.sessionId
          });
        }
        
        // For non-CommLog data, return as before
        session.conversationHistory.push({
          timestamp: new Date(),
          question: question,
          system: 'txql',
          result: result
        });

        return res.json({
          success: true,
          answer: result.data,
          sqlQuery: result.sqlQuery,
          executionResults: result.executionResults,
          system: 'txql',
          sessionId: session.sessionId
        });
      } else {
        return res.status(500).json({
          success: false,
          error: result.friendlyError,
          system: 'txql',
          sessionId: session.sessionId
        });
      }
    }

    // Route to AI Voice
    if (targetSystem === 'aivoice') {
      console.log(`ðŸŸ¢ Routing to AI Voice system...`);

      // Check if this is a license key query FIRST (before OpenAI)
      const isLicenseQuery = config.isLicenseKeyQuery(question);
      
      // Check if this is a call direction query (inbound/outbound)
      const isDirectionQuery = config.isCallDirectionQuery(question);
      
      // Resolve context from conversation history
      const context = services.resolveFromContext(question, session);
      
      // Use context-resolved values or extract from current question
      const extractedKey = context.licenseKey || services.extractLicenseKey(question);
      
      if (isDirectionQuery) {
        console.log(`📞 Detected call direction query (bypassing OpenAI)`);
        
        // Use resolved dates from context or current question
        const startDate = context.dates?.startDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        const endDate = context.dates?.endDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        
        console.log(`   Date range: ${startDate} to ${endDate}`);
        
        // Fetch call data
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
        
        // Count calls by direction
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
      
      if (isLicenseQuery) {
        console.log(`ðŸ”‘ Detected license key query (bypassing OpenAI)`);
        
        // Use resolved dates from context or current question
        const startDate = context.dates?.startDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        const endDate = context.dates?.endDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        
        // Log context usage
        if (context.dates && !config.detectSingleDateFromQuestion(question)) {
          console.log(`   ðŸ” Using date from conversation history: ${startDate} to ${endDate}`);
        }
        if (context.licenseKey && !services.extractLicenseKey(question)) {
          console.log(`   ðŸ” Using license key from conversation history: ${extractedKey.substring(0, 20)}...`);
        }
        
        console.log(`   Date range: ${startDate} to ${endDate}`);
        
        // Fetch call data
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
          // User is asking about a specific license key
          console.log(`   Looking for specific license key: ${extractedKey.substring(0, 20)}...`);
          const licenseKeyResult = services.getCallsForLicenseKey(callResult.rawData, extractedKey);
          
          let answer;
          if (licenseKeyResult.found) {
            answer = `ðŸ”‘ **License Key Analysis**\n\n`;
            answer += `**License Key:** \`${licenseKeyResult.licenseKey.substring(0, 40)}...\`\n`;
            answer += `**Date Range:** ${startDate} to ${endDate}\n\n`;
            answer += `ðŸ“Š **Statistics:**\n`;
            answer += `- Total Calls: **${licenseKeyResult.count}**\n`;
            answer += `- Total Cost: **$${licenseKeyResult.totalCost.toFixed(2)}**\n`;
            answer += `- Average Duration: **${licenseKeyResult.avgDuration} seconds**\n`;
          } else {
            // Build helpful error message with available license keys
            let answer = `❌ **No Calls Found**\n\n`;
            answer += `**License Key:** \`${extractedKey.substring(0, 40)}...\`\n`;
            answer += `**Date Range:** ${startDate} to ${endDate}\n\n`;
            
            if (licenseKeyResult.availableLicenseKeys && licenseKeyResult.availableLicenseKeys.length > 0) {
              answer += `**💡 Available License Keys for this date range:**\n`;
              licenseKeyResult.availableLicenseKeys.forEach((key, idx) => {
                answer += `${idx + 1}. \`${key}\`\n`;
              });
              answer += `\n**Possible Issues:**\n`;
              answer += `- The license key format might be different in the database\n`;
              answer += `- This office might not have any calls for the selected date range\n`;
              answer += `- Try selecting a different office from the dropdown\n`;
            } else {
              answer += `No calls found for license key \`${extractedKey.substring(0, 40)}...\` in the date range ${startDate} to ${endDate}.`;
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
          // User is asking for a breakdown by all license keys
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

      // Not a license key query or direction query, proceed with normal OpenAI flow
      // Check if OpenAI API key is configured for general AI Voice questions
      if (!config.OPENAI_API_KEY) {
        return res.status(500).json({
          success: false,
          error: 'AI Voice system is not configured (missing OpenAI API key)',
          friendlyError: 'AI Voice system is currently unavailable for general questions. However, I can still help you with specific queries about call counts, inbound/outbound calls, or license key breakdowns without OpenAI.\n\nExamples:\n- "How many inbound calls did we get today?"\n- "Show me calls by license key"\n- "How many calls yesterday?"'
        });
      }
      
      const prefilledDates = config.detectSingleDateFromQuestion(question);
      
      // Build conversation history for OpenAI context
      const conversationMessages = [
        {
          role: 'system',
          content: 'You are an AI assistant for PatientXpress AI voice call analysis. Provide clear, concise answers. IMPORTANT: When users ask follow-up questions without specifying dates, you MUST use the date range from the most recent query in the conversation. Look for date context provided in the user message.'
        }
      ];
      
      // Add recent conversation history for context (last 3 exchanges)
      const recentHistory = session.conversationHistory.slice(-3);
      for (const entry of recentHistory) {
        if (entry.system === 'aivoice' && entry.question) {
          conversationMessages.push({
            role: 'user',
            content: entry.question
          });
          
          if (entry.result?.answer) {
            conversationMessages.push({
              role: 'assistant',
              content: entry.result.answer
            });
          }
        }
      }
      
      // Resolve date context from history if not in current question
      const contextDates = prefilledDates || (() => {
        const recentDate = [...recentHistory].reverse().find(entry => 
          entry.system === 'aivoice' && entry.result?.dateRange
        );
        return recentDate?.result?.dateRange || null;
      })();
      
      // Log context usage
      if (contextDates && !prefilledDates) {
        console.log(`   ðŸ“… Using date context from conversation history: ${contextDates.startDate} to ${contextDates.endDate}`);
      }
      
      // Add current question with explicit date context
      let currentPrompt = question;
      if (contextDates) {
        currentPrompt += `\n\n[CONTEXT: Use date range ${contextDates.startDate} to ${contextDates.endDate} for this query]`;
      }
      
      conversationMessages.push({
        role: 'user',
        content: currentPrompt
      });
      
      // Build tool definition with smart defaults from context
      const toolDescription = contextDates 
        ? `Fetches AI Voice call records between startDate and endDate. If dates are not provided, use startDate="${contextDates.startDate}" and endDate="${contextDates.endDate}" from the current context.`
        : 'Fetches AI Voice call records between startDate and endDate';
      
      const tools = [{
        type: 'function',
        function: {
          name: 'aivoice_get_call_details',
          description: toolDescription,
          parameters: {
            type: 'object',
            properties: {
              startDate: { 
                type: 'string', 
                description: contextDates 
                  ? `Start date (YYYY-MM-DD). Default from context: ${contextDates.startDate}` 
                  : 'Start date (YYYY-MM-DD)'
              },
              endDate: { 
                type: 'string', 
                description: contextDates 
                  ? `End date (YYYY-MM-DD). Default from context: ${contextDates.endDate}` 
                  : 'End date (YYYY-MM-DD)'
              },
              includeTranscript: { type: 'boolean', default: false },
              includeAudio: { type: 'boolean', default: false }
            },
            required: contextDates ? [] : ['startDate', 'endDate']
          }
        }
      }];

      let aiResponse = await services.callOpenAI(conversationMessages, tools);
      let responseMessage = aiResponse.choices[0].message;
      let result;

      if (responseMessage.tool_calls) {
        const toolCall = responseMessage.tool_calls[0];
        const functionArgs = JSON.parse(toolCall.function.arguments || '{}');
        
        // Use context dates as fallback if OpenAI didn't provide them
        const startDate = functionArgs.startDate || contextDates?.startDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        const endDate = functionArgs.endDate || contextDates?.endDate || config.getCurrentDateInTimezone(config.TIMEZONE);
        
        console.log(`   ðŸ“… Using dates for API call: ${startDate} to ${endDate}`);
        if (!functionArgs.startDate && contextDates) {
          console.log(`   ðŸ”„ Dates came from conversation context`);
        }

        const callResult = await services.fetchCallDetails(
          startDate,
          endDate,
          Boolean(functionArgs.includeTranscript),
          Boolean(functionArgs.includeAudio)
        );

        // Check if call fetch failed and return friendly error
        if (!callResult.success && callResult.friendlyError) {
          session.conversationHistory.push({
            timestamp: new Date(),
            question: question,
            system: 'aivoice',
            result: { success: false, error: callResult.error }
          });

          return res.status(500).json({
            success: false,
            error: callResult.error,
            friendlyError: callResult.friendlyError,
            system: 'aivoice',
            sessionId: session.sessionId
          });
        }

        // ==================== FILTER BY LICENSE KEY ====================
        // Check if a license key was extracted from the question
        const extractedKey = services.extractLicenseKey(question);
        let filteredCallData = callResult.rawData || callResult.data || [];
        let licenseKeyInfo = null;
        
        if (extractedKey) {
          console.log(`   🔑 Filtering calls by license key: ${extractedKey.substring(0, 20)}...`);
          const licenseKeyResult = services.getCallsForLicenseKey(callResult.rawData || callResult.data, extractedKey);
          
          if (licenseKeyResult.found) {
            filteredCallData = licenseKeyResult.calls;
            licenseKeyInfo = {
              licenseKey: extractedKey,
              count: licenseKeyResult.count,
              totalCost: licenseKeyResult.totalCost,
              avgDuration: licenseKeyResult.avgDuration
            };
            console.log(`   ✅ Filtered to ${licenseKeyResult.count} calls for this license key`);
          } else {
            console.log(`   ⚠️ No calls found for license key: ${extractedKey.substring(0, 20)}...`);
            filteredCallData = [];
          }
        } else {
          console.log(`   ℹ️ No license key in query - using all calls`);
        }
        
        // Build AI response context with filtered data
        const callContext = services.buildConversationContext(filteredCallData);
        
        const aiToolPayload = {
          success: callResult.success,
          summary: callContext, // Use context built from FILTERED data
          count: filteredCallData.length,
          message: extractedKey 
            ? `Fetched ${filteredCallData.length} calls for license key ${extractedKey.substring(0, 20)}...`
            : `Fetched ${filteredCallData.length} calls.`,
          licenseKeyFilter: extractedKey ? {
            applied: true,
            licenseKey: extractedKey.substring(0, 40) + '...',
            matchedCalls: filteredCallData.length
          } : { applied: false }
        };

        conversationMessages.push(responseMessage);
        conversationMessages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          name: 'aivoice_get_call_details',
          content: JSON.stringify(aiToolPayload)
        });

        aiResponse = await services.callOpenAI(conversationMessages, tools);
        responseMessage = aiResponse.choices[0].message;

        result = {
          success: true,
          answer: responseMessage.content,
          callHistory: filteredCallData, // Use filtered data
          system: 'aivoice',
          dateRange: { startDate, endDate },
          licenseKey: extractedKey || null,
          licenseKeyInfo: licenseKeyInfo
        };
      } else {
        result = {
          success: true,
          answer: responseMessage.content,
          system: 'aivoice'
        };
      }

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
        system: 'aivoice',
        sessionId: session.sessionId
      });
    }

  } catch (error) {
    console.error('ðŸ’¥ Error in unified chat:', error);
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
  console.log(`\nðŸ”µ TXQL System (Database Queries):`);
  console.log(`   Questions: users, tables, orders, California, age, etc.`);
  console.log(`\nðŸŸ¢ AI Voice System (Call Analysis):`);
  console.log(`   Questions: calls, appointments, patients, sentiment, license keys, etc.`);
  console.log(`\nðŸ“‹ Legacy Endpoints:`);
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