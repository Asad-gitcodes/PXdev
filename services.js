// services.js - Business Logic Layer
// All core service functions extracted from server.js

const axios = require('axios');
const config = require('./config');

// ==================== LICENSE KEY PREPROCESSOR ====================

/**
 * Preprocesses user question to detect and extract license key pattern
 * Pattern: "In <license-key> <actual-query>"
 * 
 * @param {string} question - The user's question
 * @returns {object} - { hasLicenseKey, licenseKey, actualQuery, originalQuestion }
 */
function preprocessLicenseKeyFromQuestion(question) {
  if (!question || typeof question !== 'string') {
    return {
      hasLicenseKey: false,
      licenseKey: null,
      actualQuery: question,
      originalQuestion: question
    };
  }

  // Pattern: "In <license-key> <rest-of-query>"
  // License key pattern: base64-like string (30+ chars of A-Za-z0-9+/=)
  const licenseKeyPattern = /^In\s+([A-Za-z0-9+/=]{30,})\s+(.+)$/i;
  
  const match = question.match(licenseKeyPattern);
  
  if (match) {
    const licenseKey = match[1].trim();
    const actualQuery = match[2].trim();
    
    console.log(`\n🔑 LICENSE KEY DETECTED IN QUESTION:`);
    console.log(`   📋 Original: "${question}"`);
    console.log(`   🔑 License Key: ${licenseKey.substring(0, 40)}...`);
    console.log(`   💬 Actual Query: "${actualQuery}"`);
    
    return {
      hasLicenseKey: true,
      licenseKey: licenseKey,
      actualQuery: actualQuery,
      originalQuestion: question
    };
  }
  
  console.log(`   ℹ️  No license key pattern detected - processing as normal query`);
  
  return {
    hasLicenseKey: false,
    licenseKey: null,
    actualQuery: question,
    originalQuestion: question
  };
}

// ==================== TXQL LICENSE KEY PREPROCESSOR ====================

/**
 * Preprocess TXQL questions to detect license key pattern
 * Pattern: "In <license-key> <actual-query>"
 * 
 * CRITICAL: The extracted key is used to SELECT which database to query
 * This is DIFFERENT from AI Voice where key is used for filtering
 * 
 * @param {string} question - The user's question
 * @returns {object} - { hasTXQLKey, txqlLicenseKey, actualQuery, originalQuestion }
 */
function preprocessTXQLLicenseKey(question) {
  if (!question || typeof question !== 'string') {
    return {
      hasTXQLKey: false,
      txqlLicenseKey: null,
      actualQuery: question,
      originalQuestion: question
    };
  }

  // Pattern: "In <license-key> <actual-query>"
  // License key: 30+ base64-like characters
  const pattern = /^In\s+([A-Za-z0-9+/=]{30,})\s+(.+)$/i;
  const match = question.match(pattern);
  
  if (match) {
    const licenseKey = match[1].trim();
    const actualQuery = match[2].trim();
    
    console.log(`\n🔑 TXQL LICENSE KEY DETECTED IN QUESTION:`);
    console.log(`   License Key: ${licenseKey.substring(0, 40)}...`);
    console.log(`   Actual Query: "${actualQuery}"`);
    console.log(`   ⚠️  IMPORTANT: This key will be used to SELECT which database to query!`);
    
    return {
      hasTXQLKey: true,
      txqlLicenseKey: licenseKey,
      actualQuery: actualQuery,
      originalQuestion: question
    };
  }
  
  return {
    hasTXQLKey: false,
    txqlLicenseKey: null,
    actualQuery: question,
    originalQuestion: question
  };
}

// ==================== CONNECTION TESTS ====================

async function testTXQLConnection() {
  const TXQL_API_URL = 'https://txql.8px.us/api/sql/query';
  console.log(`\nðŸ” Testing TXQL service connectivity...`);
  console.log(`   Endpoint: ${TXQL_API_URL}`);
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second test timeout
    
    const response = await fetch(TXQL_API_URL, {
      method: 'POST',
      headers: {
        'accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        question: 'SELECT 1 as test',
        session_id: 'health_check_' + Date.now()
      }),
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (response.ok) {
      console.log(`   âœ… TXQL service is reachable (Status: ${response.status})`);
      return true;
    } else {
      console.warn(`   âš ï¸  TXQL service responded with status: ${response.status}`);
      return false;
    }
  } catch (error) {
    if (error.name === 'AbortError') {
      console.error(`   âŒ TXQL service connection timed out`);
    } else {
      console.error(`   âŒ Cannot reach TXQL service: ${error.message}`);
    }
    console.error(`   ðŸ’¡ Database queries will not work until TXQL service is available`);
    return false;
  }
}

/**
 * Test AI Voice API connectivity
 */
async function testAIVoiceConnection() {
  console.log(`\nðŸ” Testing AI Voice API connectivity...`);
  console.log(`   Base URL: ${config.BASE_URL}`);
  console.log(`   Endpoint: ${config.BASE_URL}/api/config/get/aivoice/detail`);
  
  try {
    // Test with a date range that should work (yesterday)
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];
    
    const url = `${config.BASE_URL}/api/config/get/aivoice/detail`;
    const response = await axios.get(url, {
      params: {
        licenseKey: config.HARDCODED_LICENSE_KEY,
        startDate: dateStr,
        endDate: dateStr,
        size: 100,
        page: 1
      },
      headers: {
        'Authorization': `Bearer ${config.HARDCODED_BEARER}`
      },
      timeout: 5000,
      validateStatus: function (status) {
        return status < 500; // Accept any non-5xx status for connectivity test
      }
    });
    
    if (response.status === 404) {
      console.error(`   âŒ AI Voice API endpoint not found (404)`);
      console.error(`   ðŸ’¡ The endpoint may have changed or config.BASE_URL is incorrect`);
      console.error(`   ðŸ’¡ Call analysis features will NOT work`);
      return false;
    } else if (response.status === 401 || response.status === 403) {
      console.warn(`   âš ï¸  AI Voice API authentication issue (Status: ${response.status})`);
      console.warn(`   ðŸ’¡ Check your license key and bearer token`);
      return false;
    } else if (response.status >= 200 && response.status < 300) {
      console.log(`   âœ… AI Voice API is reachable (Status: ${response.status})`);
      return true;
    } else {
      console.warn(`   âš ï¸  AI Voice API responded with status: ${response.status}`);
      return false;
    }
  } catch (error) {
    if (error.code === 'ECONNREFUSED') {
      console.error(`   âŒ Cannot connect to ${config.BASE_URL} (connection refused)`);
    } else if (error.code === 'ENOTFOUND') {
      console.error(`   âŒ Cannot resolve hostname: ${config.BASE_URL}`);
    } else if (error.code === 'ETIMEDOUT') {
      console.error(`   âŒ AI Voice API connection timed out`);
    } else {
      console.error(`   âŒ Cannot reach AI Voice API: ${error.message}`);
    }
    console.error(`   ðŸ’¡ Call analysis features will NOT work until AI Voice API is available`);
    return false;
  }
}

// ==================== SESSION MANAGEMENT ====================

const activeSessions = new Map();

function generateSessionId() {
  return 'session_' + Date.now() + '_' + Math.random().toString(36).substring(2, 15);
}

function getOrCreateSession(userId = 'anonymous') {
  const key = `user_${userId}`;
  
  if (!activeSessions.has(key)) {
    const sessionId = generateSessionId();
    activeSessions.set(key, {
      sessionId,
      createdAt: new Date(),
      lastActive: new Date(),
      conversationHistory: [],
      txqlSessionId: generateSessionId()
    });
    console.log(`âœ¨ Created new session: ${sessionId} for user: ${userId}`);
  } else {
    const session = activeSessions.get(key);
    session.lastActive = new Date();
  }
  
  return activeSessions.get(key);
}

function getSession(userId = 'anonymous') {
  return activeSessions.get(`user_${userId}`);
}

function cleanupOldSessions(maxAgeMinutes = 30) {
  const now = new Date();
  let cleaned = 0;
  
  for (const [key, session] of activeSessions.entries()) {
    const ageMinutes = (now - session.lastActive) / (1000 * 60);
    if (ageMinutes > maxAgeMinutes) {
      activeSessions.delete(key);
      cleaned++;
    }
  }
  
  if (cleaned > 0) {
    console.log(`ðŸ§¹ Cleaned up ${cleaned} old sessions`);
  }
}

setInterval(() => cleanupOldSessions(), 10 * 60 * 1000);

// ==================== GREETING DETECTION ====================

/**
 * Check if the question is asking about license keys
 */
function isLicenseKeyQuery(question) {
  const lowerQ = question.toLowerCase();
  const licenseKeyPatterns = [
    'license key',
    'licensekey',
    'license keys',
    'calls per license',
    'calls by license',
    'how many calls for',
    'count by license',
    'group by license',
    'breakdown by license',
    'associated with',
    'calls associated',
    'for this license',
    'with this license'
  ];
  
  return licenseKeyPatterns.some(pattern => lowerQ.includes(pattern));
}

/**
 * Check if the question is asking about call direction (inbound/outbound)
 */
function isCallDirectionQuery(question) {
  const lowerQ = question.toLowerCase();
  const callDirectionPatterns = [
    'inbound call',
    'inbound',
    'outbound call',
    'outbound',
    'incoming call',
    'incoming',
    'outgoing call',
    'outgoing',
    'how many calls came in',
    'how many calls did we receive',
    'how many calls did we get',
    'how many calls did we make',
    'calls we received',
    'calls we made',
    'calls coming in',
    'calls going out'
  ];
  
  return callDirectionPatterns.some(pattern => lowerQ.includes(pattern));
}

/**
 * Extract license key from question if mentioned
 */
function extractLicenseKey(question) {
  // Try to extract a license key pattern (base64-like strings)
  const matches = question.match(/([A-Za-z0-9+/=]{20,})/);
  return matches ? matches[1] : null;
}

/**
 * Check if the question is a simple greeting or casual conversation
 */
function isGreeting(question) {
  const lowerQ = question.toLowerCase().trim();
  const greetings = [
    'hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening',
    'how are you', 'what\'s up', 'whats up', 'sup', 'yo', 'greetings',
    'hola', 'howdy', 'hi there', 'hello there'
  ];
  
  // Check for exact matches or very short casual messages
  if (greetings.includes(lowerQ) || (lowerQ.length <= 3 && /^[a-z]+$/.test(lowerQ))) {
    return true;
  }
  
  return false;
}

/**
 * Generate a friendly greeting response
 */
function getGreetingResponse() {
  const greetings = [
    "Hello! ðŸ‘‹ I'm your AI assistant. I can help you with:\n\nðŸŸ¢ **Call Analysis** - Ask about appointments, call records, sentiment, costs\nðŸ”µ **Database Queries** - Ask about users, orders, tables, customers\n\nWhat would you like to know?",
    "Hi there! ðŸ‘‹ I'm here to help! I can assist with:\n\nðŸŸ¢ Call data analysis\nðŸ”µ Database queries\n\nJust ask me anything!",
    "Hey! ðŸ˜Š I can help you analyze call data or query your database. What can I do for you today?"
  ];
  
  return greetings[Math.floor(Math.random() * greetings.length)];
}

// ==================== INTELLIGENT ROUTING ====================

function determineSystem(question) {
  const lowerQ = question.toLowerCase();
  
  // AI Voice keywords (including license key queries)
  const aiVoiceKeywords = [
    'call', 'calls', 'voice', 'phone', 'patient', 'appointment', 
    'booked', 'cancelled', 'rescheduled', 'upsell', 'sentiment',
    'transcript', 'audio', 'follow-up', 'callback', 'cost of calls',
    'call duration', 'call summary', 'call records', 'inbound', 'outbound',
    'thumbs up', 'thumbs down', 'voicemail', 'license key', 'license keys',
    'licensekey', 'how many calls', 'call count', 'calls per license'
  ];
  
  // TXQL keywords
  const txqlKeywords = [
    'users', 'customers', 'orders', 'products', 'inventory',
    'california', 'texas', 'state', 'city', 'address',
    'age', 'email', 'show me', 'count', 'list', 'find',
    'table', 'tables', 'database', 'records', 'rows',
    'filter', 'sort', 'group by', 'join', 'select', 'transaction'
  ];
  
  const aiVoiceScore = aiVoiceKeywords.filter(kw => lowerQ.includes(kw)).length;
  const txqlScore = txqlKeywords.filter(kw => lowerQ.includes(kw)).length;
  
  console.log(`ðŸ¤” Question Analysis: "${question}"`);
  console.log(`   AI Voice Score: ${aiVoiceScore}`);
  console.log(`   TXQL Score: ${txqlScore}`);
  
  // If both scores are 0, default to TXQL
  if (aiVoiceScore === 0 && txqlScore === 0) {
    console.log(`   â†’ Routing to: TXQL (default)`);
    return 'txql';
  }
  
  const system = aiVoiceScore > txqlScore ? 'aivoice' : 'txql';
  console.log(`   â†’ Routing to: ${system.toUpperCase()}`);
  return system;
}

// ==================== SQL EXECUTION FUNCTION ====================

/**
 * Helper function to build SQL for note queries when TXQL fails
 * This handles common patterns like "summarize notes for patnum = X"
 * Gets notes from BOTH procnote and patientnote tables
 */
function buildNoteQuerySQL(question) {
  // Extract patient number from question
  const patNumMatch = question.match(/patnum\s*=?\s*(\d+)/i);
  if (!patNumMatch) {
    return null;
  }
  
  const patNum = patNumMatch[1];
  
  // Build comprehensive notes query - gets ALL notes from ALL tables including commlog
  const sql = `SELECT 
    'Procedure Note' AS NoteType,
    pn.EntryDateTime AS DateTime,
    pn.Note
FROM procnote pn
INNER JOIN procedurelog pl ON pn.ProcNum = pl.ProcNum
WHERE pl.PatNum = ${patNum}

UNION ALL

SELECT 
    'Patient Note' AS NoteType, 
    NULL AS DateTime,
    Medical AS Note
FROM patientnote
WHERE PatNum = ${patNum}

UNION ALL

SELECT
    'Patient Note (Field)' AS NoteType,
    NULL AS DateTime,
    FamFinancial AS Note
FROM patientnote
WHERE PatNum = ${patNum}
AND FamFinancial IS NOT NULL
AND FamFinancial != ''

UNION ALL

SELECT
    'Patient Note (Field)' AS NoteType,
    NULL AS DateTime,
    ApptPhone AS Note
FROM patientnote
WHERE PatNum = ${patNum}
AND ApptPhone IS NOT NULL
AND ApptPhone != ''

UNION ALL

SELECT
    'Communication Log' AS NoteType,
    CommDateTime AS DateTime,
    Note AS Note
FROM commlog
WHERE PatNum = ${patNum}
AND Note IS NOT NULL
AND Note != ''

ORDER BY DateTime DESC`;

  console.log(`   💡 Built comprehensive note query SQL for PatNum: ${patNum}`);
  console.log(`   📋 Will fetch from: procnote, patientnote (Medical, FamFinancial, ApptPhone), commlog`);
  return sql;
}

/**
 * Build SQL to search for multiple patients with pricing information in notes
 */
function buildMultiPatientPricingSearchSQL(question) {
  // Extract limit if specified (e.g., "find 10 patients", "top 20 patients")
  const limitMatch = question.match(/(\d+)\s+(patient|patnum)/i);
  const limit = limitMatch ? parseInt(limitMatch[1]) : 10; // Default to 10
  
  // Build SQL to find patients with pricing information in commlog
  const sql = `SELECT 
    c.PatNum,
    CONCAT(p.LName, ', ', p.FName) AS PatientName,
    COUNT(DISTINCT c.CommlogNum) AS NotesWithPricing,
    MAX(c.CommDateTime) AS LatestPricingDiscussion,
    LEFT(MAX(c.Note), 500) AS LatestNote
FROM commlog c
INNER JOIN patient p ON c.PatNum = p.PatNum
WHERE c.Note IS NOT NULL
AND (
    c.Note LIKE '%$ % per month%'
    OR c.Note LIKE '%$% for%'
    OR c.Note LIKE '%pricing%'
    OR c.Note LIKE '%PX Summary%pricing%'
    OR c.Note LIKE '%fee%waived%'
    OR c.Note LIKE '%monthly package%'
    OR c.Note LIKE '%setup fee%'
    OR c.Note LIKE '%subscription%'
    OR c.Note LIKE '%cost%'
)
GROUP BY c.PatNum, p.LName, p.FName
ORDER BY LatestPricingDiscussion DESC
LIMIT ${limit}`;

  console.log(`   💡 Built multi-patient pricing search SQL`);
  console.log(`   📋 Searching commlog for pricing keywords, limit: ${limit}`);
  return sql;
}

/**
 * Extract SQL query from TXQL response
 */
function extractSQLFromTXQL(txqlResponse) {
  try {
    console.log('ðŸ” Extracting SQL from TXQL response...');
    console.log('   Response type:', typeof txqlResponse);
    console.log('   Response preview:', JSON.stringify(txqlResponse).substring(0, 200));
    
    // Case 1: Response is a string containing SQL
    if (typeof txqlResponse === 'string') {
      // Try to extract SQL from markdown code blocks
      const sqlMatch = txqlResponse.match(/```sql\s*([\s\S]*?)\s*```/i);
      if (sqlMatch && sqlMatch[1]) {
        console.log('   âœ… Found SQL in markdown code block');
        return sqlMatch[1].trim();
      }
      
      // Try to extract from "query": "..." pattern
      const queryMatch = txqlResponse.match(/"query"\s*:\s*"([^"]+)"/);
      if (queryMatch && queryMatch[1]) {
        console.log('   âœ… Found SQL in "query" field');
        return queryMatch[1].trim();
      }
      
      // Check if the string itself looks like SQL
      const trimmed = txqlResponse.trim();
      if (trimmed.match(/^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)/i)) {
        console.log('   âœ… String appears to be SQL');
        return trimmed;
      }
    }
    
    // Case 2: Response is an object
    if (typeof txqlResponse === 'object' && txqlResponse !== null) {
      // Check common field names
      const possibleFields = ['sql', 'query', 'sqlQuery', 'sql_query', 'statement'];
      for (const field of possibleFields) {
        if (txqlResponse[field] && typeof txqlResponse[field] === 'string') {
          console.log(`   âœ… Found SQL in field: ${field}`);
          return txqlResponse[field].trim();
        }
      }
      
      // Check nested data object
      if (txqlResponse.data) {
        for (const field of possibleFields) {
          if (txqlResponse.data[field] && typeof txqlResponse.data[field] === 'string') {
            console.log(`   âœ… Found SQL in data.${field}`);
            return txqlResponse.data[field].trim();
          }
        }
      }
      
      // Check if response object contains the SQL as a string representation
      const responseStr = JSON.stringify(txqlResponse);
      const queryMatch = responseStr.match(/"query"\s*:\s*"((?:[^"\\]|\\.)*)"/);
      if (queryMatch && queryMatch[1]) {
        // Unescape the JSON string
        const unescaped = queryMatch[1]
          .replace(/\\"/g, '"')
          .replace(/\\n/g, '\n')
          .replace(/\\t/g, '\t')
          .replace(/\\\\/g, '\\');
        console.log('   âœ… Found and unescaped SQL from JSON string');
        return unescaped.trim();
      }
      
      // If response has a 'response' or 'answer' field, check that
      if (txqlResponse.response && typeof txqlResponse.response === 'string') {
        const sqlMatch = txqlResponse.response.match(/```sql\s*([\s\S]*?)\s*```/i);
        if (sqlMatch && sqlMatch[1]) {
          console.log('   âœ… Found SQL in response field markdown');
          return sqlMatch[1].trim();
        }
      }
      
      if (txqlResponse.answer && typeof txqlResponse.answer === 'string') {
        const sqlMatch = txqlResponse.answer.match(/```sql\s*([\s\S]*?)\s*```/i);
        if (sqlMatch && sqlMatch[1]) {
          console.log('   âœ… Found SQL in answer field markdown');
          return sqlMatch[1].trim();
        }
      }
    }
    
    console.log('   âš ï¸  Could not extract SQL from response');
    return null;
  } catch (error) {
    console.error('âŒ Error extracting SQL:', error);
    return null;
  }
}

/**
/**
 * Execute SQL query using the query execution endpoint
 * @param {string} sqlQuery - The SQL query to execute
 * @param {string|null} customKey - Optional custom license key for database selection
 */
async function executeSQL(sqlQuery, customKey = null) {
  try {
    console.log(`🔐 Executing SQL query...`);
    console.log(`   Full Query:\n${sqlQuery}`);
    console.log(`   Endpoint: ${config.QUERY_EXEC_URL}`);
    
    // Use custom key if provided, otherwise use default
    const queryKey = customKey || config.QUERY_EXEC_KEY;
    
    if (customKey) {
      console.log(`   🆕 Using CUSTOM license key: ${customKey.substring(0, 40)}...`);
      console.log(`   ⚠️  This will query the database for THIS specific practice!`);
    } else {
      console.log(`   ℹ️  Using DEFAULT license key: ${queryKey.substring(0, 40)}...`);
    }
    
    const payload = {
      key: queryKey,
      query: sqlQuery.trim()
    };
    
    console.log(`   Payload (key hidden):`, JSON.stringify({ ...payload, key: payload.key.substring(0, 20) + '...' }, null, 2));
    
    const response = await axios.post(config.QUERY_EXEC_URL, payload, {
      headers: {
        'Authorization': config.QUERY_EXEC_AUTH,
        'Content-Type': 'application/json'
      },
      timeout: 30000,
      validateStatus: function (status) {
        return status < 500; // Don't throw on 4xx errors
      }
    });
    
    // Check if response was successful
    if (response.status >= 400) {
      console.error(`âŒ SQL execution failed with status ${response.status}`);
      console.error(`   Response:`, JSON.stringify(response.data, null, 2));
      
      return {
        success: false,
        error: `Query execution failed (${response.status}): ${response.data?.error || response.data?.message || 'Unknown error'}`,
        statusCode: response.status,
        responseData: response.data,
        data: null
      };
    }
    
    console.log(`âœ… SQL execution successful`);
    console.log(`   Rows returned: ${response.data?.data?.length || 0}`);
    
    return {
      success: true,
      data: response.data,
      rowCount: response.data?.data?.length || 0
    };
  } catch (error) {
    console.error('âŒ SQL execution failed:', error.message);
    if (error.response) {
      console.error(`   Status: ${error.response.status}`);
      console.error(`   Response:`, JSON.stringify(error.response.data, null, 2));
    }
    return {
      success: false,
      error: error.response?.data?.error || error.response?.data?.message || error.message,
      statusCode: error.response?.status,
      data: null
    };
  }
}

// ==================== PAYMENT ANALYZER ====================

/**
 * Analyzes pricing details for a patient from SQL query results
 */
function analyzePricingDetails(queryResults, patNum) {
  if (!queryResults || queryResults.length === 0) {
    return {
      success: false,
      message: `No payment data found for patient ${patNum}`,
      patientNumber: patNum
    };
  }

  const analysis = {
    success: true,
    patientNumber: patNum,
    totalRecords: queryResults.length,
    dateRange: { earliest: null, latest: null },
    financial: {
      totalBilled: 0,
      totalInsurancePaid: 0,
      totalWriteOffs: 0,
      totalAdjustments: 0,
      totalPatientPayments: 0,
      netPatientBalance: 0,
      insuranceCoverage: 0
    },
    procedures: [],
    summary: {},
    breakdown: {
      byYear: {},
      paymentStatus: { fullyPaid: 0, partiallyPaid: 0, unpaid: 0, zeroCharge: 0 }
    }
  };

  queryResults.forEach(record => {
    const procFee = parseFloat(record.ProcFee) || 0;
    const insPayment = parseFloat(record.InsPayAmt) || 0;
    const writeOff = parseFloat(record.WriteOff) || 0;
    const adjustment = parseFloat(record.Adjustments) || 0;
    const patPayment = parseFloat(record.Payments) || 0;
    const remainingBalance = procFee - insPayment - writeOff - adjustment - patPayment;

    analysis.financial.totalBilled += procFee;
    analysis.financial.totalInsurancePaid += insPayment;
    analysis.financial.totalWriteOffs += writeOff;
    analysis.financial.totalAdjustments += Math.abs(adjustment);
    analysis.financial.totalPatientPayments += patPayment;

    const procDate = new Date(record.ProcDate);
    if (!analysis.dateRange.earliest || procDate < new Date(analysis.dateRange.earliest)) {
      analysis.dateRange.earliest = record.ProcDate;
    }
    if (!analysis.dateRange.latest || procDate > new Date(analysis.dateRange.latest)) {
      analysis.dateRange.latest = record.ProcDate;
    }

    if (procFee === 0) {
      analysis.breakdown.paymentStatus.zeroCharge++;
    } else if (remainingBalance <= 0) {
      analysis.breakdown.paymentStatus.fullyPaid++;
    } else if (insPayment > 0 || patPayment > 0 || writeOff > 0) {
      analysis.breakdown.paymentStatus.partiallyPaid++;
    } else {
      analysis.breakdown.paymentStatus.unpaid++;
    }

    const year = procDate.getFullYear();
    if (!analysis.breakdown.byYear[year]) {
      analysis.breakdown.byYear[year] = { year, totalBilled: 0, totalPaid: 0, count: 0 };
    }
    analysis.breakdown.byYear[year].totalBilled += procFee;
    analysis.breakdown.byYear[year].totalPaid += insPayment + patPayment;
    analysis.breakdown.byYear[year].count++;

    analysis.procedures.push({
      date: record.ProcDate,
      procFee, insurancePaid: insPayment, writeOff, adjustments: adjustment,
      patientPaid: patPayment, remainingBalance,
      status: remainingBalance <= 0 ? 'Paid' : remainingBalance < procFee ? 'Partial' : 'Unpaid'
    });
  });

  analysis.financial.netPatientBalance = 
    analysis.financial.totalBilled - analysis.financial.totalInsurancePaid - 
    analysis.financial.totalWriteOffs - analysis.financial.totalPatientPayments;

  if (analysis.financial.totalBilled > 0) {
    analysis.financial.insuranceCoverage = 
      (analysis.financial.totalInsurancePaid / analysis.financial.totalBilled * 100).toFixed(2);
  }

  analysis.summary = buildPaymentSummary(analysis);
  analysis.recommendations = generatePaymentRecommendations(analysis);

  return analysis;
}

function buildPaymentSummary(analysis) {
  return {
    overview: `Payment Analysis for Patient ${analysis.patientNumber}`,
    period: `${analysis.dateRange.earliest || 'N/A'} to ${analysis.dateRange.latest || 'N/A'}`,
    totalProcedures: analysis.totalRecords,
    financialSummary: [
      `ðŸ’° Total Billed: $${analysis.financial.totalBilled.toFixed(2)}`,
      `ðŸ¥ Insurance Paid: $${analysis.financial.totalInsurancePaid.toFixed(2)} (${analysis.financial.insuranceCoverage}% coverage)`,
      `ðŸ“ Write-offs: $${analysis.financial.totalWriteOffs.toFixed(2)}`,
      `âš–ï¸ Adjustments: $${analysis.financial.totalAdjustments.toFixed(2)}`,
      `ðŸ‘¤ Patient Paid: $${analysis.financial.totalPatientPayments.toFixed(2)}`,
      ``,
      `ðŸ“Š Net Patient Balance: $${analysis.financial.netPatientBalance.toFixed(2)}`
    ].join('\n'),
    paymentStatus: [
      `âœ… Fully Paid: ${analysis.breakdown.paymentStatus.fullyPaid} procedures`,
      `â³ Partially Paid: ${analysis.breakdown.paymentStatus.partiallyPaid} procedures`,
      `âŒ Unpaid: ${analysis.breakdown.paymentStatus.unpaid} procedures`,
      `ðŸ†“ Zero Charge: ${analysis.breakdown.paymentStatus.zeroCharge} procedures`
    ].join('\n'),
    yearlyBreakdown: Object.values(analysis.breakdown.byYear)
      .sort((a, b) => b.year - a.year)
      .map(year => `${year.year}: ${year.count} procedures | Billed: $${year.totalBilled.toFixed(2)} | Paid: $${year.totalPaid.toFixed(2)}`)
      .join('\n')
  };
}

function generatePaymentRecommendations(analysis) {
  const recommendations = [];

  if (analysis.financial.netPatientBalance > 0) {
    recommendations.push(`âš ï¸ Outstanding Balance: Patient has $${analysis.financial.netPatientBalance.toFixed(2)} remaining balance.`);
  } else if (analysis.financial.netPatientBalance < 0) {
    recommendations.push(`â„¹ï¸ Credit Balance: Patient has a credit of $${Math.abs(analysis.financial.netPatientBalance).toFixed(2)}.`);
  } else {
    recommendations.push(`âœ… Account Balanced: No outstanding balance.`);
  }

  const coverage = parseFloat(analysis.financial.insuranceCoverage);
  if (coverage < 50 && analysis.financial.totalInsurancePaid > 0) {
    recommendations.push(`ðŸ“‰ Low Insurance Coverage: Only ${coverage}% of charges covered by insurance.`);
  } else if (coverage >= 80) {
    recommendations.push(`ðŸ“ˆ Good Insurance Coverage: ${coverage}% of charges covered by insurance.`);
  }

  if (analysis.breakdown.paymentStatus.unpaid > 0) {
    recommendations.push(`ðŸ’³ Unpaid Procedures: ${analysis.breakdown.paymentStatus.unpaid} procedure(s) have no payments recorded.`);
  }

  if (analysis.financial.totalWriteOffs > 0) {
    const writeOffPercent = (analysis.financial.totalWriteOffs / analysis.financial.totalBilled * 100).toFixed(2);
    recommendations.push(`ðŸ“ Write-offs: $${analysis.financial.totalWriteOffs.toFixed(2)} (${writeOffPercent}% of total billed) written off.`);
  }

  return recommendations;
}

function formatPricingAnalysisForChat(analysis) {
  if (!analysis.success) {
    return analysis.message;
  }

  let output = `## ðŸ“Š Payment Analysis for Patient ${analysis.patientNumber}

### ðŸ“… Period
${analysis.summary.period} (${analysis.totalRecords} procedures)

---

### ðŸ’° Financial Summary

${analysis.summary.financialSummary}

---

### ðŸ“ˆ Payment Status Distribution

${analysis.summary.paymentStatus}

---

### ðŸ“† Yearly Breakdown

${analysis.summary.yearlyBreakdown}

---

### ðŸ“‹ Detailed Procedure Records

`;

  output += '| Date | Billed | Insurance | Write-off | Adjustments | Patient Paid | Balance | Status |\n';
  output += '|------|--------|-----------|-----------|-------------|--------------|---------|--------|\n';
  
  analysis.procedures.forEach(proc => {
    output += `| ${proc.date} | $${proc.procFee.toFixed(2)} | $${proc.insurancePaid.toFixed(2)} | $${proc.writeOff.toFixed(2)} | $${proc.adjustments.toFixed(2)} | $${proc.patientPaid.toFixed(2)} | $${proc.remainingBalance.toFixed(2)} | ${proc.status} |\n`;
  });

  if (analysis.recommendations && analysis.recommendations.length > 0) {
    output += '\n---\n\n### ðŸ’¡ Recommendations\n\n';
    analysis.recommendations.forEach(rec => {
      output += `${rec}\n`;
    });
  }

  return output;
}

/**
 * Detects if user is asking for pricing/payment analysis
 */
function isPricingAnalysisQuery(question) {
  const lowerQ = question.toLowerCase();
  const pricingKeywords = [
    'pricing details',
    'analyze pricing',
    'payment details',
    'analyze payment',
    'financial analysis',
    'billing analysis',
    'payment analysis',
    'cost analysis',
    'analyze costs',
    'analyze charges'
  ];
  
  return pricingKeywords.some(keyword => lowerQ.includes(keyword));
}

/**
 * Detects if user wants an AI-powered summary of notes
 */
function isNoteSummaryQuery(question) {
  const lowerQ = question.toLowerCase();
  const summaryKeywords = [
    'summarize notes',
    'summarize all notes',
    'summary of notes',
    'analyze notes',
    'review notes',
    'overview of notes',
    'what do the notes say',
    'tell me about the notes'
  ];
  
  return summaryKeywords.some(keyword => lowerQ.includes(keyword));
}

/**
 * Detects if user wants to search for patients with pricing information
 */
function isMultiPatientPricingSearch(question) {
  const lowerQ = question.toLowerCase();
  
  // Check for patterns like:
  // "find patients with pricing"
  // "search for pricing information"
  // "extract 10 patnum with pricing"
  // "list patients with PX Summary pricing"
  
  const hasSearchIntent = /find|search|extract|list|show|get/i.test(question);
  const hasMultiplePattern = /patients|patnums?|customers|all/i.test(question);
  const hasPricingKeyword = /pricing|price|cost|fee|charge|\$|payment/i.test(question);
  const hasNotesReference = /notes?|commlog|summary|px summary/i.test(question);
  
  // Also check for number patterns like "10 patients", "top 20", etc.
  const hasNumberPattern = /\d+\s+(patient|patnum)/i.test(question);
  
  return (hasSearchIntent && hasMultiplePattern && hasPricingKeyword) ||
         (hasSearchIntent && hasNumberPattern && hasPricingKeyword) ||
         (hasSearchIntent && hasPricingKeyword && hasNotesReference);
}

/**
 * Detects if user wants pricing-specific information from notes
 */
function isPricingFromNotesQuery(question) {
  const lowerQ = question.toLowerCase();
  
  // Must contain both "notes" and pricing keywords
  const hasNoteReference = /\bnote|notes\b/i.test(question);
  const hasPricingKeyword = /pricing|price|cost|fee|charge|payment|bill/i.test(question);
  
  return hasNoteReference && hasPricingKeyword;
}

/**
 * Generate AI-powered summary of patient notes using OpenAI
 */
async function generateNoteSummary(notes, patientNumber, focusOnPricing = false) {
  if (!config.OPENAI_API_KEY) {
    return {
      success: false,
      error: 'OpenAI API key not configured',
      summary: 'AI summarization unavailable - API key not set'
    };
  }

  if (!notes || notes.length === 0) {
    return {
      success: true,
      summary: `No notes found for patient ${patientNumber}.`
    };
  }

  try {
    console.log(`🤖 Generating AI summary for ${notes.length} notes...`);
    if (focusOnPricing) {
      console.log(`   💰 Focus: PRICING INFORMATION ONLY`);
    }
    
    // Prepare notes for AI
    const notesText = notes.map((note, idx) => {
      return `
--- Note ${idx + 1} ---
Type: ${note.NoteType || 'Unknown'}
Date: ${note.DateTime || 'No date'}
Content: ${note.Note || 'No content'}
`;
    }).join('\n');

    let prompt;
    
    if (focusOnPricing) {
      // Specialized prompt for pricing information extraction
      prompt = `You are analyzing patient notes to extract ONLY pricing and financial information.

Patient Number: ${patientNumber}
Total Notes: ${notes.length}

${notesText}

CRITICAL INSTRUCTIONS:
- Search through ALL notes, including call transcripts and summaries
- Look for dollar amounts ($), monthly costs, fees, setup charges, packages
- Extract pricing even if buried in long transcripts or mixed with other content
- Common patterns: "$ X per month", "$ Y for", "total $ Z", "fee waived", "package"
- Include subscription pricing, one-time fees, setup costs, service packages
- Note any discounts, waivers, or special offers

Please provide:
1. **Pricing Summary** - All fees, costs, monthly charges, and packages mentioned (be specific with amounts)
2. **Payment Information** - Payment terms, billing frequency, setup fees
3. **Financial Discussions** - Key points from pricing conversations (who, what, when)
4. **Special Offers** - Discounts, waivers, promotional pricing
5. **Insurance/Coverage** - Any mentions of insurance or coverage

If NO pricing information is found after thoroughly searching all notes, clearly state: "No pricing or financial information found in notes."

Format your response in clear markdown with headers and bullet points. Be specific with dollar amounts and dates when available.`;
    } else {
      // Regular comprehensive summary prompt
      prompt = `You are a medical assistant reviewing patient notes. Please provide a comprehensive summary of the following patient notes.

Patient Number: ${patientNumber}
Total Notes: ${notes.length}

${notesText}

Please provide:
1. **Overall Summary** - A brief overview of the patient's situation
2. **Medical History** - Key medical conditions, allergies, medications
3. **Treatment Timeline** - Chronological summary of procedures and appointments
4. **Important Flags** - Any urgent issues, allergies, or concerns
5. **Follow-up Items** - Any pending actions or appointments

Format your response in clear markdown with headers and bullet points.`;
    }

    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o',
        messages: [
          {
            role: 'system',
            content: focusOnPricing 
              ? 'You are a financial analyst extracting pricing and cost information from medical practice notes and call transcripts. Search thoroughly through ALL content including long transcripts. Focus ONLY on financial details like monthly costs, fees, packages, setup charges, discounts, and payment terms. Extract specific dollar amounts.'
              : 'You are a medical assistant helping to summarize patient notes. Be thorough, accurate, and highlight important medical information.'
          },
          {
            role: 'user',
            content: prompt
          }
        ],
        temperature: 0.3,
        max_tokens: 3000
      },
      {
        headers: {
          'Authorization': `Bearer ${config.OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );

    const summary = response.data.choices[0].message.content;
    console.log(`✅ AI summary generated (${summary.length} characters)`);

    return {
      success: true,
      summary: summary,
      noteCount: notes.length,
      patientNumber: patientNumber,
      focusedOnPricing: focusOnPricing
    };

  } catch (error) {
    console.error('❌ Error generating AI summary:', error.message);
    return {
      success: false,
      error: error.message,
      summary: 'Failed to generate AI summary. Showing raw notes instead.'
    };
  }
}

/**
 * Extracts patient number from the query results
 */
function extractPatientNumber(queryResults) {
  if (!queryResults || queryResults.length === 0) return null;
  
  // Try to find PatNum in the first record
  const firstRecord = queryResults[0];
  return firstRecord.PatNum || firstRecord.patNum || firstRecord.PatientNumber || null;
}

// ==================== END PAYMENT ANALYZER ====================

// ==================== CHART DETECTION ====================

/**
 * Detect if data should be visualized as a chart
 * Returns chart configuration or null
 */
function detectChartData(rows, columns) {
  if (!rows || rows.length === 0 || rows.length > 100) return null;
  
  console.log(`   📊 Analyzing data for chart potential...`);
  console.log(`      Rows: ${rows.length}, Columns: ${columns.length}`);
  
  // Find date columns
  const dateColumns = columns.filter(col => {
    const sample = rows[0][col];
    return sample && (
      /^\d{4}-\d{2}-\d{2}/.test(String(sample)) ||
      col.toLowerCase().includes('date') ||
      col.toLowerCase().includes('time')
    );
  });
  
  // Find numeric columns
  const numericColumns = columns.filter(col => {
    return rows.slice(0, 5).every(row => {
      const val = row[col];
      return typeof val === 'number' || (!isNaN(parseFloat(val)) && val !== null && val !== '');
    });
  });
  
  console.log(`      Date columns: ${dateColumns.join(', ') || 'none'}`);
  console.log(`      Numeric columns: ${numericColumns.join(', ') || 'none'}`);
  
  // TIME SERIES: Date + Number
  if (dateColumns.length > 0 && numericColumns.length > 0) {
    const dateCol = dateColumns[0];
    const valueCol = numericColumns[0];
    
    console.log(`   ✅ Chart detected: LINE chart (${dateCol} vs ${valueCol})`);
    
    return {
      type: 'line',
      data: rows.map(row => ({
        date: String(row[dateCol]),
        value: parseFloat(row[valueCol]) || 0,
        label: String(row[dateCol])
      })),
      config: {
        xKey: 'date',
        yKey: 'value',
        title: `${valueCol} over time`,
        xLabel: dateCol,
        yLabel: valueCol
      }
    };
  }
  
  // CATEGORY COMPARISON: Text + Number
  const categoryColumns = columns.filter(col => {
    const uniqueValues = new Set(rows.map(r => r[col]));
    return uniqueValues.size > 1 && uniqueValues.size <= 20 && !dateColumns.includes(col);
  });
  
  if (categoryColumns.length > 0 && numericColumns.length > 0) {
    const catCol = categoryColumns[0];
    const valueCol = numericColumns[0];
    
    console.log(`   ✅ Chart detected: BAR chart (${catCol} vs ${valueCol})`);
    
    return {
      type: 'bar',
      data: rows.map(row => ({
        category: String(row[catCol]),
        value: parseFloat(row[valueCol]) || 0
      })),
      config: {
        xKey: 'category',
        yKey: 'value',
        title: `${valueCol} by ${catCol}`,
        xLabel: catCol,
        yLabel: valueCol
      }
    };
  }
  
  // SENTIMENT/STATUS BREAKDOWN: Counts (PIE CHART)
  if (rows.length <= 10 && numericColumns.length > 0) {
    const labelCol = columns[0];
    const valueCol = numericColumns[0];
    
    console.log(`   ✅ Chart detected: PIE chart (${labelCol} distribution)`);
    
    return {
      type: 'pie',
      data: rows.map(row => ({
        name: String(row[labelCol]),
        value: parseFloat(row[valueCol]) || 0
      })),
      config: {
        title: `Distribution of ${labelCol}`
      }
    };
  }
  
  console.log(`   ℹ️  No chart pattern detected`);
  return null;
}

// ==================== END CHART DETECTION ====================

/**
 * Format SQL results for display - WITH CHART DETECTION
 */
async function formatSQLResults(sqlResults, sqlQuery, originalQuestion = '') {
  if (!sqlResults.success || !sqlResults.data) {
    let errorOutput = `## âŒ Query Execution Failed\n\n`;
    errorOutput += `**Error:** ${sqlResults.error || 'Unknown error'}\n\n`;
    
    if (sqlResults.statusCode) {
      errorOutput += `**Status Code:** ${sqlResults.statusCode}\n\n`;
    }
    
    if (sqlResults.responseData) {
      errorOutput += `**Server Response:** \`\`\`json\n${JSON.stringify(sqlResults.responseData, null, 2)}\n\`\`\`\n\n`;
    }
    
    errorOutput += `### SQL Query That Failed:\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n\n`;
    errorOutput += `ðŸ’¡ **Troubleshooting Tips:**\n`;
    errorOutput += `- Verify the SQL syntax is correct for your database engine\n`;
    errorOutput += `- Check that all table and column names exist\n`;
    errorOutput += `- Ensure date formats match database requirements\n`;
    errorOutput += `- Try simplifying the query to isolate the issue\n`;
    
    return errorOutput;
  }
  
  
  const rows = sqlResults.data.data || sqlResults.data;
  
  // ========== CRITICAL FIX: Comprehensive empty result handling ==========
  // This prevents "Cannot convert undefined or null to object" errors
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    console.log(`ℹ️ Query executed successfully but returned no results`);
    const helpfulMessage = `## 📊 Query Results

**Status:** ✅ Query executed successfully

**Records Found:** 0

### Possible Reasons:
- The PatNum doesn't exist in the database
- The specified conditions returned no matches  
- The table is empty
- PatNum might need to be a number instead of a string

💡 **Tip:** Try removing quotes from PatNum values (use \`PatNum = 123\` instead of \`PatNum = '123'\`)

### SQL Query:
\`\`\`sql
${sqlQuery}
\`\`\``;
    return helpfulMessage;
  }
  
  // ========== CRITICAL FIX: Verify rows[0] exists before Object.keys() ==========
  if (!rows[0] || typeof rows[0] !== 'object') {
    console.log(`⚠️ Query returned rows but data structure is invalid`);
    console.log(`   Rows type: ${typeof rows}, Length: ${rows.length}`);
    console.log(`   First element: ${JSON.stringify(rows[0]).substring(0, 100)}`);
    
    return `## 📊 Query Results

**Status:** ⚠️ Query returned unexpected data format

**Records:** ${rows.length}

**Debug Info:** ${JSON.stringify(rows).substring(0, 300)}

### SQL Query:
\`\`\`sql
${sqlQuery}
\`\`\``;
  }
  // ========== END CRITICAL FIXES ==========
  
  const columns = Object.keys(rows[0]);
  
  // ========== NEW: Check if this is a note summary query ==========
  const isNoteSummary = isNoteSummaryQuery(originalQuestion);
  const isPricingFromNotes = isPricingFromNotesQuery(originalQuestion);
  const hasNoteColumns = columns.some(col => 
    col === 'Note' || col === 'NoteType' || col.toLowerCase().includes('note')
  );
  
  if ((isNoteSummary || isPricingFromNotes) && hasNoteColumns) {
    const focusType = isPricingFromNotes ? 'pricing-focused' : 'comprehensive';
    console.log(`📝 Detected ${focusType} note summary query - generating AI summary`);
    const patNum = extractPatientNumber(rows);
    
    // Return a promise that will be awaited in the calling function
    return generateNoteSummary(rows, patNum, isPricingFromNotes).then(summaryResult => {
      if (summaryResult.success) {
        const summaryTypeLabel = isPricingFromNotes ? '💰 AI-Powered Pricing Information from Notes' : '📋 AI-Powered Note Summary';
        let output = `## ${summaryTypeLabel}\n\n`;
        output += `**Patient Number:** ${patNum}\n`;
        output += `**Total Notes Analyzed:** ${summaryResult.noteCount}\n`;
        if (isPricingFromNotes) {
          output += `**Focus:** Pricing and Financial Information Only\n`;
        }
        output += `\n---\n\n`;
        output += summaryResult.summary;
        output += `\n\n---\n\n`;
        output += `<details>\n<summary>🔍 View All ${rows.length} Raw Notes</summary>\n\n`;
        
        // Add raw notes
        rows.forEach((note, idx) => {
          output += `### Note ${idx + 1}\n`;
          output += `**Type:** ${note.NoteType || 'Unknown'}\n`;
          output += `**Date:** ${note.DateTime || 'No date'}\n`;
          output += `**Content:** ${note.Note || 'No content'}\n\n`;
        });
        
        output += `</details>\n\n`;
        output += `<details>\n<summary>🔍 View SQL Query Used</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>\n`;
        
        return output;
      } else {
        // If AI summary fails, fall back to regular formatting
        console.log(`⚠️ AI summary failed, falling back to regular display`);
        console.log(`   Error: ${summaryResult.error || 'Unknown error'}`);
        
        // If it's because OpenAI key is missing, add a helpful message
        if (!config.OPENAI_API_KEY) {
          console.log(`   💡 OpenAI API key is not configured`);
          console.log(`   💡 Set OPENAI_API_KEY in your .env file to enable AI summaries`);
        }
        
        return formatCleanResults(rows, columns, sqlQuery);
      }
    });
  }
  // ========== END: Note summary detection ==========
  
  // ========== NEW: Check if this is a pricing analysis query ==========
  const isPricingQuery = isPricingAnalysisQuery(originalQuestion);
  const hasPaymentColumns = columns.some(col => 
    ['ProcFee', 'InsPayAmt', 'WriteOff', 'Adjustments', 'Payments'].includes(col)
  );
  
  if ((isPricingQuery || hasPaymentColumns) && hasPaymentColumns) {
    console.log(`ðŸ’° Detected pricing analysis query - applying payment analyzer`);
    const patNum = extractPatientNumber(rows);
    
    if (patNum) {
      const pricingAnalysis = analyzePricingDetails(rows, patNum);
      
      // Return pricing analysis WITH the original SQL query shown
      let output = formatPricingAnalysisForChat(pricingAnalysis);
      output += `\n\n<details>\n<summary>ðŸ” View SQL Query Used</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>\n`;
      
      return output;
    }
  }
  // ========== END: Pricing analysis detection ==========
  
  // ========== NEW: CHART DETECTION ==========
  const chartData = detectChartData(rows, columns);
  
  if (chartData) {
    console.log(`   📊 Chart will be generated: ${chartData.type}`);
    
    // Generate text output
    const textOutput = formatCleanResults(rows, columns, sqlQuery);
    
    // Return object with both text and chart data
    return {
      text: textOutput,
      chart: chartData
    };
  }
  // ========== END: Chart detection ==========
  
  // Generate clean, structured output - NO MORE BROKEN TABLES
  return formatCleanResults(rows, columns, sqlQuery);
}

/**
 * Format multi-patient pricing search results
 */
function formatMultiPatientPricingResults(sqlResults, sqlQuery, originalQuestion) {
  if (!sqlResults.success || !sqlResults.data) {
    return `## ❌ Search Failed\n\n**Error:** ${sqlResults.error || 'Unknown error'}\n\n\`\`\`sql\n${sqlQuery}\n\`\`\``;
  }
  
  const rows = sqlResults.data.data || sqlResults.data;
  
  if (!rows || rows.length === 0) {
    return `## 🔍 Multi-Patient Pricing Search\n\n**Status:** ✅ Query executed successfully\n\n**Results:** No patients found with pricing information in their notes.\n\n<details>\n<summary>🔍 View SQL Query Used</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>`;
  }
  
  // Build formatted output
  let output = `## 🔍 Patients with Pricing Information\n\n`;
  output += `**Found:** ${rows.length} patient${rows.length !== 1 ? 's' : ''} with pricing discussions\n\n`;
  output += `---\n\n`;
  
  // Show results in card format
  rows.forEach((row, idx) => {
    output += `### ${idx + 1}. ${row.PatientName || 'Unknown Patient'}\n\n`;
    output += `**Patient Number:** \`${row.PatNum}\`\n`;
    output += `**Notes with Pricing:** ${row.NotesWithPricing || 0}\n`;
    output += `**Latest Discussion:** ${row.LatestPricingDiscussion || 'Unknown'}\n\n`;
    
    if (row.LatestNote) {
      const notePreview = row.LatestNote.substring(0, 300);
      output += `**Latest Note Preview:**\n`;
      output += `> ${notePreview}${row.LatestNote.length > 300 ? '...' : ''}\n\n`;
      
      // Extract pricing if visible in preview
      const pricingPattern = /\$(\d+)(?:\s*per\s*month)?/gi;
      const matches = row.LatestNote.match(pricingPattern);
      if (matches && matches.length > 0) {
        output += `**Pricing Mentioned:** ${matches.slice(0, 3).join(', ')}\n\n`;
      }
    }
    
    output += `**Quick Actions:**\n`;
    output += `- To see full summary: \`summarize only pricing from notes for patnum = ${row.PatNum}\`\n`;
    output += `- To see all notes: \`summarize all notes for patnum = ${row.PatNum}\`\n\n`;
    
    if (idx < rows.length - 1) {
      output += `---\n\n`;
    }
  });
  
  output += `\n---\n\n`;
  output += `💡 **Tip:** Click on a patient number to get detailed pricing analysis.\n\n`;
  output += `<details>\n<summary>🔍 View SQL Query Used</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>\n`;
  
  return output;
}

/**
 * Format clean, structured results - THE MAIN FIX
 */
function formatCleanResults(rows, columns, sqlQuery) {
  const rowCount = rows.length;
  
  // Build output
  let output = '';
  
  // Header
  output += `## ðŸ“Š Query Results\n\n`;
  output += `âœ… Found **${rowCount.toLocaleString()}** record${rowCount !== 1 ? 's' : ''}\n\n`;
  output += `---\n\n`;
  
  // Decide format based on size - using configuration
  if (rowCount <= config.DISPLAY_CONFIG.CARD_THRESHOLD) {
    // Use card layout for small results
    output += formatAsCards(rows, columns);
  } else {
    // Use table for larger results  
    output += formatAsTable(rows, columns);
  }
  
  // SQL query at bottom
  output += `\n---\n\n`;
  output += `<details>\n<summary>ðŸ” View SQL Query</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>\n`;
  
  return output;
}

/**
 * Format as cards - for small datasets (â‰¤5 records)
 */
function formatAsCards(rows, columns) {
  let output = '';
  
  rows.forEach((row, index) => {
    output += `### ðŸ“‹ Record ${index + 1}\n\n`;
    
    columns.forEach(col => {
      const value = cleanValue(row[col]);
      if (value && value !== 'â€”') {
        output += `**${col}:** ${value}\n\n`;
      }
    });
    
    if (index < rows.length - 1) {
      output += `---\n\n`;
    }
  });
  
  return output;
}

/**
 * Format as table - for larger datasets (>5 records)
 */
function formatAsTable(rows, columns) {
  const MAX_ROWS = config.DISPLAY_CONFIG.MAX_ROWS; // Using configuration value
  const displayRows = rows.slice(0, MAX_ROWS);
  
  // Select key columns if too many - DISABLED to show all columns
  const displayColumns = columns; // Show ALL columns (previously limited to 8)
  
  // Build table
  let table = '';
  
  // Header row
  table += '| ' + displayColumns.map(col => `**${col}**`).join(' | ') + ' |\n';
  table += '|' + displayColumns.map(() => '---').join('|') + '|\n';
  
  // Data rows - CRITICAL: Each row must be ONE line, no breaks
  displayRows.forEach(row => {
    const cells = displayColumns.map(col => cleanValue(row[col]));
    table += '| ' + cells.join(' | ') + ' |\n';
  });
  
  // Summary
  if (rows.length > MAX_ROWS) {
    table += `\n> Showing first ${MAX_ROWS} of ${rows.length.toLocaleString()} total records\n`;
  }
  
  return table;
}

/**
 * Clean and format a single value - CRITICAL for preventing table breaks
 */
function cleanValue(val) {
  // Handle null/undefined/empty
  if (val === null || val === undefined || val === '') return 'â€”';
  
  // Handle dates
  if (typeof val === 'string' && val.includes('T')) {
    if (val.startsWith('0001-01-01')) return 'â€”';
    const parts = val.split('T');
    const date = parts[0];
    const time = parts[1];
    if (time && !time.startsWith('00:00:00')) {
      return `${date} ${time.substring(0, 8)}`;
    }
    return date;
  }
  
  // Handle numbers
  if (typeof val === 'number') {
    return val.toLocaleString();
  }
  
  // Handle strings - CLEAN EVERYTHING
  let str = String(val);
  
  // Remove ALL newlines and tabs
  str = str.replace(/[\r\n\t]+/g, ' ');
  
  // Collapse multiple spaces
  str = str.replace(/\s+/g, ' ');
  
  // Remove pipe characters (break markdown tables)
  str = str.replace(/\|/g, 'â˜');
  
  // Trim
  str = str.trim();
  
  // Truncate if too long - Using configuration value (0 = no limit)
  const MAX_LENGTH = config.DISPLAY_CONFIG.MAX_CHAR_LENGTH;
  if (MAX_LENGTH > 0 && str.length > MAX_LENGTH) {
    str = str.substring(0, MAX_LENGTH - 3) + '...';
  }
  
  return str;
}

/**
 * Select key columns when there are too many
 */
function selectKeyColumns(columns) {
  // Priority patterns
  const priority = [
    /^id$/i, /num$/i, /^name/i, /^date/i, /^time/i, 
    /^type/i, /^status/i, /^note/i, /^desc/i
  ];
  
  const keyColumns = [];
  const otherColumns = [];
  
  columns.forEach(col => {
    if (priority.some(p => p.test(col))) {
      keyColumns.push(col);
    } else {
      otherColumns.push(col);
    }
  });
  
  // Return top 6 priority + 2 others = 8 total
  return [...keyColumns.slice(0, 6), ...otherColumns.slice(0, 2)];
}


/**
 * Analyze data to determine best visualization type
 */
function analyzeDataForVisualization(rows, columns) {
  // Check for time-series data
  const dateColumns = columns.filter(col => {
    const sampleValue = rows[0][col];
    return sampleValue && (
      typeof sampleValue === 'string' && (
        sampleValue.match(/^\d{4}-\d{2}-\d{2}/) || 
        sampleValue.includes('T00:00:00')
      )
    );
  });
  
  // Check for numeric columns
  const numericColumns = columns.filter(col => {
    return rows.some(row => typeof row[col] === 'number' && row[col] !== 0);
  });
  
  // Check for categorical columns
  const categoricalColumns = columns.filter(col => {
    const uniqueValues = new Set(rows.map(r => r[col]));
    return uniqueValues.size <= 20 && uniqueValues.size > 1;
  });
  
  // Determine visualization type
  if (dateColumns.length > 0 && numericColumns.length > 0) {
    return {
      type: 'chart',
      chartType: 'line',
      xAxis: dateColumns[0],
      yAxis: numericColumns[0],
      description: 'Time-series trend'
    };
  }
  
  if (categoricalColumns.length > 0 && numericColumns.length > 0 && rows.length <= 20) {
    return {
      type: 'chart',
      chartType: 'bar',
      xAxis: categoricalColumns[0],
      yAxis: numericColumns[0],
      description: 'Category comparison'
    };
  }
  
  if (numericColumns.length >= 2) {
    return {
      type: 'chart',
      chartType: 'scatter',
      xAxis: numericColumns[0],
      yAxis: numericColumns[1],
      description: 'Correlation analysis'
    };
  }
  
  return { type: 'table' };
}

/**
 * Generate chart visualization in Mermaid format
 */
function generateChartVisualization(visualization, rows) {
  let output = `### ðŸ“ˆ Visual Analysis: ${visualization.description}\n\n`;
  
  const xCol = visualization.xAxis;
  const yCol = visualization.yAxis;
  
  if (visualization.chartType === 'line' || visualization.chartType === 'bar') {
    // Create ASCII-style chart representation
    output += `\`\`\`\n`;
    output += `${yCol} by ${xCol}\n`;
    output += `${'='.repeat(50)}\n\n`;
    
    const chartData = rows.slice(0, 15).map(row => {
      const xVal = formatChartLabel(row[xCol]);
      const yVal = row[yCol] || 0;
      return { x: xVal, y: yVal };
    });
    
    const maxY = Math.max(...chartData.map(d => d.y), 1);
    const barWidth = 30;
    
    chartData.forEach(d => {
      const barLength = Math.round((d.y / maxY) * barWidth);
      const bar = 'â–ˆ'.repeat(barLength);
      output += `${d.x.padEnd(12)} | ${bar} ${d.y.toLocaleString()}\n`;
    });
    
    if (rows.length > 15) {
      output += `\n... (${rows.length - 15} more records)\n`;
    }
    
    output += `\`\`\`\n`;
  }
  
  return output;
}

/**
 * Format value for chart label
 */
function formatChartLabel(val) {
  if (val === null || val === undefined) return 'N/A';
  if (typeof val === 'string' && val.includes('T00:00:00')) {
    return val.split('T')[0];
  }
  const str = String(val);
  return str.length > 12 ? str.substring(0, 9) + '...' : str;
}

/**
 * Generate creative, professional data display
 */
// ==================== INTELLIGENT TXQL ENHANCEMENT SYSTEM ====================

/**
 * Extract SQL entities from user questions
 * Helps TXQL understand what the user really wants
 */
function extractSQLEntities(question) {
  const lowerQ = question.toLowerCase();
  
  const entities = {
    // Patient identification
    patientName: null,      // "John Doe" -> need to look up PatNum
    patientNum: null,       // Direct PatNum reference
    
    // Date filters
    dateRange: null,        // { startDate, endDate }
    dateContext: null,      // 'today', 'yesterday', 'this week', etc.
    
    // Status filters
    isActive: null,         // Filter for active records
    includeDeleted: false,  // Whether to include deleted records
    
    // Limit/pagination
    limit: null,            // Number of results to return
    needsLimit: true,       // Should we add a LIMIT?
    
    // Common filters
    state: null,            // US state
    status: null,           // Generic status field
    
    // Table hints
    tableType: null         // 'patient', 'appointment', 'payment', etc.
  };
  
  // ========== Patient Name Extraction ==========
  // Look for "for [Name]" or "patient [Name]"
  const namePatterns = [
    /\b(?:for|patient|of)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\b/,
    /\b([A-Z][a-z]+\s+[A-Z][a-z]+)'s?\b/,
  ];
  
  for (const pattern of namePatterns) {
    const match = question.match(pattern);
    if (match) {
      entities.patientName = match[1];
      console.log(`   👤 Detected patient name: ${entities.patientName}`);
      break;
    }
  }
  
  // ========== PatNum Extraction ==========
  const patNumMatch = lowerQ.match(/\bpatnum\s*=?\s*(\d+)/i);
  if (patNumMatch) {
    entities.patientNum = parseInt(patNumMatch[1]);
    console.log(`   🔢 Detected PatNum: ${entities.patientNum}`);
  }
  
  // ========== Date Range Detection ==========
  entities.dateRange = config.detectSingleDateFromQuestion(question);
  
  if (lowerQ.includes('today')) {
    entities.dateContext = 'today';
  } else if (lowerQ.includes('yesterday')) {
    entities.dateContext = 'yesterday';
  } else if (lowerQ.match(/this week|last week/)) {
    entities.dateContext = 'week';
  } else if (lowerQ.match(/this month|last month/)) {
    entities.dateContext = 'month';
  }
  
  if (entities.dateRange) {
    console.log(`   📅 Date range: ${entities.dateRange.startDate} to ${entities.dateRange.endDate}`);
  }
  
  // ========== Active/Deleted Filters ==========
  if (lowerQ.match(/\bactive\b/)) {
    entities.isActive = true;
    console.log(`   ✅ Filter: Active only`);
  }
  
  if (lowerQ.match(/\bdeleted\b/)) {
    entities.includeDeleted = true;
    console.log(`   🗑️ Include deleted records`);
  }
  
  // ========== State Filter ==========
  const statePattern = /\b(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|CA|NY|TX|FL|IL)\b/i;
  const stateMatch = question.match(statePattern);
  if (stateMatch) {
    entities.state = stateMatch[1];
    console.log(`   🗺️ State filter: ${entities.state}`);
  }
  
  // ========== Table Type Detection ==========
  const tableKeywords = {
    patient: ['patient', 'patients'],
    appointment: ['appointment', 'appointments', 'appt', 'schedule'],
    payment: ['payment', 'payments', 'transaction', 'transactions'],
    procedure: ['procedure', 'procedures', 'treatment', 'treatments'],
    note: ['note', 'notes', 'comment', 'comments']
  };
  
  for (const [table, keywords] of Object.entries(tableKeywords)) {
    if (keywords.some(kw => lowerQ.includes(kw))) {
      entities.tableType = table;
      console.log(`   📋 Table type: ${table}`);
      break;
    }
  }
  
  // ========== Limit Detection ==========
  const limitMatch = lowerQ.match(/\b(?:top|first|limit)\s+(\d+)/);
  if (limitMatch) {
    entities.limit = parseInt(limitMatch[1]);
    entities.needsLimit = false; // User specified, don't auto-add
    console.log(`   🔢 User-specified limit: ${entities.limit}`);
  } else if (lowerQ.match(/\ball\b/)) {
    entities.needsLimit = false; // User wants all results
    console.log(`   ∞ User requested all results`);
  }
  
  return entities;
}

/**
 * Enhance SQL query with smart defaults and safety checks
 */
function enhanceSQLQuery(sqlQuery, entities) {
  let enhanced = sqlQuery.trim();
  const upperQuery = enhanced.toUpperCase();
  
  console.log(`\n   🔧 Enhancing SQL query...`);
  console.log(`   📝 Original: ${enhanced.substring(0, 100)}...`);
  
  // ========== 1. Add LIMIT if missing (SAFETY) ==========
  if (entities.needsLimit && !upperQuery.includes('LIMIT')) {
    const defaultLimit = entities.limit || 100;
    
    // Remove trailing semicolon if present
    if (enhanced.endsWith(';')) {
      enhanced = enhanced.slice(0, -1).trim();
    }
    
    enhanced += ` LIMIT ${defaultLimit}`;
    console.log(`   ✅ Added safety LIMIT ${defaultLimit}`);
  }
  
  // ========== 2. Add Active Filter if requested ==========
  if (entities.isActive && !upperQuery.includes('ISACTIVE')) {
    const hasWhere = upperQuery.includes('WHERE');
    
    if (hasWhere) {
      // Find the WHERE clause and add condition
      enhanced = enhanced.replace(/WHERE/i, 'WHERE IsActive = 1 AND');
    } else {
      // Add WHERE clause before ORDER BY, LIMIT, or at the end
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ' WHERE IsActive = 1' + enhanced.slice(insertPoint);
      } else {
        // Remove LIMIT temporarily if it exists
        const limitMatch = enhanced.match(/\s+LIMIT\s+\d+$/i);
        if (limitMatch) {
          const limitClause = limitMatch[0];
          enhanced = enhanced.slice(0, -limitClause.length) + ' WHERE IsActive = 1' + limitClause;
        } else {
          enhanced += ' WHERE IsActive = 1';
        }
      }
    }
    console.log(`   ✅ Added IsActive = 1 filter`);
  }
  
  // ========== 3. Exclude Deleted Records (unless explicitly requested) ==========
  if (!entities.includeDeleted && (upperQuery.includes('FROM PATIENT') || upperQuery.includes('FROM `PATIENT`'))) {
    const hasWhere = upperQuery.includes('WHERE');
    
    // Common deleted record patterns
    const deletedCondition = "(PatStatus != 2)"; // PatStatus 2 = deleted
    
    if (hasWhere && !upperQuery.includes('PATSTATUS')) {
      enhanced = enhanced.replace(/WHERE/i, `WHERE ${deletedCondition} AND`);
      console.log(`   ✅ Added deleted record filter (PatStatus != 2)`);
    } else if (!hasWhere && !upperQuery.includes('PATSTATUS')) {
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${deletedCondition}` + enhanced.slice(insertPoint);
      } else {
        // Remove LIMIT temporarily if it exists
        const limitMatch = enhanced.match(/\s+LIMIT\s+\d+$/i);
        if (limitMatch) {
          const limitClause = limitMatch[0];
          enhanced = enhanced.slice(0, -limitClause.length) + ` WHERE ${deletedCondition}` + limitClause;
        } else {
          enhanced += ` WHERE ${deletedCondition}`;
        }
      }
      console.log(`   ✅ Added deleted record filter (PatStatus != 2)`);
    }
  }
  
  // ========== 4. Add Date Range Filter ==========
  if (entities.dateRange) {
    // Try to find date column in query
    const dateColumns = ['AptDateTime', 'ProcDate', 'DateEntry', 'CreatedDate', 'ModifiedDate', 'CommDateTime'];
    
    let dateColumn = null;
    for (const col of dateColumns) {
      if (upperQuery.includes(col.toUpperCase())) {
        dateColumn = col;
        break;
      }
    }
    
    if (dateColumn) {
      const hasWhere = upperQuery.includes('WHERE');
      const dateFilter = `DATE(${dateColumn}) BETWEEN '${entities.dateRange.startDate}' AND '${entities.dateRange.endDate}'`;
      
      if (hasWhere) {
        enhanced = enhanced.replace(/WHERE/i, `WHERE ${dateFilter} AND`);
      } else {
        const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
        if (insertPoint !== -1) {
          enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${dateFilter}` + enhanced.slice(insertPoint);
        } else {
          // Remove LIMIT temporarily if it exists
          const limitMatch = enhanced.match(/\s+LIMIT\s+\d+$/i);
          if (limitMatch) {
            const limitClause = limitMatch[0];
            enhanced = enhanced.slice(0, -limitClause.length) + ` WHERE ${dateFilter}` + limitClause;
          } else {
            enhanced += ` WHERE ${dateFilter}`;
          }
        }
      }
      console.log(`   ✅ Added date range filter on ${dateColumn}`);
    }
  }
  
  // ========== 5. Add State Filter ==========
  if (entities.state) {
    const hasWhere = upperQuery.includes('WHERE');
    const stateFilter = `State = '${entities.state}'`;
    
    if (hasWhere) {
      enhanced = enhanced.replace(/WHERE/i, `WHERE ${stateFilter} AND`);
    } else {
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${stateFilter}` + enhanced.slice(insertPoint);
      } else {
        // Remove LIMIT temporarily if it exists
        const limitMatch = enhanced.match(/\s+LIMIT\s+\d+$/i);
        if (limitMatch) {
          const limitClause = limitMatch[0];
          enhanced = enhanced.slice(0, -limitClause.length) + ` WHERE ${stateFilter}` + limitClause;
        } else {
          enhanced += ` WHERE ${stateFilter}`;
        }
      }
    }
    console.log(`   ✅ Added state filter: ${entities.state}`);
  }
  
  console.log(`   📝 Enhanced: ${enhanced.substring(0, 150)}...`);
  return enhanced;
}

/**
 * Look up PatNum for a patient name
 */
async function lookupPatientNumber(patientName) {
  console.log(`   🔍 Looking up PatNum for: ${patientName}`);
  
  try {
    // Split name into parts
    const nameParts = patientName.trim().split(/\s+/);
    const firstName = nameParts[0];
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : '';
    
    // Build search query
    let searchQuery = `SELECT PatNum, LName, FName FROM patient WHERE `;
    
    if (lastName) {
      searchQuery += `LName LIKE '%${lastName}%' AND FName LIKE '%${firstName}%'`;
    } else {
      searchQuery += `(LName LIKE '%${firstName}%' OR FName LIKE '%${firstName}%')`;
    }
    
    searchQuery += ` AND PatStatus != 2 LIMIT 10`;
    
    // Execute lookup query
    const result = await executeSQL(searchQuery);
    
    if (result.success && result.data && result.data.data && result.data.data.length > 0) {
      const patients = result.data.data;
      console.log(`   ✅ Found ${patients.length} matching patient(s)`);
      
      if (patients.length === 1) {
        return {
          found: true,
          patNum: patients[0].PatNum,
          name: `${patients[0].FName} ${patients[0].LName}`,
          matches: patients
        };
      } else {
        return {
          found: true,
          multiple: true,
          matches: patients,
          message: `Found ${patients.length} patients matching "${patientName}"`
        };
      }
    } else {
      return {
        found: false,
        message: `No patient found matching "${patientName}"`
      };
    }
  } catch (error) {
    console.error(`   ❌ Error looking up patient: ${error.message}`);
    return {
      found: false,
      error: error.message
    };
  }
}

/**
 * Substitute patient name with PatNum in query
 */
function substitutePatientNumber(sqlQuery, patientName, patNum) {
  console.log(`   🔄 Substituting "${patientName}" with PatNum = ${patNum}`);
  
  let modified = sqlQuery;
  
  // Pattern 1: WHERE ... LIKE '%Name%'
  const likePattern = new RegExp(`LIKE\\s+['"]%?${patientName}%?['"]`, 'gi');
  modified = modified.replace(likePattern, `= ${patNum}`);
  
  // Pattern 2: WHERE Name = 'John Doe'
  const equalPattern = new RegExp(`=\\s+['"]${patientName}['"]`, 'gi');
  modified = modified.replace(equalPattern, `= ${patNum}`);
  
  // Pattern 3: Just the name in quotes
  const quotedPattern = new RegExp(`['"]${patientName}['"]`, 'gi');
  modified = modified.replace(quotedPattern, patNum.toString());
  
  // Pattern 4: Column name containing the name
  const columnPattern = new RegExp(`(FName|LName|PatientName)\\s*=\\s*['"]${patientName}['"]`, 'gi');
  modified = modified.replace(columnPattern, `PatNum = ${patNum}`);
  
  return modified;
}

// ==================== TXQL FUNCTION (MODIFIED) ====================

/**
 * Validates SQL query for common syntax errors
 */
function validateSQL(query) {
  if (!query) {
    return { valid: false, error: 'Query is empty or null' };
  }

  const upperQuery = query.toUpperCase();
  
  // Check for incomplete subqueries (SELECT without FROM before WHERE)
  const selectWithoutFrom = /SELECT[\s\S]+?WHERE/i;
  if (selectWithoutFrom.test(query)) {
    const fromCheck = /SELECT[\s\S]+?FROM[\s\S]+?WHERE/i;
    if (!fromCheck.test(query)) {
      return { 
        valid: false, 
        error: 'Invalid SQL: SELECT statement missing FROM clause before WHERE'
      };
    }
  }
  
  // Check for unclosed comments
  if ((query.match(/\/\*/g) || []).length !== (query.match(/\*\//g) || []).length) {
    return { 
      valid: false, 
      error: 'Invalid SQL: Unclosed comment block (/* without */)'
    };
  }
  
  // Check for basic SQL structure
  if (upperQuery.includes('SELECT')) {
    // For SELECT queries, ensure basic structure is present
    if (!upperQuery.includes('FROM') && !upperQuery.includes('DUAL')) {
      return { 
        valid: false, 
        error: 'Invalid SQL: SELECT statement missing FROM clause'
      };
    }
  }
  
  return { valid: true };
}

/**
 * Ensures SQL query has a LIMIT clause to prevent database errors
 */
function ensureQueryHasLimit(query, defaultLimit = 1000) {
  if (!query) return query;
  
  const trimmedQuery = query.trim();
  const upperQuery = trimmedQuery.toUpperCase();
  
  // Check if query already has LIMIT
  if (upperQuery.includes('LIMIT')) {
    console.log('   âœ… Query already has LIMIT clause');
    return trimmedQuery;
  }
  
  // Remove trailing semicolon if present
  const cleanQuery = trimmedQuery.endsWith(';') 
    ? trimmedQuery.slice(0, -1) 
    : trimmedQuery;
  
  // Add LIMIT clause
  const modifiedQuery = `${cleanQuery} LIMIT ${defaultLimit};`;
  console.log(`   ðŸ”§ Auto-added LIMIT ${defaultLimit} to query`);
  return modifiedQuery;
}


// ==================== INTELLIGENT TXQL ENHANCEMENT SYSTEM ====================

/**
 * Extract SQL entities from user questions
 */
function extractSQLEntities(question) {
  const lowerQ = question.toLowerCase();
  
  const entities = {
    patientName: null,
    patientNum: null,
    dateRange: null,
    dateContext: null,
    isActive: null,
    includeDeleted: false,
    limit: null,
    needsLimit: true,
    state: null,
    tableType: null
  };
  
  // Patient Name Extraction
  const namePatterns = [
    /\b(?:for|patient|of)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\b/,
    /\b([A-Z][a-z]+\s+[A-Z][a-z]+)'s?\b/,
  ];
  
  for (const pattern of namePatterns) {
    const match = question.match(pattern);
    if (match) {
      entities.patientName = match[1];
      console.log(`   👤 Detected patient name: ${entities.patientName}`);
      break;
    }
  }
  
  // PatNum Extraction
  const patNumMatch = lowerQ.match(/\bpatnum\s*=?\s*(\d+)/i);
  if (patNumMatch) {
    entities.patientNum = parseInt(patNumMatch[1]);
    console.log(`   🔢 Detected PatNum: ${entities.patientNum}`);
  }
  
  // Date Range Detection
  entities.dateRange = config.detectSingleDateFromQuestion(question);
  if (lowerQ.includes('today')) entities.dateContext = 'today';
  else if (lowerQ.includes('yesterday')) entities.dateContext = 'yesterday';
  else if (lowerQ.match(/this week|last week/)) entities.dateContext = 'week';
  else if (lowerQ.match(/this month|last month/)) entities.dateContext = 'month';
  
  if (entities.dateRange) {
    console.log(`   📅 Date range: ${entities.dateRange.startDate} to ${entities.dateRange.endDate}`);
  }
  
  // Active/Deleted Filters
  if (lowerQ.match(/\bactive\b/)) {
    entities.isActive = true;
    console.log(`   ✅ Filter: Active only`);
  }
  
  if (lowerQ.match(/\bdeleted\b/)) {
    entities.includeDeleted = true;
    console.log(`   🗑️ Include deleted records`);
  }
  
  // State Filter
  const statePattern = /\b(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|CA|NY|TX|FL|IL)\b/i;
  const stateMatch = question.match(statePattern);
  if (stateMatch) {
    entities.state = stateMatch[1];
    console.log(`   🗺️ State filter: ${entities.state}`);
  }
  
  // Table Type Detection
  const tableKeywords = {
    patient: ['patient', 'patients'],
    appointment: ['appointment', 'appointments', 'appt', 'schedule'],
    payment: ['payment', 'payments', 'transaction', 'transactions'],
    procedure: ['procedure', 'procedures', 'treatment', 'treatments'],
    note: ['note', 'notes', 'comment', 'comments']
  };
  
  for (const [table, keywords] of Object.entries(tableKeywords)) {
    if (keywords.some(kw => lowerQ.includes(kw))) {
      entities.tableType = table;
      console.log(`   📋 Table type: ${table}`);
      break;
    }
  }
  
  // Limit Detection
  const limitMatch = lowerQ.match(/\b(?:top|first|limit)\s+(\d+)/);
  if (limitMatch) {
    entities.limit = parseInt(limitMatch[1]);
    entities.needsLimit = false;
    console.log(`   🔢 User-specified limit: ${entities.limit}`);
  } else if (lowerQ.match(/\ball\b/)) {
    entities.needsLimit = false;
    console.log(`   ∞ User requested all results`);
  }
  
  return entities;
}

/**
 * Enhance SQL query with smart defaults
 */
function enhanceSQLQuery(sqlQuery, entities) {
  let enhanced = sqlQuery.trim();
  const upperQuery = enhanced.toUpperCase();
  
  console.log(`\n   🔧 Enhancing SQL query...`);
  console.log(`   📝 Original: ${enhanced.substring(0, 100)}...`);
  
  // Add LIMIT if missing
  if (entities.needsLimit && !upperQuery.includes('LIMIT')) {
    const defaultLimit = entities.limit || 100;
    if (enhanced.endsWith(';')) enhanced = enhanced.slice(0, -1).trim();
    enhanced += ` LIMIT ${defaultLimit}`;
    console.log(`   ✅ Added safety LIMIT ${defaultLimit}`);
  }
  
  // Add Active Filter
  if (entities.isActive && !upperQuery.includes('ISACTIVE')) {
    const hasWhere = upperQuery.includes('WHERE');
    if (hasWhere) {
      enhanced = enhanced.replace(/WHERE/i, 'WHERE IsActive = 1 AND');
    } else {
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ' WHERE IsActive = 1' + enhanced.slice(insertPoint);
      } else {
        enhanced += ' WHERE IsActive = 1';
      }
    }
    console.log(`   ✅ Added IsActive = 1 filter`);
  }
  
  // Exclude Deleted Records
  if (!entities.includeDeleted && (upperQuery.includes('FROM PATIENT') || upperQuery.includes('FROM `PATIENT`'))) {
    const hasWhere = upperQuery.includes('WHERE');
    const deletedCondition = "(PatStatus != 2)";
    
    if (hasWhere && !upperQuery.includes('PATSTATUS')) {
      enhanced = enhanced.replace(/WHERE/i, `WHERE ${deletedCondition} AND`);
      console.log(`   ✅ Added deleted record filter (PatStatus != 2)`);
    } else if (!hasWhere && !upperQuery.includes('PATSTATUS')) {
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${deletedCondition}` + enhanced.slice(insertPoint);
      } else {
        enhanced += ` WHERE ${deletedCondition}`;
      }
      console.log(`   ✅ Added deleted record filter (PatStatus != 2)`);
    }
  }
  
  // Add Date Range Filter
  if (entities.dateRange) {
    const dateColumns = ['AptDateTime', 'ProcDate', 'DateEntry', 'CreatedDate', 'ModifiedDate', 'CommDateTime'];
    let dateColumn = null;
    for (const col of dateColumns) {
      if (upperQuery.includes(col.toUpperCase())) {
        dateColumn = col;
        break;
      }
    }
    
    if (dateColumn) {
      const hasWhere = upperQuery.includes('WHERE');
      const dateFilter = `DATE(${dateColumn}) BETWEEN '${entities.dateRange.startDate}' AND '${entities.dateRange.endDate}'`;
      
      if (hasWhere) {
        enhanced = enhanced.replace(/WHERE/i, `WHERE ${dateFilter} AND`);
      } else {
        const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
        if (insertPoint !== -1) {
          enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${dateFilter}` + enhanced.slice(insertPoint);
        } else {
          enhanced += ` WHERE ${dateFilter}`;
        }
      }
      console.log(`   ✅ Added date range filter on ${dateColumn}`);
    }
  }
  
  // Add State Filter
  if (entities.state) {
    const hasWhere = upperQuery.includes('WHERE');
    const stateFilter = `State = '${entities.state}'`;
    
    if (hasWhere) {
      enhanced = enhanced.replace(/WHERE/i, `WHERE ${stateFilter} AND`);
    } else {
      const insertPoint = enhanced.search(/\s+(ORDER BY|LIMIT)/i);
      if (insertPoint !== -1) {
        enhanced = enhanced.slice(0, insertPoint) + ` WHERE ${stateFilter}` + enhanced.slice(insertPoint);
      } else {
        enhanced += ` WHERE ${stateFilter}`;
      }
    }
    console.log(`   ✅ Added state filter: ${entities.state}`);
  }
  
  console.log(`   📝 Enhanced: ${enhanced.substring(0, 150)}...`);
  return enhanced;
}

/**
 * Look up PatNum for a patient name
 */
async function lookupPatientNumber(patientName) {
  console.log(`   🔍 Looking up PatNum for: ${patientName}`);
  
  try {
    const nameParts = patientName.trim().split(/\s+/);
    const firstName = nameParts[0];
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : '';
    
    let searchQuery = `SELECT PatNum, LName, FName FROM patient WHERE `;
    if (lastName) {
      searchQuery += `LName LIKE '%${lastName}%' AND FName LIKE '%${firstName}%'`;
    } else {
      searchQuery += `(LName LIKE '%${firstName}%' OR FName LIKE '%${firstName}%')`;
    }
    searchQuery += ` AND PatStatus != 2 LIMIT 10`;
    
    const result = await executeSQL(searchQuery);
    
    if (result.success && result.data && result.data.data && result.data.data.length > 0) {
      const patients = result.data.data;
      console.log(`   ✅ Found ${patients.length} matching patient(s)`);
      
      if (patients.length === 1) {
        return {
          found: true,
          patNum: patients[0].PatNum,
          name: `${patients[0].FName} ${patients[0].LName}`,
          matches: patients
        };
      } else {
        return {
          found: true,
          multiple: true,
          matches: patients,
          message: `Found ${patients.length} patients matching "${patientName}"`
        };
      }
    } else {
      return {
        found: false,
        message: `No patient found matching "${patientName}"`
      };
    }
  } catch (error) {
    console.error(`   ❌ Error looking up patient: ${error.message}`);
    return {
      found: false,
      error: error.message
    };
  }
}

/**
 * Substitute patient name with PatNum in query
 */
function substitutePatientNumber(sqlQuery, patientName, patNum) {
  console.log(`   🔄 Substituting "${patientName}" with PatNum = ${patNum}`);
  
  let modified = sqlQuery;
  
  // Pattern 1: WHERE ... LIKE '%Name%'
  const likePattern = new RegExp(`LIKE\\s+['"]%?${patientName}%?['"]`, 'gi');
  modified = modified.replace(likePattern, `= ${patNum}`);
  
  // Pattern 2: WHERE Name = 'John Doe'
  const equalPattern = new RegExp(`=\\s+['"]${patientName}['"]`, 'gi');
  modified = modified.replace(equalPattern, `= ${patNum}`);
  
  // Pattern 3: Just the name in quotes
  const quotedPattern = new RegExp(`['"]${patientName}['"]`, 'gi');
  modified = modified.replace(quotedPattern, patNum.toString());
  
  // Pattern 4: Column name containing the name
  const columnPattern = new RegExp(`(FName|LName|PatientName)\\s*=\\s*['"]${patientName}['"]`, 'gi');
  modified = modified.replace(columnPattern, `PatNum = ${patNum}`);
  
  return modified;
}

async function queryTXQL(question, sessionId, maxRetries = 3, timeout = 60000, customKey = null) {
  if (!question || question.trim() === '') {
    return {
      success: false,
      error: 'Please provide a valid question.',
      friendlyError: 'Please provide a valid question.'
    };
  }

  if (!sessionId) {
    return {
      success: false,
      error: 'Session ID is required.',
      friendlyError: 'Session error occurred. Please refresh and try again.'
    };
  }

  // ========== STEP 1: Extract Entities from Question ==========
  console.log(`\n🎯 TXQL Enhancement: Extracting entities from question...`);
  const entities = extractSQLEntities(question);
  
  // ========== STEP 2: Patient Name Lookup (if detected) ==========
  let patientLookup = null;
  if (entities.patientName && !entities.patientNum) {
    console.log(`\n🔍 Patient name detected, looking up PatNum...`);
    patientLookup = await lookupPatientNumber(entities.patientName);
    
    if (patientLookup.found && !patientLookup.multiple) {
      entities.patientNum = patientLookup.patNum;
      console.log(`   ✅ Resolved to PatNum: ${entities.patientNum}`);
    } else if (patientLookup.multiple) {
      // Multiple patients found - return clarification
      console.log(`   ⚠️  Multiple patients found, need clarification`);
      
      let clarificationMessage = `## 👥 Multiple Patients Found\n\n`;
      clarificationMessage += `I found **${patientLookup.matches.length}** patients matching "${entities.patientName}":\n\n`;
      
      patientLookup.matches.forEach((patient, idx) => {
        clarificationMessage += `${idx + 1}. **${patient.FName} ${patient.LName}** (PatNum: \`${patient.PatNum}\`)\n`;
      });
      
      clarificationMessage += `\n💡 **Please rephrase your question with the specific PatNum**, for example:\n`;
      clarificationMessage += `- "Show appointments for PatNum = ${patientLookup.matches[0].PatNum}"\n`;
      clarificationMessage += `- "Get notes for PatNum ${patientLookup.matches[0].PatNum}"\n`;
      
      return {
        success: true,
        data: clarificationMessage,
        needsClarification: true,
        patientMatches: patientLookup.matches,
        sessionId: sessionId,
        system: 'txql'
      };
    } else {
      // No patients found
      console.log(`   ❌ No patients found matching "${entities.patientName}"`);
      
      return {
        success: true,
        data: `## ❌ No Patient Found\n\nI couldn't find any patient matching "${entities.patientName}".\n\n💡 **Suggestions:**\n- Check the spelling\n- Try using first and last name\n- Use PatNum if you know it: "Show appointments for PatNum = 123"`,
        sessionId: sessionId,
        system: 'txql'
      };
    }
  }

  // SPECIAL CASE: Multi-patient pricing search
  // Handle queries like "find 10 patients with pricing information"
  if (isMultiPatientPricingSearch(question)) {
    console.log(`🔍 Detected multi-patient pricing search query`);
    console.log(`   Bypassing TXQL - using direct SQL for pricing search`);
    
    const searchSQL = buildMultiPatientPricingSearchSQL(question);
    if (searchSQL) {
      const safeSqlQuery = ensureQueryHasLimit(searchSQL);
      const sqlResults = await executeSQL(safeSqlQuery, customKey);
      
      // Format results specifically for pricing search
      const formattedOutput = formatMultiPatientPricingResults(sqlResults, safeSqlQuery, question);
      
      return {
        success: true,
        data: formattedOutput,
        sqlQuery: safeSqlQuery,
        executionResults: sqlResults,
        sessionId: sessionId,
        attempts: 1,
        system: 'txql',
        queryType: 'multi_patient_pricing_search'
      };
    }
  }

  let lastError = null;
  const TXQL_API_URL = 'https://txql.8px.us/api/sql/query';

  // Log initial connection attempt details
  console.log(`🔗 TXQL Connection Details:`);
  console.log(`   Endpoint: ${TXQL_API_URL}`);
  console.log(`   Session: ${sessionId}`);
  console.log(`   Question: ${question.trim()}`);
  console.log(`   Timeout: ${timeout}ms`);
  console.log(`   Max Retries: ${maxRetries}`);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const startTime = Date.now(); // Moved outside try block
    try {
      console.log(`🔄 TXQL Attempt ${attempt}/${maxRetries}: Querying...`);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        console.warn(`⏱️  Timeout triggered after ${timeout}ms on attempt ${attempt}`);
        controller.abort();
      }, timeout);

      const response = await fetch(TXQL_API_URL, {
        method: 'POST',
        headers: {
          'accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          question: question.trim(),
          session_id: sessionId
        }),
        signal: controller.signal
      });

      const elapsedTime = Date.now() - startTime;
      clearTimeout(timeoutId);
      
      console.log(`   ⏱️  Request completed in ${elapsedTime}ms`);

      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unknown error');
        throw new Error(`Server responded with status ${response.status}: ${errorText}`);
      }

      const data = await response.json();
      console.log(`✅ TXQL: Successfully received response (Attempt ${attempt})`);
      console.log(`   Response keys:`, Object.keys(data));

      // ========== STEP 3: Extract SQL and Enhance It ==========
      const sqlQuery = extractSQLFromTXQL(data);
      
      if (sqlQuery) {
        console.log(`📊 Extracted SQL query, validating...`);
        
        // SPECIAL HANDLING: Check if TXQL query is incomplete for note requests
        const isNoteSummaryRequest = isNoteSummaryQuery(question) || isPricingFromNotesQuery(question);
        
        // Check if TXQL generated the WRONG type of query (procedure pricing instead of notes)
        const txqlGeneratedWrongQuery = isNoteSummaryRequest && 
                                         !sqlQuery.toLowerCase().includes('procnote') && 
                                         !sqlQuery.toLowerCase().includes('patientnote');
        
        // Check if TXQL query is incomplete (only procnote, missing other tables)
        const txqlQueryIncomplete = isNoteSummaryRequest && 
                                     sqlQuery.includes("procnote") && 
                                     !sqlQuery.includes("UNION");
        
        let finalSqlQuery = sqlQuery;
        
        if (txqlGeneratedWrongQuery) {
          console.log(`   ⚠️  TXQL generated WRONG query type (procedure pricing instead of notes)`);
          console.log(`   💡 Forcing note query for: "${question}"`);
          const betterSQL = buildNoteQuerySQL(question);
          if (betterSQL) {
            finalSqlQuery = betterSQL;
            console.log(`   ✅ Switched to comprehensive note query`);
          }
        } else if (txqlQueryIncomplete) {
          console.log(`   ⚠️  TXQL query incomplete (missing patientnote table)`);
          console.log(`   💡 Using comprehensive note query to get ALL notes`);
          const betterSQL = buildNoteQuerySQL(question);
          if (betterSQL) {
            finalSqlQuery = betterSQL;
            console.log(`   ✅ Switched to comprehensive query`);
          }
        }
        
        // ========== STEP 4: Substitute Patient Name with PatNum ==========
        if (entities.patientName && entities.patientNum && patientLookup) {
          finalSqlQuery = substitutePatientNumber(finalSqlQuery, entities.patientName, entities.patientNum);
        }
        
        // ========== STEP 5: Enhance Query with Smart Defaults ==========
        finalSqlQuery = enhanceSQLQuery(finalSqlQuery, entities);
        
        // Validate SQL syntax
        const validation = validateSQL(finalSqlQuery);
        if (!validation.valid) {
          console.error(`❌ SQL Validation Failed: ${validation.error}`);
          
          return {
            success: false,
            error: `Invalid SQL generated by TXQL: ${validation.error}`,
            friendlyError: `The generated SQL query has syntax errors. ${validation.error}. Please rephrase your question or try a simpler query.`,
            invalidSQL: finalSqlQuery,
            sessionId: sessionId,
            attempts: attempt,
            system: 'txql'
          };
        }
        
        console.log(`✅ SQL validation passed`);
        
        // ========== STEP 6: Execute Enhanced Query ==========
        const sqlResults = await executeSQL(finalSqlQuery, customKey);
        
        // ========== STEP 7: Format Results ==========
        // Note: formatSQLResults may return a promise for AI summaries, so await it
        const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, finalSqlQuery, question));
        
        return {
          success: true,
          data: formattedOutput,
          sqlQuery: finalSqlQuery,
          originalQuery: sqlQuery, // Keep original for comparison
          executionResults: sqlResults,
          sessionId: sessionId,
          attempts: attempt,
          system: 'txql',
          enhancementApplied: true,
          entities: entities, // Include entities for debugging
          patientLookup: patientLookup // Include patient lookup result if any
        };
      } else {
        // If no SQL found, check if TXQL returned a natural language response
        console.log(`⚠️  Could not extract SQL from TXQL response`);
        
        
        // FALLBACK: Try to build SQL directly for note queries
        const fallbackSQL = buildNoteQuerySQL(question);
        if (fallbackSQL) {
          console.log('   💡 Using fallback SQL generation for note query');
          console.log('   📋 Building comprehensive note query with UNION');
          
          const enhancedFallbackSQL = enhanceSQLQuery(fallbackSQL, entities);
          const sqlResults = await executeSQL(enhancedFallbackSQL, customKey);
          const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, enhancedFallbackSQL, question));
          
          return {
            success: true,
            data: formattedOutput,
            sqlQuery: enhancedFallbackSQL,
            executionResults: sqlResults,
            sessionId: sessionId,
            attempts: attempt,
            system: 'txql',
            usedFallback: true,
            enhancementApplied: true,
            entities: entities
          };
        }
        
        // Try to return a useful response anyway
        let responseText = '';
        
        if (typeof data === 'string') {
          responseText = data;
        } else if (data.response) {
          responseText = data.response;
        } else if (data.answer) {
          responseText = data.answer;
        } else if (data.message) {
          responseText = data.message;
        } else {
          responseText = JSON.stringify(data, null, 2);
        }
        
        return {
          success: true,
          data: responseText,
          sessionId: sessionId,
          attempts: attempt,
          system: 'txql',
          noSqlExtracted: true
        };
      }

    } catch (error) {
      lastError = error;
      const elapsedTime = Date.now() - startTime;

      if (error.name === 'AbortError') {
        console.warn(`⏱️  TXQL Attempt ${attempt} timed out after ${timeout}ms`);
        console.warn(`   This usually means the TXQL service is slow or unresponsive`);
      } else if (error.message.includes('fetch failed') || error.code === 'ECONNREFUSED') {
        console.warn(`🌐 TXQL Network error on attempt ${attempt}: ${error.message}`);
        console.warn(`   Cannot reach ${TXQL_API_URL}`);
        console.warn(`   Please verify the TXQL service is running and accessible`);
      } else if (error.message.includes('ENOTFOUND')) {
        console.warn(`🌐 DNS Resolution failed on attempt ${attempt}`);
        console.warn(`   Cannot resolve hostname: txql.8px.us`);
      } else {
        console.warn(`⚠️  TXQL Error on attempt ${attempt}: ${error.message}`);
        console.warn(`   Error type: ${error.name || 'Unknown'}`);
        console.warn(`   Time elapsed: ${elapsedTime}ms`);
      }

      if (attempt < maxRetries) {
        const retryDelay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`   ⏳ Retrying in ${retryDelay}ms...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  console.error(`❌ TXQL: All ${maxRetries} retry attempts failed`);
  console.error(`   Last error: ${lastError.message}`);
  console.error(`   Error type: ${lastError.name}`);

  // FALLBACK: If this is a note summary request and TXQL failed, use direct SQL
  const isNoteSummaryRequest = isNoteSummaryQuery(question) || isPricingFromNotesQuery(question);
  
  if (isNoteSummaryRequest) {
    console.log('💡 TXQL failed, but this is a note query - attempting fallback');
    const fallbackSQL = buildNoteQuerySQL(question);
    
    if (fallbackSQL) {
      console.log('   ✅ Using fallback SQL generation for note query');
      const enhancedFallbackSQL = enhanceSQLQuery(fallbackSQL, entities);
      const sqlResults = await executeSQL(enhancedFallbackSQL, customKey);
      const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, enhancedFallbackSQL, question));
      
      return {
        success: true,
        data: formattedOutput,
        sqlQuery: enhancedFallbackSQL,
        executionResults: sqlResults,
        sessionId: sessionId,
        attempts: maxRetries,
        system: 'txql',
        usedFallback: true,
        enhancementApplied: true,
        entities: entities,
        reason: 'TXQL timeout - used direct SQL fallback'
      };
    }
  }


  let friendlyMessage = '❌ Error: Sorry, I couldn\'t connect to the database server. ';

  if (lastError.name === 'AbortError') {
    friendlyMessage += 'The request took too long (timed out after ' + (timeout/1000) + ' seconds). ';
    friendlyMessage += 'The TXQL service might be unavailable or overloaded. ';
    friendlyMessage += '\n\n💡 **Suggestions:**\n';
    friendlyMessage += '- Try a simpler query\n';
    friendlyMessage += '- Wait a moment and try again\n';
    friendlyMessage += '- Ask about call data instead (AI Voice system is working)';
  } else if (lastError.message.includes('fetch failed') || lastError.code === 'ECONNREFUSED' || lastError.message.includes('ENOTFOUND')) {
    friendlyMessage += 'Cannot reach the TXQL service at `' + TXQL_API_URL + '`. ';
    friendlyMessage += '\n\n💡 **Possible issues:**\n';
    friendlyMessage += '- The TXQL service may be down\n';
    friendlyMessage += '- Network connectivity issues\n';
    friendlyMessage += '- Firewall or DNS problems\n\n';
    friendlyMessage += 'Please check if the TXQL service is running and accessible.';
  } else {
    friendlyMessage += 'The TXQL service encountered an error. ';
    friendlyMessage += '\n\n**Error details:** ' + lastError.message;
    friendlyMessage += '\n\n💡 You can try asking about call data instead (AI Voice system is working).';
  }

  return {
    success: false,
    error: lastError.message,
    friendlyError: friendlyMessage,
    errorType: lastError.name,
    sessionId: sessionId,
    attempts: maxRetries,
    system: 'txql',
    endpoint: TXQL_API_URL
  };
}
function resolveFromContext(currentQuestion, session) {
  const result = {
    dates: null,
    licenseKey: null
  };
  
  // Try current question first
  result.dates = detectSingleDateFromQuestion(currentQuestion);
  result.licenseKey = extractLicenseKey(currentQuestion);
  
  // If we have everything, no need to check history
  if (result.dates && result.licenseKey) {
    return result;
  }
  
  // Check conversation history for missing context
  if (!result.dates || !result.licenseKey) {
    const recentHistory = session.conversationHistory.slice(-5).reverse();
    
    for (const entry of recentHistory) {
      // Only look at AI Voice queries
      if (entry.system !== 'aivoice') continue;
      
      // Resolve dates from history if not in current question
      if (!result.dates && entry.result?.dateRange) {
        result.dates = {
          startDate: entry.result.dateRange.startDate,
          endDate: entry.result.dateRange.endDate
        };
      }
      
      // Resolve license key from history if not in current question
      if (!result.licenseKey && entry.result?.licenseKey) {
        result.licenseKey = entry.result.licenseKey;
      }
      
      // Stop if we have both
      if (result.dates && result.licenseKey) break;
    }
  }
  
  return result;
}
// I'm keeping them as-is from your original code

function normalizeCall(raw, options = {}) {
  // Helper function to safely extract nested values with fallbacks
  const safeGet = (obj, paths, defaultValue = null) => {
    // Accept both single path string or array of paths to try
    const pathsToTry = Array.isArray(paths) ? paths : [paths];
    
    for (const path of pathsToTry) {
      const keys = path.split('.');
      let value = obj;
      let found = true;
      
      for (const key of keys) {
        if (value && typeof value === 'object' && key in value) {
          value = value[key];
        } else {
          found = false;
          break;
        }
      }
      
      if (found && value !== undefined && value !== null) {
        return value;
      }
    }
    
    return defaultValue;
  };

  // Try multiple possible field names and structures for each property
  const normalized = {
    id: safeGet(raw, ['aiVoiceMetaId', 'id'], 0),
    callSessionID: safeGet(raw, ['callSessionID', 'sessionId', 'meta.sessionId']),
    patientNumber: safeGet(raw, ['patientNumber', 'patient.number'], '0'),
    patientName: safeGet(raw, ['GuestName', 'patientName', 'patient.name', 'meta.guestName']),
    dateTime: safeGet(raw, ['createdAt', 'startTime', 'dateTime', 'meta.createdAt']),
    callType: safeGet(raw, ['callDirection', 'call_type', 'callType', 'type', 'direction', 'meta.callType'], 'unknown'),
    callDirection: safeGet(raw, ['callDirection', 'call_type', 'direction'], 'unknown'),
    outcome: safeGet(raw, ['call_status', 'callStatus', 'outcome', 'meta.callStatus', 'call_successful'], 'unknown'),
    sentiment: safeGet(raw, ['user_sentiment', 'sentiments', 'sentiment', 'userSentiment', 'analysis.sentiments', 'analysis.userSentiment'], 'neutral'),
    duration: safeGet(raw, ['callDuration', 'duration_ms', 'duration', 'costs.durationSec', 'durationSec'], 0),
    cost: safeGet(raw, ['totalCost', 'cost', 'costs.total'], 0),
    summary: safeGet(raw, ['callSummary', 'summary'], 'No summary available'),
    phoneNumber: safeGet(raw, ['phoneNumber', 'phone']),
    licenseKey: safeGet(raw, ['licenseKey']),
    // Additional fields from the actual API response
    appointmentBooked: safeGet(raw, ['isAppointmentBooked', 'appt_booked'], false),
    appointmentRescheduled: safeGet(raw, ['isAppointmentRescheduled', 'reschedule_success'], false),
    appointmentCancelled: safeGet(raw, ['isAppointmentCancelled'], false),
    callSuccessful: safeGet(raw, ['call_successful', 'callSuccessful'], null),
    disconnectionReason: safeGet(raw, ['disconnection_reason']),
    audioUrl: safeGet(raw, ['audioRecordingURL', 'audioUrl'])
  };

  if (options.includeTranscript && raw.transcript) {
    normalized.transcript = raw.transcript;
  }

  if (options.includeAudio && normalized.audioUrl) {
    normalized.audioUrl = normalized.audioUrl;
  }

  return normalized;
}

/**
 * Group calls by license key and return statistics
 */
function groupCallsByLicenseKey(rawCalls) {
  const grouped = {};
  
  rawCalls.forEach(call => {
    const licenseKey = call.licenseKey || 'unknown';
    
    if (!grouped[licenseKey]) {
      grouped[licenseKey] = {
        licenseKey: licenseKey,
        callCount: 0,
        totalCost: 0,
        totalDuration: 0,
        calls: []
      };
    }
    
    grouped[licenseKey].callCount++;
    grouped[licenseKey].totalCost += parseFloat(call.totalCost || 0);
    grouped[licenseKey].totalDuration += parseInt(call.callDuration || call.duration_ms || 0);
    grouped[licenseKey].calls.push(call);
  });
  
  return grouped;
}

/**
 * Build a summary of calls grouped by license key
 */
function buildLicenseKeySummary(rawCalls) {
  if (!rawCalls || rawCalls.length === 0) {
    return "No call data available.";
  }
  
  const grouped = groupCallsByLicenseKey(rawCalls);
  const licenseKeys = Object.keys(grouped);
  
  let summary = `ðŸ“Š **Call Distribution by License Key**\n\n`;
  summary += `Total License Keys: ${licenseKeys.length}\n`;
  summary += `Total Calls: ${rawCalls.length}\n\n`;
  
  // Sort by call count (descending)
  const sorted = Object.values(grouped).sort((a, b) => b.callCount - a.callCount);
  
  sorted.forEach((stats, index) => {
    const keyPreview = stats.licenseKey.length > 20 
      ? stats.licenseKey.substring(0, 20) + '...' 
      : stats.licenseKey;
    
    summary += `${index + 1}. **License Key:** \`${keyPreview}\`\n`;
    summary += `   - Calls: ${stats.callCount}\n`;
    summary += `   - Total Cost: $${stats.totalCost.toFixed(2)}\n`;
    summary += `   - Avg Duration: ${Math.round(stats.totalDuration / stats.callCount / 1000)}s\n\n`;
  });
  
  return summary;
}

/**
 * Find calls for a specific license key
 */
function getCallsForLicenseKey(rawCalls, targetLicenseKey) {
  if (!rawCalls || rawCalls.length === 0) {
    return {
      found: false,
      count: 0,
      message: "No call data available."
    };
  }
  
  // Debug: Log all unique license keys in the dataset
  const uniqueLicenseKeys = [...new Set(rawCalls.map(call => call.licenseKey))];
  console.log(`   📋 Available license keys in dataset: ${uniqueLicenseKeys.length}`);
  uniqueLicenseKeys.forEach((key, idx) => {
    console.log(`      ${idx + 1}. ${key ? key.substring(0, 40) : 'null'}...`);
  });
  console.log(`   🔍 Looking for: ${targetLicenseKey ? targetLicenseKey.substring(0, 40) : 'null'}...`);
  
  // Try 1: Exact match
  let calls = rawCalls.filter(call => call.licenseKey === targetLicenseKey);
  
  if (calls.length === 0) {
    // Try 2: Case-insensitive match
    console.log(`   🔄 Trying case-insensitive match...`);
    calls = rawCalls.filter(call => 
      call.licenseKey && 
      call.licenseKey.toLowerCase().trim() === targetLicenseKey.toLowerCase().trim()
    );
  }
  
  if (calls.length === 0) {
    // Try 3: Prefix match (handles truncated keys)
    console.log(`   🔄 Trying prefix match (first 30 chars)...`);
    const targetPrefix = targetLicenseKey.substring(0, 30).toLowerCase();
    calls = rawCalls.filter(call => 
      call.licenseKey && 
      call.licenseKey.substring(0, 30).toLowerCase() === targetPrefix
    );
    
    if (calls.length > 0) {
      console.log(`   ✅ Found ${calls.length} calls using prefix match!`);
    }
  }
  
  if (calls.length === 0) {
    // Try 4: Check if target key is a prefix of any database key
    console.log(`   🔄 Trying reverse prefix match...`);
    calls = rawCalls.filter(call => 
      call.licenseKey && 
      call.licenseKey.toLowerCase().startsWith(targetLicenseKey.toLowerCase())
    );
    
    if (calls.length > 0) {
      console.log(`   ✅ Found ${calls.length} calls using reverse prefix match!`);
    }
  }
  
  if (calls.length === 0) {
    // Try 5: Check if database key is a prefix of target key
    console.log(`   🔄 Trying contains match...`);
    calls = rawCalls.filter(call => 
      call.licenseKey && 
      targetLicenseKey.toLowerCase().startsWith(call.licenseKey.toLowerCase())
    );
    
    if (calls.length > 0) {
      console.log(`   ✅ Found ${calls.length} calls using contains match!`);
    }
  }
  
  if (calls.length === 0) {
    console.log(`   ❌ No matches found with any strategy`);
    return {
      found: false,
      count: 0,
      message: `No calls found for license key: ${targetLicenseKey}`,
      availableLicenseKeys: uniqueLicenseKeys.map(k => k ? k.substring(0, 40) + '...' : 'null'),
      searchedKey: targetLicenseKey
    };
  }
  
  const totalCost = calls.reduce((sum, c) => sum + (parseFloat(c.totalCost || 0)), 0);
  const totalDuration = calls.reduce((sum, c) => sum + (parseInt(c.callDuration || c.duration_ms || 0)), 0);
  
  console.log(`   ✅ Found ${calls.length} matching calls (total cost: $${totalCost.toFixed(2)})`);
  
  return {
    found: true,
    count: calls.length,
    licenseKey: targetLicenseKey,
    totalCost: totalCost,
    avgDuration: Math.round(totalDuration / calls.length / 1000),
    calls: calls,
    message: `Found ${calls.length} call(s) for license key: ${targetLicenseKey.substring(0, 20)}...`
  };
}

function buildConversationContext(calls) {
  if (!calls || calls.length === 0) {
    return "No call data available.";
  }

  const total = calls.length;
  const totalCost = calls.reduce((sum, c) => sum + (parseFloat(c.cost) || 0), 0);
  const avgDuration = calls.reduce((sum, c) => sum + (parseInt(c.duration) || 0), 0) / total;

  // Count call types (inbound/outbound)
  const callTypes = {};
  calls.forEach(c => {
    callTypes[c.callType] = (callTypes[c.callType] || 0) + 1;
  });

  const outcomes = {};
  calls.forEach(c => {
    outcomes[c.outcome] = (outcomes[c.outcome] || 0) + 1;
  });

  const sentiments = {};
  calls.forEach(c => {
    sentiments[c.sentiment] = (sentiments[c.sentiment] || 0) + 1;
  });

  return `
Call Data Summary:
- Total Calls: ${total}
- Total Cost: $${totalCost.toFixed(2)}
- Average Duration: ${Math.round(avgDuration)} seconds

Call Direction:
${Object.entries(callTypes).map(([k, v]) => `  - ${k}: ${v}`).join('\n')}

Outcomes:
${Object.entries(outcomes).map(([k, v]) => `  - ${k}: ${v}`).join('\n')}

Sentiments:
${Object.entries(sentiments).map(([k, v]) => `  - ${k}: ${v}`).join('\n')}
  `.trim();
}

/**
 * Count calls by direction (inbound/outbound)
 */
function countCallsByDirection(rawCalls) {
  if (!rawCalls || rawCalls.length === 0) {
    return {
      total: 0,
      inbound: 0,
      outbound: 0,
      unknown: 0,
      breakdown: []
    };
  }

  let inboundCount = 0;
  let outboundCount = 0;
  let unknownCount = 0;

  rawCalls.forEach(call => {
    const direction = (call.callDirection || call.call_type || '').toLowerCase();
    
    if (direction === 'inbound' || direction === 'incoming') {
      inboundCount++;
    } else if (direction === 'outbound' || direction === 'outgoing') {
      outboundCount++;
    } else {
      unknownCount++;
    }
  });

  return {
    total: rawCalls.length,
    inbound: inboundCount,
    outbound: outboundCount,
    unknown: unknownCount,
    inboundPercentage: ((inboundCount / rawCalls.length) * 100).toFixed(1),
    outboundPercentage: ((outboundCount / rawCalls.length) * 100).toFixed(1),
    breakdown: [
      { direction: 'Inbound', count: inboundCount },
      { direction: 'Outbound', count: outboundCount },
      { direction: 'Unknown', count: unknownCount }
    ]
  };
}

/**
 * Build summary message for inbound/outbound call counts
 */
function buildCallDirectionSummary(directionStats, dateRange) {
  let summary = `📞 **Call Direction Summary**\n\n`;
  summary += `**Date Range:** ${dateRange.startDate} to ${dateRange.endDate}\n\n`;
  summary += `📊 **Overall Statistics:**\n`;
  summary += `- Total Calls: **${directionStats.total}**\n`;
  summary += `- Inbound Calls: **${directionStats.inbound}** (${directionStats.inboundPercentage}%)\n`;
  summary += `- Outbound Calls: **${directionStats.outbound}** (${directionStats.outboundPercentage}%)\n`;
  
  if (directionStats.unknown > 0) {
    summary += `- Unknown Direction: ${directionStats.unknown}\n`;
  }
  
  return summary;
}

/**
 * Get current date in specified timezone
 * @param {string} timezone - IANA timezone string (e.g., 'America/Los_Angeles')
 * @returns {string} Date in YYYY-MM-DD format
 */
function getCurrentDateInTimezone(timezone = config.TIMEZONE) {
  try {
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    });
    
    const parts = formatter.formatToParts(new Date());
    const year = parts.find(p => p.type === 'year').value;
    const month = parts.find(p => p.type === 'month').value;
    const day = parts.find(p => p.type === 'day').value;
    
    return `${year}-${month}-${day}`;
  } catch (error) {
    console.error(`âŒ Error getting date in timezone ${timezone}:`, error.message);
    // Fallback to UTC
    return new Date().toISOString().split('T')[0];
  }
}

/**
 * Validate date format (YYYY-MM-DD)
 */
function validateDateFormat(dateStr) {
  const match = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return false;
  
  const [, year, month, day] = match;
  const date = new Date(dateStr);
  const isValid = date instanceof Date && !isNaN(date) &&
                  date.getUTCFullYear() === parseInt(year) &&
                  date.getUTCMonth() + 1 === parseInt(month) &&
                  date.getUTCDate() === parseInt(day);
  
  return isValid;
}

function detectSingleDateFromQuestion(question) {
  const lowerQ = question.toLowerCase();
  
  // Get current date in configured timezone
  const todayStr = getCurrentDateInTimezone(config.TIMEZONE);
  console.log(`ðŸ“… Current date in ${config.TIMEZONE}: ${todayStr}`);

  // Check for explicit YYYY-MM-DD format first
  const datePattern = /(\d{4})-(\d{2})-(\d{2})/g;
  const matches = question.match(datePattern);
  
  if (matches && matches.length > 0) {
    // If we find date patterns, use them
    const dates = matches.filter(d => validateDateFormat(d));
    
    if (dates.length === 1) {
      // Single date found
      console.log(`   âœ… Detected explicit date: ${dates[0]}`);
      return { startDate: dates[0], endDate: dates[0] };
    } else if (dates.length === 2) {
      // Date range found
      console.log(`   âœ… Detected date range: ${dates[0]} to ${dates[1]}`);
      return { startDate: dates[0], endDate: dates[1] };
    } else if (dates.length > 2) {
      // Use first and last
      console.log(`   âœ… Detected multiple dates, using range: ${dates[0]} to ${dates[dates.length - 1]}`);
      return { startDate: dates[0], endDate: dates[dates.length - 1] };
    }
  }

  // Check for keyword-based dates
  if (lowerQ.includes('today')) {
    console.log(`   âœ… Detected "today" - using date: ${todayStr}`);
    return { startDate: todayStr, endDate: todayStr };
  }

  if (lowerQ.includes('yesterday')) {
    const today = new Date(todayStr);
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    console.log(`   âœ… Detected "yesterday" - using date: ${yesterdayStr}`);
    return { startDate: yesterdayStr, endDate: yesterdayStr };
  }

  if (lowerQ.includes('this week')) {
    const today = new Date(todayStr);
    const startOfWeek = new Date(today);
    startOfWeek.setDate(today.getDate() - today.getDay());
    const startStr = startOfWeek.toISOString().split('T')[0];
    console.log(`   âœ… Detected "this week" - using range: ${startStr} to ${todayStr}`);
    return {
      startDate: startStr,
      endDate: todayStr
    };
  }

  if (lowerQ.includes('last week')) {
    const today = new Date(todayStr);
    const lastWeekEnd = new Date(today);
    lastWeekEnd.setDate(today.getDate() - today.getDay() - 1);
    const lastWeekStart = new Date(lastWeekEnd);
    lastWeekStart.setDate(lastWeekEnd.getDate() - 6);
    const startStr = lastWeekStart.toISOString().split('T')[0];
    const endStr = lastWeekEnd.toISOString().split('T')[0];
    console.log(`   âœ… Detected "last week" - using range: ${startStr} to ${endStr}`);
    return {
      startDate: startStr,
      endDate: endStr
    };
  }

  if (lowerQ.includes('this month')) {
    const today = new Date(todayStr);
    const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const startStr = startOfMonth.toISOString().split('T')[0];
    console.log(`   âœ… Detected "this month" - using range: ${startStr} to ${todayStr}`);
    return {
      startDate: startStr,
      endDate: todayStr
    };
  }

  if (lowerQ.includes('last month')) {
    const today = new Date(todayStr);
    const lastMonthStart = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
    const startStr = lastMonthStart.toISOString().split('T')[0];
    const endStr = lastMonthEnd.toISOString().split('T')[0];
    console.log(`   âœ… Detected "last month" - using range: ${startStr} to ${endStr}`);
    return {
      startDate: startStr,
      endDate: endStr
    };
  }

  return null;
}

async function fetchCallDetails(startDate, endDate, includeTranscript = false, includeAudio = false) {
  try {
    // Validate dates before making API call
    if (!validateDateFormat(startDate) || !validateDateFormat(endDate)) {
      console.error(`   âŒ Date validation failed`);
      console.error(`      Start: ${startDate}`);
      console.error(`      End: ${endDate}`);
      
      return {
        success: false,
        error: 'Invalid date format',
        friendlyError: `âŒ Invalid date format. Dates must be YYYY-MM-DD.\n\nReceived:\n- Start: ${startDate}\n- End: ${endDate}`,
        data: [],
        count: 0,
        summary: 'Invalid date format provided.'
      };
    }
    
    console.log(`   âœ… Date validation passed`);
    
    const url = `${config.BASE_URL}/api/config/get/aivoice/detail`;
    
    console.log(`\nðŸ” Fetching AI Voice call details with pagination...`);
    console.log(`   Endpoint: ${url}`);
    console.log(`   Date Range: ${startDate} to ${endDate}`);
    console.log(`   License Key: ${config.HARDCODED_LICENSE_KEY.substring(0, 10)}...`);
    
    // ==================== PAGINATION LOGIC ====================
    let allCallData = [];
    let currentPage = 1;
    let hasMoreData = true;
    const pageSize = 1000;
    let totalPagesProcessed = 0;
    
    while (hasMoreData) {
      console.log(`   ðŸ“„ Fetching page ${currentPage}...`);
      
      const response = await axios.get(url, {
        params: {
          licenseKey: config.HARDCODED_LICENSE_KEY,
          startDate,
          endDate,
          size: pageSize,
          page: currentPage
        },
        headers: {
          'Authorization': `Bearer ${config.HARDCODED_BEARER}`
        },
        timeout: 30000, // 30 second timeout
        validateStatus: function (status) {
          return status < 500; // Don't throw on 4xx errors
        }
      });

      console.log(`   Response Status: ${response.status}`);

      // Handle 404 specifically
      if (response.status === 404) {
        console.error(`   âŒ API endpoint not found (404)`);
        console.error(`   ðŸ’¡ The endpoint ${url} may not exist or the API structure has changed`);
        return {
          success: false,
          error: 'API endpoint not found',
          friendlyError: `âŒ The AI Voice API endpoint was not found.\n\n**Endpoint:** \`${url}\`\n\n**Possible issues:**\n- The API endpoint may have changed\n- The config.BASE_URL (${config.BASE_URL}) might be incorrect\n- The API service may be down\n\nðŸ’¡ **Action needed:** Please verify the correct API endpoint with your AI Voice service provider.`,
          data: [],
          count: 0,
          summary: 'API endpoint not found.'
        };
      }

      // Handle other 4xx errors
      if (response.status >= 400 && response.status < 500) {
        console.error(`   âŒ Client error (${response.status})`);
        console.error(`   Response:`, response.data);
        
        let errorMsg = 'Authentication or request error';
        if (response.status === 401) {
          errorMsg = 'Invalid authentication credentials';
        } else if (response.status === 403) {
          errorMsg = 'Access forbidden - check API permissions';
        } else if (response.data?.message) {
          errorMsg = response.data.message;
        }
        
        return {
          success: false,
          error: errorMsg,
          friendlyError: `âŒ Error fetching call data: ${errorMsg}\n\n**Status:** ${response.status}\n\nðŸ’¡ Please check your API credentials and permissions.`,
          data: [],
          count: 0,
          summary: 'Failed to fetch call details.'
        };
      }

      // Extract the actual call data - API might return different structures
      let callData = [];
      if (response.data) {
        if (Array.isArray(response.data)) {
          callData = response.data;
        } else if (response.data.content && Array.isArray(response.data.content)) {
          callData = response.data.content;
        } else if (response.data.data && Array.isArray(response.data.data)) {
          callData = response.data.data;
        } else if (response.data.items && Array.isArray(response.data.items)) {
          callData = response.data.items;
        }
      }

      // Add this page's data to our collection
      if (callData.length > 0) {
        allCallData.push(...callData);
        totalPagesProcessed++;
        console.log(`   âœ… Page ${currentPage}: Fetched ${callData.length} calls (Total so far: ${allCallData.length})`);
        
        // Check if we need to fetch more pages
        // If we got exactly pageSize records, there might be more
        if (callData.length === pageSize) {
          currentPage++;
          hasMoreData = true;
          
          // Safety limit to prevent infinite loops (max 50 pages = 50,000 calls)
          if (currentPage > 50) {
            console.warn(`   âš ï¸  Reached maximum page limit (50 pages). Stopping pagination.`);
            hasMoreData = false;
          }
        } else {
          // Got less than pageSize, this is the last page
          hasMoreData = false;
          console.log(`   â„¹ï¸  Last page detected (received ${callData.length} < ${pageSize})`);
        }
      } else {
        // No data on this page, we're done
        hasMoreData = false;
        if (currentPage === 1) {
          console.log(`   â„¹ï¸  No calls found for date range`);
        }
      }
    }
    
    console.log(`\n   ðŸŽ‰ PAGINATION COMPLETE:`);
    console.log(`      - Total pages processed: ${totalPagesProcessed}`);
    console.log(`      - Total calls fetched: ${allCallData.length}`);
    // ==================== END PAGINATION LOGIC ====================

    if (allCallData.length > 0) {
      // Log sample structure for debugging (only for first call)
      if (allCallData[0]) {
        const sampleJson = JSON.stringify(allCallData[0], null, 2);
        const preview = sampleJson.length > 500 ? sampleJson.substring(0, 500) + '...' : sampleJson;
        console.log(`   ðŸ“Š Sample call structure:`);
        console.log(preview);
        console.log(`   ðŸ” Available fields:`, Object.keys(allCallData[0]).join(', '));
      }
      
      // Analyze data quality before normalization
      const hasOutcome = allCallData.filter(c => 
        c.outcome || c.callStatus || c.meta?.callStatus
      ).length;
      const hasSentiment = allCallData.filter(c => 
        c.sentiment || c.userSentiment || c.analysis?.sentiments || c.analysis?.userSentiment
      ).length;
      const hasCost = allCallData.filter(c => 
        c.cost || c.costs?.total || (typeof c.costs?.total === 'number')
      ).length;
      const hasDuration = allCallData.filter(c =>
        c.duration || c.costs?.durationSec || c.durationSec
      ).length;
      
      console.log(`   ðŸ“ˆ Data quality check:`);
      console.log(`      - Calls with outcome: ${hasOutcome}/${allCallData.length} (${Math.round(hasOutcome/allCallData.length*100)}%)`);
      console.log(`      - Calls with sentiment: ${hasSentiment}/${allCallData.length} (${Math.round(hasSentiment/allCallData.length*100)}%)`);
      console.log(`      - Calls with cost: ${hasCost}/${allCallData.length} (${Math.round(hasCost/allCallData.length*100)}%)`);
      console.log(`      - Calls with duration: ${hasDuration}/${allCallData.length} (${Math.round(hasDuration/allCallData.length*100)}%)`);
      
      // Count unique license keys
      const uniqueLicenseKeys = new Set(allCallData.map(c => c.licenseKey).filter(Boolean));
      console.log(`      - Unique license keys: ${uniqueLicenseKeys.size}`);
      
      const normalized = allCallData.map(call => 
        normalizeCall(call, { includeTranscript, includeAudio })
      );

      const summary = buildConversationContext(normalized);

      return {
        success: true,
        data: normalized,
        rawData: allCallData, // Include raw data for license key analysis
        count: normalized.length,
        summary,
        paginationInfo: {
          totalPages: totalPagesProcessed,
          pageSize: pageSize,
          totalRecords: allCallData.length
        }
      };
    } else {
      console.log(`   â„¹ï¸  No calls found for date range`);
      return {
        success: true,
        data: [],
        count: 0,
        summary: 'No calls found for the specified date range.'
      };
    }
  } catch (error) {
    console.error('âŒ Error fetching call details:', error.message);
    
    if (error.code === 'ECONNREFUSED') {
      console.error(`   Cannot connect to ${config.BASE_URL}`);
      return {
        success: false,
        error: 'Connection refused',
        friendlyError: `âŒ Cannot connect to the AI Voice service.\n\n**Server:** ${config.BASE_URL}\n\n**Error:** Connection refused\n\nðŸ’¡ Please verify the AI Voice service is running and accessible.`,
        data: [],
        count: 0,
        summary: 'Failed to connect to AI Voice service.'
      };
    }
    
    if (error.code === 'ENOTFOUND') {
      console.error(`   DNS resolution failed for ${config.BASE_URL}`);
      return {
        success: false,
        error: 'DNS resolution failed',
        friendlyError: `âŒ Cannot resolve hostname.\n\n**Server:** ${config.BASE_URL}\n\nðŸ’¡ Please check the config.BASE_URL configuration.`,
        data: [],
        count: 0,
        summary: 'Failed to resolve AI Voice service hostname.'
      };
    }
    
    if (error.code === 'ETIMEDOUT' || error.message.includes('timeout')) {
      console.error(`   Request timed out`);
      return {
        success: false,
        error: 'Request timeout',
        friendlyError: `âŒ Request timed out after 30 seconds.\n\n**Server:** ${config.BASE_URL}\n\nðŸ’¡ The AI Voice service may be slow or unresponsive.`,
        data: [],
        count: 0,
        summary: 'Request to AI Voice service timed out.'
      };
    }
    
    return {
      success: false,
      error: error.message,
      friendlyError: `âŒ Error fetching call data: ${error.message}\n\n**Server:** ${config.BASE_URL}\n\nðŸ’¡ Please check server logs for more details.`,
      data: [],
      count: 0,
      summary: 'Failed to fetch call details.'
    };
  }
}

async function callOpenAI(messages, tools) {
  try {
    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o',
        messages,
        tools,
        tool_choice: 'auto'
      },
      {
        headers: {
          'Authorization': `Bearer ${config.OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );
    return response.data;
  } catch (error) {
    console.error('OpenAI API Error:', error.response?.data || error.message);
    throw error;
  }
}

// ==================== UNIFIED CHAT ENDPOINT ====================


// ==================== ENHANCED AI VOICE ANALYSIS ====================

/**
 * Comprehensive field mapping for AI Voice data
 * Maps user-friendly terms to actual API fields
 */
const AI_VOICE_FIELD_MAP = {
  // Sentiment & Quality
  sentiment: ['sentiments', 'user_sentiment'],
  quality: ['thumbs_up', 'thumbs_down', 'thumbsUpStatus'],
  successful: ['call_successful', 'confirmation_success', 'reschedule_success', 'successful_upsell'],
  
  // Appointment Related
  appointment: ['isAppointmentBooked', 'appt_booked', 'appointmentBookedDateTime', 'appointmentNumber'],
  reschedule: ['isAppointmentRescheduled', 'reschedule_success', 'appointmentRescheduledDateTime', 'is_reschedule_opportunity'],
  cancel: ['isAppointmentCancelled', 'appointmentCanceledDateTime'],
  
  // Upsell & Opportunities
  upsell: ['upsellOpportunity', 'upsellOpportunityDetails', 'upsell_opportunity', 'successful_upsell'],
  opportunity: ['appt_opportunity', 'missed_appt_opportunity', 'upsell_opportunity'],
  
  // Call Details
  duration: ['callDuration', 'duration_ms'],
  cost: ['totalCost', 'sttCost', 'llmCost', 'ttsCost', 'vapiCost'],
  direction: ['callDirection', 'call_type'],
  
  // Patient Info
  patient: ['patientNumber', 'GuestName', 'GuestEmail'],
  name: ['GuestName'],
  
  // Content
  transcript: ['transcript'],
  summary: ['callSummary'],
  topics: ['topics', 'intents'],
  
  // Review & Feedback
  review: ['ai_reviewed', 'what_went_well', 'what_did_not_go_well', 'area_to_improve'],
  callback: ['is_call_back', 'call_back_due_date', 'CallBackContext'],
  
  // Meta
  language: ['languageUsed'],
  device: ['deviceType'],
  confirmation: ['is_confirmation', 'confirmation_success'],
  reminder: ['is_reminder']
};

/**
 * Detect which fields are relevant to a question
 */
function detectRelevantFields(question) {
  const lowerQ = question.toLowerCase();
  const relevantFields = [];
  
  for (const [category, fields] of Object.entries(AI_VOICE_FIELD_MAP)) {
    if (lowerQ.includes(category)) {
      relevantFields.push(...fields);
    }
  }
  
  // Also check for field names directly mentioned
  const allPossibleFields = Object.values(AI_VOICE_FIELD_MAP).flat();
  allPossibleFields.forEach(field => {
    const fieldLower = field.toLowerCase().replace(/_/g, ' ');
    if (lowerQ.includes(fieldLower)) {
      relevantFields.push(field);
    }
  });
  
  return [...new Set(relevantFields)]; // Remove duplicates
}

/**
 * Filter calls based on dynamic criteria from question
 */
function filterCallsByQuestion(calls, question) {
  const lowerQ = question.toLowerCase();
  let filtered = [...calls];
  
  console.log(`   🔍 Filtering ${calls.length} calls based on question...`);
  
  // Sentiment filters
  if (lowerQ.includes('positive sentiment') || lowerQ.includes('good sentiment')) {
    filtered = filtered.filter(c => 
      c.sentiments === 'Positive' || c.user_sentiment === 'positive'
    );
    console.log(`   ✅ Filtered by positive sentiment: ${filtered.length} calls`);
  }
  if (lowerQ.includes('negative sentiment') || lowerQ.includes('bad sentiment')) {
    filtered = filtered.filter(c => 
      c.sentiments === 'Negative' || c.user_sentiment === 'negative'
    );
    console.log(`   ❌ Filtered by negative sentiment: ${filtered.length} calls`);
  }
  
  // Appointment filters
  if (lowerQ.includes('booked') && lowerQ.includes('appointment')) {
    filtered = filtered.filter(c => 
      c.isAppointmentBooked === 1 || c.appt_booked === '1'
    );
    console.log(`   📅 Filtered by booked appointments: ${filtered.length} calls`);
  }
  if (lowerQ.includes('rescheduled')) {
    filtered = filtered.filter(c => 
      c.isAppointmentRescheduled === 1 || c.reschedule_success === '1'
    );
    console.log(`   🔄 Filtered by rescheduled: ${filtered.length} calls`);
  }
  if (lowerQ.includes('cancelled') || lowerQ.includes('canceled')) {
    filtered = filtered.filter(c => 
      c.isAppointmentCancelled === 1
    );
    console.log(`   ❌ Filtered by cancelled: ${filtered.length} calls`);
  }
  
  // Upsell filters
  if (lowerQ.includes('upsell')) {
    if (lowerQ.includes('successful') || lowerQ.includes('successful upsell')) {
      filtered = filtered.filter(c => c.successful_upsell === '1');
      console.log(`   💰 Filtered by successful upsell: ${filtered.length} calls`);
    } else {
      filtered = filtered.filter(c => 
        c.upsellOpportunity === 'Yes' || c.upsell_opportunity === '1'
      );
      console.log(`   💡 Filtered by upsell opportunity: ${filtered.length} calls`);
    }
  }
  
  // Quality filters
  if (lowerQ.includes('thumbs up') || lowerQ.includes('positive feedback')) {
    filtered = filtered.filter(c => c.thumbs_up === '1');
    console.log(`   👍 Filtered by thumbs up: ${filtered.length} calls`);
  }
  if (lowerQ.includes('thumbs down') || lowerQ.includes('negative feedback')) {
    filtered = filtered.filter(c => c.thumbs_down === '1');
    console.log(`   👎 Filtered by thumbs down: ${filtered.length} calls`);
  }
  
  // Success filters
  if (lowerQ.includes('unsuccessful') || lowerQ.includes('failed')) {
    filtered = filtered.filter(c => c.call_successful === '0' || c.call_successful === 'false');
    console.log(`   ⚠️ Filtered by unsuccessful: ${filtered.length} calls`);
  }
  if (lowerQ.includes('successful call')) {
    filtered = filtered.filter(c => c.call_successful === '1' || c.call_successful === 'true');
    console.log(`   ✅ Filtered by successful: ${filtered.length} calls`);
  }
  
  // Duration filters
  const longerMatch = lowerQ.match(/longer than (\d+)/);
  if (longerMatch) {
    const minutes = parseInt(longerMatch[1]);
    filtered = filtered.filter(c => parseInt(c.callDuration || 0) > minutes);
    console.log(`   ⏱️ Filtered by duration > ${minutes} min: ${filtered.length} calls`);
  }
  const shorterMatch = lowerQ.match(/shorter than (\d+)/);
  if (shorterMatch) {
    const minutes = parseInt(shorterMatch[1]);
    filtered = filtered.filter(c => parseInt(c.callDuration || 0) < minutes);
    console.log(`   ⏱️ Filtered by duration < ${minutes} min: ${filtered.length} calls`);
  }
  
  // Cost filters
  const costMatch = lowerQ.match(/cost (more|over|greater) than [\$]?(\d+\.?\d*)/);
  if (costMatch) {
    const amount = parseFloat(costMatch[2]);
    filtered = filtered.filter(c => parseFloat(c.totalCost || 0) > amount);
    console.log(`   💵 Filtered by cost > $${amount}: ${filtered.length} calls`);
  }
  
  // Name filters
  const nameMatch = lowerQ.match(/for ([A-Z][a-z]+ [A-Z][a-z]+)/);
  if (nameMatch) {
    const searchName = nameMatch[1].toLowerCase();
    filtered = filtered.filter(c => 
      c.GuestName && c.GuestName.toLowerCase().includes(searchName)
    );
    console.log(`   👤 Filtered by name "${searchName}": ${filtered.length} calls`);
  }
  
  // Language filters
  if (lowerQ.includes('spanish')) {
    filtered = filtered.filter(c => c.languageUsed?.toLowerCase() === 'spanish');
    console.log(`   🇪🇸 Filtered by Spanish: ${filtered.length} calls`);
  }
  if (lowerQ.includes('english')) {
    filtered = filtered.filter(c => c.languageUsed?.toLowerCase() === 'english');
    console.log(`   🇺🇸 Filtered by English: ${filtered.length} calls`);
  }
  
  // Follow-up required
  if (lowerQ.includes('follow-up') || lowerQ.includes('callback')) {
    filtered = filtered.filter(c => c.isfollowuprequired === 1 || c.is_call_back === '1');
    console.log(`   📞 Filtered by follow-up required: ${filtered.length} calls`);
  }
  
  return filtered;
}

/**
 * Build comprehensive analysis summary
 */
function buildComprehensiveCallAnalysis(calls, question) {
  if (!calls || calls.length === 0) {
    return "No calls match your criteria.";
  }
  
  const relevantFields = detectRelevantFields(question);
  console.log(`   📊 Detected relevant fields:`, relevantFields.join(', '));
  
  let summary = `## 📊 Call Analysis Results\n\n`;
  summary += `**Total Calls:** ${calls.length}\n\n`;
  
  // Appointment Stats
  const apptBooked = calls.filter(c => c.isAppointmentBooked === 1 || c.appt_booked === '1').length;
  const apptRescheduled = calls.filter(c => c.isAppointmentRescheduled === 1).length;
  const apptCancelled = calls.filter(c => c.isAppointmentCancelled === 1).length;
  
  if (relevantFields.some(f => f.includes('appointment') || f.includes('appt'))) {
    summary += `### 📅 Appointments\n`;
    summary += `- Booked: **${apptBooked}**\n`;
    summary += `- Rescheduled: **${apptRescheduled}**\n`;
    summary += `- Cancelled: **${apptCancelled}**\n\n`;
  }
  
  // Upsell Stats
  const upsellOpp = calls.filter(c => c.upsellOpportunity === 'Yes' || c.upsell_opportunity === '1').length;
  const upsellSuccess = calls.filter(c => c.successful_upsell === '1').length;
  
  if (relevantFields.some(f => f.includes('upsell'))) {
    summary += `### 💰 Upsell Performance\n`;
    summary += `- Opportunities: **${upsellOpp}**\n`;
    summary += `- Successful: **${upsellSuccess}**\n`;
    if (upsellOpp > 0) {
      summary += `- Success Rate: **${((upsellSuccess / upsellOpp) * 100).toFixed(1)}%**\n`;
    }
    summary += `\n`;
  }
  
  // Sentiment Analysis
  const sentiments = {};
  calls.forEach(c => {
    const sentiment = c.sentiments || c.user_sentiment || 'Unknown';
    sentiments[sentiment] = (sentiments[sentiment] || 0) + 1;
  });
  
  if (relevantFields.some(f => f.includes('sentiment'))) {
    summary += `### 😊 Sentiment Breakdown\n`;
    Object.entries(sentiments).forEach(([s, count]) => {
      const emoji = s === 'Positive' ? '✅' : s === 'Negative' ? '❌' : '➖';
      summary += `${emoji} ${s}: **${count}** (${((count / calls.length) * 100).toFixed(1)}%)\n`;
    });
    summary += `\n`;
  }
  
  // Quality Scores
  const thumbsUp = calls.filter(c => c.thumbs_up === '1').length;
  const thumbsDown = calls.filter(c => c.thumbs_down === '1').length;
  
  if (relevantFields.some(f => f.includes('thumbs') || f.includes('quality'))) {
    summary += `### 👍 Quality Scores\n`;
    summary += `- Thumbs Up: **${thumbsUp}**\n`;
    summary += `- Thumbs Down: **${thumbsDown}**\n\n`;
  }
  
  // Cost Analysis
  const totalCost = calls.reduce((sum, c) => sum + parseFloat(c.totalCost || 0), 0);
  const avgCost = totalCost / calls.length;
  const avgDuration = calls.reduce((sum, c) => sum + parseInt(c.callDuration || 0), 0) / calls.length;
  
  if (relevantFields.some(f => f.includes('cost') || f.includes('duration'))) {
    summary += `### 💵 Cost & Duration\n`;
    summary += `- Total Cost: **$${totalCost.toFixed(2)}**\n`;
    summary += `- Average Cost: **$${avgCost.toFixed(3)}**\n`;
    summary += `- Average Duration: **${Math.round(avgDuration)} minutes**\n\n`;
  }
  
  // Call Direction
  const inbound = calls.filter(c => c.callDirection === 'inbound').length;
  const outbound = calls.filter(c => c.callDirection === 'outbound').length;
  
  if (relevantFields.some(f => f.includes('direction'))) {
    summary += `### 📞 Call Direction\n`;
    summary += `- Inbound: **${inbound}**\n`;
    summary += `- Outbound: **${outbound}**\n\n`;
  }
  
  return summary;
}

/**
 * Format call details for specific questions (like transcripts)
 */
function formatCallDetails(calls, question, showFields = []) {
  if (!calls || calls.length === 0) return "No calls found.";
  
  const lowerQ = question.toLowerCase();
  let output = '';
  
  // Check if user wants transcripts
  if (lowerQ.includes('transcript')) {
    output += `## 📝 Call Transcripts\n\n`;
    output += `**Found ${calls.length} call${calls.length !== 1 ? 's' : ''}**\n\n`;
    
    calls.slice(0, 10).forEach((call, idx) => {
      output += `### Call ${idx + 1}: ${call.GuestName || 'Unknown'}\n`;
      output += `**Date:** ${call.startTime || call.createdAt}\n`;
      output += `**Duration:** ${call.callDuration || 0} minutes\n`;
      output += `**Phone:** ${call.phoneNumber || 'N/A'}\n\n`;
      
      if (call.transcript && call.transcript.trim()) {
        output += `**Transcript:**\n${call.transcript}\n\n`;
      } else {
        output += `*No transcript available*\n\n`;
      }
      
      output += `---\n\n`;
    });
    
    if (calls.length > 10) {
      output += `\n*Showing first 10 of ${calls.length} transcripts*\n`;
    }
    
    return output;
  }
  
  // Check if user wants summaries
  if (lowerQ.includes('summary') || lowerQ.includes('summaries')) {
    output += `## 📋 Call Summaries\n\n`;
    output += `**Found ${calls.length} call${calls.length !== 1 ? 's' : ''}**\n\n`;
    
    calls.slice(0, 20).forEach((call, idx) => {
      output += `### ${idx + 1}. ${call.GuestName || 'Unknown'}\n`;
      output += `**Date:** ${call.startTime || call.createdAt}\n`;
      output += `**Summary:** ${call.callSummary || 'No summary'}\n\n`;
      
      if (call.upsellOpportunityDetails && call.upsellOpportunityDetails !== 'NA') {
        output += `**Upsell:** ${call.upsellOpportunityDetails}\n\n`;
      }
      
      output += `---\n\n`;
    });
    
    if (calls.length > 20) {
      output += `\n*Showing first 20 of ${calls.length} summaries*\n`;
    }
    
    return output;
  }
  
  // Check if user wants specific review/feedback
  if (lowerQ.includes('review') || lowerQ.includes('feedback') || lowerQ.includes('improve')) {
    output += `## 🔍 Call Reviews & Feedback\n\n`;
    
    const reviewedCalls = calls.filter(c => c.ai_reviewed === '1');
    
    if (reviewedCalls.length === 0) {
      output += `*No AI-reviewed calls found in the filtered results*\n`;
      return output;
    }
    
    output += `**Found ${reviewedCalls.length} reviewed call${reviewedCalls.length !== 1 ? 's' : ''}**\n\n`;
    
    reviewedCalls.slice(0, 10).forEach((call, idx) => {
      output += `### ${idx + 1}. ${call.GuestName || 'Unknown'} - ${call.startTime || call.createdAt}\n\n`;
      
      if (call.what_went_well) {
        output += `✅ **What Went Well:**\n${call.what_went_well}\n\n`;
      }
      
      if (call.what_did_not_go_well) {
        output += `❌ **What Didn't Go Well:**\n${call.what_did_not_go_well}\n\n`;
      }
      
      if (call.area_to_improve) {
        output += `💡 **Areas to Improve:**\n${call.area_to_improve}\n\n`;
      }
      
      output += `---\n\n`;
    });
    
    if (reviewedCalls.length > 10) {
      output += `\n*Showing first 10 of ${reviewedCalls.length} reviews*\n`;
    }
    
    return output;
  }
  
  // Default: Show comprehensive analysis
  return buildComprehensiveCallAnalysis(calls, question);
}

// ==================== INTELLIGENT NLP ROUTING SYSTEM ====================

/**
 * Classify the intent of a user's AI Voice question
 * Returns: 'filter', 'count', 'content', 'analysis', or null
 */
function classifyAIVoiceIntent(question) {
  const lowerQ = question.toLowerCase();
  
  // Intent Groups with their keywords
  // IMPORTANT: Check in priority order - count > filter > content > analysis
  const intents = {
    // COUNT intent - user wants statistics/numbers (HIGHEST PRIORITY for "how many")
    count: {
      keywords: ['how many', 'count', 'total', 'number of', 'how much'],
      confidence: 0,
      priority: 4
    },
    
    // FILTER intent - user wants filtered/specific data
    filter: {
      keywords: ['show', 'which', 'find', 'list', 'give me', 'provide', 'display', 'where'],
      // REMOVED 'get' - too generic and conflicts with "how many calls did we get"
      confidence: 0,
      priority: 3
    },
    
    // CONTENT intent - user wants text content
    content: {
      keywords: ['transcript', 'transcripts', 'summary', 'summaries', 'text', 'details', 'read', 'what.*say', 'what.*said'],
      confidence: 0,
      priority: 2
    },
    
    // ANALYSIS intent - user wants insights/review
    analysis: {
      keywords: ['analyze', 'review', 'feedback', 'performance', 'insights', 'what went well', 'what went wrong', 'improve'],
      confidence: 0,
      priority: 1
    }
  };
  
  // Calculate confidence scores
  for (const [intent, data] of Object.entries(intents)) {
    data.confidence = data.keywords.filter(kw => {
      // Support regex patterns in keywords
      if (kw.includes('.*')) {
        return new RegExp(kw).test(lowerQ);
      }
      return lowerQ.includes(kw);
    }).length;
  }
  
  // Get primary intent (highest confidence, then by priority)
  const sortedIntents = Object.entries(intents)
    .sort((a, b) => {
      // First sort by confidence
      if (b[1].confidence !== a[1].confidence) {
        return b[1].confidence - a[1].confidence;
      }
      // Tie-breaker: use priority
      return b[1].priority - a[1].priority;
    });
  
  const primaryIntent = sortedIntents[0];
  
  if (primaryIntent[1].confidence > 0) {
    console.log(`   🎯 Intent detected: ${primaryIntent[0]} (confidence: ${primaryIntent[1].confidence})`);
    return primaryIntent[0];
  }
  
  return null;
}

/**
 * Extract entities (data fields) from the question
 * Returns an object with all detected entities
 */
function extractCallEntities(question) {
  const lowerQ = question.toLowerCase();
  
  const entities = {
    // Status entities
    appointmentStatus: null,  // 'booked', 'cancelled', 'rescheduled'
    callSuccess: null,         // true, false
    sentiment: null,           // 'positive', 'negative'
    quality: null,             // 'thumbs_up', 'thumbs_down'
    
    // Opportunity entities
    hasUpsell: null,          // true
    needsFollowup: null,      // true
    
    // Attribute entities
    language: null,            // 'spanish', 'english'
    duration: null,            // { operator: '>', value: number }
    cost: null,                // { operator: '>', value: number }
    name: null,                // patient name string
    
    // Time entities (handled by detectSingleDateFromQuestion)
    dates: null
  };
  
  // ========== Appointment Status ==========
  if (lowerQ.match(/\b(book|booked|scheduled|made.*appointment)\b/)) {
    entities.appointmentStatus = 'booked';
  }
  if (lowerQ.match(/\b(cancel|cancelled|canceled)\b/)) {
    entities.appointmentStatus = 'cancelled';
  }
  if (lowerQ.match(/\b(reschedule|rescheduled|changed|moved)\b/)) {
    entities.appointmentStatus = 'rescheduled';
  }
  
  // ========== Call Success ==========
  // Negative success patterns (unsuccessful, failed, didn't work)
  if (lowerQ.match(/\b(unsuccessful|failed|didn'?t (work|succeed)|not successful|wasn'?t successful|no success)\b/)) {
    entities.callSuccess = false;
  }
  // Positive success patterns
  if (lowerQ.match(/\b(successful|succeeded|worked|did work|was successful)\b/) && !entities.callSuccess) {
    entities.callSuccess = true;
  }
  
  // ========== Sentiment ==========
  if (lowerQ.match(/\b(positive|good|happy|satisfied|pleased)\b.*\b(sentiment|feedback|feeling)\b/i)) {
    entities.sentiment = 'positive';
  }
  if (lowerQ.match(/\b(negative|bad|unhappy|unsatisfied|upset|angry)\b.*\b(sentiment|feedback|feeling)\b/i)) {
    entities.sentiment = 'negative';
  }
  // Also match reversed order: "sentiment was positive"
  if (lowerQ.match(/\b(sentiment|feedback|feeling)\b.*\b(positive|good|happy)\b/i)) {
    entities.sentiment = 'positive';
  }
  if (lowerQ.match(/\b(sentiment|feedback|feeling)\b.*\b(negative|bad|unhappy)\b/i)) {
    entities.sentiment = 'negative';
  }
  
  // ========== Quality Scores ==========
  if (lowerQ.match(/\b(thumbs? up|positive feedback|good rating|high score)\b/)) {
    entities.quality = 'thumbs_up';
  }
  if (lowerQ.match(/\b(thumbs? down|negative feedback|bad rating|low score)\b/)) {
    entities.quality = 'thumbs_down';
  }
  
  // ========== Upsell Opportunities ==========
  if (lowerQ.match(/\b(upsell|upgrade|additional service|extra service|more.*service)\b/)) {
    entities.hasUpsell = true;
  }
  
  // ========== Follow-up Required ==========
  if (lowerQ.match(/\b(follow-up|follow up|callback|call back|need.*contact|requires?.*follow|need.*call)\b/)) {
    entities.needsFollowup = true;
  }
  
  // ========== Language ==========
  if (lowerQ.match(/\bspanish\b/)) entities.language = 'spanish';
  if (lowerQ.match(/\benglish\b/)) entities.language = 'english';
  
  // ========== Duration (with numbers) ==========
  const longerMatch = lowerQ.match(/\b(longer|more|greater|over|above)\s+than\s+(\d+)\s*(min|minute|sec|second)?/i);
  if (longerMatch) {
    entities.duration = { 
      operator: '>', 
      value: parseInt(longerMatch[2])
    };
  }
  
  const shorterMatch = lowerQ.match(/\b(shorter|less|under|below|fewer)\s+than\s+(\d+)\s*(min|minute|sec|second)?/i);
  if (shorterMatch) {
    entities.duration = { 
      operator: '<', 
      value: parseInt(shorterMatch[2])
    };
  }
  
  // ========== Cost ==========
  const costMatch = lowerQ.match(/\b(more|over|greater|above)\s+than\s+\$?(\d+\.?\d*)/i);
  if (costMatch) {
    entities.cost = { 
      operator: '>', 
      value: parseFloat(costMatch[2])
    };
  }
  
  const lessCostMatch = lowerQ.match(/\b(less|under|below)\s+than\s+\$?(\d+\.?\d*)/i);
  if (lessCostMatch) {
    entities.cost = { 
      operator: '<', 
      value: parseFloat(lessCostMatch[2])
    };
  }
  
  // ========== Patient Name (Title Case) ==========
  const nameMatch = question.match(/\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b/);
  if (nameMatch) {
    entities.name = nameMatch[1];
  }
  
  // Log detected entities
  const detectedEntities = Object.entries(entities)
    .filter(([_, v]) => v !== null)
    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
    .join(', ');
  
  if (detectedEntities) {
    console.log(`   📊 Entities detected: ${detectedEntities}`);
  }
  
  return entities;
}

/**
 * Filter calls based on extracted entities (more intelligent than keyword matching)
 */
function filterCallsByEntities(calls, entities) {
  let filtered = [...calls];
  const initialCount = filtered.length;
  
  console.log(`   🔍 Filtering ${initialCount} calls with entity-based filters...`);
  
  // ========== Appointment Status Filters ==========
  if (entities.appointmentStatus === 'booked') {
    filtered = filtered.filter(c => 
      c.isAppointmentBooked === 1 || c.appt_booked === '1'
    );
    console.log(`      📅 Appointment booked: ${filtered.length} calls`);
  }
  
  if (entities.appointmentStatus === 'cancelled') {
    filtered = filtered.filter(c => c.isAppointmentCancelled === 1);
    console.log(`      ❌ Appointment cancelled: ${filtered.length} calls`);
  }
  
  if (entities.appointmentStatus === 'rescheduled') {
    filtered = filtered.filter(c => 
      c.isAppointmentRescheduled === 1 || c.reschedule_success === '1'
    );
    console.log(`      🔄 Appointment rescheduled: ${filtered.length} calls`);
  }
  
  // ========== Call Success Filters ==========
  if (entities.callSuccess === false) {
    filtered = filtered.filter(c => 
      c.call_successful === '0' || 
      c.call_successful === 'false' ||
      c.confirmation_success === '0' ||
      c.call_status === 'failed'
    );
    console.log(`      ⚠️  Unsuccessful calls: ${filtered.length} calls`);
  }
  
  if (entities.callSuccess === true) {
    filtered = filtered.filter(c => 
      c.call_successful === '1' || 
      c.call_successful === 'true' ||
      c.confirmation_success === '1' ||
      c.call_status === 'success'
    );
    console.log(`      ✅ Successful calls: ${filtered.length} calls`);
  }
  
  // ========== Sentiment Filters ==========
  if (entities.sentiment === 'positive') {
    filtered = filtered.filter(c => 
      c.sentiments === 'Positive' || c.user_sentiment === 'positive'
    );
    console.log(`      😊 Positive sentiment: ${filtered.length} calls`);
  }
  
  if (entities.sentiment === 'negative') {
    filtered = filtered.filter(c => 
      c.sentiments === 'Negative' || c.user_sentiment === 'negative'
    );
    console.log(`      😞 Negative sentiment: ${filtered.length} calls`);
  }
  
  // ========== Quality Filters ==========
  if (entities.quality === 'thumbs_up') {
    filtered = filtered.filter(c => c.thumbs_up === '1');
    console.log(`      👍 Thumbs up: ${filtered.length} calls`);
  }
  
  if (entities.quality === 'thumbs_down') {
    filtered = filtered.filter(c => c.thumbs_down === '1');
    console.log(`      👎 Thumbs down: ${filtered.length} calls`);
  }
  
  // ========== Upsell Filters ==========
  if (entities.hasUpsell) {
    filtered = filtered.filter(c => 
      c.upsellOpportunity === 'Yes' || c.upsell_opportunity === '1'
    );
    console.log(`      💰 Upsell opportunities: ${filtered.length} calls`);
  }
  
  // ========== Follow-up Filters ==========
  if (entities.needsFollowup) {
    filtered = filtered.filter(c => 
      c.isfollowuprequired === 1 || c.is_call_back === '1'
    );
    console.log(`      📞 Follow-up required: ${filtered.length} calls`);
  }
  
  // ========== Language Filters ==========
  if (entities.language) {
    filtered = filtered.filter(c => 
      c.languageUsed?.toLowerCase() === entities.language
    );
    console.log(`      🌐 Language (${entities.language}): ${filtered.length} calls`);
  }
  
  // ========== Duration Filters ==========
  if (entities.duration) {
    filtered = filtered.filter(c => {
      const duration = parseInt(c.callDuration || c.duration_ms || 0);
      if (entities.duration.operator === '>') {
        return duration > entities.duration.value;
      } else {
        return duration < entities.duration.value;
      }
    });
    console.log(`      ⏱️  Duration ${entities.duration.operator} ${entities.duration.value}: ${filtered.length} calls`);
  }
  
  // ========== Cost Filters ==========
  if (entities.cost) {
    filtered = filtered.filter(c => {
      const cost = parseFloat(c.totalCost || 0);
      if (entities.cost.operator === '>') {
        return cost > entities.cost.value;
      } else {
        return cost < entities.cost.value;
      }
    });
    console.log(`      💵 Cost ${entities.cost.operator} $${entities.cost.value}: ${filtered.length} calls`);
  }
  
  // ========== Name Filters ==========
  if (entities.name) {
    filtered = filtered.filter(c => 
      c.GuestName && c.GuestName.toLowerCase().includes(entities.name.toLowerCase())
    );
    console.log(`      👤 Name contains "${entities.name}": ${filtered.length} calls`);
  }
  
  console.log(`   ✅ Final filtered count: ${filtered.length} of ${initialCount} calls`);
  return filtered;
}

// ==================== EXPORTS ====================

// ==================== CHART DATA GENERATION ====================

/**
 * Generate chart data based on question and response data
 * This function detects if a response should include visualizations
 * 
 * @param {string} question - The user's question
 * @param {Array} data - The raw data array from API response
 * @param {string} systemType - The system that generated the response ('aivoice', 'txql', 'commlog')
 * @returns {Object|null} - Chart configuration object or null if no chart applicable
 */
function generateChartData(question, data, systemType) {
  if (!data || !Array.isArray(data) || data.length === 0) {
    return null;
  }

  const lowerQ = question.toLowerCase();
  
  // ========== AI VOICE SYSTEM CHARTS ==========
  if (systemType === 'aivoice') {
    
    // CHART 1: Sentiment Distribution (Pie Chart)
    if (lowerQ.includes('sentiment')) {
      const sentiments = {};
      data.forEach(call => {
        const sentiment = call.sentiments || call.user_sentiment || 'Unknown';
        sentiments[sentiment] = (sentiments[sentiment] || 0) + 1;
      });
      
      // Only create chart if we have data
      if (Object.keys(sentiments).length > 0) {
        return {
          type: 'pie',
          title: 'Sentiment Distribution',
          data: Object.entries(sentiments).map(([name, value]) => ({
            name,
            value,
            color: name.toLowerCase().includes('positive') ? '#22c55e' : 
                   name.toLowerCase().includes('negative') ? '#ef4444' : '#6b7280'
          }))
        };
      }
    }
    
    // CHART 2: Call Direction (Pie Chart)
    if (lowerQ.includes('direction') || lowerQ.includes('inbound') || lowerQ.includes('outbound')) {
      const directions = { inbound: 0, outbound: 0, unknown: 0 };
      data.forEach(call => {
        const dir = (call.callDirection || call.call_type || '').toLowerCase();
        if (dir === 'inbound' || dir === 'incoming') {
          directions.inbound++;
        } else if (dir === 'outbound' || dir === 'outgoing') {
          directions.outbound++;
        } else {
          directions.unknown++;
        }
      });
      
      // Filter out zero values
      const chartData = Object.entries(directions)
        .filter(([_, value]) => value > 0)
        .map(([name, value]) => ({
          name: name.charAt(0).toUpperCase() + name.slice(1),
          value,
          color: name === 'inbound' ? '#3b82f6' : 
                 name === 'outbound' ? '#8b5cf6' : '#9ca3af'
        }));
      
      if (chartData.length > 0) {
        return {
          type: 'pie',
          title: 'Call Direction Breakdown',
          data: chartData
        };
      }
    }
    
    // CHART 3: Calls by License Key (Bar Chart)
    if (lowerQ.includes('license key') || lowerQ.includes('by license')) {
      const grouped = {};
      data.forEach(call => {
        const key = call.licenseKey || 'Unknown';
        const shortKey = key.length > 20 ? key.substring(0, 20) + '...' : key;
        grouped[shortKey] = (grouped[shortKey] || 0) + 1;
      });
      
      const chartData = Object.entries(grouped)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value)
        .slice(0, 10); // Top 10 only
      
      if (chartData.length > 0) {
        return {
          type: 'bar',
          title: 'Calls by License Key (Top 10)',
          data: chartData
        };
      }
    }
    
    // CHART 4: Appointment Outcomes (Pie Chart)
    if (lowerQ.includes('appointment') && !lowerQ.includes('how many')) {
      const outcomes = {
        booked: 0,
        rescheduled: 0,
        cancelled: 0,
        none: 0
      };
      
      data.forEach(call => {
        if (call.isAppointmentBooked === 1 || call.appt_booked === '1') {
          outcomes.booked++;
        } else if (call.isAppointmentRescheduled === 1) {
          outcomes.rescheduled++;
        } else if (call.isAppointmentCancelled === 1) {
          outcomes.cancelled++;
        } else {
          outcomes.none++;
        }
      });
      
      const chartData = Object.entries(outcomes)
        .filter(([_, value]) => value > 0)
        .map(([name, value]) => ({
          name: name.charAt(0).toUpperCase() + name.slice(1),
          value,
          color: name === 'booked' ? '#22c55e' :
                 name === 'rescheduled' ? '#f59e0b' :
                 name === 'cancelled' ? '#ef4444' : '#6b7280'
        }));
      
      if (chartData.length > 1) { // Only show if there's variety
        return {
          type: 'pie',
          title: 'Appointment Outcomes',
          data: chartData
        };
      }
    }
    
    // CHART 5: Call Success Rate (Pie Chart)
    if (lowerQ.includes('success') && !lowerQ.includes('upsell')) {
      const success = { successful: 0, unsuccessful: 0 };
      data.forEach(call => {
        if (call.call_successful === '1' || call.call_successful === 'true') {
          success.successful++;
        } else if (call.call_successful === '0' || call.call_successful === 'false') {
          success.unsuccessful++;
        }
      });
      
      if (success.successful > 0 || success.unsuccessful > 0) {
        return {
          type: 'pie',
          title: 'Call Success Rate',
          data: [
            { name: 'Successful', value: success.successful, color: '#22c55e' },
            { name: 'Unsuccessful', value: success.unsuccessful, color: '#ef4444' }
          ].filter(item => item.value > 0)
        };
      }
    }
    
    // CHART 6: Quality Scores (Pie Chart)
    if (lowerQ.includes('thumbs') || lowerQ.includes('quality') || lowerQ.includes('feedback')) {
      const quality = { thumbsUp: 0, thumbsDown: 0, noFeedback: 0 };
      data.forEach(call => {
        if (call.thumbs_up === '1') {
          quality.thumbsUp++;
        } else if (call.thumbs_down === '1') {
          quality.thumbsDown++;
        } else {
          quality.noFeedback++;
        }
      });
      
      const chartData = [
        { name: 'Thumbs Up', value: quality.thumbsUp, color: '#22c55e' },
        { name: 'Thumbs Down', value: quality.thumbsDown, color: '#ef4444' },
        { name: 'No Feedback', value: quality.noFeedback, color: '#6b7280' }
      ].filter(item => item.value > 0);
      
      if (chartData.length > 1) {
        return {
          type: 'pie',
          title: 'Quality Feedback',
          data: chartData
        };
      }
    }
    
    // CHART 7: Language Distribution (Pie Chart)
    if (lowerQ.includes('language')) {
      const languages = {};
      data.forEach(call => {
        const lang = call.languageUsed || 'Unknown';
        languages[lang] = (languages[lang] || 0) + 1;
      });
      
      const chartData = Object.entries(languages)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value);
      
      if (chartData.length > 0) {
        return {
          type: 'pie',
          title: 'Language Distribution',
          data: chartData
        };
      }
    }
  }
  
  // ========== COMMLOG SYSTEM CHARTS ==========
  if (systemType === 'commlog') {
    
    // CHART 1: Communication Types (Bar Chart)
    if (lowerQ.includes('type') || lowerQ.includes('commlog')) {
      const types = {};
      data.forEach(record => {
        const type = getCommTypeLabel(record.CommType);
        types[type] = (types[type] || 0) + 1;
      });
      
      const chartData = Object.entries(types)
        .map(([name, value]) => ({ 
          name: name.replace(/[📋📧📞💬🎯📊]/g, '').trim(), // Remove emojis
          value 
        }))
        .sort((a, b) => b.value - a.value);
      
      if (chartData.length > 0) {
        return {
          type: 'bar',
          title: 'Communication Types',
          data: chartData
        };
      }
    }
    
    // CHART 2: Sent vs Received (Pie Chart)
    if (lowerQ.includes('sent') || lowerQ.includes('received')) {
      const direction = { sent: 0, received: 0, unknown: 0 };
      data.forEach(record => {
        if (record.SentOrReceived === 1) {
          direction.sent++;
        } else if (record.SentOrReceived === 2) {
          direction.received++;
        } else {
          direction.unknown++;
        }
      });
      
      const chartData = Object.entries(direction)
        .filter(([_, value]) => value > 0)
        .map(([name, value]) => ({
          name: name.charAt(0).toUpperCase() + name.slice(1),
          value,
          color: name === 'sent' ? '#3b82f6' : 
                 name === 'received' ? '#8b5cf6' : '#6b7280'
        }));
      
      if (chartData.length > 0) {
        return {
          type: 'pie',
          title: 'Communication Direction',
          data: chartData
        };
      }
    }
  }
  
  // ========== TXQL SYSTEM CHARTS ==========
  if (systemType === 'txql') {
    // TXQL charts would need more context about the query results
    // For now, return null (can be expanded later)
    return null;
  }
  
  return null; // No chart applicable
}

module.exports = {
  // Session management
  generateSessionId,
  getOrCreateSession,
  getSession,
  cleanupOldSessions,
  activeSessions,
  
  // Health checks
  testTXQLConnection,
  testAIVoiceConnection,
  
  // License Key Preprocessors (NEW)
  preprocessLicenseKeyFromQuestion,
  preprocessTXQLLicenseKey,
  
  // Greeting & routing
  extractLicenseKey,
  isGreeting,
  getGreetingResponse,
  determineSystem,
  
  // SQL functions
  buildNoteQuerySQL,
  buildMultiPatientPricingSearchSQL,
  extractSQLFromTXQL,
  executeSQL,
  validateSQL,
  ensureQueryHasLimit,
  
  // TXQL Enhancement (NEW)
  extractSQLEntities,
  enhanceSQLQuery,
  lookupPatientNumber,
  substitutePatientNumber,
  
  // TXQL Enhancement (NEW)
  extractSQLEntities,
  enhanceSQLQuery,
  lookupPatientNumber,
  substitutePatientNumber,
  
  // TXQL
  queryTXQL,
  resolveFromContext,
  
  // Pricing analysis
  analyzePricingDetails,
  buildPaymentSummary,
  generatePaymentRecommendations,
  formatPricingAnalysisForChat,
  isPricingAnalysisQuery,
  
  // Note handling
  isNoteSummaryQuery,
  isMultiPatientPricingSearch,
  isPricingFromNotesQuery,
  generateNoteSummary,
  extractPatientNumber,
  
  // Formatting
  formatSQLResults,
  formatMultiPatientPricingResults,
  formatCleanResults,
  formatAsCards,
  formatAsTable,
  cleanValue,
  selectKeyColumns,
  analyzeDataForVisualization,
  generateChartVisualization,
  formatChartLabel,
  detectChartData,  // NEW: Chart detection
  
  // AI Voice functions
  normalizeCall,
  groupCallsByLicenseKey,
  buildLicenseKeySummary,
  getCallsForLicenseKey,
  buildConversationContext,
  countCallsByDirection,
  buildCallDirectionSummary,
  validateDateFormat,
  fetchCallDetails,
  analyzeCommLog,
  formatCommLogForDisplay,
  getCommTypeLabel,
  getModeLabel,
  extractPhoneCallDetails,
  formatCommLogAsStructuredData,
  callOpenAI,
  
  // NEW: Enhanced AI Voice analysis
  detectRelevantFields,
  filterCallsByQuestion,
  buildComprehensiveCallAnalysis,
  formatCallDetails,
  
  // NEW: Intelligent NLP routing
  classifyAIVoiceIntent,
  extractCallEntities,
  filterCallsByEntities,
  
  // NEW: Chart generation
  generateChartData
};
// ==================== COMMLOG FUNCTIONS ====================

/**
 * Format CommLog records for clean display
 */
function formatCommLogForDisplay(records) {
  return records.map(record => {
    const isPhoneCall = record.CommType === 386 || 
                        (record.Note && record.Note.includes('duration of'));
    
    const durationMatch = record.Note ? record.Note.match(/duration of (\d+) minutes? (\d+) seconds?/) : null;
    const phoneMatch = record.Note ? record.Note.match(/to (\+?\d+)/) : null;
    
    let cleanNote = record.Note || '';
    if (cleanNote.includes('PX Summary')) {
      const summaryStart = cleanNote.indexOf('PX Summary');
      const summaryEnd = Math.min(summaryStart + 500, cleanNote.length);
      cleanNote = cleanNote.substring(summaryStart, summaryEnd);
    } else if (cleanNote.length > 300) {
      cleanNote = cleanNote.substring(0, 300) + '...';
    }
    
    return {
      id: record.CommlogNum,
      date: new Date(record.CommDateTime).toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: true
      }),
      type: getCommTypeLabel(record.CommType),
      direction: record.SentOrReceived === 1 ? '📤 Outbound' : record.SentOrReceived === 2 ? '📥 Inbound' : '📋 Note',
      phone: phoneMatch ? phoneMatch[1] : null,
      duration: durationMatch ? `${durationMatch[1]}m ${durationMatch[2]}s` : null,
      message: cleanNote,
      mode: getModeLabel(record.Mode_),
      isCall: isPhoneCall
    };
  });
}

/**
 * Get human-readable CommType label
 */
function getCommTypeLabel(type) {
  const types = {
    0: '📝 General Note',
    224: '📅 Appointment',
    228: '💬 Text Message',
    386: '☎️ Phone Call',
    387: '📋 Form Completed',
    384: '⭐ Feedback',
    385: '📧 Email Campaign',
    388: '💼 Demo/Sales',
    399: '📊 Feedback Request',
    530: '🤖 AI Transcript',
    582: '🏥 Clinical Note',
    593: '💌 Patient Message'
  };
  return types[type] || `📄 Type ${type}`;
}

/**
 * Get human-readable Mode label
 */
function getModeLabel(mode) {
  const modes = {
    0: 'System',
    1: 'Email',
    3: 'Phone',
    4: 'Web',
    5: 'SMS',
    6: 'Other'
  };
  return modes[mode] || `Mode ${mode}`;
}

/**
 * Analyze CommLog communication records
 */
function analyzeCommLog(records) {
  const analysis = {
    totalRecords: records.length,
    byCommType: {},
    byMode: {},
    sentVsReceived: { sent: 0, received: 0, unknown: 0 },
    recentActivity: [],
    callSummaries: [],
    timeline: [],
    formattedRecords: []
  };
  
  // Format records for clean display
  analysis.formattedRecords = formatCommLogForDisplay(records);
  
  records.forEach(record => {
    // Count by CommType
    const type = record.CommType || 'Unknown';
    analysis.byCommType[type] = (analysis.byCommType[type] || 0) + 1;
    
    // Count by Mode
    const mode = record.Mode_ || 'Unknown';
    analysis.byMode[mode] = (analysis.byMode[mode] || 0) + 1;
    
    // Sent vs Received
    if (record.SentOrReceived === 1) analysis.sentVsReceived.sent++;
    else if (record.SentOrReceived === 2) analysis.sentVsReceived.received++;
    else analysis.sentVsReceived.unknown++;
    
    // Extract call summaries from Note field
    if (record.Note && record.Note.includes('PX Summary')) {
      analysis.callSummaries.push({
        date: record.CommDateTime,
        note: record.Note,
        commType: record.CommType
      });
    }
    
    // Build timeline
    analysis.timeline.push({
      date: record.CommDateTime,
      type: record.CommType,
      mode: record.Mode_,
      direction: record.SentOrReceived === 1 ? 'Sent' : record.SentOrReceived === 2 ? 'Received' : 'Unknown',
      preview: record.Note ? record.Note.substring(0, 100) : ''
    });
  });
  
  // Sort timeline by date (most recent first)
  analysis.timeline.sort((a, b) => new Date(b.date) - new Date(a.date));
  analysis.recentActivity = analysis.timeline.slice(0, 10);
  
  return analysis;
}

/**
 * Extract phone call details from CommLog
 */
function extractPhoneCallDetails(records) {
  const calls = records.filter(r => 
    r.Note && (
      r.Note.includes('phone call') || 
      r.Note.includes('duration of') ||
      r.CommType === 386
    )
  );
  
  return calls.map(call => {
    const durationMatch = call.Note.match(/duration of (\d+) minutes? (\d+) seconds?/);
    const phoneMatch = call.Note.match(/to (\+?\d+)/);
    
    return {
      date: call.CommDateTime,
      phone: phoneMatch ? phoneMatch[1] : 'Unknown',
      duration: durationMatch ? `${durationMatch[1]}m ${durationMatch[2]}s` : 'Unknown',
      direction: call.SentOrReceived === 1 ? 'Outbound' : 'Inbound',
      summary: call.Note.includes('PX Summary') ? 
        call.Note.substring(call.Note.indexOf('PX Summary'), call.Note.indexOf('PX Summary') + 200) : 
        'No summary'
    };
  });
}

/**
 * Format CommLog query results as STRUCTURED JSON for frontend components
 */
function formatCommLogAsStructuredData(records) {
  if (!records || records.length === 0) {
    return {
      type: 'structured_commlog',
      data: {
        records: [],
        phoneCalls: [],
        analysis: {
          totalRecords: 0,
          byCommType: {},
          sentVsReceived: { sent: 0, received: 0, unknown: 0 }
        }
      }
    };
  }

  // Analyze the records
  const analysis = analyzeCommLog(records);
  const phoneCallDetails = extractPhoneCallDetails(records);
  
  return {
    type: 'structured_commlog',
    data: {
      records: analysis.formattedRecords.slice(0, 100),
      phoneCalls: phoneCallDetails.slice(0, 20),
      analysis: {
        totalRecords: records.length,
        byCommType: analysis.byCommType,
        sentVsReceived: analysis.sentVsReceived,
        callCount: phoneCallDetails.length,
        recentActivity: analysis.formattedRecords.slice(0, 10).map(r => ({
          date: r.date,
          direction: r.direction,
          preview: r.message ? r.message.substring(0, 150) : ''
        }))
      }
    }
  };
}