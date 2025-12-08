// services.js - Business Logic Layer
// All core service functions extracted from server.js

const axios = require('axios');
const config = require('./config');

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
 * Execute SQL query using the query execution endpoint
 */
async function executeSQL(sqlQuery) {
  try {
    console.log(`ðŸ” Executing SQL query...`);
    console.log(`   Full Query:\n${sqlQuery}`);
    console.log(`   Endpoint: ${config.QUERY_EXEC_URL}`);
    
    const payload = {
      key: config.QUERY_EXEC_KEY,
      query: sqlQuery.trim()
    };
    
    console.log(`   Payload:`, JSON.stringify(payload, null, 2));
    
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

/**
 * Format SQL results for display
 */
/**
 * Format SQL results for display
 */
function formatSQLResults(sqlResults, sqlQuery, originalQuestion = '') {
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

async function queryTXQL(question, sessionId, maxRetries = 3, timeout = 60000) {
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

  // SPECIAL CASE: Multi-patient pricing search
  // Handle queries like "find 10 patients with pricing information"
  if (isMultiPatientPricingSearch(question)) {
    console.log(`🔍 Detected multi-patient pricing search query`);
    console.log(`   Bypassing TXQL - using direct SQL for pricing search`);
    
    const searchSQL = buildMultiPatientPricingSearchSQL(question);
    if (searchSQL) {
      const safeSqlQuery = ensureQueryHasLimit(searchSQL);
      const sqlResults = await executeSQL(safeSqlQuery);
      
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
  console.log(`ðŸ” TXQL Connection Details:`);
  console.log(`   Endpoint: ${TXQL_API_URL}`);
  console.log(`   Session: ${sessionId}`);
  console.log(`   Question: ${question.trim()}`);
  console.log(`   Timeout: ${timeout}ms`);
  console.log(`   Max Retries: ${maxRetries}`);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const startTime = Date.now(); // Moved outside try block
    try {
      console.log(`ðŸ”„ TXQL Attempt ${attempt}/${maxRetries}: Querying...`);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        console.warn(`â±ï¸  Timeout triggered after ${timeout}ms on attempt ${attempt}`);
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
      
      console.log(`   â±ï¸  Request completed in ${elapsedTime}ms`);

      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unknown error');
        throw new Error(`Server responded with status ${response.status}: ${errorText}`);
      }

      const data = await response.json();
      console.log(`âœ… TXQL: Successfully received response (Attempt ${attempt})`);
      console.log(`   Response keys:`, Object.keys(data));

      // NEW: Extract SQL and execute it
      const sqlQuery = extractSQLFromTXQL(data);
      
      if (sqlQuery) {
        console.log(`ðŸ“Š Extracted SQL query, validating...`);
        
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
        
        // Validate SQL syntax
        const validation = validateSQL(finalSqlQuery);
        if (!validation.valid) {
          console.error(`âŒ SQL Validation Failed: ${validation.error}`);
          
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
        
        console.log(`âœ… SQL validation passed, checking for LIMIT clause...`);
        
        // Ensure query has LIMIT clause to prevent database errors
        const safeSqlQuery = ensureQueryHasLimit(finalSqlQuery);
        
        const sqlResults = await executeSQL(safeSqlQuery);
        
        // Format the combined response - pass question for pricing analysis detection
        // Note: formatSQLResults may return a promise for AI summaries, so await it
        const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, safeSqlQuery, question));
        
        return {
          success: true,
          data: formattedOutput,
          sqlQuery: safeSqlQuery,
          executionResults: sqlResults,
          sessionId: sessionId,
          attempts: attempt,
          system: 'txql'
        };
      } else {
        // If no SQL found, check if TXQL returned a natural language response
        console.log(`âš ï¸  Could not extract SQL from TXQL response`);
        
        
        // FALLBACK: Try to build SQL directly for note queries
        const fallbackSQL = buildNoteQuerySQL(question);
        if (fallbackSQL) {
          console.log('   💡 Using fallback SQL generation for note query');
          console.log('   📋 Building comprehensive note query with UNION');
          
          const safeSqlQuery = ensureQueryHasLimit(fallbackSQL);
          const sqlResults = await executeSQL(safeSqlQuery);
          const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, safeSqlQuery, question));
          
          return {
            success: true,
            data: formattedOutput,
            sqlQuery: safeSqlQuery,
            executionResults: sqlResults,
            sessionId: sessionId,
            attempts: attempt,
            system: 'txql',
            usedFallback: true
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
        console.warn(`â±ï¸  TXQL Attempt ${attempt} timed out after ${timeout}ms`);
        console.warn(`   This usually means the TXQL service is slow or unresponsive`);
      } else if (error.message.includes('fetch failed') || error.code === 'ECONNREFUSED') {
        console.warn(`ðŸŒ TXQL Network error on attempt ${attempt}: ${error.message}`);
        console.warn(`   Cannot reach ${TXQL_API_URL}`);
        console.warn(`   Please verify the TXQL service is running and accessible`);
      } else if (error.message.includes('ENOTFOUND')) {
        console.warn(`ðŸŒ DNS Resolution failed on attempt ${attempt}`);
        console.warn(`   Cannot resolve hostname: txql.8px.us`);
      } else {
        console.warn(`âš ï¸  TXQL Error on attempt ${attempt}: ${error.message}`);
        console.warn(`   Error type: ${error.name || 'Unknown'}`);
        console.warn(`   Time elapsed: ${elapsedTime}ms`);
      }

      if (attempt < maxRetries) {
        const retryDelay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`   â³ Retrying in ${retryDelay}ms...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  console.error(`âŒ TXQL: All ${maxRetries} retry attempts failed`);
  console.error(`   Last error: ${lastError.message}`);
  console.error(`   Error type: ${lastError.name}`);

  // FALLBACK: If this is a note summary request and TXQL failed, use direct SQL
  const isNoteSummaryRequest = isNoteSummaryQuery(question) || isPricingFromNotesQuery(question);
  
  if (isNoteSummaryRequest) {
    console.log('💡 TXQL failed, but this is a note query - attempting fallback');
    const fallbackSQL = buildNoteQuerySQL(question);
    
    if (fallbackSQL) {
      console.log('   ✅ Using fallback SQL generation for note query');
      const safeSqlQuery = ensureQueryHasLimit(fallbackSQL);
      const sqlResults = await executeSQL(safeSqlQuery);
      const formattedOutput = await Promise.resolve(formatSQLResults(sqlResults, safeSqlQuery, question));
      
      return {
        success: true,
        data: formattedOutput,
        sqlQuery: safeSqlQuery,
        executionResults: sqlResults,
        sessionId: sessionId,
        attempts: maxRetries,
        system: 'txql',
        usedFallback: true,
        reason: 'TXQL timeout - used direct SQL fallback'
      };
    }
  }


  let friendlyMessage = 'âŒ Error: Sorry, I couldn\'t connect to the database server. ';

  if (lastError.name === 'AbortError') {
    friendlyMessage += 'The request took too long (timed out after ' + (timeout/1000) + ' seconds). ';
    friendlyMessage += 'The TXQL service might be unavailable or overloaded. ';
    friendlyMessage += '\n\nðŸ’¡ **Suggestions:**\n';
    friendlyMessage += '- Try a simpler query\n';
    friendlyMessage += '- Wait a moment and try again\n';
    friendlyMessage += '- Ask about call data instead (AI Voice system is working)';
  } else if (lastError.message.includes('fetch failed') || lastError.code === 'ECONNREFUSED' || lastError.message.includes('ENOTFOUND')) {
    friendlyMessage += 'Cannot reach the TXQL service at `' + TXQL_API_URL + '`. ';
    friendlyMessage += '\n\nðŸ’¡ **Possible issues:**\n';
    friendlyMessage += '- The TXQL service may be down\n';
    friendlyMessage += '- Network connectivity issues\n';
    friendlyMessage += '- Firewall or DNS problems\n\n';
    friendlyMessage += 'Please check if the TXQL service is running and accessible.';
  } else {
    friendlyMessage += 'The TXQL service encountered an error. ';
    friendlyMessage += '\n\n**Error details:** ' + lastError.message;
    friendlyMessage += '\n\nðŸ’¡ You can try asking about call data instead (AI Voice system is working).';
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

// ==================== AI VOICE FUNCTIONS ====================
// (Keeping all the original AI Voice functions - normalizeCall, buildConversationContext, etc.)

/**
 * Resolve query parameters from conversation context
 * This enables multi-turn conversations where users don't need to repeat dates/license keys
 */
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


// ==================== EXPORTS ====================

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
  callOpenAI
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