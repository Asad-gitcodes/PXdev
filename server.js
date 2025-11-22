const express = require('express');
const cors = require('cors');
const axios = require('axios');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Hardcoded credentials
const HARDCODED_LICENSE_KEY = "MzdkNzUzNjJkMGVhZDQ5YzYzNmNhZDdkYzY3YWZh";
const HARDCODED_BEARER = "U0FZV1ZMTURNNFlaWk1aSkVXWkQ6TXpka056VXpOakprTUdWaFpEUTVZell6Tm1OaFpEZGtZelkzWVdaaA==";
const BASE_URL = process.env.BASE_URL || "https://patcon.8px.us";

// Query execution configuration
const QUERY_EXEC_URL = "https://query.8px.us/api/run/query";
const QUERY_EXEC_AUTH = "3edfgbhnjkuyt";
const QUERY_EXEC_KEY = "YjY4M2VlM2MtMGU5ZS00Y2MxLWI2OWEtYmM2ZmQ0";

// OpenAI configuration
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// ==================== TIMEZONE CONFIGURATION ====================
// Set your timezone here - defaults to Pacific Time
const TIMEZONE = process.env.TIMEZONE || 'America/Los_Angeles';

// ==================== TXQL HEALTH CHECK ====================

/**
 * Test TXQL service connectivity
 */
async function testTXQLConnection() {
  const TXQL_API_URL = 'https://txql.8px.us/api/sql/query';
  console.log(`\n🔍 Testing TXQL service connectivity...`);
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
      console.log(`   ✅ TXQL service is reachable (Status: ${response.status})`);
      return true;
    } else {
      console.warn(`   ⚠️  TXQL service responded with status: ${response.status}`);
      return false;
    }
  } catch (error) {
    if (error.name === 'AbortError') {
      console.error(`   ❌ TXQL service connection timed out`);
    } else {
      console.error(`   ❌ Cannot reach TXQL service: ${error.message}`);
    }
    console.error(`   💡 Database queries will not work until TXQL service is available`);
    return false;
  }
}

/**
 * Test AI Voice API connectivity
 */
async function testAIVoiceConnection() {
  console.log(`\n🔍 Testing AI Voice API connectivity...`);
  console.log(`   Base URL: ${BASE_URL}`);
  console.log(`   Endpoint: ${BASE_URL}/api/config/get/aivoice/detail`);
  
  try {
    // Test with a date range that should work (yesterday)
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];
    
    const url = `${BASE_URL}/api/config/get/aivoice/detail`;
    const response = await axios.get(url, {
      params: {
        licenseKey: HARDCODED_LICENSE_KEY,
        startDate: dateStr,
        endDate: dateStr,
        size: 100,
        page: 1
      },
      headers: {
        'Authorization': `Bearer ${HARDCODED_BEARER}`
      },
      timeout: 5000,
      validateStatus: function (status) {
        return status < 500; // Accept any non-5xx status for connectivity test
      }
    });
    
    if (response.status === 404) {
      console.error(`   ❌ AI Voice API endpoint not found (404)`);
      console.error(`   💡 The endpoint may have changed or BASE_URL is incorrect`);
      console.error(`   💡 Call analysis features will NOT work`);
      return false;
    } else if (response.status === 401 || response.status === 403) {
      console.warn(`   ⚠️  AI Voice API authentication issue (Status: ${response.status})`);
      console.warn(`   💡 Check your license key and bearer token`);
      return false;
    } else if (response.status >= 200 && response.status < 300) {
      console.log(`   ✅ AI Voice API is reachable (Status: ${response.status})`);
      return true;
    } else {
      console.warn(`   ⚠️  AI Voice API responded with status: ${response.status}`);
      return false;
    }
  } catch (error) {
    if (error.code === 'ECONNREFUSED') {
      console.error(`   ❌ Cannot connect to ${BASE_URL} (connection refused)`);
    } else if (error.code === 'ENOTFOUND') {
      console.error(`   ❌ Cannot resolve hostname: ${BASE_URL}`);
    } else if (error.code === 'ETIMEDOUT') {
      console.error(`   ❌ AI Voice API connection timed out`);
    } else {
      console.error(`   ❌ Cannot reach AI Voice API: ${error.message}`);
    }
    console.error(`   💡 Call analysis features will NOT work until AI Voice API is available`);
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
    console.log(`✨ Created new session: ${sessionId} for user: ${userId}`);
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
    console.log(`🧹 Cleaned up ${cleaned} old sessions`);
  }
}

setInterval(() => cleanupOldSessions(), 10 * 60 * 1000);

// ==================== GREETING DETECTION ====================

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
    "Hello! 👋 I'm your AI assistant. I can help you with:\n\n🟢 **Call Analysis** - Ask about appointments, call records, sentiment, costs\n🔵 **Database Queries** - Ask about users, orders, tables, customers\n\nWhat would you like to know?",
    "Hi there! 👋 I'm here to help! I can assist with:\n\n🟢 Call data analysis\n🔵 Database queries\n\nJust ask me anything!",
    "Hey! 😊 I can help you analyze call data or query your database. What can I do for you today?"
  ];
  
  return greetings[Math.floor(Math.random() * greetings.length)];
}

// ==================== INTELLIGENT ROUTING ====================

function determineSystem(question) {
  const lowerQ = question.toLowerCase();
  
  // AI Voice keywords
  const aiVoiceKeywords = [
    'call', 'calls', 'voice', 'phone', 'patient', 'appointment', 
    'booked', 'cancelled', 'rescheduled', 'upsell', 'sentiment',
    'transcript', 'audio', 'follow-up', 'callback', 'cost of calls',
    'call duration', 'call summary', 'call records', 'inbound', 'outbound',
    'thumbs up', 'thumbs down', 'voicemail'
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
  
  console.log(`🤔 Question Analysis: "${question}"`);
  console.log(`   AI Voice Score: ${aiVoiceScore}`);
  console.log(`   TXQL Score: ${txqlScore}`);
  
  // If both scores are 0, default to TXQL
  if (aiVoiceScore === 0 && txqlScore === 0) {
    console.log(`   → Routing to: TXQL (default)`);
    return 'txql';
  }
  
  const system = aiVoiceScore > txqlScore ? 'aivoice' : 'txql';
  console.log(`   → Routing to: ${system.toUpperCase()}`);
  return system;
}

// ==================== SQL EXECUTION FUNCTION ====================

/**
 * Extract SQL query from TXQL response
 */
function extractSQLFromTXQL(txqlResponse) {
  try {
    console.log('🔍 Extracting SQL from TXQL response...');
    console.log('   Response type:', typeof txqlResponse);
    console.log('   Response preview:', JSON.stringify(txqlResponse).substring(0, 200));
    
    // Case 1: Response is a string containing SQL
    if (typeof txqlResponse === 'string') {
      // Try to extract SQL from markdown code blocks
      const sqlMatch = txqlResponse.match(/```sql\s*([\s\S]*?)\s*```/i);
      if (sqlMatch && sqlMatch[1]) {
        console.log('   ✅ Found SQL in markdown code block');
        return sqlMatch[1].trim();
      }
      
      // Try to extract from "query": "..." pattern
      const queryMatch = txqlResponse.match(/"query"\s*:\s*"([^"]+)"/);
      if (queryMatch && queryMatch[1]) {
        console.log('   ✅ Found SQL in "query" field');
        return queryMatch[1].trim();
      }
      
      // Check if the string itself looks like SQL
      const trimmed = txqlResponse.trim();
      if (trimmed.match(/^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)/i)) {
        console.log('   ✅ String appears to be SQL');
        return trimmed;
      }
    }
    
    // Case 2: Response is an object
    if (typeof txqlResponse === 'object' && txqlResponse !== null) {
      // Check common field names
      const possibleFields = ['sql', 'query', 'sqlQuery', 'sql_query', 'statement'];
      for (const field of possibleFields) {
        if (txqlResponse[field] && typeof txqlResponse[field] === 'string') {
          console.log(`   ✅ Found SQL in field: ${field}`);
          return txqlResponse[field].trim();
        }
      }
      
      // Check nested data object
      if (txqlResponse.data) {
        for (const field of possibleFields) {
          if (txqlResponse.data[field] && typeof txqlResponse.data[field] === 'string') {
            console.log(`   ✅ Found SQL in data.${field}`);
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
        console.log('   ✅ Found and unescaped SQL from JSON string');
        return unescaped.trim();
      }
      
      // If response has a 'response' or 'answer' field, check that
      if (txqlResponse.response && typeof txqlResponse.response === 'string') {
        const sqlMatch = txqlResponse.response.match(/```sql\s*([\s\S]*?)\s*```/i);
        if (sqlMatch && sqlMatch[1]) {
          console.log('   ✅ Found SQL in response field markdown');
          return sqlMatch[1].trim();
        }
      }
      
      if (txqlResponse.answer && typeof txqlResponse.answer === 'string') {
        const sqlMatch = txqlResponse.answer.match(/```sql\s*([\s\S]*?)\s*```/i);
        if (sqlMatch && sqlMatch[1]) {
          console.log('   ✅ Found SQL in answer field markdown');
          return sqlMatch[1].trim();
        }
      }
    }
    
    console.log('   ⚠️  Could not extract SQL from response');
    return null;
  } catch (error) {
    console.error('❌ Error extracting SQL:', error);
    return null;
  }
}

/**
 * Execute SQL query using the query execution endpoint
 */
async function executeSQL(sqlQuery) {
  try {
    console.log(`🔍 Executing SQL query...`);
    console.log(`   Full Query:\n${sqlQuery}`);
    console.log(`   Endpoint: ${QUERY_EXEC_URL}`);
    
    const payload = {
      key: QUERY_EXEC_KEY,
      query: sqlQuery.trim()
    };
    
    console.log(`   Payload:`, JSON.stringify(payload, null, 2));
    
    const response = await axios.post(QUERY_EXEC_URL, payload, {
      headers: {
        'Authorization': QUERY_EXEC_AUTH,
        'Content-Type': 'application/json'
      },
      timeout: 30000,
      validateStatus: function (status) {
        return status < 500; // Don't throw on 4xx errors
      }
    });
    
    // Check if response was successful
    if (response.status >= 400) {
      console.error(`❌ SQL execution failed with status ${response.status}`);
      console.error(`   Response:`, JSON.stringify(response.data, null, 2));
      
      return {
        success: false,
        error: `Query execution failed (${response.status}): ${response.data?.error || response.data?.message || 'Unknown error'}`,
        statusCode: response.status,
        responseData: response.data,
        data: null
      };
    }
    
    console.log(`✅ SQL execution successful`);
    console.log(`   Rows returned: ${response.data?.data?.length || 0}`);
    
    return {
      success: true,
      data: response.data,
      rowCount: response.data?.data?.length || 0
    };
  } catch (error) {
    console.error('❌ SQL execution failed:', error.message);
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

/**
 * Format SQL results for display
 */
/**
 * Format SQL results for display
 */
function formatSQLResults(sqlResults, sqlQuery) {
  if (!sqlResults.success || !sqlResults.data) {
    let errorOutput = `## ❌ Query Execution Failed\n\n`;
    errorOutput += `**Error:** ${sqlResults.error || 'Unknown error'}\n\n`;
    
    if (sqlResults.statusCode) {
      errorOutput += `**Status Code:** ${sqlResults.statusCode}\n\n`;
    }
    
    if (sqlResults.responseData) {
      errorOutput += `**Server Response:** \`\`\`json\n${JSON.stringify(sqlResults.responseData, null, 2)}\n\`\`\`\n\n`;
    }
    
    errorOutput += `### SQL Query That Failed:\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n\n`;
    errorOutput += `💡 **Troubleshooting Tips:**\n`;
    errorOutput += `- Verify the SQL syntax is correct for your database engine\n`;
    errorOutput += `- Check that all table and column names exist\n`;
    errorOutput += `- Ensure date formats match database requirements\n`;
    errorOutput += `- Try simplifying the query to isolate the issue\n`;
    
    return errorOutput;
  }
  
  const rows = sqlResults.data.data || sqlResults.data;
  
  if (!rows || rows.length === 0) {
    return `## 📊 Query Results\n\n**Status:** ✅ Query executed successfully\n\n**Records Found:** 0\n\n### SQL Query:\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n\n💡 **Note:** No records match your query criteria.`;
  }
  
  const columns = Object.keys(rows[0]);
  
  // Analyze data to determine best visualization
  const visualization = analyzeDataForVisualization(rows, columns);
  
  // Start building output
  let output = `## 📊 Query Results\n\n`;
  output += `**Status:** ✅ Success  \n`;
  output += `**Records Found:** ${rows.length}\n\n`;
  
  // Show SQL query in collapsible format
  output += `<details>\n<summary>🔍 View SQL Query</summary>\n\n\`\`\`sql\n${sqlQuery}\n\`\`\`\n</details>\n\n`;
  
  output += `---\n\n`;
  
  // Add visualization based on analysis
  if (visualization.type === 'chart' && visualization.chartData) {
    output += generateChartVisualization(visualization, rows);
    output += `\n\n---\n\n`;
  }
  
  // Always show data table
  output += `### 📋 Data Table\n\n`;
  output += generateDataTable(rows, columns);
  
  // Add statistics for numeric columns
  const stats = generateStatistics(rows, columns);
  if (stats) {
    output += `\n\n---\n\n`;
    output += stats;
  }
  
  return output;
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
  let output = `### 📈 Visual Analysis: ${visualization.description}\n\n`;
  
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
      const bar = '█'.repeat(barLength);
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
 * Generate data table
 */
function generateDataTable(rows, columns) {
  // Format values for better readability
  const formatValue = (val) => {
    if (val === null || val === undefined) return '—';
    if (typeof val === 'string' && val.includes('T00:00:00')) {
      return val.split('T')[0];
    }
    if (typeof val === 'number') {
      return val.toLocaleString();
    }
    const str = String(val);
    return str.length > 50 ? str.substring(0, 47) + '...' : str;
  };
  
  let table = '| ' + columns.map(col => `**${col}**`).join(' | ') + ' |\n';
  table += '|' + columns.map(() => ':---').join('|') + '|\n';
  
  const displayRows = rows.slice(0, 50);
  for (const row of displayRows) {
    table += '| ' + columns.map(col => formatValue(row[col])).join(' | ') + ' |\n';
  }
  
  if (rows.length > 50) {
    table += `\n💡 **Note:** Showing first 50 of ${rows.length} total records.`;
  }
  
  return table;
}

/**
 * Generate statistics for numeric columns
 */
function generateStatistics(rows, columns) {
  const numericColumns = columns.filter(col => {
    return rows.some(row => typeof row[col] === 'number' && row[col] !== 0);
  });
  
  if (numericColumns.length === 0) return null;
  
  let stats = `### 📈 Statistical Summary\n\n`;
  
  numericColumns.forEach(col => {
    const values = rows.map(r => r[col]).filter(v => typeof v === 'number');
    if (values.length > 0) {
      const sum = values.reduce((a, b) => a + b, 0);
      const avg = sum / values.length;
      const max = Math.max(...values);
      const min = Math.min(...values);
      const sorted = [...values].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      
      stats += `**${col}:**\n`;
      stats += `- Count: ${values.length}\n`;
      stats += `- Sum: ${sum.toLocaleString()}\n`;
      stats += `- Average: ${avg.toFixed(2)}\n`;
      stats += `- Median: ${median.toFixed(2)}\n`;
      stats += `- Min: ${min.toLocaleString()} | Max: ${max.toLocaleString()}\n`;
      
      // Calculate distribution
      if (values.length > 1) {
        const stdDev = Math.sqrt(values.reduce((sq, n) => sq + Math.pow(n - avg, 2), 0) / values.length);
        stats += `- Std Dev: ${stdDev.toFixed(2)}\n`;
      }
      
      stats += `\n`;
    }
  });
  
  return stats;
}

// ==================== TXQL FUNCTION (MODIFIED) ====================

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

  let lastError = null;
  const TXQL_API_URL = 'https://txql.8px.us/api/sql/query';

  // Log initial connection attempt details
  console.log(`🔍 TXQL Connection Details:`);
  console.log(`   Endpoint: ${TXQL_API_URL}`);
  console.log(`   Session: ${sessionId}`);
  console.log(`   Question: ${question.trim()}`);
  console.log(`   Timeout: ${timeout}ms`);
  console.log(`   Max Retries: ${maxRetries}`);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`🔄 TXQL Attempt ${attempt}/${maxRetries}: Querying...`);
      const startTime = Date.now();

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

      // NEW: Extract SQL and execute it
      const sqlQuery = extractSQLFromTXQL(data);
      
      if (sqlQuery) {
        console.log(`📊 Extracted SQL query, executing...`);
        const sqlResults = await executeSQL(sqlQuery);
        
        // Format the combined response
        const formattedOutput = formatSQLResults(sqlResults, sqlQuery);
        
        return {
          success: true,
          data: formattedOutput,
          sqlQuery: sqlQuery,
          executionResults: sqlResults,
          sessionId: sessionId,
          attempts: attempt,
          system: 'txql'
        };
      } else {
        // If no SQL found, check if TXQL returned a natural language response
        console.log(`⚠️  Could not extract SQL from TXQL response`);
        
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

// ==================== AI VOICE FUNCTIONS ====================
// (Keeping all the original AI Voice functions - normalizeCall, buildConversationContext, etc.)
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
    patientName: safeGet(raw, ['patientName', 'patient.name', 'meta.guestName']),
    dateTime: safeGet(raw, ['dateTime', 'createdAt', 'startTime', 'meta.createdAt']),
    callType: safeGet(raw, ['callType', 'type', 'meta.callType'], 'unknown'),
    outcome: safeGet(raw, ['outcome', 'callStatus', 'meta.callStatus', 'flags.callSuccessful'], 'unknown'),
    sentiment: safeGet(raw, ['sentiment', 'userSentiment', 'analysis.sentiments', 'analysis.userSentiment'], 'neutral'),
    duration: safeGet(raw, ['duration', 'costs.durationSec', 'durationSec'], 0),
    cost: safeGet(raw, ['cost', 'costs.total'], 0),
    summary: safeGet(raw, ['summary'], 'No summary available')
  };

  if (options.includeTranscript && raw.transcript) {
    normalized.transcript = raw.transcript;
  }

  if (options.includeAudio && raw.audioUrl) {
    normalized.audioUrl = raw.audioUrl;
  }

  return normalized;
}

function buildConversationContext(calls) {
  if (!calls || calls.length === 0) {
    return "No call data available.";
  }

  const total = calls.length;
  const totalCost = calls.reduce((sum, c) => sum + (parseFloat(c.cost) || 0), 0);
  const avgDuration = calls.reduce((sum, c) => sum + (parseInt(c.duration) || 0), 0) / total;

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

Outcomes:
${Object.entries(outcomes).map(([k, v]) => `  - ${k}: ${v}`).join('\n')}

Sentiments:
${Object.entries(sentiments).map(([k, v]) => `  - ${k}: ${v}`).join('\n')}
  `.trim();
}

/**
 * Get current date in specified timezone
 * @param {string} timezone - IANA timezone string (e.g., 'America/Los_Angeles')
 * @returns {string} Date in YYYY-MM-DD format
 */
function getCurrentDateInTimezone(timezone = TIMEZONE) {
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
    console.error(`❌ Error getting date in timezone ${timezone}:`, error.message);
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
  const todayStr = getCurrentDateInTimezone(TIMEZONE);
  console.log(`📅 Current date in ${TIMEZONE}: ${todayStr}`);

  if (lowerQ.includes('today')) {
    console.log(`   ✅ Detected "today" - using date: ${todayStr}`);
    return { startDate: todayStr, endDate: todayStr };
  }

  if (lowerQ.includes('yesterday')) {
    const today = new Date(todayStr);
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    console.log(`   ✅ Detected "yesterday" - using date: ${yesterdayStr}`);
    return { startDate: yesterdayStr, endDate: yesterdayStr };
  }

  if (lowerQ.includes('this week')) {
    const today = new Date(todayStr);
    const startOfWeek = new Date(today);
    startOfWeek.setDate(today.getDate() - today.getDay());
    const startStr = startOfWeek.toISOString().split('T')[0];
    console.log(`   ✅ Detected "this week" - using range: ${startStr} to ${todayStr}`);
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
    console.log(`   ✅ Detected "last week" - using range: ${startStr} to ${endStr}`);
    return {
      startDate: startStr,
      endDate: endStr
    };
  }

  if (lowerQ.includes('this month')) {
    const today = new Date(todayStr);
    const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const startStr = startOfMonth.toISOString().split('T')[0];
    console.log(`   ✅ Detected "this month" - using range: ${startStr} to ${todayStr}`);
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
    console.log(`   ✅ Detected "last month" - using range: ${startStr} to ${endStr}`);
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
      console.error(`   ❌ Date validation failed`);
      console.error(`      Start: ${startDate}`);
      console.error(`      End: ${endDate}`);
      
      return {
        success: false,
        error: 'Invalid date format',
        friendlyError: `❌ Invalid date format. Dates must be YYYY-MM-DD.\n\nReceived:\n- Start: ${startDate}\n- End: ${endDate}`,
        data: [],
        count: 0,
        summary: 'Invalid date format provided.'
      };
    }
    
    console.log(`   ✅ Date validation passed`);
    
    const url = `${BASE_URL}/api/config/get/aivoice/detail`;
    
    console.log(`\n🔍 Fetching AI Voice call details...`);
    console.log(`   Endpoint: ${url}`);
    console.log(`   Date Range: ${startDate} to ${endDate}`);
    console.log(`   License Key: ${HARDCODED_LICENSE_KEY.substring(0, 10)}...`);
    
    const response = await axios.get(url, {
      params: {
        licenseKey: HARDCODED_LICENSE_KEY,
        startDate,
        endDate,
        size: 1000, // Fetch up to 1000 calls
        page: 1
      },
      headers: {
        'Authorization': `Bearer ${HARDCODED_BEARER}`
      },
      timeout: 30000, // 30 second timeout
      validateStatus: function (status) {
        return status < 500; // Don't throw on 4xx errors
      }
    });

    console.log(`   Response Status: ${response.status}`);


    // Handle 404 specifically
    if (response.status === 404) {
      console.error(`   ❌ API endpoint not found (404)`);
      console.error(`   💡 The endpoint ${url} may not exist or the API structure has changed`);
      return {
        success: false,
        error: 'API endpoint not found',
        friendlyError: `❌ The AI Voice API endpoint was not found.\n\n**Endpoint:** \`${url}\`\n\n**Possible issues:**\n- The API endpoint may have changed\n- The BASE_URL (${BASE_URL}) might be incorrect\n- The API service may be down\n\n💡 **Action needed:** Please verify the correct API endpoint with your AI Voice service provider.`,
        data: [],
        count: 0,
        summary: 'API endpoint not found.'
      };
    }

    // Handle other 4xx errors
    if (response.status >= 400 && response.status < 500) {
      console.error(`   ❌ Client error (${response.status})`);
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
        friendlyError: `❌ Error fetching call data: ${errorMsg}\n\n**Status:** ${response.status}\n\n💡 Please check your API credentials and permissions.`,
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

    if (callData.length > 0) {
      console.log(`   ✅ Successfully fetched ${callData.length} calls`);
      
      // Log sample structure for debugging
      if (callData[0]) {
        const sampleJson = JSON.stringify(callData[0], null, 2);
        const preview = sampleJson.length > 500 ? sampleJson.substring(0, 500) + '...' : sampleJson;
        console.log(`   📊 Sample call structure:`);
        console.log(preview);
        console.log(`   🔍 Available fields:`, Object.keys(callData[0]).join(', '));
      }
      
      // Analyze data quality before normalization
      const hasOutcome = callData.filter(c => 
        c.outcome || c.callStatus || c.meta?.callStatus
      ).length;
      const hasSentiment = callData.filter(c => 
        c.sentiment || c.userSentiment || c.analysis?.sentiments || c.analysis?.userSentiment
      ).length;
      const hasCost = callData.filter(c => 
        c.cost || c.costs?.total || (typeof c.costs?.total === 'number')
      ).length;
      const hasDuration = callData.filter(c =>
        c.duration || c.costs?.durationSec || c.durationSec
      ).length;
      
      console.log(`   📈 Data quality check:`);
      console.log(`      - Calls with outcome: ${hasOutcome}/${callData.length} (${Math.round(hasOutcome/callData.length*100)}%)`);
      console.log(`      - Calls with sentiment: ${hasSentiment}/${callData.length} (${Math.round(hasSentiment/callData.length*100)}%)`);
      console.log(`      - Calls with cost: ${hasCost}/${callData.length} (${Math.round(hasCost/callData.length*100)}%)`);
      console.log(`      - Calls with duration: ${hasDuration}/${callData.length} (${Math.round(hasDuration/callData.length*100)}%)`);
      
      const normalized = callData.map(call => 
        normalizeCall(call, { includeTranscript, includeAudio })
      );

      const summary = buildConversationContext(normalized);

      return {
        success: true,
        data: normalized,
        count: normalized.length,
        summary
      };
    } else {
      console.log(`   ℹ️  No calls found for date range`);
      return {
        success: true,
        data: [],
        count: 0,
        summary: 'No calls found for the specified date range.'
      };
    }
  } catch (error) {
    console.error('❌ Error fetching call details:', error.message);
    
    if (error.code === 'ECONNREFUSED') {
      console.error(`   Cannot connect to ${BASE_URL}`);
      return {
        success: false,
        error: 'Connection refused',
        friendlyError: `❌ Cannot connect to the AI Voice service.\n\n**Server:** ${BASE_URL}\n\n**Error:** Connection refused\n\n💡 Please verify the AI Voice service is running and accessible.`,
        data: [],
        count: 0,
        summary: 'Failed to connect to AI Voice service.'
      };
    }
    
    if (error.code === 'ENOTFOUND') {
      console.error(`   DNS resolution failed for ${BASE_URL}`);
      return {
        success: false,
        error: 'DNS resolution failed',
        friendlyError: `❌ Cannot resolve hostname.\n\n**Server:** ${BASE_URL}\n\n💡 Please check the BASE_URL configuration.`,
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
        friendlyError: `❌ Request timed out after 30 seconds.\n\n**Server:** ${BASE_URL}\n\n💡 The AI Voice service may be slow or unresponsive.`,
        data: [],
        count: 0,
        summary: 'Request to AI Voice service timed out.'
      };
    }
    
    return {
      success: false,
      error: error.message,
      friendlyError: `❌ Error fetching call data: ${error.message}\n\n**Server:** ${BASE_URL}\n\n💡 Please check server logs for more details.`,
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
          'Authorization': `Bearer ${OPENAI_API_KEY}`,
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
    console.log(`📩 NEW QUESTION: "${question}"`);
    console.log(`👤 User ID: ${userId}`);
    console.log(`${'='.repeat(70)}\n`);

    const session = getOrCreateSession(userId);

    // Check for greetings first
    if (isGreeting(question)) {
      console.log(`👋 Detected greeting, returning friendly response`);
      const greetingResponse = getGreetingResponse();
      
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
    const targetSystem = determineSystem(question);

    // Route to TXQL
    if (targetSystem === 'txql') {
      console.log(`🔵 Routing to TXQL system...`);
      const result = await queryTXQL(question, session.txqlSessionId);

      if (result.success) {
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
      console.log(`🟢 Routing to AI Voice system...`);

      if (!OPENAI_API_KEY) {
        return res.status(500).json({
          success: false,
          error: 'AI Voice system is not configured (missing OpenAI API key)',
          friendlyError: 'AI Voice system is currently unavailable. Please try asking about database queries instead.'
        });
      }

      const prefilledDates = detectSingleDateFromQuestion(question);
      const tools = [{
        type: 'function',
        function: {
          name: 'aivoice_get_call_details',
          description: 'Fetches AI Voice call records between startDate and endDate',
          parameters: {
            type: 'object',
            properties: {
              startDate: { type: 'string', description: 'Start date (YYYY-MM-DD)' },
              endDate: { type: 'string', description: 'End date (YYYY-MM-DD)' },
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
          content: 'You are an AI assistant for PatientXpress AI voice call analysis. Provide clear, concise answers.'
        },
        {
          role: 'user',
          content: prefilledDates 
            ? `${question}\n\n(Dates: ${prefilledDates.startDate} to ${prefilledDates.endDate})`
            : question
        }
      ];

      let aiResponse = await callOpenAI(conversationMessages, tools);
      let responseMessage = aiResponse.choices[0].message;
      let result;

      if (responseMessage.tool_calls) {
        const toolCall = responseMessage.tool_calls[0];
        const functionArgs = JSON.parse(toolCall.function.arguments || '{}');
        const startDate = functionArgs.startDate || prefilledDates?.startDate;
        const endDate = functionArgs.endDate || prefilledDates?.endDate;

        const callResult = await fetchCallDetails(
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

        const aiToolPayload = {
          success: callResult.success,
          summary: callResult.summary,
          count: callResult.count,
          message: `Fetched ${callResult.count} calls.`
        };

        conversationMessages.push(responseMessage);
        conversationMessages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          name: 'aivoice_get_call_details',
          content: JSON.stringify(aiToolPayload)
        });

        aiResponse = await callOpenAI(conversationMessages, tools);
        responseMessage = aiResponse.choices[0].message;

        result = {
          success: true,
          answer: responseMessage.content,
          callHistory: callResult.data,
          system: 'aivoice'
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
    console.error('💥 Error in unified chat:', error);
    res.status(500).json({
      success: false,
      error: 'An unexpected error occurred',
      friendlyError: 'Something went wrong. Please try again.',
      technicalError: error.message
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

  const session = getOrCreateSession(userId);
  const result = await queryTXQL(question, session.txqlSessionId, maxRetries, timeout);

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

    if (!question || !OPENAI_API_KEY) {
      return res.status(400).json({ error: 'Invalid request' });
    }

    const prefilledDates = detectSingleDateFromQuestion(question);
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

    let aiResponse = await callOpenAI(conversationMessages, tools);
    let responseMessage = aiResponse.choices[0].message;

    if (responseMessage.tool_calls) {
      const toolCall = responseMessage.tool_calls[0];
      const functionArgs = JSON.parse(toolCall.function.arguments || '{}');
      const startDate = functionArgs.startDate || prefilledDates?.startDate;
      const endDate = functionArgs.endDate || prefilledDates?.endDate;

      if (startDate && endDate) {
        const result = await fetchCallDetails(startDate, endDate, functionArgs.includeTranscript, functionArgs.includeAudio);
        
        conversationMessages.push(responseMessage);
        conversationMessages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          name: 'aivoice_get_call_details',
          content: JSON.stringify({ success: result.success, summary: result.summary, count: result.count })
        });

        aiResponse = await callOpenAI(conversationMessages, tools);
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
  const session = getSession(userId);

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
  
  if (activeSessions.has(key)) {
    activeSessions.delete(key);
    res.json({ success: true, message: 'Session cleared' });
  } else {
    res.json({ success: false, message: 'No session found' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    activeSessions: activeSessions.size,
    systems: {
      aivoice: 'AI Voice Call Analysis',
      txql: 'SQL Database Queries (with execution)',
      greeting: 'Greeting Handler'
    }
  });
});

// Start server
app.listen(PORT, async () => {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`🚀 UNIFIED CHATBOT SERVER (WITH SQL EXECUTION)`);
  console.log(`${'='.repeat(70)}`);
  console.log(`\n📡 Main Endpoint (Auto-routes to correct system):`);
  console.log(`   → POST http://localhost:${PORT}/api/chat`);
  console.log(`\n✨ NEW: SQL Query Execution`);
  console.log(`   TXQL returns SQL → Executes via query.8px.us → Shows actual results!`);
  console.log(`\n⏰ Timezone Configuration:`);
  console.log(`   Current timezone: ${TIMEZONE}`);
  console.log(`   Current date: ${getCurrentDateInTimezone(TIMEZONE)}`);
  console.log(`\n🔵 TXQL System (Database Queries):`);
  console.log(`   Questions: users, tables, orders, California, age, etc.`);
  console.log(`\n🟢 AI Voice System (Call Analysis):`);
  console.log(`   Questions: calls, appointments, patients, sentiment, etc.`);
  console.log(`\n📋 Legacy Endpoints:`);
  console.log(`   → POST http://localhost:${PORT}/api/txql/chat (TXQL only)`);
  console.log(`   → POST http://localhost:${PORT}/api/ask (AI Voice only)`);
  console.log(`\n💚 Health: http://localhost:${PORT}/api/health`);
  console.log(`\n${'='.repeat(70)}\n`);
  
  if (!OPENAI_API_KEY) {
    console.warn('⚠️  WARNING: OPENAI_API_KEY not set (AI Voice will not work)');
  }
  
  console.log(`🎯 FEATURE: SQL queries are now executed and results displayed!`);
  console.log(`   Auth: ${QUERY_EXEC_AUTH}`);
  console.log(`   Key: ${QUERY_EXEC_KEY}\n`);
  
  // Test both services on startup
  const txqlOk = await testTXQLConnection();
  const aiVoiceOk = await testAIVoiceConnection();
  
  console.log(`\n${'='.repeat(70)}`);
  console.log(`📊 SERVICE STATUS SUMMARY`);
  console.log(`${'='.repeat(70)}`);
  console.log(`   TXQL Database Service:     ${txqlOk ? '✅ ONLINE' : '❌ OFFLINE'}`);
  console.log(`   AI Voice Call Service:     ${aiVoiceOk ? '✅ ONLINE' : '❌ OFFLINE'}`);
  
  if (!txqlOk || !aiVoiceOk) {
    console.log(`\n⚠️  WARNING: Some services are unavailable`);
    if (!txqlOk) {
      console.log(`   • Database queries will not work`);
    }
    if (!aiVoiceOk) {
      console.log(`   • Call analysis will not work`);
    }
  } else {
    console.log(`\n✅ All systems operational!`);
  }
  
  console.log(`${'='.repeat(70)}\n`);
});