require('dotenv').config();

// ==================== HARDCODED CREDENTIALS ====================
const HARDCODED_LICENSE_KEY = "MzdkNzUzNjJkMGVhZDQ5YzYzNmNhZDdkYzY3YWZh";
const HARDCODED_BEARER = "U0FZV1ZMTURNNFlaWk1aSkVXWkQ6TXpka056VXpOakprTUdWaFpEUTVZell6Tm1OaFpEZGtZelkzWVdaaA==";
const BASE_URL = process.env.BASE_URL || "https://patcon.8px.us";

// ==================== API CONFIGURATION ====================
const QUERY_EXEC_URL = "https://query.8px.us/api/run/query";
const QUERY_EXEC_AUTH = "3edfgbhnjkuyt";
const QUERY_EXEC_KEY = "MzdkNzUzNjJkMGVhZDQ5YzYzNmNhZDdkYzY3YWZh";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const TXQL_API_URL = 'https://txql.8px.us/api/sql/query';

// ==================== DATA DISPLAY CONFIGURATION ====================
const DISPLAY_CONFIG = {
  MAX_ROWS: parseInt(process.env.DISPLAY_MAX_ROWS) || 1000,
  MAX_CHAR_LENGTH: parseInt(process.env.DISPLAY_MAX_CHARS) || 500,
  MAX_COLUMNS: parseInt(process.env.DISPLAY_MAX_COLS) || 999,
  CARD_THRESHOLD: parseInt(process.env.DISPLAY_CARD_THRESHOLD) || 5,
};

// ==================== TIMEZONE CONFIGURATION ====================
const TIMEZONE = process.env.TIMEZONE || 'America/Los_Angeles';

// ==================== PORT CONFIGURATION ====================
const PORT = process.env.PORT || 3001;

// ==================== TIMEZONE UTILITY FUNCTIONS ====================

/**
 * Get current date in specified timezone (YYYY-MM-DD format)
 */
function getCurrentDateInTimezone(timezone) {
  const date = new Date().toLocaleString('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).split(',')[0]; // Format: MM/DD/YYYY
  
  const [month, day, year] = date.split('/');
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`; // Return YYYY-MM-DD
}

/**
 * Get date N days ago in specified timezone (YYYY-MM-DD format)
 */
function getDateDaysAgo(daysAgo, timezone) {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  const dateStr = date.toLocaleString('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).split(',')[0]; // Format: MM/DD/YYYY
  
  const [month, day, year] = dateStr.split('/');
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`; // Return YYYY-MM-DD
}

/**
 * Convert various date formats to YYYY-MM-DD
 */
function normalizeDate(dateStr, timezone = TIMEZONE) {
  if (!dateStr) return null;
  
  dateStr = dateStr.trim();
  
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    return dateStr;
  }
  
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(dateStr)) {
    const [month, day, year] = dateStr.split('/');
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
  }
  
  if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(dateStr)) {
    const [month, day, year] = dateStr.split('-');
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
  }
  
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      return null;
    }
    const formatted = date.toLocaleString('en-US', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).split(',')[0]; // Format: MM/DD/YYYY
    
    const [month, day, year] = formatted.split('/');
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`; // Return YYYY-MM-DD
  } catch (e) {
    return null;
  }
}

// ==================== DATE DETECTION ====================

/**
 * Detect single date or date range from question
 */
function detectSingleDateFromQuestion(question) {
  const lowerQ = question.toLowerCase();
  const today = getCurrentDateInTimezone(TIMEZONE);
  const yesterday = getDateDaysAgo(1, TIMEZONE);
  const weekAgo = getDateDaysAgo(7, TIMEZONE);
  const monthAgo = getDateDaysAgo(30, TIMEZONE);
  
  if (lowerQ.includes('today')) {
    return { startDate: today, endDate: today };
  }
  if (lowerQ.includes('yesterday')) {
    return { startDate: yesterday, endDate: yesterday };
  }
  if (lowerQ.includes('last week') || lowerQ.includes('past week') || lowerQ.includes('this week')) {
    return { startDate: weekAgo, endDate: today };
  }
  if (lowerQ.includes('last month') || lowerQ.includes('past month') || lowerQ.includes('this month')) {
    return { startDate: monthAgo, endDate: today };
  }
  
  const dateRegex = /\b(\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}|\d{4}-\d{2}-\d{2})\b/g;
  const matches = question.match(dateRegex);
  if (matches && matches.length >= 1) {
    const start = normalizeDate(matches[0], TIMEZONE);
    const end = matches.length >= 2 ? normalizeDate(matches[1], TIMEZONE) : start;
    if (start && end) {
      return { startDate: start, endDate: end };
    }
  }
  
  return null;
}

// ==================== QUERY CLASSIFICATION ====================

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
    'inbound',
    'outbound',
    'incoming',
    'outgoing',
    'call direction',
    'direction of',
    'type of call',
    'call type'
  ];
  
  return callDirectionPatterns.some(pattern => lowerQ.includes(pattern));
}

/**
 * Check if question is a simple greeting
 */
function isSimpleGreeting(question) {
  const lowerQ = question.toLowerCase().trim();
  const greetings = [
    'hi', 'hello', 'hey', 'greetings', 'good morning', 'good afternoon',
    'good evening', 'howdy', 'what\'s up', 'whats up', 'sup', 'yo'
  ];
  
  return greetings.includes(lowerQ) || 
         greetings.some(g => lowerQ === g + '!' || lowerQ === g + '?');
}

/**
 * Check if question should be routed to AI Voice system
 */
function isAIVoiceQuestion(question) {
  const lowerQ = question.toLowerCase();
  
  const aiVoiceKeywords = [
    'call', 'calls', 'patient', 'patients', 'appointment', 'appointments',
    'sentiment', 'conversation', 'transcript', 'audio', 'recording',
    'disposition', 'outcome', 'phone', 'voicemail', 'message',
    'contact', 'reach', 'spoke', 'talked', 'answered', 'missed',
    'duration', 'length', 'time of call', 'when did', 'who called',
    'license key', 'licensekey', 'practice', 'location'
  ];
  
  return aiVoiceKeywords.some(keyword => lowerQ.includes(keyword));
}

/**
 * Check if question should be routed to CommLog system
 */
function isCommLogQuestion(question) {
  const lowerQ = question.toLowerCase();
  
  const commlogKeywords = [
    'commlog', 'comm log', 'communication log', 'communication',
    'email', 'emails', 'text', 'texts', 'sms', 'message', 'messages',
    'contact attempt', 'outreach', 'correspondence'
  ];
  
  const hasCommLogKeyword = commlogKeywords.some(keyword => lowerQ.includes(keyword));
  const hasCallKeyword = lowerQ.includes('call') || lowerQ.includes('phone');
  
  return hasCommLogKeyword && !hasCallKeyword;
}

// ==================== TEXT FORMATTING UTILITIES ====================

/**
 * Truncate text to specified length
 */
function truncateText(text, maxLength) {
  if (maxLength === 0 || !text) return text;
  if (typeof text !== 'string') text = String(text);
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
}

/**
 * Format field name for display
 */
function formatFieldName(field) {
  return field
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .trim()
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}

// ==================== LOGGING UTILITIES ====================

function logConfig() {
  console.log('📊 Data Display Configuration:');
  console.log(`   Max Rows: ${DISPLAY_CONFIG.MAX_ROWS}`);
  console.log(`   Max Character Length: ${DISPLAY_CONFIG.MAX_CHAR_LENGTH === 0 ? 'UNLIMITED' : DISPLAY_CONFIG.MAX_CHAR_LENGTH}`);
  console.log(`   Max Columns: ${DISPLAY_CONFIG.MAX_COLUMNS === 999 ? 'ALL' : DISPLAY_CONFIG.MAX_COLUMNS}`);
  console.log(`   Card Threshold: ${DISPLAY_CONFIG.CARD_THRESHOLD} records`);
}

// ==================== EXPORTS ====================

module.exports = {
  // Configuration
  HARDCODED_LICENSE_KEY,
  HARDCODED_BEARER,
  BASE_URL,
  QUERY_EXEC_URL,
  QUERY_EXEC_AUTH,
  QUERY_EXEC_KEY,
  OPENAI_API_KEY,
  TXQL_API_URL,
  DISPLAY_CONFIG,
  TIMEZONE,
  PORT,
  
  // Utility functions
  getCurrentDateInTimezone,
  getDateDaysAgo,
  normalizeDate,
  detectSingleDateFromQuestion,
  isLicenseKeyQuery,
  isCallDirectionQuery,
  isSimpleGreeting,
  isAIVoiceQuestion,
  isCommLogQuestion,
  truncateText,
  formatFieldName,
  logConfig
};