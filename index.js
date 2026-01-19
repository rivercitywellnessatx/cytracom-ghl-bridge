const EventSource = require('eventsource');
const fetch = require('node-fetch');

const CYTRACOM_TOKEN = process.env.CYTRACOM_TOKEN;
const GHL_WEBHOOK_URL = process.env.GHL_WEBHOOK_URL;

// Validate environment variables on startup
if (!CYTRACOM_TOKEN) {
  console.error('❌ CYTRACOM_TOKEN is not set');
  process.exit(1);
}

if (!GHL_WEBHOOK_URL) {
  console.error('❌ GHL_WEBHOOK_URL is not set');
  process.exit(1);
}

console.log('Starting Cytracom → GoHighLevel bridge...');

// Track processed calls to avoid duplicates
const processedCalls = new Set();
const pendingCalls = new Map();

const url = 'https://api.cytracom.net/v1.0/events/sse/chan';
const eventSource = new EventSource(url, {
  headers: {
    'Authorization': 'Basic ' + Buffer.from(`token:${CYTRACOM_TOKEN}`).toString('base64')
  }
});

eventSource.addEventListener('chan.hangup', async (e) => {
  try {
    const data = JSON.parse(e.data);
    
    // Only process inbound calls with no answer
    if (data.direction === 'inbound' && data.channel?.dial_status === 'no answer') {
      // Check if we already processed this call
      if (processedCalls.has(data.linked_id)) {
        console.log(`⏭️ Skipping duplicate for ${data.linked_id}`);
        return;
      }
      
      const callerNumber = cleanPhone(data.channel.number);
      
      if (!callerNumber) {
        console.log('⚠️ Could not extract caller phone number');
        return;
      }
      
      // Cancel any existing pending timer for this call
      if (pendingCalls.has(data.linked_id)) {
        clearTimeout(pendingCalls.get(data.linked_id));
      }
      
      // Wait 20 seconds after last "no answer" before sending
      const timer = setTimeout(async () => {
        processedCalls.add(data.linked_id);
        pendingCalls.delete(data.linked_id);
        
        console.log(`📞 Missed call from ${callerNumber}`);
        
        try {
          const response = await fetch(GHL_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              phone: callerNumber,
              event: 'missed_call',
              timestamp: data.event_time,
              linked_id: data.linked_id
            }),
            timeout: 10000 // 10 second timeout
          });
          
          if (response.ok) {
            console.log(`✅ Sent to GoHighLevel (${response.status})`);
          } else {
            console.error(`❌ GoHighLevel returned error: ${response.status}`);
          }
        } catch (err) {
          console.error('❌ Failed to send to GoHighLevel:', err.message);
          // Don't crash - just log and continue
        }
        
        // Clean up tracking after 5 minutes
        setTimeout(() => {
          processedCalls.delete(data.linked_id);
        }, 5 * 60 * 1000);
      }, 20000); // 20 second delay
      
      pendingCalls.set(data.linked_id, timer);
      console.log(`⏱️ Detected missed call from ${callerNumber}, waiting 20s for call to finish...`);
    }
  } catch (err) {
    console.error('❌ Error processing hangup event:', err.message);
    // Don't crash - continue processing other events
  }
});

eventSource.onopen = () => {
  console.log('✅ Connected to Cytracom SSE');
  console.log('🎧 Listening for missed calls...');
};

eventSource.onerror = (err) => {
  console.error('❌ SSE connection error:', err);
  // EventSource will automatically try to reconnect
};

function cleanPhone(phone) {
  if (!phone) return null;
  const cleaned = phone.replace(/\D/g, '');
  return cleaned.length >= 10 ? cleaned : null;
}

// Periodic cleanup of old processed calls (every 10 minutes)
setInterval(() => {
  const now = Date.now();
  console.log(`🧹 Cleanup check - tracking ${processedCalls.size} processed calls, ${pendingCalls.size} pending`);
  
  // Keep memory from growing indefinitely
  if (processedCalls.size > 1000) {
    console.log('⚠️ Clearing processed calls cache (over 1000 entries)');
    processedCalls.clear();
  }
}, 10 * 60 * 1000);

// Health check log every 5 minutes
setInterval(() => {
  console.log('⏰ Still running and listening...');
}, 5 * 60 * 1000);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  
  // Clear all pending timers
  for (const timer of pendingCalls.values()) {
    clearTimeout(timer);
  }
  
  eventSource.close();
  process.exit(0);
});

// Handle uncaught errors to prevent crashes
process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught exception:', err);
  // Don't exit - try to keep running
});

process.on('unhandledRejection', (err) => {
  console.error('❌ Unhandled rejection:', err);
  // Don't exit - try to keep running
});
