const EventSource = require('eventsource');
const fetch = require('node-fetch');

const CYTRACOM_TOKEN = process.env.CYTRACOM_TOKEN;
const GHL_WEBHOOK_URL = process.env.GHL_WEBHOOK_URL;

console.log('Starting Cytracom → GoHighLevel bridge...');

const url = 'https://api.cytracom.net/v1.0/events/sse/chan';
const eventSource = new EventSource(url, {
  headers: {
    'Authorization': 'Basic ' + Buffer.from(`token:${CYTRACOM_TOKEN}`).toString('base64')
  }
});

eventSource.addEventListener('chan.hangup', async (e) => {
  try {
    const data = JSON.parse(e.data);
    
    if (data.direction === 'inbound' && data.channel?.dial_status === 'no answer') {
      const callerNumber = extractPhone(data);
      
      if (callerNumber) {
        console.log(`📞 Missed call from ${callerNumber}`);
        
        await fetch(GHL_WEBHOOK_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            phone: callerNumber,
            event: 'missed_call',
            timestamp: data.event_time,
            linked_id: data.linked_id
          })
        });
        
        console.log(`✅ Sent to GoHighLevel`);
      }
    }
  } catch (err) {
    console.error('Error processing event:', err);
  }
});

eventSource.onopen = () => {
  console.log('✅ Connected to Cytracom SSE');
};

eventSource.onerror = (err) => {
  console.error('❌ SSE error:', err);
};

function extractPhone(data) {
  if (data.channel?.connected_number) {
    return cleanPhone(data.channel.connected_number);
  }
  if (data.caller?.number) {
    return cleanPhone(data.caller.number);
  }
  if (data.original_caller_id) {
    const match = data.original_caller_id.match(/\d{10,}/);
    return match ? match[0] : null;
  }
  return null;
}

function cleanPhone(phone) {
  const cleaned = phone.replace(/\D/g, '');
  return cleaned.length >= 10 ? cleaned : null;
}
