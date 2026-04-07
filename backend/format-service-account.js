/**
 * Helper Script: Format Service Account JSON for Render Environment Variable
 * 
 * Usage: node format-service-account.js
 * 
 * This will read your serviceAccountKey.json and output a single-line JSON
 * that you can copy-paste into Render's FIREBASE_SERVICE_ACCOUNT environment variable.
 */

const fs = require('fs');
const path = require('path');

const serviceAccountPath = path.join(__dirname, 'serviceAccountKey.json');

try {
  // Read the service account file
  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, 'utf8'));
  
  // Convert to single-line JSON (no pretty printing)
  const singleLineJson = JSON.stringify(serviceAccount);
  
  console.log('\n✅ Service Account JSON formatted for Render:\n');
  console.log('Copy the line below and paste it as the value for FIREBASE_SERVICE_ACCOUNT environment variable:\n');
  console.log('─'.repeat(80));
  console.log(singleLineJson);
  console.log('─'.repeat(80));
  console.log('\n📋 Instructions:');
  console.log('1. Go to Render Dashboard → Your Backend Service → Environment');
  console.log('2. Click "Add Environment Variable"');
  console.log('3. Key: FIREBASE_SERVICE_ACCOUNT');
  console.log('4. Value: [paste the line above]');
  console.log('5. Click "Save Changes"\n');
  
  // Also save to a file for easy copying
  const outputPath = path.join(__dirname, 'service-account-formatted.txt');
  fs.writeFileSync(outputPath, singleLineJson);
  console.log(`✅ Also saved to: ${outputPath}\n`);
  
} catch (error) {
  console.error('❌ Error:', error.message);
  console.error('\nMake sure serviceAccountKey.json exists in the backend directory.\n');
  process.exit(1);
}
