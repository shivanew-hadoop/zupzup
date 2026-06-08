// Usage: node scripts/create-license.js user@email.com "User Name" 30
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const [emailArg, nameArg, daysArg] = process.argv.slice(2);
if (!emailArg) {
  console.error('Usage: npm run create-license -- user@email.com "User Name" 30');
  process.exit(1);
}

const email = emailArg.trim().toLowerCase();
const name = nameArg || email;
const days = Number(daysArg || 30);
const validTill = new Date(Date.now() + days * 86400000).toISOString().slice(0, 10);
const licenseKey = 'MCS-' + crypto.randomBytes(12).toString('hex').toUpperCase().match(/.{1,4}/g).join('-');
const file = path.join(__dirname, '..', 'users.json');
const users = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {};
users[email] = { name, licenseKey, plan: 'monthly', validTill, active: true };
fs.writeFileSync(file, JSON.stringify(users, null, 2));
console.log('\nCreated license:\n');
console.log('Email      :', email);
console.log('License Key:', licenseKey);
console.log('Valid Till :', validTill);
console.log('\nSend this email + license key to customer after payment.\n');
