import nodemailer from "nodemailer";
import dotenv from "dotenv";

dotenv.config();

// Configure email transporter
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_APP_PASSWORD // Use app-specific password for Gmail
  }
});

interface Alert {
  video_id: string;
  source: string;
  misuse_category: string;
  misuse_reasoning: string;
  risk_score: number;
  region: string;
  confidence: number;
  embedding_score: number;
  timestamp: Date;
}

export async function sendCriticalAlert(alert: Alert) {
  /**
   * Send email notification for CRITICAL priority alerts
   */
  try {
    const htmlContent = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%); padding: 20px; color: white; border-radius: 8px 8px 0 0;">
          <h1 style="margin: 0;">🚨 CRITICAL NetraX Alert</h1>
          <p style="margin: 5px 0 0 0;">Digital Asset Protection System</p>
        </div>
        
        <div style="background: #f9fafb; padding: 30px; border-radius: 0 0 8px 8px; border: 1px solid #e5e7eb; border-top: none;">
          
          <h2 style="color: #1f2937; margin-top: 0;">Detection Summary</h2>
          
          <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600; width: 40%;">Video ID</td>
              <td style="padding: 12px; color: #1f2937;"><strong>${alert.video_id}</strong></td>
            </tr>
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">Source Platform</td>
              <td style="padding: 12px; color: #1f2937;">${alert.source}</td>
            </tr>
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">Misuse Category</td>
              <td style="padding: 12px; color: #1f2937;"><strong>${alert.misuse_category}</strong></td>
            </tr>
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">Risk Score</td>
              <td style="padding: 12px; color: #dc2626;">
                <strong>${alert.risk_score}%</strong>
                <div style="background: #fee2e2; border-radius: 4px; height: 6px; margin-top: 4px; width: 100%;">
                  <div style="background: #dc2626; height: 100%; width: ${alert.risk_score}%; border-radius: 4px;"></div>
                </div>
              </td>
            </tr>
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">AI Confidence</td>
              <td style="padding: 12px; color: #1f2937;">
                pHash: ${alert.confidence}% | Embedding: ${alert.embedding_score}%
              </td>
            </tr>
            <tr style="border-bottom: 1px solid #e5e7eb;">
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">Geographic Location</td>
              <td style="padding: 12px; color: #1f2937;">${alert.region}</td>
            </tr>
            <tr>
              <td style="padding: 12px; color: #6b7280; font-weight: 600;">Detection Time</td>
              <td style="padding: 12px; color: #1f2937;">${alert.timestamp.toISOString()}</td>
            </tr>
          </table>
          
          <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 4px; margin: 20px 0;">
            <p style="margin: 0; color: #92400e;"><strong>📝 Reasoning:</strong></p>
            <p style="margin: 8px 0 0 0; color: #b45309;">${alert.misuse_reasoning}</p>
          </div>
          
          <div style="margin: 30px 0; text-align: center;">
            <a href="${process.env.FRONTEND_URL || 'https://netrax-frontend.vercel.app'}" 
               style="background: #dc2626; color: white; padding: 12px 32px; text-decoration: none; border-radius: 6px; font-weight: 600; display: inline-block;">
              View Dashboard & Take Action
            </a>
          </div>
          
          <div style="background: #f3f4f6; padding: 15px; border-radius: 6px; margin-top: 20px; font-size: 12px; color: #6b7280;">
            <p style="margin: 0;"><strong>Recommended Next Steps:</strong></p>
            <ul style="margin: 8px 0 0 0; padding-left: 20px;">
              <li>Review the alert in the dashboard</li>
              <li>Verify the misuse classification</li>
              <li>Auto-generate DMCA takedown notice</li>
              <li>Dispatch to relevant platforms</li>
            </ul>
          </div>
          
        </div>
        
        <div style="text-align: center; padding: 20px; color: #9ca3af; font-size: 12px;">
          <p style="margin: 0;">NetraX Digital Asset Protection System</p>
          <p style="margin: 5px 0 0 0;">Real-time piracy detection & enforcement</p>
        </div>
      </div>
    `;
    
    const mailOptions = {
      from: `NetraX Alerts <${process.env.EMAIL_USER}>`,
      to: process.env.ALERT_RECIPIENT_EMAIL || process.env.EMAIL_USER,
      subject: `🚨 CRITICAL: ${alert.misuse_category} Detected - ${alert.video_id}`,
      html: htmlContent,
      headers: {
        "X-Alert-Level": "CRITICAL",
        "X-Risk-Score": String(alert.risk_score),
        "X-Misuse-Category": alert.misuse_category
      }
    };
    
    const info = await transporter.sendMail(mailOptions);
    console.log(`📧 Critical alert email sent: ${info.messageId}`);
    return true;
    
  } catch (error) {
    console.error(`❌ Failed to send critical alert email: ${error}`);
    return false;
  }
}

export async function sendMediumAlert(alert: Alert) {
  /**
   * Send email notification for MEDIUM priority alerts (less urgent formatting)
   */
  try {
    const mailOptions = {
      from: `NetraX Alerts <${process.env.EMAIL_USER}>`,
      to: process.env.ALERT_RECIPIENT_EMAIL || process.env.EMAIL_USER,
      subject: `⚠️ MEDIUM: ${alert.misuse_category} Detected`,
      html: `
        <h2>NetraX Alert - Medium Priority</h2>
        <p><strong>Category:</strong> ${alert.misuse_category}</p>
        <p><strong>Video:</strong> ${alert.video_id}</p>
        <p><strong>Source:</strong> ${alert.source}</p>
        <p><strong>Risk Score:</strong> ${alert.risk_score}%</p>
        <p><a href="${process.env.FRONTEND_URL}/alerts">View in Dashboard</a></p>
      `
    };
    
    const info = await transporter.sendMail(mailOptions);
    console.log(`📧 Medium alert email sent: ${info.messageId}`);
    return true;
    
  } catch (error) {
    console.error(`❌ Failed to send medium alert email: ${error}`);
    return false;
  }
}

export async function sendDailyDigest(alerts: Alert[]) {
  /**
   * Send daily summary email with all alerts from the day
   */
  try {
    const criticalCount = alerts.filter(a => a.risk_score > 85).length;
    const mediumCount = alerts.filter(a => a.risk_score > 60 && a.risk_score <= 85).length;
    const lowCount = alerts.filter(a => a.risk_score <= 60).length;
    
    const htmlContent = `
      <div style="font-family: Arial, sans-serif;">
        <h1>NetraX Daily Digest</h1>
        <p>Total Alerts: ${alerts.length}</p>
        <ul>
          <li>🚨 Critical: ${criticalCount}</li>
          <li>⚠️ Medium: ${mediumCount}</li>
          <li>ℹ️ Low: ${lowCount}</li>
        </ul>
        <p><a href="${process.env.FRONTEND_URL}/dashboard">View Full Dashboard</a></p>
      </div>
    `;
    
    const mailOptions = {
      from: `NetraX Alerts <${process.env.EMAIL_USER}>`,
      to: process.env.ALERT_RECIPIENT_EMAIL || process.env.EMAIL_USER,
      subject: `📊 NetraX Daily Digest - ${alerts.length} Alerts`,
      html: htmlContent
    };
    
    const info = await transporter.sendMail(mailOptions);
    console.log(`📧 Daily digest email sent: ${info.messageId}`);
    return true;
    
  } catch (error) {
    console.error(`❌ Failed to send daily digest: ${error}`);
    return false;
  }
}

export async function testEmailConnection(): Promise<boolean> {
  /**
   * Test email configuration
   */
  try {
    await transporter.verify();
    console.log("✅ Email transporter verified successfully");
    return true;
  } catch (error) {
    console.error(`❌ Email transporter error: ${error}`);
    return false;
  }
}
