'use client';

import { useState } from 'react';
// The URL for your external upload API
const EXTERNAL_API_URL = 'http://localhost:8001/v1/index/documents';

export default function UploadPage() {
  const [fileName, setFileName] = useState('');
  const [message, setMessage] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    setMessage('');

    if (!fileName.trim()) {
      setMessage('Please enter a file name (e.g. file:///home/contoso/invoice.pdf).');
      setIsLoading(false);
      return;
    }

    // 1. Construct the payload
    const payload = {
      source_url: `${fileName.trim()}`,
      'content_type': 'application/pdf',
    };
    
    try {
      // 2. Direct POST request to the external API
      const response = await fetch(EXTERNAL_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload), 
      });

      const data = await response.json();
      const isSuccess = response.ok;
      
      if (isSuccess) {
        // 🎯 UPDATED: Display job_id and indexing_status from the response data
        const jobId = data.job_id || 'N/A';
        const status = data.indexing_status || 'Unknown Status';
        
        setMessage(`
          ✅ Success! Request Submitted.
          Job ID: ${jobId}
          Status: ${status}
          Source URL: ${data.metadata?.source_url || fileName}
        `);
        setFileName('');
      } else {
        // Handle external API errors
        setMessage(`❌ Upload Failed (${response.status}): ${JSON.stringify(data)}`);
      }
    } catch (error) {
      // Handle network errors (CORS, connection failure, etc.)
      setMessage(`🚨 An error occurred while connecting to the API: ${error.message}. Check browser console for CORS issues.`);
    } finally {
      setIsLoading(false);
    }
  };

  const isSuccessMessage = message.includes('✅ Success!');

  return (
    // 'page-layout' centers the whole component on the screen
    <div className="page-layout">
        <div className="upload-container">
            <h2 className="title">Hybrid Search</h2>
            <p className="subtitle">Enter the file name to be indexed</p>
            
            <form onSubmit={handleSubmit} className="upload-form">
                <label htmlFor="fileName" className="form-label">
                    File Name:
                </label>
                <input
                    id="fileName"
                    type="text"
                    value={fileName}
                    onChange={(e) => setFileName(e.target.value)}
                    placeholder="e.g., file:///home/contoso/invoice.pdf"
                    required
                    disabled={isLoading}
                    className="form-input"
                />
                <button 
                    type="submit" 
                    disabled={isLoading || !fileName.trim()}
                    className="submit-button"
                >
                    {isLoading ? 'Sending...' : 'Send Upload Request'}
                </button>
            </form>
            
            {message && (
                <div className={`message-box ${isSuccessMessage ? 'success-box' : 'failure-box'}`}>
                    <pre className="message-text">
                        {message}
                    </pre>
                </div>
            )}
        </div>
    </div>
  );
}