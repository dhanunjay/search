'use client';

import { useState } from 'react';
// Assuming you import your styles in app/layout.jsx or app/layout.tsx
// e.g., import './styles.css'; 

const EXTERNAL_API_BASE_URL = 'http://localhost:8000/v1/query/documents:search';
const DEFAULT_LIMIT = 20; 

// Define the shape of a single search result item
interface SearchResult {
  title: string;
  snippet: string;
  link: string;
}

export default function HomePage() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[] | null>(null); 
  const [error, setError] = useState<string | null>(null); 
  const [isLoading, setIsLoading] = useState(false);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);
    setResults(null);

    if (!query.trim()) {
      setError('Please enter a search query.'); 
      setIsLoading(false);
      return;
    }

    const searchParams = new URLSearchParams({
      q: query.trim(),
      limit: DEFAULT_LIMIT.toString(), 
    });
    
    const apiUrl = `${EXTERNAL_API_BASE_URL}?${searchParams.toString()}`;

    try {
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        const errorData = await response.json();
        
        // 🎯 CRITICAL CHANGE: Extract 'detail' if available, otherwise fallback to generic messages
        const errorMessage = errorData.detail 
          ? `API Error (${response.status}): ${errorData.detail}`
          : `API Error (${response.status}): ${errorData.message || 'Unknown server error'}`;
        
        throw new Error(errorMessage);
      }

      const responseData = await response.json();
      const fetchedResults = responseData.result || [];

      if (Array.isArray(fetchedResults)) {
        setResults(fetchedResults);
      } else {
        throw new Error('Invalid response format: "result" field is not an array.');
      }

    } catch (err) {
      setError(`${err instanceof Error ? err.message : 'An unknown error occurred'}`);
      setResults([]);
    } finally {
      setIsLoading(false);
    }
  };

  const renderResults = () => {
    if (isLoading) {
      return <p className="message">Searching...</p>;
    }
    if (error) {
      return <p className="message error-message">Error: {error}</p>;
    }
    if (results === null) {
      return null;
    }
    if (results.length === 0) {
      return <p className="message">No results found for "{query}".</p>;
    }

    return (
      <div className="results-area">
        <h3>Found {results.length} Documents (Limit: {DEFAULT_LIMIT})</h3>
        <ul className="results-list">
          {results.map((doc, index) => (
            <li key={doc.link || index} className="result-item">
              <a 
                href={doc.link} 
                target="_blank" 
                rel="noopener noreferrer" 
                className="result-link"
              >
                {doc.title || `Document ${index + 1}`}
              </a>
              <p className="result-url">
                 {doc.link}
              </p>
              <p className="result-snippet">
                {doc.snippet || 'No snippet available.'}
              </p>
            </li>
          ))}
        </ul>
      </div>
    );
  };

  return (
    <div className="container">
      <div className="header-container">
        <h1 className="title">
          Hybrid Search
        </h1>
        <form onSubmit={handleSearch}>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search documents..."
            disabled={isLoading}
            className="search-input"
          />
          <button 
            type="submit" 
            disabled={isLoading || !query.trim()}
            className="search-button"
          >
            {isLoading ? 'Searching...' : 'Search'}
          </button>
        </form>
      </div>

      {renderResults()}
    </div>
  );
}