from flask import Flask, request, jsonify
from flask_cors import CORS
import csv
import os
import requests
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class Account:
    username: str
    password: str
    api_token: str
    daily_limit: int = 1000
    current_count: int = 0
    last_reset: str = ""

@dataclass
class URLResult:
    original_url: str
    short_url: str
    account_used: str
    timestamp: str
    success: bool
    error_message: str = ""

class BitlyAPIManager:
    def __init__(self, accounts: List[Account]):
        self.accounts = accounts
        self.current_account_index = 0
        self.base_url = "https://api-ssl.bitly.com/v4"
        logging.info(f"Initialized with {len(accounts)} accounts")
    
    def shorten_url(self, long_url: str) -> URLResult:
        max_retries = 3
        
        for attempt in range(max_retries):
            account = self._get_available_account()
            
            if not account:
                return URLResult(
                    original_url=long_url,
                    short_url="",
                    account_used="",
                    timestamp=datetime.now().isoformat(),
                    success=False,
                    error_message="No available accounts"
                )
            
            headers = {
                'Authorization': f'Bearer {account.api_token}',
                'Content-Type': 'application/json',
            }
            
            data = {'long_url': long_url, 'domain': 'bit.ly'}
            
            try:
                response = requests.post(f"{self.base_url}/shorten", headers=headers, json=data, timeout=15)
                
                if response.status_code in [200, 201]:
                    result = response.json()
                    account.current_count += 1
                    logging.info(f"✓ Shortened: {long_url[:50]}... -> {result['link']}")
                    
                    return URLResult(
                        original_url=long_url,
                        short_url=result['link'],
                        account_used=account.username,
                        timestamp=datetime.now().isoformat(),
                        success=True
                    )
                
                elif response.status_code == 429:
                    logging.warning(f"Rate limited, trying next account...")
                    continue
                
                else:
                    error_msg = f"API Error: {response.status_code}"
                    if attempt < max_retries - 1:
                        continue
                    
                    return URLResult(
                        original_url=long_url,
                        short_url="",
                        account_used=account.username,
                        timestamp=datetime.now().isoformat(),
                        success=False,
                        error_message=error_msg
                    )
            
            except Exception as e:
                if attempt < max_retries - 1:
                    continue
                return URLResult(
                    original_url=long_url,
                    short_url="",
                    account_used=account.username,
                    timestamp=datetime.now().isoformat(),
                    success=False,
                    error_message=str(e)
                )
        
        return URLResult(
            original_url=long_url,
            short_url="",
            account_used="",
            timestamp=datetime.now().isoformat(),
            success=False,
            error_message="Max retries exceeded"
        )
    
    def _get_available_account(self) -> Optional[Account]:
        if not self.accounts:
            return None
        
        for _ in range(len(self.accounts)):
            account = self.accounts[self.current_account_index]
            self.current_account_index = (self.current_account_index + 1) % len(self.accounts)
            
            today = datetime.now().strftime('%Y-%m-%d')
            if account.last_reset != today:
                account.current_count = 0
                account.last_reset = today
            
            if account.current_count < account.daily_limit and account.api_token:
                return account
        
        return None

def load_accounts_from_csv(csv_file: str = 'accounts.csv') -> List[Account]:
    accounts = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                username = row.get('username') or row.get('email') or row.get('Username') or row.get('Email')
                password = row.get('password') or row.get('Password')
                api_token = row.get('api_token') or row.get('token') or row.get('API Token') or row.get('Token')
                
                if username and api_token:
                    accounts.append(Account(
                        username=username.strip(),
                        password=password.strip() if password else "",
                        api_token=api_token.strip(),
                        daily_limit=int(row.get('daily_limit', 1000)),
                        current_count=int(row.get('current_count', 0)),
                        last_reset=row.get('last_reset', '')
                    ))
                    logging.info(f"Loaded account: {username}")
        
        logging.info(f"Total accounts loaded: {len(accounts)}")
        return accounts
    
    except FileNotFoundError:
        logging.error(f"CSV file not found!")
        return []
    except Exception as e:
        logging.error(f"Error loading accounts: {str(e)}")
        return []

accounts = load_accounts_from_csv('accounts.csv')
api_manager = BitlyAPIManager(accounts) if accounts else None

@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "service": "URL Shortener API",
        "accounts_loaded": len(accounts) if accounts else 0,
        "endpoints": {
            "POST /api/shorten": "Shorten single URL",
            "POST /api/shorten-bulk": "Shorten multiple URLs",
            "GET /api/health": "Health check"
        }
    })

@app.route('/api/shorten', methods=['POST'])
def shorten():
    if not api_manager or not accounts:
        return jsonify({"success": False, "error": "No accounts configured"}), 500
    
    try:
        data = request.json
        url = data.get('url', '').strip()
        
        if not url:
            return jsonify({"success": False, "error": "URL is required"}), 400
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        result = api_manager.shorten_url(url)
        
        return jsonify({
            "success": result.success,
            "original_url": result.original_url,
            "short_url": result.short_url,
            "account_used": result.account_used,
            "error": result.error_message if not result.success else None
        })
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/shorten-bulk', methods=['POST'])
def shorten_bulk():
    if not api_manager or not accounts:
        return jsonify({"success": False, "error": "No accounts configured"}), 500
    
    try:
        data = request.json
        urls = data.get('urls', [])
        
        if not urls:
            return jsonify({"success": False, "error": "URLs required"}), 400
        
        results = []
        for url in urls:
            url = url.strip()
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            result = api_manager.shorten_url(url)
            results.append({
                "success": result.success,
                "original_url": result.original_url,
                "short_url": result.short_url,
                "account_used": result.account_used,
                "error": result.error_message if not result.success else None
            })
        
        successful = sum(1 for r in results if r['success'])
        
        return jsonify({
            "success": True,
            "total": len(results),
            "successful": successful,
            "failed": len(results) - successful,
            "results": results
        })
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/health')
def health():
    return jsonify({
        "status": "healthy",
        "accounts_loaded": len(accounts) if accounts else 0,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
