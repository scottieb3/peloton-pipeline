# Token Exchange Script

A simple Python script for handling OAuth 2.0 token exchange operations.

## Features

- Client credentials flow
- Authorization code flow  
- Refresh token flow
- Token expiration checking
- Authenticated API requests

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from token_exchange import TokenExchange

# Initialize
exchange = TokenExchange("https://api.example.com")

# Client credentials flow
token_data = exchange.authenticate_client_credentials(
    "your_client_id", 
    "your_client_secret"
)

# Make authenticated request
result = exchange.make_authenticated_request("GET", "/api/endpoint")
```

## Configuration

Update the `config` dictionary in `main()` with your actual API credentials and endpoints.

## Security Notes

- Never commit client secrets to version control
- Use environment variables for sensitive data in production
- Implement proper error handling for production use
