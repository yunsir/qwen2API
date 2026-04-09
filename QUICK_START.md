# Quick Start Guide

## Prerequisites

The Qwen2API gateway requires you to provide your own Qwen accounts. No accounts are pre-configured.

## Setup Steps

1. **Start the system**
   ```bash
   python start.py
   ```
   - Backend runs on `http://localhost:7860`
   - Frontend runs on `http://localhost:5174`

2. **Add Qwen Accounts**
   - Open browser: `http://localhost:5174`
   - Navigate to **Accounts** tab
   - Click "Add Account" button
   - Enter your Qwen email and password (they are stored locally in `backend/data/accounts.json`)
   - System will auto-verify token validity

3. **Verify Setup**
   - Settings tab shows configured models and concurrency limits
   - Admin panel available at `http://localhost:7860/api/admin/settings`
   - Use `Authorization: Bearer admin` header

4. **Test API**
   ```bash
   curl http://localhost:7860/v1/chat/completions \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer admin" \
     -d '{
       "model": "qwen3.6-plus",
       "messages": [{"role": "user", "content": "Hello"}],
       "stream": false
     }'
   ```

## Expected Behavior

- **Without accounts**: System waits for available account (times out after 60s)
- **With accounts**: Responses come back within seconds for text, ~10-30s for tool calling
- **Rate limiting**: Accounts are rotated automatically on rate limit
- **Account bans**: Automatically detected and account marked as invalid

## Data Files

- `backend/data/accounts.json` - User accounts (email, token, status)
- `backend/data/users.json` - API users and quotas
- `backend/data/captures.json` - Chat history/captures for debugging

All files are created automatically on first run.
