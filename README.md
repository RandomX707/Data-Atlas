# Data Atlas

A metadata management platform for exploring, profiling, and governing your data assets.

## Features

- **Connections** — Connect to databases and data sources
- **Data Profiling** — Analyze data quality and statistics
- **Schema Explorer** — Browse and understand table structures
- **Data Lineage** — Track data flow and dependencies
- **Quality Checks** — Define and run data quality validations
- **Metadata Catalog** — Centralized metadata repository
- **Audit Logs** — Track all platform activity
- **AI-Powered Explore** — Natural language data exploration

## Tech Stack

- **Frontend**: React 18, Vite, D3.js
- **Backend**: Node.js, Express
- **Database**: PostgreSQL
- **Auth**: JWT

## Getting Started

### Prerequisites

- Node.js 18+
- PostgreSQL 14+

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/RandomX707/Data-Atlas.git
   cd Data-Atlas
   ```

2. Configure the backend environment:
   ```bash
   cp backend/.env.example backend/.env
   # Edit backend/.env with your database credentials and secrets
   ```

3. Install dependencies:
   ```bash
   cd backend && npm install
   cd ../frontend && npm install
   ```

4. Start the backend:
   ```bash
   cd backend && npm start
   ```

5. Start the frontend:
   ```bash
   cd frontend && npm run dev
   ```

6. Open your browser at `http://localhost:5175`

## Environment Variables

See `backend/.env.example` for all required variables.

| Variable | Description |
|----------|-------------|
| `PG_HOST` | PostgreSQL host |
| `PG_DATABASE` | Database name |
| `PG_USER` | Database user |
| `PG_PASSWORD` | Database password |
| `JWT_SECRET` | Secret key for JWT tokens |
| `ENCRYPTION_KEY` | 32-byte hex key for encrypting credentials |
| `LLM_API_URL` | LLM API endpoint (for AI features) |
| `LLM_API_KEY` | LLM API key |
