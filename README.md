
# Strava Analysis Project

This project analyzes workout data, focusing on activities like running and cycling.

## Project Structure

```
.
├── database
│   ├── database.db              # Database
├── requirements.txt             # Python dependencies
├── src
├── main.py                      
```

## Setup

### Prerequisites
- Python 3.12 or higher
- `pip` (Python package manager)
- A Strava account with API access

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/strava-analysis.git
   cd strava-analysis
   ```

2. Set up a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. (Optional) Set up the project in editable mode for development:
   ```bash
   pip install -e .
   ```

### Setting Up the `.env` File

Create a `.env` file in the root of the project and add the following content, replacing the values with your own Strava credentials:

```
STRAVA_CLIENT_ID=your_client_id
STRAVA_CLIENT_SECRET=your_client_secret
STRAVA_REFRESH_TOKEN=your_refresh_token
STRAVA_ATHLETE_ID=your_athlete_id
```

## Usage

### Running the Analysis

To run the main script, execute:

```bash
python main.py
```

## Strava API Client

The project includes a client for interacting with the Strava API:

- **Strava Client**: Fetches activity data from Strava.


## Database

The project uses SQLite databases to store activity, gear, and weather data. You can explore the database schema and write custom queries using the `db_manager.py` and `queries.py` modules.
