# Game Deals Data Pipeline

This project fetches game deals data from cheapshark-game-deals API, normalizes the data, and stores it in a **PostgreSQL** database hosted on **Neon**. The stored data is then visualized using **Power BI**.

## Features

- Fetches game deals data from an API
- Cleans and normalizes the JSON response
- Stores the data in a **PostgreSQL** database
- Uses **Power BI** for visualization
- Implements **bulk inserts** for efficiency


## Technologies Used

- **Python** (for API calls and database handling)
- **PostgreSQL** (Neon database)
- **psycopg2** (PostgreSQL adapter for Python)
- **Power BI** (Data visualization)

## Setup Instructions
git clone https://github.com/your-username/game-deals-data-pipeline.git
cd game-deals-data-pipeline

# Install dependencies
pip install -r requirements.txt



