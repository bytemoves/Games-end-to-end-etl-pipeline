
import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
# from dotenv import load_dotenv

# import pyscopg2

# load_dotenv()

# Database connection parameters
# db_config = {
#     "host": os.getenv("PGHOST"),
#     "database": os.getenv("PGDATABASE"),
#     "user": os.getenv("PGUSER"),
#     "password": os.getenv("PGPASSWORD"),
#     "sslmode": "require" if "neon.tech" in os.getenv("PGHOST", "") else "disable"
# }
DATABASE_URL=os.getenv("DATABASE_URL")


def get_deals_data():
	url = "https://cheapshark-game-deals.p.rapidapi.com/games"

	querystring = {"title":"batman","exact":"0","limit":"60"}

	headers = {
		"x-rapidapi-key": os.getenv("RAPID_API_KEY"),
		"x-rapidapi-host": "cheapshark-game-deals.p.rapidapi.com"
	}
	

	response = requests.get(url, headers=headers, params=querystring)

	return response.json()

def create_database_schema():
    # create table if it does nor exist
    conn = None
    try:
        # conn = psycopg2.connect(**db_config)
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS game_deals (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(255),
                steam_app_id VARCHAR(255),
                cheapest DECIMAL(10, 2),
                cheapest_deal_id TEXT,
                external TEXT,
                internal_name VARCHAR(255),
                thumb_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        print("Table created or already exists")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            
def load_data_to_database(deals):
    if not deals:
        print("No deals data to insert")
        return
    
    # conn = None
    # try:
    # conn = psycopg2.connect(**db_config)
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
        
    values = []
    for deal in deals:
        values.append((
            deal.get('gameID'),
                deal.get('steamAppID'),
                float(deal.get('cheapest')) if deal.get('cheapest') else None,
                deal.get('cheapestDealID'),
                deal.get('external'),
                deal.get('internalName'),
                deal.get('thumb')
			
		))
        
    #insert data
    execute_values(
		cursor,
			"""
   INSERT INTO game_deals (
                game_id, steam_app_id, cheapest, cheapest_deal_id, 
                external, internal_name, thumb_url
            ) VALUES %s
   
   """,
   values
	)
    
    conn.commit()
    # print(f"Successfully inserted {len(values)} deals")
    # except Exception as e:
    #     if conn:
    #         conn.rollback()
    #     print(f"Error inserting deals data: {e}")
    # finally:
    #     if conn is not None:
    #         cursor.close()
    #         conn.close()
            
def main():
    """Main ETL function"""
    # Create table if not exists
    create_database_schema()
    
    # Extract: Fetch data from API
    deals_data = get_deals_data()
    
    # Transform and Load: Insert data into PostgreSQL
    load_data_to_database(deals_data)
    
    print("ETL process completed")

if __name__ == "__main__":
    main()