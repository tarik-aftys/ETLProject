import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = "5432"

DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

def ingest_api_data():
    start_date = "2016-09-01"
    end_date = "2018-10-31"
    
    url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=BRL&to=USD"
    print(f"📡 Appel de l'API REST: {url}")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        rates = data.get("rates", {})
        
        df = pd.DataFrame.from_dict(rates, orient='index').reset_index()
        df.columns = ["date", "usd_rate"]
        
        df['date'] = pd.to_datetime(df['date'])
        
        print(f"✅ {len(df)} taux de change historiques récupérés. Début du chargement {start_date} - {end_date}...")
        
        table_name = "exchange_rates"
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                schema="raw",      
                if_exists="replace", 
                index=False        
            )
            print(f"🎉 Table 'raw.{table_name}' chargée avec succès dans PostgreSQL !")
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion SQL : {e}")
            
    else:
        print(f"❌ Erreur lors de l'appel API. Code de statut {response.status_code}")

if __name__ == "__main__":
    ingest_api_data()