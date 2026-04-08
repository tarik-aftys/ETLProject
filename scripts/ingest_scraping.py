import os
import requests
from bs4 import BeautifulSoup
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

def ingest_wikipedia_data():
    url = "https://en.wikipedia.org/wiki/Federative_units_of_Brazil"
    print(f"🕸️ Scraping de la page web: {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Échec du scraping. Code HTTP: {response.status_code}")
        return
        
    soup = BeautifulSoup(response.text, 'html.parser')
    
    table = soup.find('table', {'class': 'wikitable sortable'})
    
    if not table:
        print("❌ Impossible de trouver le tableau HTML cible.")
        return

    data = []
    
    rows = table.find_all('tr')[2:] 
    
    for row in rows:
        cols = row.find_all(['td', 'th'])
        
        if len(cols) >= 6:
            state_name = cols[0].text.strip()
            state_code = cols[1].text.strip()
            
            population_str = cols[5].text.strip().replace(',', '')
            
            if population_str.isdigit():
                data.append({
                    "state_name": state_name,
                    "state_code": state_code,
                    "population": int(population_str)
                })

    df = pd.DataFrame(data)
    
    print(f"✅ Scraping réussi : {len(df)} États trouvés avec leur population. Début du chargement...")
    
    table_name = "states_population"
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

if __name__ == "__main__":
    ingest_wikipedia_data()