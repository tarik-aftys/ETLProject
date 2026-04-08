import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = "5432"

DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

RAW_DATA_DIR = os.path.join("data", "raw")

def ingest_data():
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    
    if not csv_files:
        print(f"❌ Aucun fichier CSV trouvé dans le dossier '{RAW_DATA_DIR}'.")
        print("Veuillez vérifier que l'archive Olist a bien été décompressée ici.")
        return

    print(f"🔍 {len(csv_files)} fichiers CSV trouvés. Début de l'ingestion...")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        table_name = file_name.replace("olist_", "").replace(".csv", "")
        
        print(f"⏳ Ingestion de '{file_name}' vers la table 'raw.{table_name}'...")
        
        chunk_iterator = pd.read_csv(file_path, chunksize=10000)
        
        try:
            for i, chunk in enumerate(chunk_iterator):
                action = 'replace' if i == 0 else 'append'
                
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw",      
                    if_exists=action,  
                    index=False        
                )
            print(f"  ✅ Table 'raw.{table_name}' chargée avec succès !")
        except Exception as e:
            print(f"  ❌ Erreur lors du chargement de {file_name} : {e}")

    print("🎉 Ingestion complète terminée !")

if __name__ == "__main__":
    ingest_data()
