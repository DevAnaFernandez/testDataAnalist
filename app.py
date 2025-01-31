from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
import psycopg2
from cryptography.fernet import Fernet

app = Flask(__name__)

# Configura la base de datos (Google Cloud SQL - PostgreSQL)
DATABASE_URL = "postgresql://postgres:testdataanalist2025@34.122.51.97:5432/postgres"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()
metadata.reflect(bind=engine)
users_table = metadata.tables['users']

# Genera una clave para cifrar datos
key = Fernet.generate_key()
cipher_suite = Fernet(key)

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        # Recibir archivo CSV desde la solicitud
        file = request.files['file']
        df = pd.read_csv(file)
        
        # Reemplazar valores vacíos o NaN con None (NULL en PostgreSQL)
        df = df.where(pd.notna(df), None)

        # Convertir tipos de datos
        df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        df['gender'] = df['gender'].astype(str).str[0]  # Guardar solo la primera letra del género

        # Manejo de valores nulos
        df.fillna({'slug': '', 'video': '', 'email': '', 'gender': 'N', 'identification_number': ''}, inplace=True)

        # Eliminar duplicados basados en `email`
        df.drop_duplicates(subset=['email'], inplace=True)

        # Cifrar datos sensibles (solo los campos de texto como "email")
        df['name'] = df['name'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode() if pd.notna(x) and x else None)
        df['email'] = df['email'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode() if pd.notna(x) and x else None)
        print(df[['name', 'email']].head())

        # Insertar datos en lotes con manejo de conflictos
        batch_size = 3000  # Tamaño de lote
        
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start+batch_size].to_dict(orient='records')
            
            for row in batch:
                stmt = insert(users_table).values(row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],  # Clave primaria
                    set_={col: stmt.excluded[col] for col in row.keys()}  # Actualizar en caso de conflicto
                )
                session.execute(stmt)
        
        session.commit()
        return jsonify({"message": "Datos cargados exitosamente"})
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

if __name__ == "__main__":
    app.run(debug=True)
