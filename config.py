import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis un fichier .env
load_dotenv()

class Config:

    # Configuration de la base de données (PRODUCTION)
    DB_CONFIG = {
        "SERVER": os.getenv("DB_SERVER"),
        "DATABASE": os.getenv("DB_DATABASE"),
        "USERNAME": os.getenv("DB_USERNAME"),
        "PASSWORD": os.getenv("DB_PASSWORD")
    }

    # Paramètres de connexion à l'API CV Order REST V2 (PRODUCTION)
    API_CONFIG = {
        "TOKEN_URL": os.getenv("API_TOKEN_URL"),
        "CALL_URL": os.getenv("API_CALL_URL"),
        "CLIENT_ID": os.getenv("API_CLIENT_ID"),
        "CLIENT_SECRET": os.getenv("API_CLIENT_SECRET"),
        "GRANT_TYPE": os.getenv("API_GRANT_TYPE")
    }

    # Paramètres de connexion à l'API CV Order REST V2 (PREPRODUCTION)
    # API_CONFIG = {
    #     "TOKEN_URL": os.getenv("API_TOKEN_URL_PP"),
    #     "CALL_URL": os.getenv("API_CALL_URL_PP"),
    #     "CLIENT_ID": os.getenv("API_CLIENT_ID_PP"),
    #     "CLIENT_SECRET": os.getenv("API_CLIENT_SECRET_PP"),
    #     "GRANT_TYPE": os.getenv("API_GRANT_TYPE_PP")
    # }