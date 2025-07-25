import os
from datetime import datetime

# Dossier et fichier de logs
LOGS_DIR = "Logs"
LOG_FILE = os.path.join(LOGS_DIR, "log.txt")

class Logger:

    # Crée le dossier Logs s'il n'existe pas
    def init_logs():
        if not os.path.exists(LOGS_DIR):  
            try:
                os.makedirs(LOGS_DIR)
            except OSError as e:
                print(f"❌ Erreur lors de la création du dossier Logs : {e}")
                return False
        return True

    # Écrit un message dans le fichier de logs avec timestamp
    def write_log(message, separator=False):
        if not os.path.exists(LOGS_DIR):
            Logger.init_logs()

        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"

        if separator:
            log_message = "-" * 50 + log_message

        try:
            with open(LOG_FILE, "a", encoding="utf-8") as log_file:
                log_file.write(log_message)
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture du log : {e}")

    # Ajoute une ligne de séparation pour distinguer chaque exécution
    def separator():
        if not os.path.exists(LOGS_DIR):
            Logger.init_logs()
            
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as log_file:
                log_file.write("-" * 50 + "\n")
        except Exception as e:
            print(f"❌ Erreur lors de l'ajout du séparateur : {e}")