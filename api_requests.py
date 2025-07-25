import requests
import re
import time

from config import Config
from logger import Logger

api_config = Config.API_CONFIG
log = Logger.write_log

class RequestsAPI:

    # Génération du token
    def get_access_token():
        url = api_config["TOKEN_URL"]
        payload = f'client_id={api_config["CLIENT_ID"]}&client_secret={api_config["CLIENT_SECRET"]}&grant_type={api_config["GRANT_TYPE"]}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(url, headers=headers, data=payload)
            time.sleep(1)

            if response.status_code == 200:
                access_token = response.json().get('access_token')
                if access_token:
                    log("✅ Token généré avec succès\n")
                    return access_token
                else:
                    log("❌ Le token d'accès n'a pas été trouvé dans la réponse")
                    return None
            else:
                token_error = response.json().get('error', 'Erreur inconnue')
                log(f"❌ Erreur {response.status_code} lors de l'obtention du token : {token_error}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir le token : {str(e)}")
            Logger.separator()
            return None
    
    ### SECTION ORDERS ###
    # Récupération les informations commandes par page depuis la route /orders
    def get_orders_info(access_token):
    
        url = f"{api_config['CALL_URL']}/orders"
        headers = {'Authorization': f'Bearer {access_token}'}
        page_index = 1
        page_size = 100
        createdAtMin = "2025-01-01T01:00:00.00"

        while True:
            paginated_url = f"{url}?pageIndex={page_index}&pageSize={page_size}&createdAtMin={createdAtMin}"
            
            try:
                log(f"📜 Récupération des commandes - Page {page_index}")
                response = requests.get(paginated_url, headers=headers)
                time.sleep(1)

                if response.status_code == 200:
                    data = response.json()
                    orders = data.get('items', [])

                    if isinstance(orders, list) and orders:
                        yield orders
                        page_index += 1
                        log(f"➡️ Passage à la page suivante : Page {page_index}")
                    else:
                        log(f"🟡 Aucune commande trouvée sur la page {page_index}")
                        log("🔚 Fin de la récupération des commandes.\n")
                        break
                else:
                    log(f"❌ Erreur {response.status_code} : {response.text}")
                    break
            
            except requests.exceptions.RequestException as e:
                log(f"❌ Erreur lors de la requête API : {str(e)}")
                break

    # Récupération de l'orderId et customer_reference depuis la route /order/{order_id}
    def get_order_id_cust_ref(access_token, order_id):
        url = f"{api_config['CALL_URL']}/orders/{order_id}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()

                # Vérifier si "customer" et "reference" existent dans la réponse
                customer_reference = data.get("customer", {}).get("reference")

                # Récupération du ou des productId par order
                lines = data.get("lines", [])
                for line in lines:
                    product_id = line.get("offer", {}).get("productId")

                if customer_reference:
                    log(f"✅ Référence client trouvée : {customer_reference}")
                else:
                    log("⚠️ Aucune référence client trouvée")

                if product_id:
                    log(f"✅ productId trouvé : {product_id}")
                else:
                    log("⚠️ Aucune référence client trouvée")

                return data
            
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations de la commande {str(e)}")
            Logger.separator()
            return None   
    
    # Récupération de toutes les informations par produit depuis la route /products/{productId}
    def get_product_info(access_token, product_id):
        url = f"{api_config['CALL_URL']}/products/{product_id}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            time.sleep(1)

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations de la commande {str(e)}")
            Logger.separator()
            return None

    ### SECTION CATEGORY ###
    # Récupération des informations catégorie par produit depuis la route /categories/{categoryReference}
    def get_categories_info(access_token, category_reference):
        url = f"{api_config['CALL_URL']}/categories/{category_reference}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            time.sleep(1)

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations de la commande {str(e)}")
            Logger.separator()
            return None