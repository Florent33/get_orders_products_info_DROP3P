from logger import Logger
from database import DatabaseSQL
from api_requests import RequestsAPI

db = DatabaseSQL()
api = RequestsAPI
log = Logger.write_log

def process_orders(conn, access_tk):
    total_inserted_orders = 0
    collected_product_ids = set() # Pour éviter les doublons
    all_orders_pages = api.get_orders_info(access_tk)

    # Récupération des informations commandes paginées depuis /orders
    all_orders_pages = api.get_orders_info(access_tk)

    if all_orders_pages:
        for orders in all_orders_pages:
            log(f"📦 {len(orders)} commandes récupérées sur cette page")

            for order in orders:
                order_id = order.get('orderId', None)

                # Récupération de l'orderId par commande
                order_id_value = api.get_order_id_cust_ref(access_tk, order_id)
                if not order_id_value:
                    log(f"⚠️ Impossible de récupérer les détails pour la commande {order_id}")
                    continue

                # Récupération du siteId associé au customerRef
                customer_reference = order.get("customer", {}).get("reference", None)
                if customer_reference != None:
                    site_id = db.get_site_id(conn, customer_reference)
                else:
                    log(f"⚠️ Pas de référence client pour la commande {order_id}")
                    continue

                # Récupération des produit(s) de la commande
                lines = order.get("lines", [])
                for line in lines:
                    product_id = line.get("offer", {}).get("productId", None)
                    if product_id:
                        collected_product_ids.add(product_id)
                    else:
                        log(f"⚠️ Aucun productId trouvé pour la commande {order_id}")

            # Insertion des données commandes dans TM_MAD_DROPFR_ventes
            total_inserted_orders += db.insert_orders_data(conn, orders, site_id)
    else:
        log("❌ Aucune commande récupérée")
        return total_inserted_orders
    
    return total_inserted_orders, list(collected_product_ids)

def process_products_categories_attributes(conn, access_tk, product_ids):
    total_inserted_products = 0
    total_inserted_categories = 0
    total_inserted_attributes = 0

    for product_id in product_ids:
        log(f"🔍 Récupération des informations du produit : {product_id}")
        
        product_info  = api.get_product_info(access_tk, product_id)

        if not product_info:
            log(f"⚠️ Aucune info trouvée pour le produit {product_id}")
            continue

        total_inserted_products += db.insert_products_data(conn, [product_info])

        # Récupération de category depuis /products/{productId}
        category_ref = product_info.get("category", None).strip()
        if not category_ref or len(category_ref) < 6:
            log("⚠️ Catégorie invalide ou manquante")
            continue

        # Extraction des catégories N1, N2 et N3
        category_levels = [category_ref[:i] for i in (2, 4, 6) if len(category_ref) >= i]

        for level in category_levels:
            category_data = api.get_categories_info(access_tk, level)
            if category_data:
                total_inserted_categories += db.insert_categories_data(conn, [category_data], access_tk)
            else:
                log(f"⚠️ Aucune information trouvée pour la catégorie {level}")

        total_inserted_attributes += db.insert_attributes_data(conn, [product_info])
    
    return total_inserted_products, total_inserted_categories, total_inserted_attributes

def main():

    try:
        Logger.separator()
        log("🚀 Démarrage du processus...\n")

        # Connexion à la base de données
        conn, connected = db.get_db_connection()
        if not conn and connected:
            return

        # Récupération du token API
        access_tk = api.get_access_token()
        if not access_tk:
            return
        
        # Suppression des données des tables TM_MAD_DROPFR_ventes / TM_MAD_DROPFR_Products / TM_MAD_DROPFR_Categories / TM_MAD_DROPFR_Attributes
        db.delete_orders_data(conn)
        db.delete_products_data(conn)
        db.delete_categories_data(conn)
        db.delete_attributes_data(conn)
        
        # Traitement des données
        inserted_orders, product_ids = process_orders(conn, access_tk)
        inserted_products, inserted_categories, inserted_attributes = process_products_categories_attributes(conn, access_tk, product_ids)
            
        # Fermeture de la connexion
        conn.close()
        log("\n")
        log(f"✅ {inserted_orders} commandes insérées dans TM_MAD_DROPFR_ventes")
        log(f"✅ {inserted_products} produits insérés dans TM_MAD_DROPFR_Products")
        log(f"✅ {inserted_categories} catégories insérées dans TM_MAD_DROPFR_Categories")
        log(f"✅ {inserted_attributes} attributs insérés dans TM_MAD_DROPFR_Attributes\n")
        
        log("🔚 Fin du processus.")
        Logger.separator()

    except Exception as e:
        log(f"❌ Erreur inattendue : {e}")
        Logger.separator()

if __name__ == "__main__":
    main()