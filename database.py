import pymssql

from typing import Optional, Tuple
from config import Config
from logger import Logger
from api_requests import RequestsAPI
from datetime import datetime

# Charger les variables d'environnement depuis un fichier .env
db_config = Config.DB_CONFIG
log = Logger.write_log
api = RequestsAPI

class DatabaseSQL:

    def __init__(self):
        pass

    # Connexion à la BDD
    def get_db_connection(self) -> Tuple[Optional[pymssql.Connection], bool]:
        conn = None
        connected = False
        try:
            # Récupérer les valeurs du dictionnaire sans paramètres interdits et connexion à la base de données
            conn = pymssql.connect(
                server = str(db_config.get("SERVER", "")),
                user = str(db_config.get("USERNAME", "")),
                password = str(db_config.get("PASSWORD", "")),
                database = str(db_config.get("DATABASE", ""))
            )

            try:
                # Vérification de la connexion
                cur = conn.cursor()
                cur.execute("SELECT @@VERSION")
                version = cur.fetchone()[0].split("\n")[0].strip()
                log(f"✅ Connexion Data Warehouse établie. Version : {version}")
                connected = True

            except (pymssql.DatabaseError, pymssql.InterfaceError) as e:
                log(f"⛔️ Erreur de connexion au Data Warehouse : {e}")
                conn.close()
                conn = None

        except Exception as e:
            log(f"⛔️ Échec de connexion au Data Warehouse : {e}")

        finally:
            return conn, connected
    
    ### SECTION ORDERS ###
    # Suppression des données de la table TM_MAD_DROPFR_ventes
    def delete_orders_data(self, conn):
        try:
            cursor = conn.cursor()

            # Vérifier si la table contient des données
            query_count_orders = "SELECT COUNT(*) FROM TM_MAD_DROPFR_ventes"
            cursor.execute(query_count_orders)
            order_count = cursor.fetchone()[0]

            deleted_rows = 0

            if order_count> 0:
                query_delete_orders = "DELETE FROM TM_MAD_DROPFR_ventes"
                cursor.execute(query_delete_orders)
                deleted_rows = cursor.rowcount
                conn.commit() # Valider la suppression
                log(f"✅ {deleted_rows} commandes supprimées de TM_MAD_DROPFR_ventes")
            else:
                log("ℹ️ Aucune commande à supprimer dans TM_MAD_DROPFR_ventes")

            return deleted_rows

        except Exception as e:
            log(f"❌ Erreur lors de la suppression des commandes : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Récupération du siteId pour l'insérer dans la table TM_MAD_DROPFR_ventes
    def get_site_id(self, conn, customer_reference):
        try:
            cursor = conn.cursor()
            query_site_id = "SELECT siteID FROM TM_MAD_SiteID (nolock) WHERE ID_CDS = %s"
            cursor.execute(query_site_id, (customer_reference),)
            row = cursor.fetchone()
            site_id = row[0] if row else 0
            if site_id:
                log(f"📍 SiteID récupéré pour cette commande : {site_id}")
            else:
                log(f"⚠️ Aucun siteID trouvé pour cette commande")
            return site_id
        
        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des commandes : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Insérer les informations commandes dans la table TM_MAD_DROPFR_ventes
    def insert_orders_data(self, conn, orders, site_id):
        try:
            cursor = conn.cursor()

            query_check_exists = """
                SELECT 1 FROM TM_MAD_DROPFR_ventes
                WHERE orderId = %(orderId)s
                AND productId = %(productId)s
                AND siteId = %(siteId)s
            """
            
            query_insert_orders = """
                INSERT INTO TM_MAD_DROPFR_ventes (
                    orderId, reference, sellerid, customerReference, siteId, companyName, purchasedAt, updatedAt, createdAt, shippedAtMax, Status,
                    offerid, productId, unitSalesPrice, shippingCost, comm_amountWithoutVat, comm_rate, promisedAtMin, promisedAtMax,
                    parcelNumber, carrierName, trackingUrl
                )
                VALUES (
                    %(orderId)s, %(reference)s, %(sellerid)s, %(customerReference)s, %(siteId)s, %(companyName)s, %(purchasedAt)s, %(updatedAt)s, %(createdAt)s, %(shippedAtMax)s, %(status)s,
                    %(offerid)s, %(productId)s, %(unitSalesPrice)s, %(shippingCost)s, %(comm_amountWithoutVat)s, %(comm_rate)s, %(promisedAtMin)s, %(promisedAtMax)s,
                    %(parcelNumber)s, %(carrierName)s, %(trackingUrl)s
                )
            """

            insert_count = 0

            for order in orders:
                order_id = order.get("orderId", None)
                if not order_id:
                    log("⚠️ orderId manquant, insertion ignorée")
                    continue                

                common_data = {
                    "orderId": order_id,
                    "reference": order.get("reference", None),
                    "sellerid": order.get("seller", {}).get("id", None),
                    "customerReference": order.get("customer", {}).get("reference", None),
                    "siteId": site_id,
                    "companyName": order.get("billingAddress", {}).get("companyName", None),
                    "purchasedAt": order.get("purchasedAt", None),
                    "updatedAt": order.get("updatedAt", None),
                    "createdAt": order.get("createdAt", None),
                    "shippedAtMax": order.get("shippedAtMax", None),
                    "status": order.get("status", None)
                }

                lines = order.get("lines", [])
                if not lines:
                    lines = [{}]

                for line in lines:
                    line_data = {
                        "offerid": line.get("offer", {}).get("id", None),
                        "productId": line.get("offer", {}).get("productId", None),
                        "unitSalesPrice": line.get("offerPrice", {}).get("unitSalesPrice", 0.0),
                        "shippingCost": line.get("offerPrice", {}).get("shippingCost", 0.0),
                        "comm_amountWithoutVat": line.get("offerPrice", {}).get("commission", {}).get("amountWithoutVat", 0.0),
                        "comm_rate": line.get("offerPrice", {}).get("commission", {}).get("rate", 0.0),
                        "promisedAtMin": line.get("delivery", {}).get("promisedAtMin", None),
                        "promisedAtMax": line.get("delivery", {}).get("promisedAtMax", None),
                    }

                    parcels = line.get("parcels", [])
                    if not parcels:
                        parcels = [{}]

                    # Savoir s'il y a au moins une ou deux informations parcels par commande
                    if len(parcels) > 1:
                        parcel_number = ";".join(p.get("parcelNumber", None) for p in parcels)
                        carrier_name = ";".join(p.get("carrierName", None) for p in parcels)
                        tracking_url = ";".join(p.get("trackingUrl", None) for p in parcels)
                        
                        parcel_data = {
                            "parcelNumber": parcel_number,
                            "carrierName": carrier_name,
                            "trackingUrl": tracking_url
                        }

                        full_data = {**common_data, **line_data, **parcel_data}

                        # Vérification doublon
                        cursor.execute(query_check_exists, full_data)
                        if cursor.fetchone():
                            log(f"⚠️ Doublon ignoré pour orderId={order_id}, productId={full_data['productId']}, status={full_data['status']}, siteId={full_data['siteId']}")
                            continue

                        log(f"📝 Insertion de la commande {order_id} dans TM_MAD_DROPFR_ventes")
                        cursor.execute(query_insert_orders, full_data)
                        insert_count += 1
                    else:
                        for parcel in parcels:
                            parcel_data = {
                                "parcelNumber": parcel.get("parcelNumber", None),
                                "carrierName": parcel.get("carrierName", None),
                                "trackingUrl": parcel.get("trackingUrl", None),
                            }

                            full_data = {**common_data, **line_data, **parcel_data}

                            cursor.execute(query_check_exists, full_data)
                            if cursor.fetchone():
                                log(f"⚠️ Doublon ignoré pour orderId={order_id}, productId={full_data['productId']}, status={full_data['status']}, siteId={full_data['siteId']}")
                                continue

                            log(f"📝 Insertion de la commande {order_id} dans TM_MAD_DROPFR_ventes")                    
                            cursor.execute(query_insert_orders, full_data)
                            insert_count += 1                

            conn.commit()

            return insert_count

        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des commandes : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    ### SECTION PRODUCTS ###
    # Suppression des données de la table TM_MAD_DROPFR_Products
    def delete_products_data(self, conn):
        try:
            cursor = conn.cursor()

            # Vérifier si la table contient des données
            query_count_products = "SELECT COUNT(*) FROM TM_MAD_DROPFR_Products"
            cursor.execute(query_count_products)
            product_count = cursor.fetchone()[0]

            deleted_rows = 0

            if product_count > 0:
                query_delete_products = "DELETE FROM TM_MAD_DROPFR_Products"
                cursor.execute(query_delete_products)
                deleted_rows = cursor.rowcount
                conn.commit()
                log(f"✅ {deleted_rows} produits supprimés de TM_MAD_DROPFR_Products")
            else :
                log("ℹ️ Aucun produit à supprimer dans TM_MAD_DROPFR_Products")
            
            return deleted_rows

        except Exception as e:
            log(f"❌ Erreur lors de la suppression des commandes : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()
    
    # Insertion des informations produits dans la table TM_MAD_DROPFR_Products
    def insert_products_data(self, conn, products):
        try:
            cursor = conn.cursor()

            query_insert_products = """
                INSERT INTO TM_MAD_DROPFR_Products (
                    ProductId, GENCOD, title, description, brand,
                    image1, image2, image3, image4, image5, image6,
                    dateCreation, dateUpdate, category
                )
                VALUES (
                    %(ProductId)s, %(GENCOD)s, %(title)s, %(description)s, %(brand)s,
                    %(image1)s, %(image2)s, %(image3)s, %(image4)s, %(image5)s, %(image6)s,
                    %(dateCreation)s, %(dateUpdate)s, %(category)s
                )
            """

            insert_count = 0

            for product in products:
                product_id = product.get("productId", None)
                if product_id == None:
                    log("⚠️ ProductId manquant, insertion ignorée")
                    continue
                
                common_data = {
                    "ProductId": product_id,
                    "GENCOD": product.get("gtin", None),
                    "title": product.get("title", None),
                    "description": product.get("description", None),
                    "brand": product.get("brand", {}).get("label", None),
                    "dateCreation": product.get("createdAt", None),
                    "dateUpdate": product.get("updatedAt", None),
                    "category": product.get("category", None)
                }

                 # Récupération des images (max 6, compléter avec None si besoin)
                images = [img.get("url", None) for img in product.get("images", [])][:6]
                images += [None] * (6 - len(images))  # Remplissage avec None si moins de 6 images

                full_data = {**common_data, **dict(zip(["image1", "image2", "image3", "image4", "image5", "image6"], images))}

                log(f"📝 Insertion du produit dans TM_MAD_DROPFR_Products")

                cursor.execute(query_insert_products, full_data)
                insert_count += 1                

            conn.commit()

            return insert_count

        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des produits : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    ### SECTION CATEGORIES ###
    # Suppression des données de la table TM_MAD_DROPFR_Categories
    def delete_categories_data(self, conn):
        try:
            cursor = conn.cursor()

            # Vérifier si la table contient des données
            query_count_categories = "SELECT COUNT(*) FROM TM_MAD_DROPFR_Categories"
            cursor.execute(query_count_categories)
            category_count = cursor.fetchone()[0]

            deleted_rows = 0

            if category_count > 0:
                query_delete_categories = "DELETE FROM TM_MAD_DROPFR_Categories"
                cursor.execute(query_delete_categories)
                deleted_rows = cursor.rowcount
                conn.commit()
                log(f"✅ {deleted_rows} catégories supprimées de TM_MAD_DROPFR_Categories")
            else:
                log("ℹ️ Aucune catégorie à supprimer dans TM_MAD_DROPFR_Categories")

            return deleted_rows

        except Exception as e:
            log(f"❌ Erreur lors de la suppression des catégories : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Insertion des informations catégories dans la table TM_MAD_DROPFR_Categories
    def insert_categories_data(self, conn, categories, access_tk):
        try:
            cursor = conn.cursor()

            query_insert_categories = """
                INSERT INTO TM_MAD_DROPFR_Categories (
                    categoryId1, label1, categoryId2, label2, categoryId3, label3, isActive
                )
                VALUES (
                    %(categoryId1)s, %(label1)s, %(categoryId2)s, %(label2)s, %(categoryId3)s, %(label3)s, %(isActive)s
                )
            """
            insert_count = 0

            for category in categories:
                if not isinstance(category, dict):
                    log("⚠️ Donnée de catégorie invalide, insertion ignorée")
                    continue

                category_reference = category.get("categoryReference", "").strip()
                is_active = category.get("isActive", False)

                # Vérifier si la catégorie est valide
                if not category_reference or len(category_reference) < 6:
                    continue

                # Extraction des niveaux de catégories selon la bonne hiérarchie
                category_levels = [category_reference[:i] for i in (2, 4, 6) if len(category_reference) >= i]

                # Vérifier qu'on a bien les 3 niveaux
                if len(category_levels) != 3:
                    continue

                categoryId1, categoryId2, categoryId3 = category_levels

                # Récupération des informations des catégories parents (N1 et N2) depuis la route /categories/{categoryReference}
                parent_data = [api.get_categories_info(access_tk, lvl) for lvl in category_levels]
                parent_labels = [p["label"] if p else None for p in parent_data]

                data_to_insert = {
                    "categoryId1": categoryId1, # 2 chiffres (Niveau bas)
                    "label1": parent_labels[0],  
                    "categoryId2": categoryId2, # 4 chiffres (Niveau intermédiaire)
                    "label2": parent_labels[1],  
                    "categoryId3": categoryId3, # 6 chiffres (Niveau haut)
                    "label3": parent_labels[2],  
                    "isActive": is_active
                }

                log(f"📝 Insertion de la catégorie hierarchisée {category_reference} dans TM_MAD_DROPFR_Categories")

                cursor.execute(query_insert_categories, data_to_insert)
                insert_count += 1

            conn.commit()
            return insert_count

        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des catégories : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    ### SECTION ATTRIBUTES ###
    # Suppression des données de la table TM_MAD_DROPFR_Categories
    def delete_attributes_data(self, conn):
        try:
            cursor = conn.cursor()

            # Vérifier si la table contient des données
            query_count_atttributes = "SELECT COUNT(*) FROM TM_MAD_DROPFR_Attributes"
            cursor.execute(query_count_atttributes)
            attributes_count = cursor.fetchone()[0]

            deleted_rows = 0

            if attributes_count > 0:
                query_delete_attributes = "DELETE FROM TM_MAD_DROPFR_Attributes"
                cursor.execute(query_delete_attributes)
                deleted_rows = cursor.rowcount
                conn.commit()
                log(f"✅ {deleted_rows} attributs supprimés de TM_MAD_DROPFR_Attributes\n")
            else:
                log("ℹ️ Aucun attribut à supprimer dans TM_MAD_DROPFR_Attributes\n")

            return deleted_rows

        except Exception as e:
            log(f"❌ Erreur lors de la suppression des catégories : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Insertion des données attributs depuis la route /products/{productId} dans la table TM_MAD_DROPFR_Attributes
    def insert_attributes_data(self, conn, products):
        try:
            cursor = conn.cursor()

            query_insert_attributes= """
                INSERT INTO TM_MAD_DROPFR_Attributes (
                    productId, code, label, value, dateUpdate
                )
                VALUES (
                    %(productId)s, %(code)s, %(label)s, %(value)s, %(dateUpdate)s
                )
            """
            insert_count = 0
            log("📥 Début de l'insertion des attributs dans TM_MAD_DROPFR_Attributes")

            for product in products:
                product_id = product.get("productId", None)
                if product_id == None:
                    log("⚠️ ProductId manquant, insertion ignorée")
                    continue
                
                attributes = product.get("attributes", [])                
                if not attributes:
                    continue

                common_data = {
                    "productId": product_id
                }

                for attribute in attributes:
                    attributes_data = {
                        "code": attribute.get("code"),
                        "label": attribute.get("label"),
                        "value": attribute.get("values"),
                        "value": ', '.join(attribute.get("values")) if isinstance(attribute.get("values"), list) else attribute.get("values"),
                        "dateUpdate": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                    
                    full_data = {**common_data, **attributes_data}
                
                    cursor.execute(query_insert_attributes, full_data)
                    insert_count += 1
                    
            conn.commit()
            log(f"✅ Fin de l'insertion des attributs : {insert_count} attributs insérés dans TM_MAD_DROPFR_Attributes")

            return insert_count

        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des attributs : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()