### version chatgpt python - finish
import requests
import pandas as pd
import time
import re
from rapidfuzz import fuzz, process
from tqdm import tqdm

# Configuration API et paramètres globaux
API_TOKEN = "ee422492cf8524fccc4914a021bca955e0153745"  # Remplacez par votre clé API PipeDrive
BASE_URL = "https://api.pipedrive.com/v1/"
THRESHOLD_SIMILARITY = 80  # Seuil pour les similarités directes
SUBSTRING_THRESHOLD = 60  # Seuil pour les sous-chaînes
LOW_THRESHOLD_SHORT_NAMES = 70  # Seuil pour les noms courts

STOPWORDS = [
    "group", "groupe", "sa", "sas", "sarl", "ltd", "france", "uk", "inc", "europe",
    "holding", "company", "corporation", "international", "laboratoire", "laboratoires",
    "technologies", "solutions", "systems", "systemes", "of", "and", "de", "des", "du", "la", "le", "les"
]

def regex_clean_name(name):
    """
    Nettoie un nom d'entreprise pour les comparaisons.
    """
    if not isinstance(name, str):
        return ""
    name = name.lower()
    for word in STOPWORDS:
        name = re.sub(rf"\b{word}\b", "", name)
    name = re.sub(r"[^a-z0-9]", " ", name)  # Garder uniquement lettres et chiffres
    return re.sub(r"\s+", " ", name).strip()

def get_organizations_from_crm(name, api_token):
    """
    Effectue une recherche dans PipeDrive pour trouver des organisations correspondant au nom donné.
    """
    search_url = f"{BASE_URL}itemSearch"
    params = {
        "term": name,
        "item_type": "organization",
        "fields": "name",
        "api_token": api_token
    }
    response = requests.get(search_url, params=params)
    time.sleep(0.2)  # Pause pour respecter les limites de l'API
    if response.status_code == 200:
        data = response.json().get("data", {}).get("items", [])
        return [{"id": item["item"]["id"], "name": item["item"]["name"]} for item in data]
    else:
        return []

def calculate_similarity(name1, name2):
    """
    Calcule les différents scores de similarité entre deux noms nettoyés.
    """
    clean_name1 = regex_clean_name(name1)
    clean_name2 = regex_clean_name(name2)

    if not clean_name1 or not clean_name2:
        return 0, 0, 0, 0, 0

    token_sort_ratio = fuzz.token_sort_ratio(clean_name1, clean_name2)
    token_set_ratio = fuzz.token_set_ratio(clean_name1, clean_name2)
    partial_ratio = fuzz.partial_ratio(clean_name1, clean_name2)
    token_ratio = fuzz.ratio(clean_name1, clean_name2)

    return max(token_sort_ratio, token_set_ratio, partial_ratio, token_ratio), token_sort_ratio, token_set_ratio, partial_ratio, token_ratio

def contains_substring(name1, name2):
    """
    Vérifie si l'un des deux noms est une sous-chaîne de l'autre.
    """
    clean_name1 = regex_clean_name(name1)
    clean_name2 = regex_clean_name(name2)
    return clean_name1 in clean_name2 or clean_name2 in clean_name1

def get_simplified_name(name):
    """
    Simplifie un nom pour une recherche secondaire.
    Exemple : "Delpharm Gaillard" -> "Delpharm"
    """
    clean_name = regex_clean_name(name)
    simplified_name = clean_name.split()[0] if clean_name else ""
    return simplified_name

def get_organizations_from_crm_with_fallback(name, api_token):
    """
    Effectue une recherche dans PipeDrive avec fallback sur un nom simplifié.
    """
    crm_orgs = get_organizations_from_crm(name, api_token)
    if not crm_orgs:
        simplified_name = get_simplified_name(name)
        if simplified_name and simplified_name != name:
            crm_orgs = get_organizations_from_crm(simplified_name, api_token)
    return crm_orgs

def find_best_match(org_name, crm_orgs):
    """
    Trouve la meilleure correspondance pour un nom d'organisation donné parmi les organisations CRM.
    """
    if not crm_orgs:
        return None, 0, 0, 0, 0, 0

    matches = []
    for org in crm_orgs:
        scores = calculate_similarity(org_name, org["name"])
        matches.append((org, *scores))

    matches.sort(key=lambda x: x[1], reverse=True)

    threshold = LOW_THRESHOLD_SHORT_NAMES if len(org_name.split()) <= 2 else THRESHOLD_SIMILARITY

    best_match = matches[0]
    if best_match[1] >= threshold:
        return best_match[0], best_match[1], best_match[2], best_match[3], best_match[4], best_match[5]

    for match in matches:
        if contains_substring(org_name, match[0]["name"]):
            return match[0], SUBSTRING_THRESHOLD, *match[2:]
    return None, 0, 0, 0, 0, 0

def main():
    filename = "list 8.csv"
    df = pd.read_csv(f"csv/{filename}", delimiter=",")

    # Initialisation des colonnes avec float
    df["org_name_crm"] = ""
    df["org_id_crm"] = ""
    df["similarity_score"] = 0.0
    # df["token_sort_score"] = 0.0
    # df["token_set_score"] = 0.0
    # df["partial_score"] = 0.0
    # df["token_ratio_score"] = 0.0

    print("#### Recherche des doublons en cours... ####")
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        org_name = row.get("Company Name", "")
        if not org_name:
            continue

        crm_orgs = get_organizations_from_crm_with_fallback(org_name, API_TOKEN)

        if not crm_orgs:
            continue

        best_match, similarity_score, token_sort, token_set, partial, token_ratio = find_best_match(org_name, crm_orgs)

        if best_match:
            df.at[index, "org_name_crm"] = best_match["name"]
            df.at[index, "org_id_crm"] = best_match["id"]
            df.at[index, "similarity_score"] = round(similarity_score, 2)
            # df.at[index, "token_sort_score"] = round(token_sort, 2)
            # df.at[index, "token_set_score"] = round(token_set, 2)
            # df.at[index, "partial_score"] = round(partial, 2)
            # df.at[index, "token_ratio_score"] = round(token_ratio, 2)

    output_file = "results/results.csv"
    df.to_csv(output_file, index=False)
    print(f"#### Résultats sauvegardés dans {output_file} ####")

if __name__ == "__main__":
    main()









############ VERSION NOVEMBRE 2024 ###############

# import config
# import requests
# import pandas as pd
# import time
#
#
# def in_pipe():
#     print("####Récupération des infos dans Pipe####")
#     filename = ("Eté 2024 - Top Cible SALES - jerem à importer.csv")
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}",delimiter=",")
#
#
#     l_orga_id=[]
#     l_orga_label_id=[]
#     l_orga_name=[]
#     # Boucle à travers toutes les company_name
#     for x in df[('Entreprise')]:
#         try:
#             x_lower = x.lower()
#             xp = x_lower.split(" (")[0].strip().capitalize()
#         except:
#             xp = ""
#         if xp.startswith('groupe'):
#             xp = xp.replace("groupe ", "").strip().capitalize()
#         elif xp.startswith('group'):
#             xp = xp.replace("group", "").strip().capitalize()
#         elif xp.endswith('groupe'):
#             xp = xp.replace(" groupe", "").strip().capitalize()
#         elif xp.endswith('group'):
#             xp = xp.replace(" group", "").strip().capitalize()
#         elif xp.endswith(' sa'):
#             xp = xp.replace(" sa", "").strip().capitalize()
#         elif xp.endswith(' sas'):
#             xp = xp.replace(" sas", "").strip().capitalize()
#         elif xp.endswith(' SA'):
#             xp = xp.replace(" SA", "").strip().capitalize()
#         elif xp.endswith(' SAS'):
#             xp = xp.replace(" SAS", "").strip().capitalize()
#         elif xp.endswith(' sarl'):
#             xp = xp.replace(" sarl", "").strip().capitalize()
#         elif xp.endswith(' ltd.'):
#             xp = xp.replace(" ltd.", "").strip().capitalize()
#         elif xp.endswith(' ltd'):
#             xp = xp.replace(" ltd", "").strip().capitalize()
#         elif xp.endswith(' france'):
#             xp = xp.replace(" france", "").strip().capitalize()
#         elif xp.endswith(' France'):
#             xp = xp.replace(" France", "").strip().capitalize()
#         elif xp.endswith(' FRANCE'):
#             xp = xp.replace(" FRANCE", "").strip().capitalize()
#         elif xp.endswith(' Inc'):
#             xp = xp.replace(" Inc", "").strip().capitalize()
#         elif xp.endswith(' Inc.'):
#             xp = xp.replace(" Inc.", "").strip().capitalize()
#         elif xp.endswith(' Europe'):
#             xp = xp.replace(" Europe", "").strip().capitalize()
#         elif xp.endswith(' europe'):
#             xp = xp.replace(" europe", "").strip().capitalize()
#         elif xp.endswith(' Holding'):
#             xp = xp.replace(" Holding", "").strip().capitalize()
#         else:
#             pass  # Si aucun des cas ci-dessus n'est vrai, laissez-le inchangé
#
#
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#
#         url = f"{base_url}term={xp}&item_types=organization&fields=name,custom_fields&api_token={config.api_pipe}"
#
#         response = requests.get(url)
#         time.sleep(1)
#         response_json = response.json()
#
#
#         try :
#             org_id = response_json["data"]["items"][0]["item"]["id"]
#         except :
#             org_id = "vide"
#
#         orga = requests.get(f'https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}')
#         orga = orga.json()
#         try :
#             org_label_id = (orga['data']["label"])
#         except: org_label_id = ""
#
#         try:
#             orga_name = (orga['data']['name'])
#         except:
#             orga_name = ""
#
#         list = (x,orga_name)
#         if orga_name =="":
#             print(list, "pas dans Pipe")
#         else :
#             print(list, "dans Pipe : à checker")
#
#         l_orga_id.append(org_id)
#         l_orga_label_id.append(org_label_id)
#         l_orga_name.append(orga_name)
#     df["org_name_pipe"]=l_orga_name
#     df["org_id_pipe"]=l_orga_id
#     df["org_label_id"]=l_orga_label_id
#     df.to_csv("csv/check_pipe.csv")
#
#
#     # récupération des id labels et noms des labels sur Pipe
# def label_pipe():
#     print("####Récupération des labels dans Pipe####")
#
#     orgafield = f"https://api.pipedrive.com/v1/organizationFields?api_token={config.api_pipe}"
#     response = requests.get(orgafield)
#     response_json = response.json()
#     org_label_id = (response_json['data'])
#     l_name=[]
#     l_id=[]
#     for i in org_label_id:
#         if "label" in i["key"]:
#             options = i["options"]
#             for x in options:
#                 org_label_name = (x["label"])
#                 l_name.append(org_label_name)
#                 org_label_id = (x["id"])
#                 l_id.append(org_label_id)
#                 # print(org_label_name, org_label_id)
#     df = pd.DataFrame(list(zip(l_id,l_name)),columns=['id','name'])
#     df.to_csv("csv/label.csv")
#
# def comparaison():
#     print("####Merge des 2 fichiers####")
#
#     df_label = pd.read_csv("csv/label.csv")
#     df_check_pipe = pd.read_csv("csv/check_pipe.csv")
#     l_label=[]
#     for c in range(len(df_check_pipe)):
#         id_pipe = (df_check_pipe["org_label_id"][c])
#         try :
#             search = df_label.loc[df_label['id'] == id_pipe, 'name'].values[0]
#         except :
#             search = ""
#         l_label.append(search)
#
#     df_check_pipe["label_name"] = l_label
#
#     # Liste des termes à exclure
#     exclude_terms = ["STOP", "Repoussoir", "Deal perdu", "Offre en cours", "Appel d’offres", "Pas de next step", "Inactif", "Client", "Only One Shot", "Test/Démo"]
#
#     # Supprimer les lignes où 'label_name' contient un des termes à exclure
#     df_filtered = df_check_pipe[~df_check_pipe['label_name'].isin(exclude_terms)]
#
#     df_filtered.to_csv("results/results.csv", index=False)
#
#
# in_pipe()
# label_pipe()
# comparaison()





############ V1 DECEMBRE 2024 ###############

# import config
# import requests
# import pandas as pd
# import time
# import re
#
#
# def regex_clean_name(name):
#     """
#     Nettoie un nom d'entreprise à l'aide de regex.
#     """
#     name = name.lower()
#     # Supprime les termes communs
#     name = re.sub(r"\b(group|groupe|sa|sas|sarl|ltd|france|uk|inc|europe|holding|company|corporation|group)\b", "", name)
#     # Supprime les espaces superflus
#     name = re.sub(r"\s+", " ", name).strip()
#     return name
#
#
# def regex_match(name_csv, name_crm):
#     """
#     Compare deux noms d'entreprises nettoyés avec une flexibilité pour les variations de noms.
#     """
#     clean_csv = regex_clean_name(name_csv)
#     clean_crm = regex_clean_name(name_crm)
#
#     # Vérifie si l'un est contenu dans l'autre
#     if clean_csv in clean_crm or clean_crm in clean_csv:
#         return True
#     return False
#
#
# def fetch_labels():
#     """
#     Récupère tous les labels disponibles dans PipeDrive.
#     """
#     print("#### Récupération des labels dans Pipe ####")
#     url = f"https://api.pipedrive.com/v1/organizationFields?api_token={config.api_pipe}"
#     response = requests.get(url)
#     response_json = response.json()
#
#     org_label_id = response_json["data"]
#     l_name, l_id = [], []
#
#     for i in org_label_id:
#         if "label" in i["key"]:
#             options = i["options"]
#             for x in options:
#                 l_name.append(x["label"])
#                 l_id.append(x["id"])
#
#     labels_df = pd.DataFrame(list(zip(l_id, l_name)), columns=["id", "name"])
#     labels_df.to_csv("csv/label.csv", index=False)
#
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un CSV mis à jour.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "Eté 2024 - Top Cible SALES - jerem à importer - Eté 2024 - Top Cible SALES - jerem à importer (1).csv"
#
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_label_id, l_orga_name = [], [], []
#     l_owner_name, l_people_count, l_subsidiaries_match = [], [], []
#
#     custom_field_id_filiales = "4118"  # ID du champ "Filiales/ Entités"
#
#     for x in df["Entreprise"]:
#         try:
#             x_cleaned = regex_clean_name(x)
#         except:
#             x_cleaned = ""
#
#         # Appel API PipeDrive pour récupérer toutes les organisations proches
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name,custom_fields&api_token={config.api_pipe}"
#         response = requests.get(url)
#         time.sleep(1)
#         response_json = response.json()
#
#         try:
#             org_list = [item["item"] for item in response_json["data"]["items"]]
#             # Comparaison regex pour trouver le meilleur match
#             matched_org = next((org for org in org_list if regex_match(x, org["name"])), None)
#
#             if matched_org:
#                 # Récupérer les détails de l'organisation correspondante
#                 org_id = matched_org["id"]
#                 org_name = matched_org["name"]
#
#                 # Appeler pour obtenir plus d'informations, y compris "Filiales/Entités"
#                 orga_details = requests.get(f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}").json()
#                 subsidiaries = orga_details["data"].get(custom_field_id_filiales, "")
#                 subsidiaries_list = [regex_clean_name(sub) for sub in subsidiaries.split(",")]
#
#                 # Vérifier si l'organisation correspond à une filiale
#                 subsidiary_match = any(regex_match(x, sub) for sub in subsidiaries_list)
#             else:
#                 raise ValueError("Aucun match suffisant")
#         except:
#             org_id, org_name, subsidiaries_list, subsidiary_match = "vide", "", [], False
#
#         if org_id != "vide":
#             try:
#                 org_label_id = orga_details["data"].get("label", "")
#             except:
#                 org_label_id = ""
#             try:
#                 owner_name = orga_details["data"]["owner_name"]
#             except:
#                 owner_name = ""
#             try:
#                 people_count = orga_details["data"]["people_count"]
#             except:
#                 people_count = 0
#         else:
#             org_label_id, owner_name, people_count = "", "", 0
#
#         # Log
#         log_status = (x, org_name, "dans Pipe : à checker" if org_name else "pas dans Pipe")
#         print(log_status)
#
#         l_orga_id.append(org_id)
#         l_orga_label_id.append(org_label_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#         l_subsidiaries_match.append("Yes" if subsidiary_match else "No")
#
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_label_id"] = l_orga_label_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df["is_subsidiary"] = l_subsidiaries_match
#     df.to_csv("csv/check_pipe.csv", index=False)
#
#
# def comparaison():
#     """
#     Fusionne les données et applique des filtres pour générer un CSV final.
#     """
#     print("#### Fusion des fichiers ####")
#     df_label = pd.read_csv("csv/label.csv")
#     df_check_pipe = pd.read_csv("csv/check_pipe.csv")
#
#     l_label = []
#     for c in range(len(df_check_pipe)):
#         id_pipe = df_check_pipe["org_label_id"][c]
#         try:
#             search = df_label.loc[df_label["id"] == id_pipe, "name"].values[0]
#         except:
#             search = ""
#         l_label.append(search)
#
#     df_check_pipe["label_name"] = l_label
#
#     # Liste des termes à exclure
#     exclude_terms = ["STOP", "Repoussoir", "Deal perdu", "Offre en cours", "Appel d’offres", "Pas de next step", "Inactif", "Client", "Only One Shot", "Test/Démo"]
#
#     # Supprimer les lignes où 'label_name' contient un des termes à exclure
#     df_filtered = df_check_pipe[~df_check_pipe["label_name"].isin(exclude_terms)]
#     df_filtered.to_csv("results/results.csv", index=False)
#
#
# if __name__ == "__main__":
#     fetch_labels()
#     in_pipe()
#     comparaison()





############ V2 DECEMBRE 2024 ###############

# import config
# import requests
# import pandas as pd
# import time
# import re
#
#
# def regex_clean_name(name):
#     """
#     Nettoie un nom d'entreprise à l'aide de regex.
#     """
#     name = name.lower()
#     # Supprime les termes communs
#     name = re.sub(r"\b(group|groupe|sa|sas|sarl|ltd|france|uk|inc|europe|holding|company|corporation|group)\b", "", name)
#     # Supprime les espaces superflus
#     name = re.sub(r"\s+", " ", name).strip()
#     return name
#
#
# def regex_match(name_csv, name_crm):
#     """
#     Compare deux noms d'entreprises nettoyés avec une flexibilité pour les variations de noms.
#     """
#     clean_csv = regex_clean_name(name_csv)
#     clean_crm = regex_clean_name(name_crm)
#
#     # Vérifie si l'un est contenu dans l'autre
#     if clean_csv in clean_crm or clean_crm in clean_csv:
#         return True
#     return False
#
#
# def fetch_labels():
#     """
#     Récupère tous les labels disponibles dans PipeDrive.
#     """
#     print("#### Récupération des labels dans Pipe ####")
#     url = f"https://api.pipedrive.com/v1/organizationFields?api_token={config.api_pipe}"
#     response = requests.get(url)
#     response_json = response.json()
#
#     org_label_id = response_json["data"]
#     l_name, l_id = [], []
#
#     for i in org_label_id:
#         if "label" in i["key"]:
#             options = i["options"]
#             for x in options:
#                 l_name.append(x["label"])
#                 l_id.append(x["id"])
#
#     labels_df = pd.DataFrame(list(zip(l_id, l_name)), columns=["id", "name"])
#     labels_df.to_csv("csv/label.csv", index=False)
#
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un CSV mis à jour.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "test doublon - 1.csv"
#
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_label_id, l_orga_name = [], [], []
#     l_owner_name, l_people_count = [], []
#
#     for x in df["Entreprise"]:
#         try:
#             x_cleaned = regex_clean_name(x)
#         except:
#             x_cleaned = ""
#
#         # Appel API PipeDrive pour récupérer toutes les organisations proches
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name&api_token={config.api_pipe}"
#         response = requests.get(url)
#         time.sleep(1)
#         response_json = response.json()
#
#         try:
#             org_list = [item["item"] for item in response_json["data"]["items"]]
#             # Comparaison regex pour trouver le meilleur match
#             matched_org = next((org for org in org_list if regex_match(x, org["name"])), None)
#
#             if matched_org:
#                 # Récupérer les détails de l'organisation correspondante
#                 org_id = matched_org["id"]
#                 org_name = matched_org["name"]
#
#                 # Appeler pour obtenir plus d'informations
#                 orga_details = requests.get(
#                     f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}"
#                 ).json()
#             else:
#                 raise ValueError("Aucun match suffisant")
#         except:
#             org_id, org_name = "vide", ""
#
#         if org_id != "vide":
#             try:
#                 org_label_id = orga_details["data"].get("label", "")
#             except:
#                 org_label_id = ""
#             try:
#                 owner_name = orga_details["data"]["owner_name"]
#             except:
#                 owner_name = ""
#             try:
#                 people_count = orga_details["data"]["people_count"]
#             except:
#                 people_count = 0
#         else:
#             org_label_id, owner_name, people_count = "", "", 0
#
#         # Log
#         log_status = (x, org_name, "dans Pipe : à checker" if org_name else "pas dans Pipe")
#         print(log_status)
#
#         l_orga_id.append(org_id)
#         l_orga_label_id.append(org_label_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_label_id"] = l_orga_label_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df.to_csv("csv/check_pipe.csv", index=False)
#
#
# def comparaison():
#     """
#     Fusionne les données et applique des filtres pour générer un CSV final.
#     """
#     print("#### Fusion des fichiers ####")
#     df_label = pd.read_csv("csv/label.csv")
#     df_check_pipe = pd.read_csv("csv/check_pipe.csv")
#
#     l_label = []
#     for c in range(len(df_check_pipe)):
#         id_pipe = df_check_pipe["org_label_id"][c]
#         try:
#             search = df_label.loc[df_label["id"] == id_pipe, "name"].values[0]
#         except:
#             search = ""
#         l_label.append(search)
#
#     df_check_pipe["label_name"] = l_label
#
#     # Liste des termes à exclure
#     exclude_terms = ["STOP", "Repoussoir", "Deal perdu", "Offre en cours", "Appel d’offres", "Pas de next step", "Inactif", "Client", "Only One Shot", "Test/Démo"]
#
#     # Supprimer les lignes où 'label_name' contient un des termes à exclure
#     df_filtered = df_check_pipe[~df_check_pipe["label_name"].isin(exclude_terms)]
#     df_filtered.to_csv("results/results.csv", index=False)
#
#
# if __name__ == "__main__":
#     fetch_labels()
#     in_pipe()
#     comparaison()






############ V3 DECEMBRE 2024 ############### Repère bien certains doublons mais donne pas de score

# import config
# import requests
# import pandas as pd
# import time
# import re
#
# def regex_clean_name(name):
#     """
#     Nettoie un nom d'entreprise à l'aide de regex.
#     """
#     name = name.lower()
#     # Supprime les termes communs
#     name = re.sub(r"\b(group|groupe|sa|sas|sarl|ltd|france|uk|inc|europe|holding|company|corporation|group|international|laboratoire|laboratoires)\b", "", name)
#     # Supprime les espaces superflus
#     name = re.sub(r"\s+", " ", name).strip()
#     return name
#
# def regex_match(name_csv, name_crm):
#     """
#     Compare deux noms d'entreprises nettoyés avec une flexibilité pour les variations de noms.
#     """
#     clean_csv = regex_clean_name(name_csv)
#     clean_crm = regex_clean_name(name_crm)
#
#     # Vérifie si l'un est contenu dans l'autre
#     return clean_csv in clean_crm or clean_crm in clean_csv
#
# def fetch_labels():
#     """
#     Récupère tous les labels disponibles dans PipeDrive (optionnel si org_label_id supprimé).
#     """
#     print("#### Récupération des labels dans Pipe ####")
#     url = f"https://api.pipedrive.com/v1/organizationFields?api_token={config.api_pipe}"
#     response = requests.get(url)
#     response_json = response.json()
#
#     org_label_id = response_json["data"]
#     l_name, l_id = [], []
#
#     for i in org_label_id:
#         if "label" in i["key"]:
#             options = i["options"]
#             for x in options:
#                 l_name.append(x["label"])
#                 l_id.append(x["id"])
#
#     labels_df = pd.DataFrame(list(zip(l_id, l_name)), columns=["id", "name"])
#     labels_df.to_csv("csv/label.csv", index=False)
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un CSV mis à jour.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "julien - a clean - results (1).csv"
#
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_name = [], []
#     l_owner_name, l_people_count = [], []
#
#     for x in df["Nom commercial"]:
#         try:
#             x_cleaned = regex_clean_name(x)
#         except Exception as e:
#             print(f"Erreur lors du nettoyage du nom : {e}")
#             x_cleaned = ""
#
#         # Appel API PipeDrive pour récupérer toutes les organisations proches
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name&api_token={config.api_pipe}"
#         response = requests.get(url)
#         time.sleep(1)
#         response_json = response.json()
#
#         try:
#             org_list = [item["item"] for item in response_json["data"]["items"]]
#             # Comparaison regex pour trouver le meilleur match
#             matched_org = next((org for org in org_list if regex_match(x, org["name"])), None)
#
#             if matched_org:
#                 # Récupérer les détails de l'organisation correspondante
#                 org_id = matched_org["id"]
#                 org_name = matched_org["name"]
#
#                 # Appeler pour obtenir plus d'informations
#                 orga_details = requests.get(
#                     f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}"
#                 ).json()
#             else:
#                 raise ValueError("Aucun match suffisant")
#         except:
#             org_id, org_name = "vide", ""
#
#         if org_id != "vide":
#             try:
#                 owner_name = orga_details["data"].get("owner_name", "")
#             except:
#                 owner_name = ""
#             try:
#                 people_count = orga_details["data"].get("people_count", 0)
#             except:
#                 people_count = 0
#         else:
#             owner_name, people_count = "", 0
#
#         # Log
#         log_status = (x, org_name, "dans Pipe : à checker" if org_name else "pas dans Pipe")
#         print(log_status)
#
#         l_orga_id.append(org_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df.to_csv("csv/check_pipe.csv", index=False)
#
# def comparaison():
#     """
#     Fusionne les données et applique des filtres pour générer un CSV final.
#     """
#     print("#### Fusion des fichiers ####")
#     df_check_pipe = pd.read_csv("csv/check_pipe.csv")
#
#     # Liste des termes à exclure
#     exclude_terms = ["STOP", "Repoussoir", "Deal perdu", "Offre en cours", "Appel d’offres", "Pas de next step", "Inactif", "Client", "Only One Shot", "Test/Démo"]
#
#     # Supprimer les lignes où 'org_name_pipe' contient un des termes à exclure
#     df_filtered = df_check_pipe[~df_check_pipe["org_name_pipe"].isin(exclude_terms)]
#     df_filtered.to_csv("results/results.csv", index=False)
#
# if __name__ == "__main__":
#     fetch_labels()
#     in_pipe()
#     comparaison()




# TEST JANVIER 2025 - A du potentiel car donne les scores autres que 100 mais ne repère pas tous les doublons
# import config
# import requests
# import pandas as pd
# import time
# import re
# from rapidfuzz import fuzz
#
#
# def regex_clean_name(name):
#     """
#     Nettoie un nom d'entreprise à l'aide de règles heuristiques sans liste manuelle de termes à exclure.
#     Cette version est plus souple et garde des termes clés comme les accents et les caractères spéciaux.
#     """
#     name = name.lower()
#
#     # Supprime les mots trop courts (moins de 3 caractères), souvent inutiles pour la comparaison
#     name = re.sub(r'\b\w{1,2}\b', '', name)
#
#     # Supprime les chiffres, mais garde les caractères spéciaux utiles
#     name = re.sub(r"[^a-zà-ÿ\s\-]", "", name)
#
#     # Supprime les espaces superflus
#     name = re.sub(r"\s+", " ", name).strip()
#
#     return name
#
#
# def is_similar(name_csv, name_crm, token_threshold=25, partial_threshold=25):
#     """
#     Compare deux noms d'entreprises nettoyés avec une tolérance basée sur la similarité.
#     """
#     clean_csv = regex_clean_name(name_csv)
#     clean_crm = regex_clean_name(name_crm)
#
#     # Vérifie si un des noms est une sous-chaîne de l'autre
#     if clean_csv in clean_crm or clean_crm in clean_csv:
#         return True
#
#     # Calcul des similarités avec RapidFuzz
#     token_ratio = fuzz.token_sort_ratio(clean_csv, clean_crm)
#     partial_ratio = fuzz.partial_ratio(clean_csv, clean_crm)
#
#     # Ajuster la logique de seuil : les cas où les différences sont plus importantes (ex : ajout de "Gaillard")
#     if token_ratio >= token_threshold or partial_ratio >= partial_threshold:
#         return True
#     # Cas spécifique de similarité plus faible à considérer
#     if token_ratio >= 20 or partial_ratio >= 20:
#         return True
#
#     return False
#
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un fichier de résultats.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "Eté 2024 - Top Cible SALES - julien à importer.csv"  # Remplacez par le nom de votre fichier CSV
#
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_name = [], []
#     l_owner_name, l_people_count = [], []
#     l_similarity_score = []  # Ajouter la similarité pour chaque correspondance
#
#     for x in df["Nom commercial"]:
#         try:
#             x_cleaned = regex_clean_name(x)
#         except Exception as e:
#             print(f"Erreur lors du nettoyage du nom : {e}")
#             x_cleaned = ""
#
#         # Appel API PipeDrive pour récupérer toutes les organisations proches
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name&api_token={config.api_pipe}"
#         response = requests.get(url)
#         time.sleep(1)
#         response_json = response.json()
#
#         try:
#             org_list = [item["item"] for item in response_json["data"]["items"]]
#             # Comparaison basée sur la similarité pour trouver le meilleur match
#             best_match = None
#             best_score = 0
#
#             for org in org_list:
#                 token_similarity = fuzz.token_sort_ratio(x, org["name"])
#                 partial_similarity = fuzz.partial_ratio(x, org["name"])
#                 similarity = max(token_similarity, partial_similarity)
#
#                 # Log des comparaisons
#                 print(
#                     f"Comparing '{x}' with '{org['name']}': token_similarity={token_similarity}, partial_similarity={partial_similarity}")
#
#                 if similarity > best_score:
#                     best_match = org
#                     best_score = similarity
#
#             if best_match and best_score >= 25:  # Seulement si le score dépasse le seuil de 25
#                 org_id = best_match["id"]
#                 org_name = best_match["name"]
#
#                 # Appeler pour obtenir plus d'informations
#                 orga_details = requests.get(
#                     f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}"
#                 ).json()
#             else:
#                 raise ValueError("Aucun match suffisant")
#         except Exception as e:
#             print(f"Erreur lors de la recherche : {e}")
#             org_id, org_name, best_score = "vide", "", 0
#
#         if org_id != "vide":
#             try:
#                 owner_name = orga_details["data"].get("owner_name", "")
#             except:
#                 owner_name = ""
#             try:
#                 people_count = orga_details["data"].get("people_count", 0)
#             except:
#                 people_count = 0
#         else:
#             owner_name, people_count = "", 0
#
#         # Log
#         log_status = (
#         x, org_name, f"Similarité : {best_score}", "dans Pipe : à checker" if org_name else "pas dans Pipe")
#         print(log_status)
#
#         l_orga_id.append(org_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#         l_similarity_score.append(best_score)
#
#     # Ajouter les colonnes de résultats au DataFrame
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df["similarity_score"] = l_similarity_score
#
#     # Enregistrer les résultats dans un fichier CSV
#     df.to_csv("results/results.csv", index=False)
#     print("#### Résultats sauvegardés dans results/results.csv ####")
#
#
# if __name__ == "__main__":
#     in_pipe()





## test 24/01 VERSION 1 - OPTION A PRENDRE

import config
import requests
import pandas as pd
import time
import re
# from rapidfuzz import fuzz
#
# def normalize_text(name):
#     """
#     Normalise le texte en supprimant les accents, les mots inutiles et les espaces superflus.
#     """
#     name = name.lower()
#     name = re.sub(r"[éèêë]", "e", name)
#     name = re.sub(r"[àâä]", "a", name)
#     name = re.sub(r"[ôö]", "o", name)
#     name = re.sub(r"[îï]", "i", name)
#     name = re.sub(r"[ûü]", "u", name)
#     name = re.sub(r"\b(group|groupe|sa|sas|sarl|ltd|france|uk|inc|europe|holding|company|corporation|international|laboratoire|laboratoires)\b", "", name)
#     name = re.sub(r"\s+", " ", name).strip()
#     return name
#
# def fuzzy_match(name_csv, name_crm, threshold=25):
#     """
#     Utilise RapidFuzz pour comparer deux noms après nettoyage.
#     Retourne True si le score dépasse le seuil spécifié.
#     """
#     clean_csv = normalize_text(name_csv)
#     clean_crm = normalize_text(name_crm)
#
#     # Calcul des similarités avec RapidFuzz
#     partial_ratio = fuzz.partial_ratio(clean_csv, clean_crm)
#     token_sort_ratio = fuzz.token_sort_ratio(clean_csv, clean_crm)
#
#     # Retourne True si l'une des similarités dépasse le seuil
#     return max(partial_ratio, token_sort_ratio) >= threshold
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un CSV mis à jour.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "test doublon - 1 (1).csv"
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_name = [], []
#     l_owner_name, l_people_count, l_similarity_score = [], [], []
#
#     for x in df["Entreprise"]:
#         try:
#             x_cleaned = normalize_text(x)
#         except Exception as e:
#             print(f"Erreur lors du nettoyage du nom : {e}")
#             x_cleaned = ""
#
#         # Appel API PipeDrive
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name&api_token={config.api_pipe}"
#         try:
#             response = requests.get(url)
#             time.sleep(1)
#             response_json = response.json()
#             org_list = [item["item"] for item in response_json.get("data", {}).get("items", [])]
#         except Exception as e:
#             print(f"Erreur API PipeDrive : {e}")
#             org_list = []
#
#         # Comparaison avec RapidFuzz
#         best_match = None
#         best_score = 0
#
#         for org in org_list:
#             score = fuzz.partial_ratio(x_cleaned, normalize_text(org["name"]))
#             print(f"Comparing '{x_cleaned}' with '{org['name']}': score={score}")
#
#             if score > best_score:
#                 best_match = org
#                 best_score = score
#
#         if best_match and best_score >= 25:  # Seuil ajustable
#             org_id = best_match["id"]
#             org_name = best_match["name"]
#             try:
#                 orga_details = requests.get(
#                     f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}"
#                 ).json()
#                 owner_name = orga_details["data"].get("owner_name", "")
#                 people_count = orga_details["data"].get("people_count", 0)
#             except:
#                 owner_name, people_count = "", 0
#         else:
#             org_id, org_name, owner_name, people_count = "vide", "", "", 0
#
#         # Enregistrer les résultats
#         l_orga_id.append(org_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#         l_similarity_score.append(best_score)
#
#     # Ajouter les colonnes au DataFrame
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df["similarity_score"] = l_similarity_score
#     df.to_csv("csv/check_pipe.csv", index=False)
#     print("#### Résultats sauvegardés dans csv/check_pipe.csv ####")
#
# def comparaison():
#     """
#     Fusionne les données et applique des filtres pour générer un CSV final.
#     """
#     print("#### Fusion des fichiers ####")
#     df_check_pipe = pd.read_csv("csv/check_pipe.csv")
#
#     exclude_terms_list = ["STOP", "Repoussoir", "Deal perdu", "Offre en cours", "Appel d’offres", "Pas de next step",
#                           "Inactif", "Client", "Only One Shot", "Test/Démo"]
#
#     df_filtered = df_check_pipe[~df_check_pipe["org_name_pipe"].isin(exclude_terms_list)]
#     df_filtered.to_csv("results/results.csv", index=False)
#     print("#### Résultats finaux sauvegardés dans results/results.csv ####")
#
# if __name__ == "__main__":
#     in_pipe()
#     comparaison()


## dernier test - OPTION A PRENDRE

# import config
# import requests
# import pandas as pd
# import time
# import re
# from fuzzywuzzy import fuzz
#
# def regex_clean_name(name):
#     """
#     Nettoie un nom d'entreprise à l'aide de regex.
#     """
#     name = name.lower()
#     # Supprime les termes communs et accessoires
#     name = re.sub(r"\b(group|groupe|sa|sas|sarl|ltd|france|uk|inc|europe|holding|company|corporation|group|international|"
#                   r"laboratoire|laboratoires|systemes|systems|ferroviaires|solutions|technologies|initial|of|and)\b", "", name)
#     # Supprime les caractères spéciaux et les espaces superflus
#     name = re.sub(r"[^a-z0-9]", " ", name)
#     name = re.sub(r"\s+", " ", name).strip()
#     return name
#
# def is_similar(name_csv, name_crm, token_threshold=85, partial_threshold=70):
#     """
#     Compare deux noms d'entreprises nettoyés avec une tolérance basée sur la similarité.
#     """
#     clean_csv = regex_clean_name(name_csv)
#     clean_crm = regex_clean_name(name_crm)
#
#     token_ratio = fuzz.token_sort_ratio(clean_csv, clean_crm)
#     partial_ratio = fuzz.partial_ratio(clean_csv, clean_crm)
#
#     return token_ratio >= token_threshold or partial_ratio >= partial_threshold
#
# def in_pipe():
#     """
#     Compare les entreprises du CSV avec celles présentes dans PipeDrive et génère un fichier de résultats.
#     """
#     print("#### Récupération des infos dans Pipe ####")
#     filename = "julien - a clean - results (1).csv"  # Remplacez par le nom de votre fichier CSV
#
#     # Charger les fichiers CSV
#     df = pd.read_csv(f"csv/{filename}", delimiter=",")
#
#     l_orga_id, l_orga_name = [], []
#     l_owner_name, l_people_count = [], []
#     l_similarity_score = []  # Ajouter la similarité pour chaque correspondance
#
#     for x in df["Nom commercial"]:
#         try:
#             x_cleaned = regex_clean_name(x)
#         except Exception as e:
#             print(f"Erreur lors du nettoyage du nom : {e}")
#             x_cleaned = ""
#
#         # Appel API PipeDrive pour récupérer toutes les organisations proches
#         base_url = "https://api.pipedrive.com/v1/itemSearch?"
#         url = f"{base_url}term={x_cleaned}&item_types=organization&fields=name&api_token={config.api_pipe}"
#         response = requests.get(url)
#         time.sleep(1.5)
#         response_json = response.json()
#
#         try:
#             org_list = [item["item"] for item in response_json["data"]["items"]]
#             # Comparaison basée sur la similarité pour trouver le meilleur match
#             best_match = None
#             best_score = 0
#
#             for org in org_list:
#                 token_similarity = fuzz.token_sort_ratio(x, org["name"])
#                 partial_similarity = fuzz.partial_ratio(x, org["name"])
#                 similarity = max(token_similarity, partial_similarity)
#
#                 if similarity > best_score:
#                     best_match = org
#                     best_score = similarity
#
#             if best_match and best_score >= 25:  # Seulement si le score dépasse le seuil
#                 org_id = best_match["id"]
#                 org_name = best_match["name"]
#
#                 # Appeler pour obtenir plus d'informations
#                 orga_details = requests.get(
#                     f"https://supertripper.pipedrive.com/api/v1/organizations/{org_id}/?api_token={config.api_pipe}"
#                 ).json()
#             else:
#                 raise ValueError("Aucun match suffisant")
#         except Exception as e:
#             print(f"Erreur lors de la recherche : {e}")
#             org_id, org_name, best_score = "vide", "", 0
#
#         if org_id != "vide":
#             try:
#                 owner_name = orga_details["data"].get("owner_name", "")
#             except:
#                 owner_name = ""
#             try:
#                 people_count = orga_details["data"].get("people_count", 0)
#             except:
#                 people_count = 0
#         else:
#             owner_name, people_count = "", 0
#
#         # Log
#         log_status = (x, org_name, f"Similarité : {best_score}", "dans Pipe : à checker" if org_name else "pas dans Pipe")
#         print(log_status)
#
#         l_orga_id.append(org_id)
#         l_orga_name.append(org_name)
#         l_owner_name.append(owner_name)
#         l_people_count.append(people_count)
#         l_similarity_score.append(best_score)
#
#     df["org_name_pipe"] = l_orga_name
#     df["org_id_pipe"] = l_orga_id
#     df["org_owner_name"] = l_owner_name
#     df["org_people_count"] = l_people_count
#     df["similarity_score"] = l_similarity_score
#     df.to_csv("results/results.csv", index=False)
#     print("#### Résultats sauvegardés dans results/results.csv ####")
#
# if __name__ == "__main__":
#     in_pipe()
