import requests
import pandas as pd
import time
import re
from rapidfuzz import fuzz, process
from tqdm import tqdm
import config

# Configuration API et paramètres globaux
BASE_URL = "https://api.pipedrive.com/v1/"
THRESHOLD_SIMILARITY = 60
SUBSTRING_THRESHOLD = 60
LOW_THRESHOLD_SHORT_NAMES = 60

CUSTOM_FIELD_KEY = "79fe67d2e07087cb5c715b74bc2e05db93b90b52"

STOPWORDS = [
    "group", "groupe", "sa", "sas", "sarl", "ltd", "france", "uk", "inc", "europe",
    "holding", "company", "corporation", "international", "laboratoire", "laboratoires",
    "technologies", "solutions", "systems", "systemes", "of", "and", "de", "des", "du", "la", "le", "les"
]

def regex_clean_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower()
    for word in STOPWORDS:
        name = re.sub(rf"\b{word}\b", "", name)
    name = re.sub(r"[^a-z0-9]", " ", name)
    return re.sub(r"\s+", " ", name).strip()

def get_organizations_from_crm(name, api_token):
    search_url = f"{BASE_URL}itemSearch"
    params = {
        "term": name,
        "item_type": "organization",
        "fields": "name",
        "api_token": api_token
    }
    response = requests.get(search_url, params=params)
    time.sleep(0.5)
    if response.status_code == 200:
        data = response.json().get("data", {}).get("items", [])
        return [{"id": item["item"]["id"], "name": item["item"]["name"]} for item in data]
    else:
        return []

def get_all_organizations_with_custom_field(api_token, custom_field_key):
    all_orgs = []
    start = 0
    limit = 100

    while True:
        url = f"{BASE_URL}organizations"
        params = {
            "start": start,
            "limit": limit,
            "api_token": api_token
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            break

        data = response.json().get("data", [])
        if not data:
            break

        for org in data:
            custom_value = org.get(custom_field_key, "")
            if custom_value:
                all_orgs.append({
                    "id": org["id"],
                    "name": org["name"],
                    "custom_field_value": custom_value
                })

        start += limit

    return all_orgs

def calculate_similarity(name1, name2):
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
    clean_name1 = regex_clean_name(name1)
    clean_name2 = regex_clean_name(name2)
    return clean_name1 in clean_name2 or clean_name2 in clean_name1

def get_simplified_name(name):
    clean_name = regex_clean_name(name)
    simplified_name = clean_name.split()[0] if clean_name else ""
    return simplified_name

def get_organizations_from_crm_with_fallback(name, api_token):
    crm_orgs = get_organizations_from_crm(name, api_token)
    if not crm_orgs:
        simplified_name = get_simplified_name(name)
        if simplified_name and simplified_name != name:
            crm_orgs = get_organizations_from_crm(simplified_name, api_token)
    return crm_orgs


def find_best_match(org_name, crm_orgs):
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


def find_match_in_custom_field(org_name, custom_orgs):
    pattern = re.compile(re.escape(org_name), re.IGNORECASE)
    for org in custom_orgs:
        if pattern.search(org.get("custom_field_value", "")):
            return org
    return None

def main():
    filename = "test_soreinza.csv"
    df = pd.read_csv(f"csv/{filename}", delimiter=",")

    df["org_name_crm"] = ""
    df["org_id_crm"] = ""
    df["similarity_score"] = 0.0

    print("#### Récupération des organisations sur le pipe... ####")
    custom_field_orgs = get_all_organizations_with_custom_field(config.API_TOKEN, CUSTOM_FIELD_KEY)

    print("#### Recherche des doublons en cours... ####")
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        org_name = row.get("Company Name", "")
        if not org_name:
            continue

        crm_orgs = get_organizations_from_crm_with_fallback(org_name, config.API_TOKEN)

        best_match, similarity_score, token_sort, token_set, partial, token_ratio = find_best_match(org_name, crm_orgs)

        if best_match:
            df.at[index, "org_name_crm"] = best_match["name"]
            df.at[index, "org_id_crm"] = best_match["id"]
            df.at[index, "similarity_score"] = round(similarity_score, 2)
            continue

        match_in_custom = find_match_in_custom_field(org_name, custom_field_orgs)
        if match_in_custom:
            df.at[index, "org_name_crm"] = match_in_custom["name"]
            df.at[index, "org_id_crm"] = match_in_custom["id"]
            df.at[index, "similarity_score"] = 100.0
            continue

    output_file = "results/results.csv"
    df.to_csv(output_file, index=False)
    print(f"#### Résultats sauvegardés dans {output_file} ####")

if __name__ == "__main__":
    main()
