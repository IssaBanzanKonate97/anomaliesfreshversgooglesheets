#!/usr/bin/env python
# coding: utf-8

# In[48]:


import requests
import pandas as pd
import time

API_KEY_FRESHWORK = **************
DOMAIN_FRESHWORK = 'institutadios'
BASE_URL = f'https://{DOMAIN_FRESHWORK}.myfreshworks.com/crm/sales/'

def get_existing_contacts(api_key, domain):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    all_contacts = {}
    page = 1
    last_contact_id = None

    while True:
        params = {'per_page': 100, 'page': page}
        if last_contact_id:
            params['last_contact_id'] = last_contact_id
        url = f"{BASE_URL}api/contacts/view/31004512042?per_page=100&sort=created_at"

        response = requests.get(url, headers=headers, params=params)
        if not response.ok:
            print(f"Erreur lors de la récupération des données : {response.status_code}")
            return

        response_data = response.json()
        if 'contacts' not in response_data:
            print("Erreur dans la structure de la réponse JSON")
            break

        contacts_data = response_data['contacts']
        if not contacts_data:
            break

        for contact in contacts_data:
            bitrix_id = contact['custom_field'].get('cf_id_bitrix')
            if bitrix_id:
                all_contacts[bitrix_id] = contact

        last_contact_id = contacts_data[-1]['id']
        page += 1
        time.sleep(1)

    print(f"Contacts récupérés avec succès : {len(all_contacts)}")
    return all_contacts

def main():
    existing_contacts = get_existing_contacts(API_KEY_FRESHWORK, DOMAIN_FRESHWORK)
    data = {
        "internal_id": [],  
        "first_name": [],
        "last_name": [],
        "email": [],
        "mobile_number": [],
        "updated_at": []
    }
    custom_field_data = {}
    
    custom_fields = set()
    for contact in existing_contacts.values():
        custom_fields.update(contact.get("custom_field", {}).keys())
    
    for field in custom_fields:
        custom_field_data[field] = []
    for contact in existing_contacts.values():

        data["internal_id"].append(contact.get("id", ""))

        data["first_name"].append(contact.get("first_name", ""))
        data["last_name"].append(contact.get("last_name", ""))
        data["email"].append(contact.get("email", ""))
        data["mobile_number"].append(contact.get("mobile_number", ""))   
        
        custom_field = contact.get("custom_field", {})
        data.setdefault("cf_id_bitrix", []).append(custom_field.get("cf_id_bitrix", ""))
        
        updated_at = contact.get("updated_at", "")
        if updated_at:
            updated_at = updated_at.split("T")[0]
        data["updated_at"].append(updated_at)

        for field in custom_fields:
            custom_field_data[field].append(custom_field.get(field, ""))

    df_main = pd.DataFrame(data)
    df_custom = pd.DataFrame(custom_field_data)
    df_combined = pd.concat([df_main, df_custom], axis=1)
    
    df_combined["updated_at"] = pd.to_datetime(df_combined["updated_at"])
    df_combined["month_year"] = df_combined["updated_at"].dt.to_period('M')
    df_combined = df_combined.sort_values(by="month_year", ascending=False)
    df_combined = df_combined.drop(columns=["month_year"])
    df_combined = df_combined.reset_index(drop=True)
    
    df_combined.to_csv('contacts_rs.csv', index=False, encoding='utf-8')
    
    return df_combined

if __name__ == '__main__':
    contacts_df = main()
    print(contacts_df)


# In[49]:


import pandas as pd

doublonss = pd.read_csv("contacts_rs.csv")

doublonss["updated_at"] = pd.to_datetime(doublonss["updated_at"])

doublonss["month_year"] = doublonss["updated_at"].dt.to_period('M')

doublonss = doublonss.sort_values(by="month_year", ascending=False)

doublonss = doublonss.drop(columns=["month_year"])

doublonss = doublonss.reset_index(drop=True)

doublonss


# In[50]:


del doublonss["cf_evenement_dacquisition"]
del doublonss["cf_budget_journalier_utm_medium"]
del doublonss["cf_webinar_time"]
del doublonss["cf_webinar_date"]
del doublonss["cf_a_assiste_au_live"]
del doublonss["cf_inscription_utm_source"]
del doublonss["cf_type_de_prospect"]
del doublonss["cf_message_publicitaire_utm_content"]
del doublonss["cf_nom_de_la_campagne_de_publicit_utm_campaign"]
del doublonss["cf_note2"]
del doublonss["cf_url_de_la_source"]
del doublonss["cf_live_room_link"]
del doublonss["cf_note1"]
del doublonss["cf_date_de_naissance"]
del doublonss["cf_canal_dacquisition"]
del doublonss["cf_replay_link"]
del doublonss["cf_time_zone"]
del doublonss["cf_ciblage_utm_term"]
del doublonss["cf_action_initiale_du_prospect"]
del doublonss["cf_nom_du_prospect"]
del doublonss["cf_objectif_formul"]
del doublonss["cf_id_bitrix"]

doublonss.to_csv("Listes_des_doublons_fresh_email.csv", index=False)
doublonss


# In[51]:


#doublons par email

import pandas as pd

df_bitrixs = pd.read_csv("Listes_des_doublons_fresh_email.csv", encoding='utf-8', low_memory=False)

df_filtre = df_bitrixs.dropna(subset=['email'])

doublons = df_filtre[df_filtre.duplicated(subset=['email'], keep=False)]

doublons = doublons.sort_values(by='email')

if doublons.empty:
    print("Aucun doublon trouvé basé sur les colonnes spécifiées.")
else:
    print(doublons)


# In[53]:


del doublons["cf_datecre_bitrix"]
del doublons["cf_id_bitrix.1"]
doublons.to_csv("Modif_Listes_des_doublons_fresh_email.csv", index=False)
doublons


# In[54]:


import gspread
import csv
from datetime import datetime


gc = gspread.service_account(filename="/home/debian/bitrix24versfreshsalesanciennementdatacleaningcron/anomalieV2.json")

sh = gc.open_by_key('1cMEvZRMf8HWIeWIuZfGCojR4oMzPaJFGjnIEb4_4q8g')

worksheet = sh.worksheet("Anomalie 1-Doublons (email)")

worksheet.clear()

with open("Modif_Listes_des_doublons_fresh_email.csv", 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

last_col_num = ord('A') + len(rows[0]) - 1
last_col_letter = chr(last_col_num) if last_col_num <= ord('Z') else 'Z'
cell_range = f'A2:{last_col_letter}{len(rows)+1}'
cell_list = worksheet.range(cell_range)
for cell, value in zip(cell_list, [cell for row in rows for cell in row]):
    cell.value = value

worksheet.update_cells(cell_list)

now = datetime.now()
date_string = now.strftime("%d/%m/%Y à %H:%M:%S")
worksheet.update_acell('J2', date_string)

worksheet = None


# In[ ]:





# In[ ]:





# In[58]:


#doublons par mobile

import pandas as pd

df_bitrixs = pd.read_csv("Listes_des_doublons_fresh_email.csv", encoding='utf-8', low_memory=False)

df_filtred = df_bitrixs.dropna(subset=['mobile_number'])

doublonses = df_filtred[df_filtred.duplicated(subset=['mobile_number'], keep=False)]

doublonses = doublonses.sort_values(by='mobile_number')

if doublonses.empty:
    print("Aucun doublon trouvé basé sur les colonnes spécifiées.")
else:
    print(doublonses)


# In[59]:


doublonses


# In[60]:


del doublonses["cf_datecre_bitrix"]
del doublonses["cf_id_bitrix.1"]
doublonses.to_csv("Modif_listes_des_doublons_fresh_work_number.csv", index=False)
doublonses


# In[61]:


import gspread
import csv
from datetime import datetime

gc = gspread.service_account(filename="/home/debian/bitrix24versfreshsalesanciennementdatacleaningcron/anomalieV2.json")

sh = gc.open_by_key('1cMEvZRMf8HWIeWIuZfGCojR4oMzPaJFGjnIEb4_4q8g')

worksheet = sh.worksheet("Anomalie 1-Doublons (mobile_number)")


worksheet.clear() 

with open("Modif_listes_des_doublons_fresh_work_number.csv", 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

last_col_num = ord('A') + len(rows[0]) - 1
last_col_letter = chr(last_col_num) if last_col_num <= ord('Z') else 'Z'
cell_range = f'A2:{last_col_letter}{len(rows)+1}'

cell_list = worksheet.range(cell_range)

for cell, value in zip(cell_list, [cell for row in rows for cell in row]):
    cell.value = value

worksheet.update_cells(cell_list)

now = datetime.now()
date_string = now.strftime("%d/%m/%Y à %H:%M:%S")
worksheet.update_acell('J2', date_string)


worksheet = None


# In[ ]:





# In[ ]:





# In[62]:


#doublons par autre numéro

import pandas as pd

df_bitrixsp = pd.read_csv("contacts_rs.csv", encoding='utf-8', low_memory=False)

df_filtredp = df_bitrixsp.dropna(subset=['cf_nom_du_prospect'])

doublonsesp = df_filtredp[df_filtredp.duplicated(subset=['cf_nom_du_prospect'], keep=False)]

doublonsesp = doublonsesp.sort_values(by='cf_nom_du_prospect')

if doublonsesp.empty:
    print("Aucun doublon trouvé basé sur les colonnes spécifiées.")
else:
    print(doublonsesp)


# In[64]:


del doublonsesp["cf_evenement_dacquisition"]
del doublonsesp["cf_budget_journalier_utm_medium"]
del doublonsesp["cf_webinar_time"]
del doublonsesp["cf_webinar_date"]
del doublonsesp["cf_a_assiste_au_live"]
del doublonsesp["cf_inscription_utm_source"]
del doublonsesp["cf_type_de_prospect"]
del doublonsesp["cf_message_publicitaire_utm_content"]
del doublonsesp["cf_nom_de_la_campagne_de_publicit_utm_campaign"]
del doublonsesp["cf_note2"]
del doublonsesp["cf_url_de_la_source"]
del doublonsesp["cf_live_room_link"]
del doublonsesp["cf_note1"]
del doublonsesp["cf_date_de_naissance"]
del doublonsesp["cf_canal_dacquisition"]
del doublonsesp["cf_replay_link"]
del doublonsesp["cf_time_zone"]
del doublonsesp["cf_ciblage_utm_term"]
del doublonsesp["cf_action_initiale_du_prospect"]
del doublonsesp["cf_objectif_formul"]
del doublonsesp["cf_id_bitrix"]
del doublonsesp["mobile_number"]
doublonsesp


# In[65]:


del doublonsesp["cf_datecre_bitrix"]
del doublonsesp["cf_id_bitrix.1"]
doublonsesp


# In[66]:


doublonsesp["Autres numeros"] = doublonsesp["cf_nom_du_prospect"]
del doublonsesp["cf_nom_du_prospect"]
doublonsesp.to_csv("Modif_Listes_des_doublons_fresh_autre_numeros.csv", index=False)
doublonsesp


# In[67]:


import gspread
import csv
from datetime import datetime

gc = gspread.service_account(filename="/home/debian/bitrix24versfreshsalesanciennementdatacleaningcron/anomalieV2.json")

sh = gc.open_by_key('1cMEvZRMf8HWIeWIuZfGCojR4oMzPaJFGjnIEb4_4q8g')

worksheet = sh.worksheet("Anomalie 1-Doublons (othernumber)")

worksheet.clear() 

with open("Modif_Listes_des_doublons_fresh_autre_numeros.csv", 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

last_col_num = ord('A') + len(rows[0]) - 1
last_col_letter = chr(last_col_num) if last_col_num <= ord('Z') else 'Z'
cell_range = f'A2:{last_col_letter}{len(rows)+1}'

cell_list = worksheet.range(cell_range)

for cell, value in zip(cell_list, [cell for row in rows for cell in row]):
    cell.value = value

worksheet.update_cells(cell_list)

now = datetime.now()
date_string = now.strftime("%d/%m/%Y à %H:%M:%S")
worksheet.update_acell('J2', date_string)

worksheet = None


# In[ ]:





# In[ ]:





# In[68]:


# Modèle de mobile identique, mais pas le même format.

import pandas as pd
import re

df_bitrixsv = pd.read_csv("Listes_des_doublons_fresh_email.csv", encoding='utf-8', low_memory=False)

df_filtredd = df_bitrixsv.dropna(subset=['mobile_number'])

def simplify_phone_number(phone):
    return re.sub(r'\D', '', phone)

df_filtredd['simplified_mobile_number'] = df_filtredd['mobile_number'].apply(simplify_phone_number)

doublonsesx = df_filtredd[df_filtredd.duplicated(subset=['simplified_mobile_number'], keep=False)]

doublonsesx = doublonsesx.groupby('simplified_mobile_number').filter(lambda x: len(x['mobile_number'].unique()) > 1)

doublonsesx = doublonsesx.sort_values(by='simplified_mobile_number')

if doublonsesx.empty:
    print("Aucun doublon trouvé basé sur les numéros simplifiés.")
else:
    print(doublonsesx[['mobile_number', 'simplified_mobile_number']])


# In[69]:


doublonsesx


# In[70]:


del doublonsesx["cf_datecre_bitrix"]
del doublonsesx["cf_id_bitrix.1"]
del doublonsesx["simplified_mobile_number"]
doublonsesx


# In[71]:


doublonsesx.to_csv("Modif_Mobile_identique_mais_pas_meme_format.csv", index=False)
doublonsesx


# In[72]:


import gspread
import csv
from datetime import datetime

gc = gspread.service_account(filename="/home/debian/bitrix24versfreshsalesanciennementdatacleaningcron/anomalieV2.json")

sh = gc.open_by_key('1cMEvZRMf8HWIeWIuZfGCojR4oMzPaJFGjnIEb4_4q8g')

worksheet = sh.worksheet("Anomalie 1-Mobile_identique_mais_pas_meme_format")

worksheet.clear()

with open("Modif_Mobile_identique_mais_pas_meme_format.csv", 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

last_col_num = ord('A') + len(rows[0]) - 1
last_col_letter = chr(last_col_num) if last_col_num <= ord('Z') else 'Z'
cell_range = f'A2:{last_col_letter}{len(rows)+1}'

cell_list = worksheet.range(cell_range)

for cell, value in zip(cell_list, [cell for row in rows for cell in row]):
    cell.value = value

worksheet.update_cells(cell_list)

now = datetime.now()
date_string = now.strftime("%d/%m/%Y à %H:%M:%S")
worksheet.update_acell('J2', date_string)

worksheet = None

