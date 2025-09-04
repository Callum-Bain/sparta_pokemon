import sqlalchemy
import requests
import pandas as pd
from pprint import pprint

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

def request_data(url):
    response = requests.get(url)
    raw_data = response.json()
    raw_data.pop('base_experience')
    raw_data.pop('is_default')
    raw_data.pop('order')
    raw_data.pop('forms')
    raw_data.pop('game_indices')
    raw_data.pop('held_items')
    raw_data.pop('location_area_encounters')
    raw_data.pop('sprites')
    raw_data.pop('cries')
    raw_data.pop('past_types')
    raw_data.pop('past_abilities')
    return raw_data

all_pokemon = []
for i in range(1, 2):
    raw_data = request_data(f'https://pokeapi.co/api/v2/pokemon/{i}')
    all_pokemon.append(raw_data)

pprint(all_pokemon[0])
df_pokemon = pd.DataFrame(columns = ['id', 'name', 'height', 'weight', 'abilities', 'moves', 'stats', 'types'])
new_rows = pd.DataFrame(all_pokemon)
df_pokemon = pd.concat([df_pokemon, new_rows], ignore_index=True)
# print(df_pokemon.head(1))

# pprint(pd.json_normalize(all_pokemon))


# https://pokeapi.co/api/v2/evolution-chain/14/
