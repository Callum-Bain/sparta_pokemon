import pandas as pd
import requests
from pprint import pprint
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database

# Configuration
def configure_pandas():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)

# API Request
def get_pokemon_data(pokemon_id):
    url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}'
    response = requests.get(url)
    return response.json()

# Fetch Multiple Pokemon (Raw Data)
def fetch_multiple_pokemon(n=10):
    all_pokemon_raw = []
    for i in range(1, n + 1):
        raw_data = get_pokemon_data(i)
        all_pokemon_raw.append(raw_data)
    return all_pokemon_raw

# Clean Raw JSON for Pokemon Table
def clean_for_pokemon_table(raw_data):
    return {
        'id': raw_data['id'],
        'name': raw_data['name'],
        'height': raw_data['height'],
        'weight': raw_data['weight']
    }

# Clean Raw JSON for Type Tables
def clean_for_type_table(raw_data):
    type_set = set()
    pokemon_types = []
    for pokemon in raw_data:
        pokemon_id = pokemon['id']
        for type in pokemon['types']:
            type_name = type['type']['name']
            type_url = type['type']['url']
            type_id = int(type_url.strip("/").split("/")[-1])
            slot = type['slot']
            type_set.add((type_id, type_name))
            pokemon_types.append({
                'pokemon_id': pokemon_id,
                'type_id': type_id,
                'slot': slot
            })
    df_type = pd.DataFrame(type_set, columns=['id', 'name']).sort_values('id')
    df_pokemon_type = pd.DataFrame(pokemon_types)
    return df_type, df_pokemon_type

# Clean Raw JSON for Abilities Tables
def clean_for_abilities_tables(raw_data):
    ability_set = set()
    pokemon_abilities = []
    for pokemon in raw_data:
        pokemon_id = pokemon['id']
        for ability in pokemon['abilities']:
            ability_name = ability['ability']['name']
            ability_url = ability['ability']['url']
            ability_id = int(ability_url.strip("/").split("/")[-1])
            slot = ability['slot']
            is_hidden = ability['is_hidden']
            ability_set.add((ability_id, ability_name))
            pokemon_abilities.append({
                'pokemon_id': pokemon_id,
                'ability_id': ability_id,
                'slot': slot,
                'is_hidden': is_hidden
            })
    df_ability = pd.DataFrame(ability_set, columns=['id', 'name']).sort_values('id')
    df_pokemon_ability = pd.DataFrame(pokemon_abilities)
    return df_ability, df_pokemon_ability

# Create Type Tables
def create_type_tables(df_type, df_pokemon_type, engine):
    df_type.to_sql(name='type', con=engine, if_exists='replace', index=False)
    df_pokemon_type.to_sql(name='pokemon_type', con=engine, if_exists='replace', index=False)
    print("Types Tables Created")

# Create Abilities Tables
def create_abilities_tables(df_ability, df_pokemon_ability, engine):
    df_ability.to_sql(name='ability', con=engine, if_exists='replace', index=False)
    df_pokemon_ability.to_sql(name='pokemon_ability', con=engine, if_exists='replace', index=False)

# Create Pokemon DataFrame
def create_pokemon_dataframe(pokemon_raw_list):
    cleaned = [clean_for_pokemon_table(p) for p in pokemon_raw_list]
    return pd.DataFrame(cleaned)

# Database Setup
def setup_database_connection():
    engine = create_engine("mssql+pyodbc://(localdb)\\MSSQLLocalDB/Pokemon?driver=ODBC+Driver+17+for+SQL+Server")
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine

# Save to SQL
def save_dataframe_to_sql(df, engine, table_name='pokemon'):
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

# Query and Print
def query_and_print_table(engine, table_name='pokemon'):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT * FROM {table_name};'))
        for row in result:
            print(row)

# Main
def main():
    configure_pandas()

    # Fetch raw data
    pokemon_raw_list = fetch_multiple_pokemon(10)

    # pprint(pokemon_raw_list[0])  # For debugging

    # Clean for Pokemon table
    df_pokemon = create_pokemon_dataframe(pokemon_raw_list)

    # Clean for types and pokemon_type tables
    df_type, df_pokemon_type = clean_for_type_table(pokemon_raw_list)

    # Clean for ability and pokemon ability table
    df_ability, df_pokemon_ability = clean_for_abilities_tables(pokemon_raw_list)

    # DB Connection
    engine = setup_database_connection()

    # Save Tables
    save_dataframe_to_sql(df_pokemon, engine, table_name='pokemon')
    create_type_tables(df_type, df_pokemon_type, engine)
    create_abilities_tables(df_ability, df_pokemon_ability, engine)

    # Query to verify
    query_and_print_table(engine, table_name='pokemon')
    query_and_print_table(engine, table_name='type')
    query_and_print_table(engine, table_name='pokemon_type')
    query_and_print_table(engine, table_name='ability')
    query_and_print_table(engine, table_name='pokemon_ability')

if __name__ == "__main__":
    main()



'''
Order:
type + pokemon_type=DONE
abilities + pokemon_abilities	
moves + pokemon_moves	
items + pokemon_held_items	
stats	
evolution_chain / species
'''

# Add paulaked as collaborator