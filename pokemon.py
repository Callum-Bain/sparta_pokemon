import pandas as pd
import requests
from pprint import pprint
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy_utils import database_exists, create_database


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
    pokemon_cleaned = []
    for pokemon in raw_data:
        pokemon_cleaned.append({
            'name': pokemon['name'],
            'height': pokemon['height'],
            'weight': pokemon['weight']
        })
    df_pokemon = pd.DataFrame(pokemon_cleaned)
    return df_pokemon


# Clean Raw JSON for Type Tables
def clean_for_type_table(raw_data):
    type_names = []
    pokemon_types = []
    for pokemon in raw_data:
        pokemon_id = pokemon['id']
        for type in pokemon['types']:
            type_name = type['type']['name']
            type_names.append({'name': type_name})
            pokemon_types.append({
                'pokemon_id': pokemon_id,
                'type_id': type_name
            })
    df_type = pd.DataFrame(type_names).drop_duplicates().sort_values('name')
    df_pokemon_type = pd.DataFrame(pokemon_types)
    return df_type, df_pokemon_type


# Clean Raw JSON for Abilities Tables
def clean_for_abilities_tables(raw_data):
    ability_names = []
    pokemon_abilities = []
    for pokemon in raw_data:
        pokemon_id = pokemon['id']
        for ability in pokemon['abilities']:
            ability_name = ability['ability']['name']
            ability_names.append({'name': ability_name})
            pokemon_abilities.append({
                'pokemon_id': pokemon_id,
                'ability_id': ability_name
            })
    df_ability = pd.DataFrame(ability_names).drop_duplicates().sort_values('name')
    df_pokemon_ability = pd.DataFrame(pokemon_abilities)
    return df_ability, df_pokemon_ability


# Clean Raw JSON for Moves Tables
def clean_for_moves_tables(raw_data):
    move_names = []
    pokemon_moves = []
    for pokemon in raw_data:
        pokemon_id = pokemon['id']
        for move in pokemon['moves']:
            move_name = move['move']['name']
            move_names.append({'name': move_name})
            methods = set()
            for version in move['version_group_details']:
                method = version['move_learn_method']['name']
                if method not in methods:
                    methods.add(method)
                    pokemon_moves.append({
                        'pokemon_id': pokemon_id,
                        'move_id': move_name,
                        'move_learn_method': method,
                    })
    df_move = pd.DataFrame(move_names).drop_duplicates().sort_values('name')
    df_pokemon_move = pd.DataFrame(pokemon_moves)
    return df_move, df_pokemon_move


# Create Empty Tables
def create_empty_tables(engine):
    metadata = MetaData()

    pokemon = Table('pokemon', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100)),
        Column('height', Integer),
        Column('weight', Integer))

    type_table = Table('types', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), unique=True))

    pokemon_type = Table('pokemon_types', metadata,
        Column('pokemon_id', Integer, ForeignKey('pokemon.id', ondelete='CASCADE')),
        Column('type_id', Integer, ForeignKey('types.id', ondelete='CASCADE')))

    ability = Table('abilities', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), unique=True))

    pokemon_ability = Table('pokemon_abilities', metadata,
        Column('pokemon_id', Integer, ForeignKey('pokemon.id', ondelete='CASCADE')),
        Column('ability_id', Integer, ForeignKey('abilities.id', ondelete='CASCADE')))

    move = Table('moves', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), unique=True))

    pokemon_move = Table('pokemon_moves', metadata,
        Column('pokemon_id', Integer, ForeignKey('pokemon.id', ondelete='CASCADE')),
        Column('move_id', Integer, ForeignKey('moves.id', ondelete='CASCADE')),
        Column('move_learn_method', String(100)))

    metadata.drop_all(engine)
    metadata.create_all(engine)
    print('All Tables Created')


# Populate Pokemon Table
def create_pokemon_table(df_pokemon, engine):
    df_pokemon.to_sql(name='pokemon', con=engine, if_exists='append', index=False)
    print('Pokemon Table Populated')


# Populate Type Tables
def create_type_tables(df_type, df_pokemon_type, engine):
    df_type.to_sql(name='types', con=engine, if_exists='append', index=False)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT id, name FROM types;')).mappings()
        id_map = {}
        for row in result:
            id_map[row['name']] = row['id']
        df_pokemon_type['type_id'] = df_pokemon_type['type_id'].map(id_map)
    df_pokemon_type.to_sql(name='pokemon_types', con=engine, if_exists='append', index=False)
    print("Type and Junction Tables Populated")


# Populate Abilities Tables
def create_abilities_tables(df_ability, df_pokemon_ability, engine):
    df_ability.to_sql(name='abilities', con=engine, if_exists='append', index=False)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT id, name FROM abilities;')).mappings()
        id_map = {}
        for row in result:
            id_map[row['name']] = row['id']
        df_pokemon_ability['ability_id'] = df_pokemon_ability['ability_id'].map(id_map)
    df_pokemon_ability.to_sql(name='pokemon_abilities', con=engine, if_exists='append', index=False)
    print("Ability and Junction Tables Populated")


# Populate Moves Tables
def create_moves_tables(df_move, df_pokemon_move, engine):
    df_move.to_sql(name='moves', con=engine, if_exists='append', index=False)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT id, name FROM moves;')).mappings()
        id_map = {}
        for row in result:
            id_map[row['name']] = row['id']
    df_pokemon_move['move_id'] = df_pokemon_move['move_id'].map(id_map)
    df_pokemon_move.to_sql(name='pokemon_moves', con=engine, if_exists='append', index=False)
    print("Move and Junction Tables Populated")


# Database Setup
def setup_database_connection():
    engine = create_engine("mssql+pyodbc://(localdb)\\MSSQLLocalDB/Pokemon?driver=ODBC+Driver+17+for+SQL+Server")
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine


# Query and Print
def query_and_print_table(engine, table_name='pokemon'):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT TOP 5 * FROM {table_name};'))
        for row in result:
            print(row)


# Main
def main():

    # Fetch raw data
    pokemon_raw_list = fetch_multiple_pokemon(10)

    # Clean for Pokemon table
    df_pokemon = clean_for_pokemon_table(pokemon_raw_list)

    # Clean for types and pokemon_type tables
    df_type, df_pokemon_type = clean_for_type_table(pokemon_raw_list)

    # Clean for ability and pokemon_ability table
    df_ability, df_pokemon_ability = clean_for_abilities_tables(pokemon_raw_list)

    # Clean for move and pokemon_move table
    df_move, df_pokemon_move = clean_for_moves_tables(pokemon_raw_list)

    # DB Connection
    engine = setup_database_connection()

    # Setup Tables
    create_empty_tables(engine)

    # Save Tables
    create_pokemon_table(df_pokemon, engine)
    create_type_tables(df_type, df_pokemon_type, engine)
    create_abilities_tables(df_ability, df_pokemon_ability, engine)
    create_moves_tables(df_move, df_pokemon_move, engine)

    # Query to verify
    print('')
    print('Sample of Pokemon Table Query:')
    query_and_print_table(engine, table_name='pokemon')
    print('')
    print('Sample of Types Table Query:')
    query_and_print_table(engine, table_name='types')
    print('')
    print('Sample of Pokemon Types Junction Table Query:')
    query_and_print_table(engine, table_name='pokemon_types')
    print('')
    print('Sample of Abilities Table Query:')
    query_and_print_table(engine, table_name='abilities')
    print('')
    print('Sample of Pokemon Abilities Junction Table Query:')
    query_and_print_table(engine, table_name='pokemon_abilities')
    print('')
    print('Sample of Moves Table Query:')
    query_and_print_table(engine, table_name='moves')
    print('')
    print('Sample of Pokemon Moves Table Query:')
    query_and_print_table(engine, table_name='pokemon_moves')

if __name__ == "__main__":
    main()
