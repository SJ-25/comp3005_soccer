# Run the matches_insert.py first to ensure that the required season_id data is loaded.
# Then run lineups_insert.py to determine the corresponding match_id.
import json
import psycopg2
import os
from glob import glob

# Database connection
conn = psycopg2.connect(
    dbname="project_database",
    user="postgres",
    password="1234",
    host="localhost"
)
cur = conn.cursor()

# Set the required season_id. If restrictions are not required, you can comment or delete them
allowed_season_ids = {'90', '42', '4', '44'}

# Check if the season is allowed. If restrictions are not required, you can comment or delete them
def is_allowed_season(match_id):
    cur.execute("SELECT season_id FROM matches WHERE match_id = %s;", (match_id,))
    result = cur.fetchone()
    if result and str(result[0]) in allowed_season_ids:
        return True
    return False

# 'def insert_xxxx()' function is an insert statement for the corresponding table
def insert_team(team_id, team_name):
    cur.execute("INSERT INTO teams (team_id, name) VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING;", (team_id, team_name))

def insert_player(player_id, player_name):
    cur.execute("INSERT INTO players (player_id, player_name) VALUES (%s, %s) ON CONFLICT (player_id) DO NOTHING;", (player_id, player_name))

def check_and_insert_country(country_id, country_name):
    if country_id is not None and country_name is not None:
        cur.execute("SELECT * FROM countries WHERE country_id = %s;", (country_id,))
        if not cur.fetchone():
            cur.execute("INSERT INTO countries (country_id, country_name) VALUES (%s, %s);", (country_id, country_name))

def insert_match_lineup(match_id, team_id, player_id, player_nickname, jersey_number, country_id, country_name=None):
    # If country_id is None, there is no need to insert it into the match_lineups table
    if country_id is not None and country_name is not None:
        check_and_insert_country(country_id, country_name)
    cur.execute("""
        INSERT INTO match_lineups (match_id, team_id, player_id, player_nickname, jersey_number, country_id) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id, team_id, player_id) 
        DO UPDATE SET player_nickname = EXCLUDED.player_nickname, 
                      jersey_number = EXCLUDED.jersey_number, 
                      country_id = EXCLUDED.country_id;
    """, (match_id, team_id, player_id, player_nickname, jersey_number, country_id))

# Read and process each JSON file, this is the root directory of my JSON file
for file_path in glob(r"C:\Users\vcck0\Desktop\final project\open-data-master\open-data-master\data\lineups\*.json"):
    # Gets match_id from the json file name
    match_id = os.path.splitext(os.path.basename(file_path))[0]
    
    # Make sure match_id is required, skip if not
    if not is_allowed_season(match_id):
        continue
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        for data in data: 
            # Use functions to insert information
            insert_team(data['team_id'], data['team_name'])
                
            # Go through and insert player and squad information
            for player in data['lineup']:
                country_info = player.get('country', {})
                country_id = country_info.get('id')
                country_name = country_info.get('name')

                # Insert only if both country_id and country_name exist
                if country_id and country_name:
                    check_and_insert_country(country_id, country_name)

                insert_player(player['player_id'], player['player_name'])

                insert_match_lineup(match_id, data['team_id'], player['player_id'], player.get('player_nickname'), player['jersey_number'], country_id, country_name)

# Commit the transaction and close the connection
conn.commit()
cur.close()
conn.close()
