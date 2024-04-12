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

# 'def insert_xxxx()' function is an insert statement for the corresponding table
def insert_country(cur, country):
    if country['id'] is not None:
        cur.execute("""
            INSERT INTO countries (country_id, country_name) 
            VALUES (%s, %s) ON CONFLICT (country_id) DO NOTHING
            """, (country['id'], country['name'])
        )

def insert_competition(cur, competition):
    cur.execute("""
        INSERT INTO competitions (competition_id, name, country_name) 
        VALUES (%s, %s, %s) ON CONFLICT (competition_id) DO NOTHING
        """, (competition['competition_id'], competition['competition_name'], competition['country_name'])
    )

def insert_season(cur, season, competition_id):
    cur.execute("""
        INSERT INTO seasons (season_id, competition_id, name) 
        VALUES (%s, %s, %s) ON CONFLICT (season_id) DO NOTHING
        """, (season['season_id'], competition_id, season['season_name'])
    )

def insert_competition_stage(cur, stage_id, name):
    cur.execute("""
        INSERT INTO competition_stages (stage_id, name) 
        VALUES (%s, %s) ON CONFLICT (stage_id) DO NOTHING
        """, (stage_id, name)
    )

def insert_stadium(cur, stadium):
    cur.execute("""
        INSERT INTO stadiums (stadium_id, name, country_id) 
        VALUES (%s, %s, %s) ON CONFLICT (stadium_id) DO NOTHING
        """, (stadium['id'], stadium['name'], stadium['country']['id'])
    )

def insert_referee(cur, referee):
    cur.execute("""
        INSERT INTO referees (referee_id, name, country_id) 
        VALUES (%s, %s, %s) ON CONFLICT (referee_id) DO NOTHING
        """, (referee['id'], referee['name'], referee['country']['id'])
    )

def insert_team(cur, team, team_id):
    # Adjusting the key names according to your JSON structure
    team_name = team['home_team_name'] if 'home_team_name' in team else team['away_team_name']
    # Using .get() to provide a default value if the key is missing
    team_gender = team.get('home_team_gender', 'unknown')
    country_id = team['country']['id']

    cur.execute("""
        INSERT INTO teams (team_id, name, gender, country_id) 
        VALUES (%s, %s, %s, %s) ON CONFLICT (team_id) DO NOTHING
        """, (team_id, team_name, team_gender, country_id)
    )

def insert_manager(cur, manager, team_id):
    # Make sure the country information is inserted
    country = manager['country']
    insert_country(cur, country)

    # Insert manager information
    cur.execute("""
        INSERT INTO managers (manager_id, name, dob, country_id, team_id) 
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (manager_id) DO NOTHING
        """, (manager['id'], manager['name'], manager['dob'], country['id'], team_id)
    )

def insert_match(cur, match, season_id):
    # Check if 'stadium' and 'referee' keys are present, if not use the default None
    stadium_id = match['stadium']['id'] if 'stadium' in match else None
    referee_id = match['referee']['id'] if 'referee' in match else None

    cur.execute("""
        INSERT INTO matches (match_id, competition_id, season_id, match_date, kick_off, 
                             stadium_id, referee_id, home_team_id, away_team_id, 
                             home_score, away_score, match_status, match_week, competition_stage_id, last_updated) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        ON CONFLICT (match_id) DO NOTHING
        """, (
            match['match_id'], match['competition']['competition_id'], season_id,
            match['match_date'], match['kick_off'], stadium_id, referee_id, 
            match['home_team']['home_team_id'], match['away_team']['away_team_id'], match['home_score'], 
            match['away_score'], match['match_status'], match['match_week'], match['competition_stage']['id'], 
            match['last_updated']
        )
    )

# Set the required season_id. If restrictions are not required, you can comment or delete them
allowed_season_ids = {'90', '42', '4', '44'}

# This is the root directory of my JSON file
root_dir = r'C:\Users\vcck0\Desktop\final project\open-data-master\open-data-master\data\matches'

# Use glob to search for all JSON files in root_dir and all its subdirectories
for file_path in glob(os.path.join(root_dir, '**', '*.json'), recursive=True):
    # Gets season_id from the json file name
    season_id = os.path.basename(file_path).split('.')[0]
    
    # Make sure season_id is required, skip if not
    if season_id not in allowed_season_ids:
        continue
    with open(file_path, 'r') as file:
        data = json.load(file)

        if isinstance(data, dict):
            data = [data]

        # Walk through each element in the list
        for data in data:
            try:
                # Insert country information
                insert_country(cur, data['home_team']['country'])
                insert_country(cur, data['away_team']['country'])
                if 'stadium' in data and 'country' in data['stadium']:
                    insert_country(cur, data['stadium']['country'])
                if 'referee' in data:
                    insert_country(cur, data['referee']['country'])
                
                # For competition country_name, adjust the structure
                competition_country = {'id': None, 'name': data['competition']['country_name']}
                insert_country(cur, competition_country)

                # Use functions to insert information
                insert_competition(cur, data['competition'])

                insert_season(cur, data['season'], data['competition']['competition_id'])

                if 'stadium' in data:
                    insert_stadium(cur, data['stadium'])

                if 'referee' in data:
                    insert_referee(cur, data['referee'])

                if 'home_team' in data:
                    insert_team(cur, data['home_team'], data['home_team']['home_team_id'])

                if 'away_team' in data:
                    insert_team(cur, data['away_team'], data['away_team']['away_team_id'])

                if 'competition_stage' in data:
                    insert_competition_stage(cur, data['competition_stage']['id'], data['competition_stage']['name'])

                if 'managers' in data['home_team']:
                    for manager in data['home_team']['managers']:
                        insert_manager(cur, manager, data['home_team']['home_team_id'])

                if 'managers' in data['away_team']:
                    for manager in data['away_team']['managers']:
                        insert_manager(cur, manager, data['away_team']['away_team_id'])

                insert_match(cur, data, season_id)

            except TypeError as e:
                print(f"Error processing item in file {file_path}: {e}")
                continue 
        
        # Submit data after each file is processed
        conn.commit()

# Close the cursor and connection
cur.close()
conn.close()