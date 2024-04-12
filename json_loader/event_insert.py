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

# Determine if match_id exists
def match_id_exists(match_id):
    cur.execute("SELECT 1 FROM matches WHERE match_id = %s;", (match_id,))
    return cur.fetchone() is not None

# 'def insert_xxxx()' function is an insert statement for the corresponding table
def insert_team(team):
    cur.execute("""
        INSERT INTO teams (team_id, name) 
        VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING
        """, (team['id'], team['name'])
    )

def insert_player(player):
    cur.execute("""
        INSERT INTO players (player_id, player_name) 
        VALUES (%s, %s) ON CONFLICT (player_id) DO NOTHING
        """, (player['id'], player['name'])
    )

def insert_position(position):
    cur.execute("""
        INSERT INTO positions (position_id, position_name) 
        VALUES (%s, %s) ON CONFLICT (position_id) DO NOTHING
        """, (position['id'], position['name'])
    )

def insert_possession_team(event_id, team_id):
    cur.execute("""
        INSERT INTO possession_teams (event_id, team_id)
        VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING
        """, (event_id, team_id))

def insert_play_pattern(event_id, pattern_id, pattern_name):
    cur.execute("""
        INSERT INTO play_patterns (event_id, pattern_id, pattern_name)
        VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING
        """, (event_id, pattern_id, pattern_name))

def insert_related_events(event_id, related_event_id):
    cur.execute("""
        INSERT INTO related_events (event_id, related_event_id)
        VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (event_id, related_event_id))

def insert_event_tactics(event_id, formation):
    cur.execute("""
        INSERT INTO event_tactics (event_id, formation)
        VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING
        """, (event_id, formation))

def ensure_position_exists(possession_id):
    # Checks whether the specified possession_id exists
    cur.execute("SELECT 1 FROM positions WHERE position_id = %s;", (possession_id,))
    # If it does not exist, insert a placeholder name or get more information to complete the field
    if cur.fetchone() is None:
        cur.execute("INSERT INTO positions (position_id, position_name) VALUES (%s, %s);", (possession_id, f"Position {possession_id}"))

def insert_event(event, match_id):
    if 'team' in event:
        insert_team(event['team'])
    player_id = None
    if 'player' in event:
        player = event['player']
        player_id = player.get('id')
        insert_player(player)

    cur.execute("""
        INSERT INTO events (event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, duration, under_pressure, off_camera, out, player_id) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
        match_id = EXCLUDED.match_id,
        index = EXCLUDED.index,
        period = EXCLUDED.period,
        timestamp = EXCLUDED.timestamp,
        minute = EXCLUDED.minute,
        second = EXCLUDED.second,
        type_id = EXCLUDED.type_id,
        type_name = EXCLUDED.type_name,
        duration = EXCLUDED.duration,
        under_pressure = EXCLUDED.under_pressure,
        off_camera = EXCLUDED.off_camera,
        out = EXCLUDED.out,
        player_id = EXCLUDED.player_id
        """, (
            event['id'],
            match_id,
            event.get('index'),
            event.get('period'),
            event.get('timestamp'),
            event.get('minute'),
            event.get('second'),
            event['type']['id'] if 'type' in event else None,
            event['type']['name'] if 'type' in event else None,
            event.get('duration'),
            event.get('under_pressure', False),
            event.get('off_camera', False),
            event.get('out', False),
            player_id
        )
    )
    if 'location' in event:
        cur.execute("""
            INSERT INTO event_locations (event_id, location) 
            VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING
            """, (
                event['id'],
                json.dumps(event['location'])
            )
        )

# Read and process each JSON file, this is the root directory of my JSON file
for file_path in glob(r"C:\Users\vcck0\Desktop\final project\open-data-master\open-data-master\data\events\*.json"):
    # Gets match_id from the json file name
    match_id = os.path.splitext(os.path.basename(file_path))[0] 
    # Check if match_id exists in the matches table, skip if not
    if not match_id_exists(match_id):
        # print(f"Skipping events for non-existent match ID: {match_id}")
        continue

    with open(file_path, 'r', encoding='utf-8') as file:
        events = json.load(file)
        for event in events:
            insert_event(event, match_id)

# Commit the transaction and close the connection
conn.commit()
cur.close()
conn.close()
