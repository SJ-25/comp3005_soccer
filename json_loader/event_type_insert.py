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
def insert_event_general_info(event, match_id):
    cur.execute("""
        INSERT INTO events (event_id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, duration, under_pressure, off_camera, out) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
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
            event.get('possession'),
            event.get('duration'),
            event.get('under_pressure', False),
            event.get('off_camera', False),
            event.get('out', False)
        )
    )

def insert_event_details(event):
    # All event_type requiring additional details
    event_type = event['type']['name']

    if event_type == "50/50":
        # Check if the '50/50' field exists
        fifty_fifty_data = event.get('50/50', {})

        # '50/50' Insert logic
        cur.execute("""
            INSERT INTO event_50_50 (event_id, outcome_id, outcome_name, counterpress)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name,
                counterpress = EXCLUDED.counterpress
            """, (
                event['id'],
                fifty_fifty_data.get('outcome', {}).get('id'),
                fifty_fifty_data.get('outcome', {}).get('name'),
                fifty_fifty_data.get('counterpress', False)
            )
        )

    elif event_type == "Bad Behaviour":
        # Check if the 'bad_behaviour' field exists
        bad_behaviour_data = event.get('bad_behaviour', {})

        # 'bad_behaviour' Insert logic
        cur.execute("""
            INSERT INTO event_bad_behaviour (event_id, card_id, card_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                card_id = EXCLUDED.card_id,
                card_name = EXCLUDED.card_name
            """, (
                event['id'],
                bad_behaviour_data.get('bad_behaviour', {}).get('card', {}).get('id'),
                bad_behaviour_data.get('bad_behaviour', {}).get('card', {}).get('name')
            )
        )

    elif event_type == "Ball Receipt":
        # Check if the 'ball_receipt' field exists
        ball_receipt_data = event.get('ball_receipt', {})

        cur.execute("""
            INSERT INTO event_ball_receipt (event_id, outcome_id, outcome_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name
            """, (
                event['id'],
                ball_receipt_data.get('ball_receipt', {}).get('outcome', {}).get('id'),
                ball_receipt_data.get('ball_receipt', {}).get('outcome', {}).get('name')
            )
        )

    elif event_type == "Ball Recovery":
        # Check if the 'ball_recovery' field exists
        ball_recovery_data = event.get('ball_recovery', {})

        cur.execute("""
            INSERT INTO event_ball_recovery (event_id, offensive, recovery_failure)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                offensive = EXCLUDED.offensive,
                recovery_failure = EXCLUDED.recovery_failure
            """, (
                event['id'],
                ball_recovery_data.get('offensive', False),
                ball_recovery_data.get('recovery_failure', False)
            )
        )

    elif event_type == "Block":
        # Check if the 'Block' field exists
        block_data = event.get('block', {})

        cur.execute("""
            INSERT INTO event_block (event_id, deflection, offensive, save_block, counterpress)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                deflection = EXCLUDED.deflection,
                offensive = EXCLUDED.offensive,
                save_block = EXCLUDED.save_block,
                counterpress = EXCLUDED.counterpress
            """, (
                event['id'],
                block_data.get('deflection', False),
                block_data.get('offensive', False),
                block_data.get('save_block', False),
                block_data.get('counterpress', False)
        )
    )

    elif event_type == "Carry":
        carry_data = event.get('carry', {})
        end_location = json.dumps(carry_data.get('end_location')) if 'end_location' in carry_data else None
        cur.execute("""
            INSERT INTO event_carry (event_id, end_location)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                end_location = EXCLUDED.end_location
            """, (
                event['id'],
                end_location
            )
        )

    elif event_type == "Clearance":
        clearance_data = event.get('clearance', {})

        cur.execute("""
            INSERT INTO event_clearance (event_id, aerial_won, body_part_id, body_part_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                aerial_won = EXCLUDED.aerial_won,
                body_part_id = EXCLUDED.body_part_id,
                body_part_name = EXCLUDED.body_part_name
            """, (
                event['id'],
                clearance_data.get('aerial_won', False),
                clearance_data.get('body_part', {}).get('id'),
                clearance_data.get('body_part', {}).get('name')
            )
        )

    elif event_type == "Dribble":
        dribble_data = event.get('dribble', {})

        cur.execute("""
            INSERT INTO event_dribble (event_id, overrun, nutmeg, outcome_id, outcome_name, no_touch)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                overrun = EXCLUDED.overrun,
                nutmeg = EXCLUDED.nutmeg,
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name,
                no_touch = EXCLUDED.no_touch
            """, (
                event['id'],
                dribble_data.get('overrun', False),  # Directly using dribble_data
                dribble_data.get('nutmeg', False),
                dribble_data.get('outcome', {}).get('id'),  # Accessing nested 'outcome' safely
                dribble_data.get('outcome', {}).get('name'),
                dribble_data.get('no_touch', False)
            )
        )

    elif event_type == "Dribbled Past":
        dribbled_past_data = event.get('dribbled_past', {})

        cur.execute("""
            INSERT INTO event_dribbled_past (event_id, counterpress)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                counterpress = EXCLUDED.counterpress
            """, (
                event['id'],
                dribbled_past_data.get('dribbled_past', {}).get('counterpress', False)
            )
        )

    elif event_type == "Duel":
        duel_data = event.get('duel', {})

        cur.execute("""
            INSERT INTO event_duel (event_id, duel_type_id, duel_type_name, outcome_id, outcome_name, counterpress)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                duel_type_id = EXCLUDED.duel_type_id,
                duel_type_name = EXCLUDED.duel_type_name,
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name,
                counterpress = EXCLUDED.counterpress
            """, (
                event['id'],
                duel_data.get('type', {}).get('id'),
                duel_data.get('type', {}).get('name'),
                duel_data.get('outcome', {}).get('id'),
                duel_data.get('outcome', {}).get('name'),
                duel_data.get('counterpress', False)
            )
        )

    elif event_type == "Foul Committed":
        foul_committed_data = event.get('foul_committed', {})
    
        cur.execute("""
            INSERT INTO event_foul_committed (event_id, counterpress, offensive, foul_type_id, foul_type_name, advantage, penalty, card_id, card_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                counterpress = EXCLUDED.counterpress,
                offensive = EXCLUDED.offensive,
                foul_type_id = EXCLUDED.foul_type_id,
                foul_type_name = EXCLUDED.foul_type_name,
                advantage = EXCLUDED.advantage,
                penalty = EXCLUDED.penalty,
                card_id = EXCLUDED.card_id,
                card_name = EXCLUDED.card_name
            """, (
                event['id'],
                foul_committed_data.get('counterpress', False),
                foul_committed_data.get('offensive', False),
                foul_committed_data.get('type', {}).get('id'),
                foul_committed_data.get('type', {}).get('name'),
                foul_committed_data.get('advantage', False),
                foul_committed_data.get('penalty', False),
                foul_committed_data.get('card', {}).get('id'),
                foul_committed_data.get('card', {}).get('name')
            )
        )

    elif event_type == "Foul Won":
        foul_won_data = event.get('foul_won', {})
    
        cur.execute("""
            INSERT INTO event_foul_won (event_id, defensive, advantage, penalty)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                defensive = EXCLUDED.defensive,
                advantage = EXCLUDED.advantage,
                penalty = EXCLUDED.penalty
            """, (
                event['id'],
                foul_won_data.get('defensive', False),
                foul_won_data.get('advantage', False),
                foul_won_data.get('penalty', False)
            )
        )

    elif event_type == "Goalkeeper":
        goalkeeper_data = event.get('goalkeeper', {})

        cur.execute("""
            INSERT INTO event_goalkeeper (event_id, position_id, position_name, technique_id, technique_name, body_part_id, body_part_name, type_id, type_name, outcome_id, outcome_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                position_id = EXCLUDED.position_id,
                position_name = EXCLUDED.position_name,
                technique_id = EXCLUDED.technique_id,
                technique_name = EXCLUDED.technique_name,
                body_part_id = EXCLUDED.body_part_id,
                body_part_name = EXCLUDED.body_part_name,
                type_id = EXCLUDED.type_id,
                type_name = EXCLUDED.type_name,
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name
            """, (
                event['id'],
                goalkeeper_data.get('position', {}).get('id'),
                goalkeeper_data.get('position', {}).get('name'),
                goalkeeper_data.get('technique', {}).get('id'),
                goalkeeper_data.get('technique', {}).get('name'),
                goalkeeper_data.get('body_part', {}).get('id'),
                goalkeeper_data.get('body_part', {}).get('name'),
                goalkeeper_data.get('type', {}).get('id'),
                goalkeeper_data.get('type', {}).get('name'),
                goalkeeper_data.get('outcome', {}).get('id'),
                goalkeeper_data.get('outcome', {}).get('name')
            )
        )

    elif event_type == "Half End":
        half_end_data = event.get('half_end', {})

        cur.execute("""
            INSERT INTO event_half_end (event_id, early_video_end, match_suspended)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                early_video_end = EXCLUDED.early_video_end,
                match_suspended = EXCLUDED.match_suspended
            """, (
                event['id'],
                half_end_data.get('early_video_end', False),
                half_end_data.get('match_suspended', False)
            )
        )

    elif event_type == "Half Start":
        half_start_data = event.get('half_start', {})

        cur.execute("""
            INSERT INTO event_half_start (event_id, late_video_start)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                late_video_start = EXCLUDED.late_video_start
            """, (
                event['id'],
                half_start_data.get('late_video_start', False)
            )
        )

    elif event_type == "Injury Stoppage":
        injury_stoppage_data = event.get('injury_stoppage', {})

        cur.execute("""
            INSERT INTO event_injury_stoppage (event_id, in_chain)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                in_chain = EXCLUDED.in_chain
            """, (
                event['id'],
                injury_stoppage_data.get('in_chain', False)
            )
        )

    elif event_type == "Interception":
        interception_data = event.get('interception', {})

        cur.execute("""
            INSERT INTO event_interception (event_id, outcome_id, outcome_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name
            """, (
                event['id'],
                interception_data.get('outcome', {}).get('id'),
                interception_data.get('outcome', {}).get('name')
            )
        )

    elif event_type == "Miscontrol":
        miscontrol_data = event.get('miscontrol', {})

        cur.execute("""
            INSERT INTO event_miscontrol (event_id, aerial_won)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                aerial_won = EXCLUDED.aerial_won
            """, (
                event['id'],
                miscontrol_data.get('aerial_won', False)
            )
        )

    elif event_type == "Pass":
        # Extract the 'pass' data for easier access
        pass_data = event.get('pass', {})
        # Extract nested data safely
        pass_type_data = pass_data.get('type', {})
        body_part_data = pass_data.get('body_part', {})
        recipient_data = pass_data.get('recipient', {})
        outcome_data = pass_data.get('outcome', {})
        technique_data = pass_data.get('technique', {})

        cur.execute("""
            INSERT INTO event_pass (event_id, recipient_id, recipient_name, pass_length, pass_angle, height_id,
            height_name, end_location, "cross", cut_back, "switch", shot_assist, goal_assist, body_part_id,
            body_part_name, pass_type_id, pass_type_name, outcome_id, outcome_name, technique_id, technique_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                recipient_id = EXCLUDED.recipient_id, recipient_name = EXCLUDED.recipient_name,
                pass_length = EXCLUDED.pass_length, pass_angle = EXCLUDED.pass_angle, 
                height_id = EXCLUDED.height_id, height_name = EXCLUDED.height_name,
                end_location = EXCLUDED.end_location, "cross" = EXCLUDED."cross",
                cut_back = EXCLUDED.cut_back, "switch" = EXCLUDED."switch",
                shot_assist = EXCLUDED.shot_assist, goal_assist = EXCLUDED.goal_assist,
                body_part_id = EXCLUDED.body_part_id, body_part_name = EXCLUDED.body_part_name,
                pass_type_id = EXCLUDED.pass_type_id, pass_type_name = EXCLUDED.pass_type_name,
                outcome_id = EXCLUDED.outcome_id, outcome_name = EXCLUDED.outcome_name,
                technique_id = EXCLUDED.technique_id, technique_name = EXCLUDED.technique_name
            """, (
                event['id'],
                recipient_data.get('id'),
                recipient_data.get('name'),
                pass_data.get('length'),
                pass_data.get('angle'),
                pass_data.get('height', {}).get('id'),
                pass_data.get('height', {}).get('name'),
                json.dumps(pass_data.get('end_location')),
                pass_data.get('cross', False),
                pass_data.get('cut_back', False),
                pass_data.get('switch', False),
                pass_data.get('shot_assist', False),
                pass_data.get('goal_assist', False),
                body_part_data.get('id'),
                body_part_data.get('name'),
                pass_type_data.get('id'),
                pass_type_data.get('name'),
                outcome_data.get('id'),
                outcome_data.get('name'),
                technique_data.get('id'),
                technique_data.get('name')
            )
        )

    elif event_type == "Player Off":
        cur.execute("""
            INSERT INTO event_player_off (event_id, permanent)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                permanent = EXCLUDED.permanent
            """, (
                event['id'],
                event.get('player_off', {}).get('permanent', False)
            )
        )

    elif event_type == "Pressure":
        pressure_data = event.get('pressure', {})
        cur.execute("""
            INSERT INTO event_pressure (event_id, counterpress)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                counterpress = EXCLUDED.counterpress
            """, (
                event['id'],
                pressure_data.get('counterpress', False)
            )
        )

    elif event_type == "Shot":
        shot_data = event.get('shot', {})
        # Extracting nested information safely using .get method
        body_part_data = shot_data.get('body_part', {})
        technique_data = shot_data.get('technique', {})
        shot_type_data = shot_data.get('type', {})
        outcome_data = shot_data.get('outcome', {})

        cur.execute("""
            INSERT INTO event_shot (
                event_id, statsbomb_xg, end_location, key_pass_id, 
                body_part_id, body_part_name, technique_id, technique_name, 
                shot_type_id, shot_type_name, outcome_id, outcome_name, 
                aerial_won, follows_dribble, first_time, freeze_frame, open_goal
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                statsbomb_xg = EXCLUDED.statsbomb_xg,
                end_location = EXCLUDED.end_location,
                key_pass_id = EXCLUDED.key_pass_id,
                body_part_id = EXCLUDED.body_part_id,
                body_part_name = EXCLUDED.body_part_name,
                technique_id = EXCLUDED.technique_id,
                technique_name = EXCLUDED.technique_name,
                shot_type_id = EXCLUDED.shot_type_id,
                shot_type_name = EXCLUDED.shot_type_name,
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name,
                aerial_won = EXCLUDED.aerial_won,
                follows_dribble = EXCLUDED.follows_dribble,
                first_time = EXCLUDED.first_time,
                freeze_frame = EXCLUDED.freeze_frame,
                open_goal = EXCLUDED.open_goal
            """, (
                event['id'],
                shot_data.get('statsbomb_xg'),
                json.dumps(shot_data.get('end_location')),
                shot_data.get('key_pass_id'),
                body_part_data.get('id'),
                body_part_data.get('name'),
                technique_data.get('id'),
                technique_data.get('name'),
                shot_type_data.get('id'),
                shot_type_data.get('name'),
                outcome_data.get('id'),
                outcome_data.get('name'),
                shot_data.get('aerial_won', False),  # 假设缺失时默认值为 False
                shot_data.get('follows_dribble', False),  # 假设缺失时默认值为 False
                shot_data.get('first_time', False),  # 假设缺失时默认值为 False
                json.dumps(shot_data.get('freeze_frame', [])),  # 假设缺失时默认值为 []
                shot_data.get('open_goal', False)  # 假设缺失时默认值为 False
            )
        )

    elif event_type == "Substitution":
        substitution_data = event.get('substitution', {})
        # Extracting nested information safely using .get method
        replacement_data = substitution_data.get('replacement', {})
        outcome_data = substitution_data.get('outcome', {})

        cur.execute("""
            INSERT INTO event_substitution (event_id, replacement_id, replacement_name, outcome_id, outcome_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                replacement_id = EXCLUDED.replacement_id,
                replacement_name = EXCLUDED.replacement_name,
                outcome_id = EXCLUDED.outcome_id,
                outcome_name = EXCLUDED.outcome_name
            """, (
                event['id'],
                replacement_data.get('id'),
                replacement_data.get('name'),
                outcome_data.get('id'),
                outcome_data.get('name')
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

    with open(file_path, 'r') as file:
        events = json.load(file) 
        for event in events:
            # Use functions to insert information
            insert_event_general_info(event, match_id)
            insert_event_details(event)

# Commit the transaction and close the connection
conn.commit()
cur.close()
conn.close()
