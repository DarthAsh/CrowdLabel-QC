import pandas as pd
import mysql.connector
import math

def is_empty_val(v):
    # Treat NaN or empty string as "empty"
    return (
        (isinstance(v, float) and math.isnan(v)) or
        (isinstance(v, str) and v.strip() == "")
    )

def fill_team_ids_and_tags(csv_path, **mysql_kwargs):
    # --------------------------------------------
    # Hard-coded DB connection from:
    # mysql://root@root:3306/quality_control
    #   user     = root
    #   password = root
    #   host     = localhost
    #   port     = 3306
    #   db       = quality_control
    # --------------------------------------------
    conn_params = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "quality_control",
    }
    conn_params.update({k: v for k, v in mysql_kwargs.items() if v is not None})

    conn = mysql.connector.connect(**conn_params)
    cursor = conn.cursor()

    # --- 1. Read CSV ---
    df = pd.read_csv(csv_path)

    # Find team_id column (allow "team_id" or "team _id")
    col_team = None
    for c in df.columns:
        if c.strip().lower() == "team_id":
            col_team = c
            break
    if col_team is None:
        raise ValueError("No column named 'team_id' (or variant) found in CSV.")

    # Find user_id column
    col_user = None
    for c in df.columns:
        if c.strip().lower() == "tagger_id":
            col_user = c
            break
    if col_user is None:
        raise ValueError("No column named 'tagger_id' found in CSV.")

    # Tags column name as given
    col_tags = "# Tags Available"
    if col_tags not in df.columns:
        # If it doesn't exist, create it with default 0 (or NaN if you prefer)
        df[col_tags] = 0

    # --- 2. Fill missing team_id values using view2 ---
    for idx, row in df.iterrows():
        team_val = row[col_team]

        if not is_empty_val(team_val):
            continue  # already has team_id, skip

        user_id = row[col_user]

        cursor.execute(
            """
            SELECT team_id
            FROM view2
            WHERE assignment_id = %s AND user_id = %s
            LIMIT 1
            """,
            (1205, user_id)
        )
        result = cursor.fetchone()

        if result is not None:
            df.at[idx, col_team] = result[0]

    # --- 3. For each row, set "# Tags Available" using the CTE query ---
    # To avoid querying repeatedly for the same team_id, cache results per team_id.
    tags_cache = {}

    tags_query = """
    WITH answered AS (
      SELECT
        a.id AS answer_id,
        q.id AS question_id,
        CASE q.questionnaire_id
          WHEN 753 THEN 2
          WHEN 754 THEN 1
          ELSE 0
        END AS replaced_questionnaire_id
      FROM response_maps rm
      JOIN responses r ON rm.id = r.map_id
      JOIN answers a ON r.id = a.response_id
      JOIN questions q ON a.question_id = q.id
      JOIN assignment_questionnaires aq ON aq.questionnaire_id = q.questionnaire_id
      WHERE rm.reviewee_id = %s
        AND r.is_submitted = 1
        AND a.comments <> ''
        AND aq.assignment_id = 1205
        AND q.type = 'Criterion'
    )
    SELECT
      SUM(replaced_questionnaire_id) AS total_replaced_value
    FROM answered
    """

    for idx, row in df.iterrows():
        team_val = row[col_team]

        if is_empty_val(team_val):
            # No team_id, cannot compute tags; skip or set to 0
            df.at[idx, col_tags] = 0
            continue

        team_id = int(team_val)

        if team_id not in tags_cache:
            cursor.execute(tags_query, (team_id,))
            result = cursor.fetchone()
            total_tags = result[0] if result and result[0] is not None else 0
            tags_cache[team_id] = total_tags

        df.at[idx, col_tags] = tags_cache[team_id]
    df = df.iloc[:-1]
    # --- 4. Overwrite the original CSV ---
    df.to_csv(csv_path, index=False)

    # --- 5. Cleanup ---
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # Example usage
    fill_team_ids_and_tags("pattern-detections-20251127-235009.csv")
