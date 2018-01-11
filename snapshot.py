import pandas as pd
import query as qy
import datetime

def get_user_history(connection, user_id):

    """
    ARGS
    connection      a pyMysql connection object
    nutrients_start_with    an iterable containing the first few characters of the required nutrients
    user_id         id of a user from the user profile table

    RETURNS
    a series, ordered by nutrient name A-Z, containing the nutrient breakdown of a user's snapshot
    """

    # Get min and max dates
    sql = "SELECT MAX(date) AS 'max_date' FROM food_history WHERE id_user=%s"
    cursor = qy.execute_query(connection=connection, sql=sql, insert_data=(user_id,))
    max_date = cursor.fetchone()['max_date']
    min_date = max_date - datetime.timedelta(days=6)

    # Get food hist for user between min and max dates
    sql = "SELECT hist.mass, ing.*" \
          " FROM app.food_history AS hist INNER JOIN app.ingredients_imputed AS ing ON hist.id_ingredient = ing.id" \
          " WHERE hist.id_user = %s" \
          " AND hist.date >= %s" \
          " AND hist.date <= %s"
    cursor = qy.execute_query(connection=connection, sql=sql, insert_data=(user_id, min_date, max_date))

    # Make the data frame
    df_history = pd.DataFrame(cursor.fetchall())

    return df_history

def get_user_snapshot(connection, nutrients_start_with, user_id):

    """
    ARGS
    connection      a pyMysql connection object
    nutrients_start_with    an iterable containing the first few characters of the required nutrients
    user_id         id of a user from the user profile table

    RETURNS
    a series, ordered by nutrient name A-Z, containing the nutrient breakdown of a user's snapshot
    """

    # Get user history
    df_history = get_user_history(connection=connection, user_id=user_id)

    # Calculate the ratios
    required_cols = [col for col in df_history if col.startswith(nutrients_start_with)]
    ordered_snapshot = df_history[required_cols].mul(df_history['mass'] / (7 * 100), axis=0).sum(axis=0).sort_index()

    return ordered_snapshot
