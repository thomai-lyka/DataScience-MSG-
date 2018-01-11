import pandas as pd
import snapshot as sn
import query as qy
from scipy.spatial.distance import cosine
from recommender import get_user_dri

def get_average_balanced_ingredient(connection, user_dri):
    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table

    RETURNS
    a dict containing the nutrient breakdown of a perfect food for that users DRI
    """

    def cosine_similarity_old(row, user_dri):
        row_dict = row.to_dict()
        vector_row = []
        vector_dri = []
        for row_key, row_value in row_dict.items():
            vector_row.append(row_value / user_dri[row_key])
            vector_dri.append(1)

        return cosine(vector_row, vector_dri)

    # Get result
    sql = "SELECT * FROM app.ingredients_imputed"
    cursor = qy.execute_query(connection=connection, sql=sql)

    # Make the data frame
    df_query = pd.DataFrame(cursor.fetchall())
    filter_cols = [col for col in df_query if col.startswith(('macro', 'vitamin', 'mineral'))]

    # Order foods by cosine-similarity to DRI
    df_query['dri_cos_sim'] = df_query[filter_cols].apply(cosine, axis=1, args=[user_dri])
    df_query.sort_values(by='dri_cos_sim', inplace=True)

    # Get median values from top 50 foods
    balanced_ingredient = df_query[filter_cols].head(n=1000).mean(axis=0).to_dict()

    return balanced_ingredient


def get_average_ingredient(connection):
    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table

    RETURNS
    a dict containing the average nutrient breakdown of all foods
    """

    # Get result
    sql = "SELECT * FROM app.ingredients_imputed"
    cursor = qy.execute_query(connection=connection, sql=sql)

    # Make the data frame
    df_query = pd.DataFrame(cursor.fetchall())
    filter_col = [col for col in df_query if col.startswith(('macro', 'vitamin', 'mineral', 'sup_energy'))]

    # Create a data frame each for nutrients and mass
    df_nutrients = df_query[filter_col]

    # Calculate average
    df_nutrient_average = df_nutrients.mean(axis=0)

    return df_nutrient_average


# TESTING CODE
if __name__ == "__main__":

    user_id = 10

    # Get a connection
    connection = qy.get_connection(user="root", password="msg@")

    # Get the user's dri
    dri = get_user_dri(connection=connection, user_id=user_id)

    # CREATE A BALANCED INGREDIENT

    # Average ingredient
    average_ingredient = get_average_ingredient(connection=connection)

    # Average of ingredients with high cosine similarity to dri
    average_dri_based_ingredient = get_average_balanced_ingredient(connection=connection, user_dri=dri)
