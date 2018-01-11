import pandas as pd
import snapshot as sn
import query as qy
from datetime import date
from scipy.spatial.distance import cosine

def get_recommended_clusters(connection, user_id, limit=None):
    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table
    num_clusters    number of recommended clusters required
    RETURNS
    a data frame containing the recommended clusters, sorted by best cluster first
    """

    df_recommended = get_recommended(connection=connection,
                                     return_type='clusters',
                                     user_id=user_id,
                                     limit=limit)

    return df_recommended

def get_recommended_ingredients(connection, user_id, limit=None):
    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table
    RETURNS
    a data frame containing the recommended ingredients, sorted by best cluster first
    """

    # Get recommended ingredients
    df_all = get_recommended(connection=connection,
                             return_type='ingredients',
                             user_id=user_id)

    df_recommended = df_all.groupby('cluster_id').head(2).reset_index()

    df_recommended.sort_values(by='cosine_sim',
                               inplace=True,
                               ascending=False)

    return df_recommended.head(limit)


def get_recommended_history(connection, user_id, limit=None):
    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table
    num_clusters    number of recommended clusters required
    RETURNS
    a data frame containing the ingredients the users should cut down on, sorted worst first
    """

    df_recommended = get_recommended(connection=connection,
                                     return_type='history',
                                     user_id=user_id,
                                     limit=limit)

    df_recommended['weighted_cos'] = df_recommended['cosine_sim'].mul(df_recommended['mass'], axis=0)

    df_recommended.sort_values(by='weighted_cos',
                             inplace=True,
                             ascending=False)

    return df_recommended


def get_user_dri(connection, nutrients_start_with, user_id):

    """
    ARGS
    connection      a pyMysql connection object
    user_id         id of a user from the user profile table
    RETURNS
    a series, ordered by nutrient name A-Z, containing the nutrient breakdown of a user's DRI
    """

    # Get user record
    sql = "SELECT * FROM app.user_profile WHERE id = " + str(user_id)
    user = qy.execute_query(connection=connection, sql=sql).fetchone()

    # Determine age group
    dob = user['dob']
    today = date.today()
    age = today.year - dob.year - (today.month < dob.month and today.day < dob.day)

    if age <= 3:
        age_group = 1
    elif age <= 8:
        age_group = 2
    elif age <= 13:
        age_group = 3
    elif age <= 18:
        age_group = 4
    elif age <= 30:
        age_group = 5
    elif age <= 50:
        age_group = 6
    elif age <= 70:
        age_group = 7
    else:
        age_group = 8

    # Get DRI record
    sql = "SELECT * FROM app.dri WHERE sex = %s AND age_group = %s LIMIT 1"
    cursor = qy.execute_query(connection=connection, sql=sql, insert_data=(user['sex'], age_group))
    series_dri = pd.Series(cursor.fetchone())
    required_cols = [key for key in series_dri.keys() if key.startswith(nutrients_start_with)]
    ordered_dri = series_dri[required_cols].sort_index()

    return ordered_dri

def get_dri_diff(snapshot, dri, nutrients_start_with):
    """
    ARGS
    snapshot                a series containing the nutrient breakdown of a user's snapshot
    dri                     a series containing the nutrient breakdown of a user's DRI
    nutrients_start_with    an iterable containing the first few characters of the required nutrients
    RETURNS
    a dict frame containing recommended ingredients/clusters which balance the user's DRI
    """

    return pd.Series({key: (snapshot[key] / value) - 1
                      for key, value in dri.items()
                      if key.startswith(nutrients_start_with)
                      }
                     )

def get_recommended(connection, return_type, user_id, limit=None, cluster_ids=None):

    """
    ARGS
    connection              a pyMysql connection object
    return_type             either 'clusters', 'ingredients' or 'history'
    user_id                 id of a user from the user profile table
    limit                   the number of records to return from the head of the data set
    cluster_ids             list of ids
    RETURNS
    a data frame containing recommended ingredients/clusters which balance the user's DRI
    """

    # Set required nutrients
    nutrients_start_with = ('macro', 'vitamin', 'mineral')

    # Get the user nutritional history and requirements
    ordered_snapshot = sn.get_user_snapshot(connection=connection,
                                            nutrients_start_with=nutrients_start_with,
                                            user_id=user_id)

    ordered_dri = get_user_dri(connection=connection,
                               nutrients_start_with=nutrients_start_with,
                               user_id=user_id)

    # Get required nutrition vector
    dri_diff = get_dri_diff(snapshot=ordered_snapshot,
                            dri=ordered_dri,
                            nutrients_start_with=nutrients_start_with)

    # Check return_type is valid
    if return_type not in ['clusters', 'ingredients', 'history']:
        raise ValueError("Invalid value for 'return_type'. Use 'clusters', 'ingredient' or 'history'")

    # Get the data to sort
    if return_type == 'history':
        df_to_sort = sn.get_user_history(connection, user_id)
        sort_ascending = True

    else:
        if return_type == 'clusters':
            sql = "SELECT * FROM app.cluster WHERE method_id = 3"
        elif return_type == 'ingredients':
            sql = "SELECT * FROM app.ingredients_imputed WHERE 1 AND NOT cluster_id IN (2, 23, 33, 7, 35, 31, 25, 14, 15, 16, 9, 22)"
        if cluster_ids:
            sql += " AND cluster_id IN (%s)" % ','.join(str(cluster_id) for cluster_id in cluster_ids)

        cursor = qy.execute_query(connection=connection, sql=sql)
        df_to_sort = pd.DataFrame(cursor.fetchall())
        sort_ascending = False

    # Convert the values of the ingredients to nutrition_vector space
    for i, col in enumerate(ordered_dri.index):
        df_to_sort[col] = df_to_sort[col].mul(1/ordered_dri[col], axis=0)

    # Sort the data
    df_sorted = sort_df_by_cosine_similarity(data_to_sort=df_to_sort,
                                             nutrition_vector=dri_diff,
                                             nutrients_start_with=nutrients_start_with,
                                             sort_ascending=sort_ascending,
                                             limit=limit)

    return df_sorted


def sort_df_by_cosine_similarity(data_to_sort, nutrition_vector, nutrients_start_with, sort_ascending, limit=None):

    """
    ARGS
    data_to_sort            a data frame containing ingredients/clusters with nutritional information
    nutrition_vector        an matrix like object containing a set of nutrients as ordered in
    nutrients_start_with    an iterable containing the first few characters of the required nutrients
    sort_ascending          a boolean specifying if the data is to be sorted in ascending order of the cosine similarity
    limit                   the number of records to return from the head of the data set
    RETURNS
    the data_set data frame ordered by cosine similarity to the nutrition_vector
    """

    required_cols = [col for col in data_to_sort if col.startswith(nutrients_start_with)]
    df_nutrients = data_to_sort[required_cols].sort_index(axis=1)

    # Confirm nutrients columns match nutrition_vector keys
    if len(nutrition_vector.keys()) != len(df_nutrients.keys()):
        raise ValueError("Columns mismatch between nutrition_vector and ingredients/clusters table")

    for i, col in enumerate(df_nutrients):
        if col != nutrition_vector.keys()[i]:
            raise ValueError("Columns mismatch between nutrition_vector and ingredients/clusters table")

    nutrition_vector_values = nutrition_vector.values.astype(float)

    # Order foods by cosine-similarity to nutrition vector
    data_to_sort['cosine_sim'] = df_nutrients.apply(cosine, raw=True, axis=1, args=[nutrition_vector_values])
    data_to_sort.sort_values(by='cosine_sim',
                             inplace=True,
                             ascending=sort_ascending)

    # Return required number of records
    if limit:
        return data_to_sort.head(n=limit)
    else:
        return data_to_sort


# TESTING CODE
if __name__ == "__main__":

    # Set user ID
    user_id = 2

    # Get a connection
    connection = qy.get_connection(user="root", password="msg@")

    # Get recommended ingredients
    print('running')
    recommended_ingredients = get_recommended_ingredients(connection=connection, user_id=user_id, limit=150)

    # Get recommended history
    recommended_history = get_recommended_history(connection=connection, user_id=user_id, limit=5)

    print("EAT MORE")
    print(recommended_ingredients[['name', 'cluster_id']])

    print(recommended_ingredients[['cluster_id']])

    print("\n\n")
    print("EAT LESS")
    print(recommended_history['name'])