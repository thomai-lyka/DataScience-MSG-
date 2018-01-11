import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import query as qy
import recommender

def query_db(query, connection):
    # query         - SQL string
    # connection    - pymysql.connect connection object
    # RETURNS       - a cursor

   with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor

connection = qy.get_connection(user="root", password="msg@")



#GET DATA
# Define the SQL query
query = "SELECT * FROM app.ingredients_imputed"

# Get the results cursor
cursor = query_db(query, connection)

# Make the data frame
df_query = pd.DataFrame(cursor.fetchall())


nutrients_start_with = ('macro', 'vitamin', 'mineral', 'sup')
filter_col = [col for col in df_query if col.startswith(nutrients_start_with)]

# Get data without NaN
df_query_complete = df_query.dropna(axis='index', subset=filter_col)


# CLUSTER

def cluster(df_to_cluster, method, num_clusters, dri_user=None):

    headers = []
    mean_col = {}
    std_col = {}

    for column in df_to_cluster:
        headers.append(column)

        # Normalise if required
        if method == "normalise":
            mean_col[column] = np.mean(df_to_cluster[column])
            std_col[column] = np.std(df_to_cluster[column])
            df_to_cluster[column] = (df_to_cluster[column] - mean_col[column]) / std_col[column]
        elif method == "dri_normalise":
            df_to_cluster[column] = df_to_cluster[column] / dri_user[column]

    clusterer = KMeans(n_clusters = num_clusters)
    clusterer.fit(df_to_cluster.as_matrix())
    centroids = clusterer.cluster_centers_
    cluster_ids_of_rows = clusterer.labels_

    list_centroids = []
    for i, centroid in enumerate(centroids):
        row_dict = dict(zip(headers, centroid))
        row_dict['cluster_id'] = i
        list_centroids.append(row_dict)

    df_centroids = pd.DataFrame(list_centroids)

    # De-normalise the centroids if required
    if method == "normalise":
        for column in df_centroids:
            if column == 'cluster_id':
                continue
            df_centroids[column] = (df_centroids[column] * mean_col[column]) + std_col[column]
    elif method == "dri_normalise":
        for column in df_centroids:
            if column == 'cluster_id':
                continue
            df_centroids[column] = df_centroids[column] * dri_user[column]

    return df_centroids, cluster_ids_of_rows, headers


# SET CLUSTER METHOD ID, and the clustering function
METHODS = {1: ("raw", ('macro', 'vitamin', 'mineral', 'sup')),
           2: ("normalise", ('macro', 'vitamin', 'mineral', 'sup')),
           3: ("dri_normalise", ('macro', 'vitamin', 'mineral'))}
METHOD_ID = 2
USER_ID = 1
NUM_CLUSTERS = 40

# Calculate params
method = METHODS[METHOD_ID][0]
nutrients_start_with = METHODS[METHOD_ID][1]
dri_user = recommender.get_user_dri(connection, nutrients_start_with, USER_ID)

# CLUSTER DATA
# Get centroids and cluster IDs for each row
df_centroids, cluster_ids_of_rows, headers = cluster(df_to_cluster=df_query_complete[filter_col],
                                                     method=method,
                                                     num_clusters=NUM_CLUSTERS,
                                                     dri_user=dri_user
                                                     )

# PUSH DATA TO SERVER
# Push cluster IDs of rows to server
df_query_complete["cluster_id"] = cluster_ids_of_rows

sql = "UPDATE app.ingredients_imputed SET cluster_method = %s, cluster_id = %s WHERE id = %s"
for row in df_query_complete.iterrows():
     qy.execute_query(connection=connection, sql=sql, insert_data=(METHOD_ID, row[1]['cluster_id'], row[1]['id']))
connection.commit()

# Push centroids to server
# Clear old data with same method ID
sql = "DELETE FROM app.cluster WHERE method_id = %s"
qy.execute_query(connection=connection, sql=sql, insert_data=(METHOD_ID,))

# Push new data
sql = "INSERT INTO app.cluster (%s) VALUES (%s)"
columns = list(df_centroids.columns.values)
for row in df_centroids.iterrows():
    column_string = 'method_id' + ", "
    column_values = str(int(METHOD_ID)) + ", "
    for column in columns:
        column_string += column + ', '
        if column == 'cluster_id':
            column_values += str(int(row[1][column])) + ', '
        else:
            column_values += str(row[1][column]) + ', '

    column_string = column_string[0:-2]    # Trim the last comma and space
    column_values = column_values[0:-2]    # Trim the last comma and space
    qy.execute_query(connection=connection, sql=sql % (column_string, column_values))

connection.commit()
connection.close()