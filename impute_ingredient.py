import pandas as pd
import query as qy
import numpy as np
from fancyimpute import KNN


# FETCH DATA
# Get a connection
connection = qy.get_connection(user="root", password="msg@")

# Get all ingredients
sql = "SELECT * FROM app.ingredients"
cursor = qy.execute_query(connection=connection, sql=sql)

# Make the data frame
df_ingredients = pd.DataFrame(cursor.fetchall())

required_columns = [col for col in df_ingredients if col.startswith(('macro', 'vitamin', 'mineral', 'sup'))]


# IMPUTE VALUES
# Scale the values for the regressions
column_scaling = {}
for column_name in required_columns:
    column_std = np.nanstd(df_ingredients[column_name])
    column_mean = np.nanmean(df_ingredients[column_name])
    column_scaling[column_name] = {'mean': column_mean, 'std': column_std}
    df_ingredients['scaled_' + column_name] = (df_ingredients[column_name] - column_mean) / column_std

    # Feature engineering zero valued columns
    df_ingredients['zero_valued_' + column_name] = (df_ingredients[column_name] == 0) - 0.5
    df_ingredients.loc[df_ingredients[column_name].isnull(), 'zero_valued_' + column_name] = None

scaled_columns = [col for col in df_ingredients if col.startswith('scaled_')]
zero_valued_columns = [col for col in df_ingredients if col.startswith('zero_valued_')]
imputation_matrix = df_ingredients[scaled_columns].as_matrix()

# Impute missing values
imputed_values = KNN(k=5).complete(imputation_matrix)
df_scaled_imputed = pd.DataFrame(imputed_values)
df_scaled_imputed.columns = scaled_columns  # + zero_valued_columns

# Un-scale the values
for column_name in required_columns:
    df_ingredients['imputed_' + column_name] = (df_scaled_imputed['scaled_' + column_name] * column_scaling[column_name]['std']) + column_scaling[column_name]['mean']


# UPDATE VALUES IN DB

# Clear old table
sql = 'DROP TABLE IF EXISTS app.ingredients_imputed; ' \
      'CREATE TABLE app.ingredients_imputed LIKE app.ingredients;' \
      'INSERT app.ingredients_imputed SELECT * FROM app.ingredients;'

# Get rows with NaNs in required cols
df_ingredients_with_nans = df_ingredients[df_ingredients[required_columns].isnull().any(axis=1)]
qy.execute_query(connection=connection, sql=sql)

# Push updates for each row with missing values
sql_pattern = "UPDATE app.ingredients_imputed SET %s WHERE id = %d"
for index, row in df_ingredients_with_nans.iterrows():
    column_values = ''
    for column_name in required_columns:
        if np.isnan(row[column_name]):
            column_value = '%s = %d, ' % (column_name, max([row['imputed_' + column_name], 0]))
            column_values += column_value

    if len(column_values) > 0:
        column_values = column_values[0:-2]
        sql = sql_pattern % (column_values, row['id'])
        qy.execute_query(connection=connection, sql=sql)

# Commit all updates
connection.commit()
