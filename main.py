from flask import Flask,render_template,jsonify,request,session, abort,flash
from flaskext.mysql import MySQL
from statistics import mean
from scipy import spatial
import operator
import datetime
from query import get_connection
import pymysql.cursors
import query as qy
import pandas as pd
app = Flask(__name__ ,template_folder='templates')
import recommender
import os

global user_id
app.secret_key = "super secret key"
# Set user ID
#user_id = 12

# Get a connection
connection = qy.get_connection(user="root", password="msg@")

@app.route('/insert')
def insert():
	global user_id
	try:
		ingredient = request.args.get('ingredient', 0, type=str)
		amount = request.args.get('amount', 0, type=str)
		if ingredient == '' or amount == '':
			return jsonify(result='Wrong input, please try again.')
		else:
			sql = ("SELECT id FROM ingredients WHERE name='%s' " % str(ingredient))
			cur = connection.cursor(pymysql.cursors.DictCursor)
			cur.execute(sql)
			rows = cur.fetchone()
			date = datetime.datetime.now().date()
			insert = cur.execute("insert into food_history (mass, date, id_user, id_ingredient) values(%s,%s,%s,%s)",(int(amount),date,user_id,int(rows['id'])))
			connection.commit()
			return jsonify(result='Insert success')
	except Exception as e:
		return str(e)
	
@app.route('/')
def homepage():
	global user_id

	if not session.get('logged_in'):
		return (render_template('login.html'))
	else:


		sql = "SELECT * FROM food_history"
		cur = connection.cursor(pymysql.cursors.DictCursor)
		cur.execute(sql)
		rows = []
		for row in cur:
			rows.append(row['id'])
		return (render_template('homepage.html', data = rows))


@app.route("/search/<string:box>")
def process(box):
	global user_id
	query = request.args.get('query')
	dictionary = {}
	suggestions = []

	if(box == 'names'):

		sql = "SELECT name FROM ingredients WHERE name LIKE '%%%s%%'  " % str(query)
		cur = connection.cursor(pymysql.cursors.DictCursor)
		cur.execute(sql)
		j = 1
		for i in cur:
			dictionary['value'] = str(i['name'])
			dictionary['data'] = str(j)
			suggestions.append(dictionary)
			dictionary = {}
			j = j + 1
		
	return jsonify({"suggestions":suggestions})

@app.route('/about',methods=["GET"])
def about():
	global user_id

	sql =( "SELECT name FROM user_profile WHERE id=%s " % user_id)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	name = cur.fetchone()

	# Display history table of user
	dictionary = {}
	food = []
	sql = ("SELECT ing.name,hist.mass,hist.date FROM app.food_history AS hist INNER JOIN app.ingredients_imputed AS ing ON hist.id_ingredient = ing.id  WHERE hist.id_user = %s ORDER BY date DESC" % user_id)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	rows = cur.fetchall()
	print (rows)
	return (render_template('about.html', name =name['name'],data=rows))

@app.route('/recommendation',methods=["GET"])
def recommendation():

	global user_id
	#Get user's name
	sql =( "SELECT name FROM user_profile WHERE id=%s " % user_id)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	name = cur.fetchone()

	# Get recommended ingredients
	recommended_ingredients = recommender.get_recommended_ingredients(connection=connection, user_id=user_id, limit=40)

	# Get recommended history
	recommended_history = recommender.get_recommended_history(connection=connection, user_id=user_id, limit=5)

	print("EAT MORE")
	print(recommended_ingredients['name'])
	print("\n\n")
	print("EAT LESS")
	print(recommended_history['name'])
	return (render_template('recommendation.html',name=name['name'], food_more =recommended_ingredients['name'],food_less=recommended_history['name']))

@app.route('/addFood',methods=["GET"])
def addFood():
	global user_id
	# Get user name
	sql =( "SELECT name FROM user_profile WHERE id=%s " % user_id)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	name = cur.fetchone()

	# Get history food
	sql = ("SELECT ing.name,hist.mass,hist.date FROM app.food_history AS hist INNER JOIN app.ingredients_imputed AS ing ON hist.id_ingredient = ing.id  WHERE hist.id_user = %s ORDER BY date DESC LIMIT 5" % user_id)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	rows = cur.fetchall()


	return (render_template('addFood.html',name=name['name'], data=rows))


 
@app.route('/login', methods=['POST'])
def do_admin_login():
	
	
	global user_id
	usid=request.form['username']
	
	
	print (type(usid))
	sql =( "SELECT id FROM user_profile WHERE username LIKE '%%%s%%' " % usid)
	cur = connection.cursor(pymysql.cursors.DictCursor)
	cur.execute(sql)
	rows = cur.fetchone()
	print ("!!!")
	user_id=rows['id']
	print (user_id)
	if request.form['password'] == 'msg@':
		session['logged_in'] = True
	else:
		flash('wrong password!')
	return homepage()

@app.route("/logout")
def logout():
    session['logged_in'] = False
    return homepage()

if __name__ == '__main__':
	app.secret_key = "super secret key"
	app.run(debug=True)


