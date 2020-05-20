#1o passo. add ip do pc na whitelist do hostgator
#2o passo. definir um horario para o script ser executado 
#3o passo. verificar se o nmr de registros de pacientes foi modificado
#4o passo. selecionar todos pacientes no servidor do cerest
#5o passo. selecionar o prontuario de todos pacientes
#6o passo. mesclar informacoes
#7o passo. inserir dados (paciente+prontuario) no servidor da hostgator


#pip install schedule
#pip install time
#python -m pip install mysql-connector

import mysql.connector
import schedule
import time

def send_hostgator():
	mydb = mysql.connector.connect(
	  host="192.185.210.227",
	  user="vinilpub_guilher",
	  passwd="302050027",
	  database="vinilpub_guilherme_cerest"
	)
	mycursor = mydb.cursor()
	mycursor.execute("SELECT * FROM ips_entraram_site")
	myresult = mycursor.fetchall()
	for x in myresult:
		print(x)

def get_cerests_server_data():
	mydb = mysql.connector.connect(
	  host="192.168.7.41",
	  user="cerest",
	  passwd="302050027",
	  database="cerestdb"
	)
	mycursor = mydb.cursor()
	mycursor.execute("SELECT * FROM relatoriofaa WHERE CGS='145780'")
	myresult = mycursor.fetchall()
	for x in myresult:
		print(x)

def job(t):
    print ("I'm working...", t)
    get_cerests_server_data()
    return

schedule.every().day.at("12:00").do(job,'It is 12:00')

while True:
    schedule.run_pending()
    time.sleep(10) # wait one minute


#########################################
