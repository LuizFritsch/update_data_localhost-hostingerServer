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
nomeArquivo='qtdRegistros.txt'

def connect_hostgators_server():
	mydb = mysql.connector.connect(
	  host="192.185.210.227",
	  user="vinilpub_guilher",
	  passwd="302050027",
	  database="vinilpub_guilherme_cerest"
	)
	mycursor=mydb.cursor()
	return mycursor

def connect_cerests_server():
	mydb = mysql.connector.connect(
	  host="192.168.7.41",
	  user="cerest",
	  passwd="302050027",
	  database="cerestdb"
	)
	mycursor = mydb.cursor()
	return mycursor

def get_hostgator():
	mycursor=connect_hostgators_server()
	mycursor.execute("SELECT * FROM ips_entraram_site")
	myresult = mycursor.fetchall()
	for x in myresult:
		print(x)

def compair_data():
	mycursor=connect_cerests_server()
	mycursor.execute("SELECT * FROM relatoriofaa")
	

	myresult = mycursor.fetchall()
	
	qtdRegistrosArquivo=ler_arquivo()
	qtdRegistrosServidorCerest=len(myresult)

	print(qtdRegistrosArquivo)
	print(qtdRegistrosServidorCerest)

	if qtdRegistrosArquivo!=qtdRegistrosServidorCerest:
		print(qtdRegistrosServidorCerest)
		sobescrever_aquivo(qtdRegistrosServidorCerest)
	#recebe uma lista de lista
	#exemplo: lista=[(registro1Nome,Registro1Tel),(registro2Nome,Registro1Te2)]
	#ordem da lista dentro da lista: COD,PROFISSIONAL,PROCEDIMENTO,FAA,DATA,CGS,PACIENTE,MES,ANO,AREA
	#for x in myresult:
		#print(x)

def abrir_arquivo_qt_linhas(operacao):
	arquivo = open(nomeArquivo,operacao)
	return arquivo

def ler_arquivo():
	arquivo=abrir_arquivo_qt_linhas('r')
	qtdRegistros = arquivo.read()
	arquivo.close()
	return qtdRegistros

def sobescrever_aquivo(qtdRegistros):
	arquivo=abrir_arquivo_qt_linhas('w')
	arquivo.writelines(qtdRegistros)
	arquivo.close()

def job(t):
    print ("I'm working...", t)
    compair_data()
    return

schedule.every().day.at("10:51").do(job,'It is 12:00')

while True:
    schedule.run_pending()
    time.sleep(10) # wait one minute


#########################################
