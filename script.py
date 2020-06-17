# -*- coding: utf-8 -*-
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
import mysql.connector as connector
import os
import requests
import hashlib
import datetime


def ha_conexao():
	url='http://www.google.com/'
	timeout=5
	try:
		_ = requests.get(url, timeout=timeout)
		return True
	except requests.ConnectionError:
		return False


def connection(host,user,passwd,database):

    config = {
	  	"host":host,
	  	"user":user,
	  	"password":passwd,
		"port":3306,
	  	"database":database
	}
    try:
		c = connector.connect(**config)
		return c
    except:
		print "connection error"
		exit(1)

def connect_hostgators_server():
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 2 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(2)
	host="192.185.210.227"
	user="vinilpub_guilher"
	passwd="302050027"
	database="vinilpub_guilherme_cerest"
	cn = connection(host,user,passwd,database)
	return cn

def connect_cerests_server():
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	host="192.168.7.41"
	user="cerest"
	passwd="302050027"
	database="cerestdb"
	cn = connection(host,user,passwd,database)
	return cn


#insert generico que basta passar o nome da tabela e a lista de dados.
#insere qqr coisa em qqr tabela(dados so precisam estarem normalizados)
def insere_hostgator(nomeTabela,dados):
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	var_string=''
	query_colunas_tabela="DESCRIBE "+nomeTabela
	cursor.execute(query_colunas_tabela)
	colunas=cursor.fetchall()
	var_string = ', '.join('?' * len(colunas))
	i=0
	print ('----------------------------------------')
	print ('inserindo dados na tabela '+nomeTabela)
	for v in dados.values():
	    cols = v.keys()
	    vals = v.values()
	    sql = """INSERT INTO """+nomeTabela+""" ({}) VALUES ({})""".format(
	        """, """.join(cols),
	        """, """.join(["""%s"""] * len(cols)));
	    try:
	    	cursor.execute(sql, vals)
	    	conn.commit()
	    except Exception as e:
	    	i+=1
	    	print"ERR0 NMR: "+str(i)+": "+str(e)
	    	pass

def delete_hostgator(nomeTabela):
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	print ('----------------------------------------')
	print ('deletando dados na tabela '+nomeTabela)
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	query_colunas_tabela="DELETE FROM "+nomeTabela+" WHERE 1=1"
	cursor.execute(query_colunas_tabela)
	conn.commit()

def verifica_se_precisa_atualizacao(nomeTabela):
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	conn=connect_cerests_server()
	cursor=conn.cursor()
	print ('----------------------------------------')
	print ('verificando dados na tabela '+nomeTabela)
	now = datetime.datetime.now()
	print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
	if nomeTabela=='relatoriofaa':
		cursor.execute("SELECT * FROM "+nomeTabela+" ORDER BY paciente")
	else:	
		cursor.execute("SELECT * FROM "+nomeTabela)
	dadosServidor=cursor.fetchall()
	nomeArquivo='qtdRegistros'+nomeTabela+'.txt'
	qtdRegistrosArquivo=ler_arquivo(nomeArquivo)
	qtdRegistrosServidorCerest=len(dadosServidor)
	if (int(qtdRegistrosArquivo)!=int(qtdRegistrosServidorCerest)):
		sobescrever_aquivo(nomeArquivo,qtdRegistrosServidorCerest)
		print('os dados da tabela '+nomeTabela+' precisam serem atualizados...')
		return dadosServidor
	else:
		print('nao eh necessario atualizar estes dados...')
		return None

def normaliza_dados(nomeTabela,dados):


	while (ha_conexao()==False):
		print '-----------------------------------------------'
		print 'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	print ('----------------------------------------')
	print ('normalizando dados na tabela '+nomeTabela)
	now = datetime.datetime.now()
	print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
	if (nomeTabela=='relatoriofaa'):
		i=0
		prontuarios={}
		nome_anterior=""
		for linha in dados:
			try:
				nome_paciente=linha[6]
				print '1'
				if nome_paciente!=nome_anterior:
					nome_anterior=nome_paciente
					nome_paciente=nome_paciente[:-1]
					print '2'
					FK_ID_USUARIO_COMUM="(SELECT ID FROM usuario_comum WHERE NOME_COMPLETO LIKE '%"+nome_paciente+"%')"
					print FK_ID_USUARIO_COMUM
					id_paciente=select_hostgator(FK_ID_USUARIO_COMUM)
					id_paciente_anterior=id_paciente
				else:
					print '3'
					id_paciente=id_paciente_anterior
				id_profissional=linha[1]
				PROCEDIMENTO=linha[2]
				FAA=linha[3]
				DATA=linha[4]
				CGS=linha[5]				
				prontuarios[i]={
					"ID":"null",
					"PROCEDIMENTO":PROCEDIMENTO,
					"FAA":FAA,
					"DATA":DATA,
					"CGS":CGS,
					"FK_ID_PROFISSIONAL":id_profissional,
					"FK_ID_PACIENTE":id_paciente
				}
				print id_paciente
			except Exception as e:
				pass
			i+=1
		return prontuarios	
		

	if (nomeTabela=='profissionais'):
		i=0
		profissionais={}
		for linha in dados:
			try:
				profissionais[i]={
					"ID":str(linha[0]),
					"NOME":str(linha[1])
				}
			except Exception as e:
				pass
			i+=1
		return profissionais	
		
	if (nomeTabela=='usuario_comum'):
		i=0
		usuario_comum={}
		for linha in dados:
			try:
				senha= hashlib.md5(str(linha[2]).encode()).hexdigest()
				USUARIO=linha[2]
				NOME_COMPLETO=linha[1]
				CPF=get_nmr(linha[13])
				CELULAR=get_nmr(linha[4])
				LOCAL_TRABALHO=linha[16]
				usuario_comum[i]={
					"ID":"null",
					"USUARIO":USUARIO,
					"SENHA":senha,
					"NOME_COMPLETO":NOME_COMPLETO,
					"CPF":CPF,
					"RG":"",
					"CELULAR":CELULAR,
					"ENDERECO":"",
					"EMAIL":"",
					"LOCAL_TRABALHO":LOCAL_TRABALHO,
					"FK_ID_FUNCAO":"2",
					"FK_ID_MUNICIPIO":"31453",
					"FK_ID_ESTADO":"158",
				}	
			except Exception as e:
				pass
			i+=1
		return usuario_comum

	if (nomeTabela=='paciente'):
		i=0
		pacientes={}
		usuario_comum={}
		for linha in dados:
			try:
				nome_paciente=linha[1]
				FK_ID_USUARIO_COMUM="""(SELECT ID FROM usuario_comum WHERE NOME_COMPLETO LIKE '%"""+nome_paciente+"""%')"""
				fk_id_usuario_comum=select_hostgator(FK_ID_USUARIO_COMUM)
				CARTAO_SUS=linha[2]
				DATA_NASCIMENTO=linha[3]
				OCUPACAO=linha[11]
				NATURALIDADE=linha[14]
				NOME_MAE=linha[15]
				PROFISSAO=linha[16]
				pacientes[i]={
					"ID":"null",
					"CARTAO_SUS":CARTAO_SUS,
					"DATA_NASCIMENTO":DATA_NASCIMENTO,
					"OCUPACAO":OCUPACAO,
					"NATURALIDADE":NATURALIDADE,
					"NOME_MAE":NOME_MAE,
					"PROFISSAO":PROFISSAO,
					"FOTO":"null",
					"STATUS_TRABALHO":"1",
					"FK_ID_USUARIO_COMUM":fk_id_usuario_comum
				}
				senha= hashlib.md5(str(linha[2]).encode()).hexdigest()
				USUARIO=linha[2]
				NOME_COMPLETO=linha[1]
				CPF=get_nmr(linha[13])
				CELULAR=get_nmr(linha[4])
				LOCAL_TRABALHO=linha[16]
				usuario_comum[i]={
					"ID":"null",
					"USUARIO":USUARIO,
					"SENHA":senha,
					"NOME_COMPLETO":NOME_COMPLETO,
					"CPF":CPF,
					"RG":"",
					"CELULAR":CELULAR,
					"ENDERECO":"",
					"EMAIL":"",
					"LOCAL_TRABALHO":LOCAL_TRABALHO,
					"FK_ID_FUNCAO":"2",
					"FK_ID_MUNICIPIO":"31453",
					"FK_ID_ESTADO":"158",
				}
			except Exception as e:
				pass
			i+=1
		return pacientes,usuario_comum

def get_nmr(x):
	if x == '' or x == None:
		return ''
	else:
		x=x.replace('.', '')
		x=x.replace('-', '')
		return x

def select_hostgator(sql):
	while (ha_conexao()==False):
		print'-----------------------------------------------'
		print'nao ha conexao, tentando reconectar em 5 seg...'
		now = datetime.datetime.now()
		print (str(now.year)+'/'+str(now.month)+'/'+str(now.day)+' '+ str(now.hour)+':'+str(now.minute)+':'+str(now.second))
		time.sleep(5)
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	try:
		cursor.execute(sql)
		select=cursor.fetchone()
		return str(select[0])
	except Exception as e:
		pass

def magica():

	nomeTabelas=['paciente','profissionais','relatoriofaa']
	nomeTabelasHostgator=['paciente','profissional','prontuario']

	'''
	Percorro as duas listas com nome de tabelas paralelamente.
	Pra cada tabela, abro conexao com o servidor do cerest e verifico se precisa ser atualizada no servidor da hostgator
	Se precisa, sobreescrevo o arquivo antigo contendo o numero de registros daquela tabela, deleto todos registros do servidor da hostgator, normalizo os dados e insiro os registros novos.
	'''
	for tabela,tabelaHostgator in zip(nomeTabelas,nomeTabelasHostgator):
		
		try:
			
			dados=verifica_se_precisa_atualizacao(tabela)

			if dados is not None:
				if tabela=='paciente':
					pacientes,usuario_comum=normaliza_dados(tabela,dados)

					insere_hostgator('usuario_comum',usuario_comum)

					insere_hostgator('paciente',pacientes)
				else:

					#delete_hostgator(tabelaHostgator)

					insere_hostgator(tabelaHostgator,normaliza_dados(tabela,dados))
		except Exception as e:
			print e
		
			
	#recebe uma lista de listas
	#exemplo: lista=[(registro1Nome,Registro1Tel),(registro2Nome,Registro1Te2)]
	#ordem da lista dentro da lista: COD,PROFISSIONAL,PROCEDIMENTO,FAA,DATA,CGS,PACIENTE,MES,ANO,AREA
	#for x in myresult:
		#printx)

def abrir_arquivo_qt_linhas(nomeArquivo,operacao):
	arquivo=open(nomeArquivo,operacao)
	return arquivo

def ler_arquivo(nomeArquivo):
	arquivo=abrir_arquivo_qt_linhas(nomeArquivo,'r')
	qtdRegistros=arquivo.read()
	arquivo.close()
	return qtdRegistros

def sobescrever_aquivo(nomeArquivo,qtdRegistros):
	arquivo=abrir_arquivo_qt_linhas(nomeArquivo,'w')
	arquivo.write(str(qtdRegistros))
	arquivo.close()

def job(t):
    print ("I'm working...", t)
    magica()
    return

#schedule.every().day.at("01:00").do(job,'It is 01:00AM keep ys safe, very spooky inside here')

job('now')

'''while True:
    schedule.run_pending()
    time.sleep(5) # wait one minute
'''