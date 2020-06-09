#!/usr/bin/python
# -*- coding: latin-1 -*-
# coding: utf-8
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
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 2 seg...')
		time.sleep(2)
	host="192.185.210.227"
	user="vinilpub_guilher"
	passwd="302050027"
	database="vinilpub_guilherme_cerest"
	cn = connection(host,user,passwd,database)
	return cn

def connect_cerests_server():
	while (ha_conexao()==False):
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
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
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
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
	    	print(vals)
	    	cursor.execute(sql, vals)
	    	conn.commit()
	    except Exception as e:
	    	i+=1
	    	print("ERR0 NMR: "+str(i)+": "+str(e))

def delete_hostgator(nomeTabela):
	while (ha_conexao()==False):
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
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
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
		time.sleep(5)
	conn=connect_cerests_server()
	cursor=conn.cursor()
	print ('----------------------------------------')
	print ('verificando dados na tabela '+nomeTabela)
	cursor.execute("SELECT * FROM "+nomeTabela)
	dadosServidor=cursor.fetchall()
	nomeArquivo='qtdRegistros'+nomeTabela+'.txt'
	qtdRegistrosArquivo=ler_arquivo(nomeArquivo)
	qtdRegistrosServidorCerest=len(dadosServidor)

	if (qtdRegistrosArquivo!=qtdRegistrosServidorCerest):
		sobescrever_aquivo(nomeArquivo,qtdRegistrosServidorCerest)
		return dadosServidor
	else:
		return None

def normaliza_dados(nomeTabela,dados):
	while (ha_conexao()==False):
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
		time.sleep(5)
	print ('----------------------------------------')
	print ('normalizando dados na tabela '+nomeTabela)
	if (nomeTabela=='relatoriofaa'):
		i=0
		prontuarios={}
		for linha in dados:
			nome_profissional=str(linha[1])
			nome_paciente=linha[6]
			PROCEDIMENTO=linha[2]
			FAA=linha[3]
			DATA=linha[4]
			CGS=linha[5]
			FK_ID_PROFISSIONAL="""(SELECT ID FROM profissional WHERE ID LIKE '%"""+nome_profissional+"""%')"""
			FK_ID_PACIENTE="""(SELECT ID FROM paciente WHERE NOME LIKE '%"""+nome_paciente+"""%')"""
			prontuarios[i]={
				"ID":"null",
				"PROCEDIMENTO":PROCEDIMENTO,
				"FAA":FAA,
				"DATA":DATA,
				"CGS":CGS,
				"FK_ID_PROFISSIONAL":select_hostgator(FK_ID_PROFISSIONAL),
				"FK_ID_PACIENTE":select_hostgator(FK_ID_PACIENTE)
			}
			i+=1
		return prontuarios

	if (nomeTabela=='profissionais'):
		i=0
		profissionais={}
		for linha in dados:
			profissionais[i]={
				"ID":linha[0],
				"NOME":linha[1]
			}
			i+=1
		return profissionais

	if (nomeTabela=='paciente'):
		i=0
		pacientes={}
		usuario_comum={}
		for linha in dados:
			nome_paciente=linha[1]
			FK_ID_USUARIO_COMUM="""(SELECT ID FROM usuario_comum WHERE NOME_COMPLETO LIKE '%"""+nome_paciente+"""%')"""
			pacientes[i]={
				"ID":"null",
				"CARTAO_SUS":linha[2],
				"DATA_NASCIMENTO":linha[3],
				"OCUPACAO":linha[11],
				"NATURALIDADE":linha[14],
				"NOME_MAE":linha[15],
				"PROFISSAO":linha[16],
				"FOTO":"null",
				"STATUS_TRABALHO":"1",
				"FK_ID_USUARIO_COMUM":select_hostgator(FK_ID_USUARIO_COMUM)
			}
			senha= hashlib.md5(linha[2].encode()).hexdigest()
			usuario_comum[i]={
				"ID":"null",
				"USUARIO":linha[2],
				"SENHA":senha,
				"NOME_COMPLETO":linha[1],
				"CPF":linha[13],
				"RG":"",
				"CELULAR":linha[4],
				"ENDERECO":"",
				"EMAIL":"",
				"LOCAL_TRABALHO":linha[16],
				"FK_ID_FUNCAO":"2",
				"FK_ID_MUNICIPIO":"31453",
				"FK_ID_ESTADO":"158",
			}
			i+=1
		return pacientes,usuario_comum

def select_hostgator(sql):
	while (ha_conexao()==False):
		print('-----------------------------------------------')
		print('nao ha conexao, tentando reconectar em 5 seg...')
		time.sleep(5)
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	cursor.execute(sql)
	select=cursor.fetchone()
	return select

def magica():

	nomeTabelas=['paciente','profissionais','relatoriofaa']
	nomeTabelasHostgator=['paciente','profissional','prontuario']
	'''
	Percorro as duas listas com nome de tabelas paralelamente.
	Pra cada tabela, abro conexao com o servidor do cerest e verifico se precisa ser atualizada no servidor da hostgator
	Se precisa, sobreescrevo o arquivo antigo contendo o numero de registros daquela tabela, deleto todos registros do servidor da hostgator, normalizo os dados e insiro os registros novos.
	'''
	for tabela,tabelaHostgator in zip(nomeTabelas,nomeTabelasHostgator):
		

		dados=verifica_se_precisa_atualizacao(tabela)

		if dados is not None:
			if tabela=='paciente':
				pacientes,usuario_comum=normaliza_dados(tabela,dados)
				print('1')
				insere_hostgator('usuario_comum',usuario_comum)
				print('2')
				delete_hostgator('paciente')
				print('3')
				insere_hostgator('paciente',pacientes)
			else:
				print('4')
				delete_hostgator(tabelaHostgator)
				print('5')
				insere_hostgator(tabelaHostgator,normaliza_dados(tabela,dados))
			
	#recebe uma lista de listas
	#exemplo: lista=[(registro1Nome,Registro1Tel),(registro2Nome,Registro1Te2)]
	#ordem da lista dentro da lista: COD,PROFISSIONAL,PROCEDIMENTO,FAA,DATA,CGS,PACIENTE,MES,ANO,AREA
	#for x in myresult:
		#print(x)

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

schedule.every().day.at("11:57").do(job,'It is 12:00')

job('now')

'''
while True:
    schedule.run_pending()
    time.sleep(5) # wait one minute
'''