# encoding=utf8
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
import json
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
	host="192.185.210.227"
	user="vinilpub_guilher"
	passwd="302050027"
	database="vinilpub_guilherme_cerest"
	cn = connection(host,user,passwd,database)
	return cn

def connect_cerests_server():
	host="192.168.7.41"
	user="cerest"
	passwd="302050027"
	database="cerestdb"
	cn = connection(host,user,passwd,database)
	return cn


#insert generico que basta passar o nome da tabela e a lista de dados.
#insere qqr coisa em qqr tabela(dados so precisam estarem normalizados)
def insere_hostgator(nomeTabela,dados):
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	var_string=''
	query_colunas_tabela="DESCRIBE "+nomeTabela
	cursor.execute(query_colunas_tabela)
	colunas=cursor.fetchall()
	var_string = ', '.join('?' * len(colunas))
	print("inserindo dados:")
	for v in dados.values():
	    cols = v.keys()
	    vals = v.values()
	    sql = "INSERT INTO "+nomeTabela+" ({}) VALUES ({})".format(
	        ', '.join(cols),
	        ', '.join(['%s'] * len(cols)));
	    try:
	    	print("sql: "+sql)
	    	print("values: "+vals)
	    	print("---")	    	
	        cursor.execute(sql, vals)
	    except Exception as e:
	        pass
	conn.commit()

def delete_hostgator(nomeTabela):
	conn=connect_hostgators_server()
	cursor=conn.cursor()
	query_colunas_tabela="DELETE FROM "+nomeTabela+" WHERE 1=1"
	cursor.execute(query_colunas_tabela)
	conn.commit()

def verifica_se_precisa_atualizacao(nomeTabela):
	conn=connect_cerests_server()
	cursor=conn.cursor()

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
	if (nomeTabela=='relatoriofaa'):
		i=0
		prontuarios={}
		for linha in dados:
			nome_profissional=str(linha[1])
			nome_paciente=linha[6]
			prontuarios[i]={
				"ID":"null",
				"PROCEDIMENTO":linha[2],
				"FAA":linha[3],
				"DATA":linha[4],
				"CGS":linha[5],
				"FK_ID_PROFISSIONAL":"(SELECT ID FROM profissional WHERE ID LIKE '%"+nome_profissional+"%')",
				"FK_ID_PACIENTE":"(SELECT ID FROM paciente WHERE NOME LIKE '%"+nome_paciente+"%')",
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
		for linha in dados:
			nome_paciente=linha[1]
			pacientes[i]={
				"ID":"null",
				"CARTAO_SUS":"",
				"DATA_NASCIMENTO":"",
				"OCUPACAO":"",
				"NATURALIDADE":"",
				"NOME_MAE":"",
				"PROFISSAO":"",
				"FOTO":"",
				"STATUS_TRABALHO":"",
				"FK_ID_USUARIO_COMUM":"(SELECT ID FROM usuario_comum WHERE NOME LIKE '%"+nome_paciente+"%')"
			}
			i+=1
		return pacientes

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
			delete_hostgator(tabelaHostgator)
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



while True:
    schedule.run_pending()
    time.sleep(5) # wait one minute
