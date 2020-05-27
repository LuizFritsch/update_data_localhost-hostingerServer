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
	var_string=''
	query_colunas_tabela="DESCRIBE "+nomeTabela
	cursor.execute(query_colunas_tabela)
	colunas=cursor.fetchall()
	var_string = ', '.join('?' * len(colunas))
	for dado in dados:
		query='INSERT INTO '+nomeTabela+''+str(colunas)+' VALUES (%s);' % var_string
		cursor.executemany(query,dado)
		cursor.commit()

def delete_hostgator(nomeTabela):
	query_colunas_tabela="DELETE FROM "+nomeTabela+" WHERE 1=1"
	cursor.execute(query_colunas_tabela)
	cursor.commit()

def verifica_se_precisa_atualizacao(nomeTabela):
	conn=connect_cerests_server()
	cursor=conn.cursor()

	cursor.execute("SELECT * FROM "+nomeTabela)
	dadosServidor=cursor.fetchall()
	nomeArquivo='qtdRegistros'+nomeTabela+'.txt'
	qtdRegistrosArquivo=ler_arquivo()
	qtdRegistrosServidorCerest=len(dadosServidor)

	if (qtdRegistrosArquivo!=qtdRegistrosServidorCerest):
		return dadosServidor
	else:
		return None

def magica():

	nomeTabelas=['relatoriofaa','paciente','profissionais']
	nomeTabelasHostgator=['prontuario','paciente','profissionais']
	for tabela,tabelaHostgator in zip(nomeTabelas,nomeTabelasHostgator):
		connCerest=connect_cerests_server()
		cursorCerest=connCerest.cursor()
		if dados=verifica_se_precisa_atualizacao(tabela) is not None:
			sobescrever_aquivo(qtdRegistrosServidorCerest)
			connHostg=connect_hostgators_server()
			cursorHostg=connCerest.cursor()
			delete_hostgator(tabelaHostgator)
			if tabela=='relatoriofaa':
			insere_hostgator(tabela,dados)

	#myresult=cursor.fetchall()
	
	'''qtdRegistrosArquivo=ler_arquivo()
	qtdRegistrosServidorCerest=len(myresult)

	#se a qtd de registros no arquivo for diferente da que tem no servidor do cerest, envia tudo para o serv da hostgator
	if qtdRegistrosArquivo!=qtdRegistrosServidorCerest:
		sobescrever_aquivo(qtdRegistrosServidorCerest)'''
	


	#recebe uma lista de lista
	#exemplo: lista=[(registro1Nome,Registro1Tel),(registro2Nome,Registro1Te2)]
	#ordem da lista dentro da lista: COD,PROFISSIONAL,PROCEDIMENTO,FAA,DATA,CGS,PACIENTE,MES,ANO,AREA
	#for x in myresult:
		#print(x)

def abrir_arquivo_qt_linhas(operacao):
	arquivo=open(nomeArquivo,operacao)
	return arquivo

def ler_arquivo():
	arquivo=abrir_arquivo_qt_linhas('r')
	qtdRegistros=arquivo.read()
	arquivo.close()
	return qtdRegistros

def sobescrever_aquivo(qtdRegistros):
	arquivo=abrir_arquivo_qt_linhas('w')
	arquivo.write(str(qtdRegistros))
	arquivo.close()

def job(t):
    print ("I'm working...", t)
    magica()
    return

schedule.every().day.at("11:59").do(job,'It is 12:00')

while True:
    schedule.run_pending()
    time.sleep(10) # wait one minute


#########################################



'''
Todo dia:
selecionar todos pacientes servidor cerest
verificar se count pacientes serv cerest != count(*) pacientes serv hostgator
	se sim, inserir todos pacientes serv cerest
		se paciente n tem usuario com mesmo cpf, criar usuario


Todo mes:
Selecionar todos relatoriofaa (prontuarios)
Verificar se count relatoriofaa =!count prontuarios serv hostgator
	Insere todos relatoriofaa em prontuarios

'''
#selecionar_dados_pacientes_servidor_cerest()

'''

def test1(): #no error method
	cn = connection()
	cur = cn.cursor()
	cur.execute("SELECT * FROM relatoriofaa WHERE CGS='145780'")
	print cur.fetchone()

def get_hostgator():
	conn=connect_cerests_server()
	mycursor=conn.cursor()
	mycursor.execute("SELECT * FROM ips_entraram_site")
	myresult=mycursor.fetchall()
	for x in myresult:
		print(x)
