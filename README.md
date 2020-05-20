# update_data_localhost-hostingerServer
Script criado para coletar dados de pacientes (informações do paciente+prontuário+histórico de atendimentos) do servidor do Cerest e enviá-los diariamente para o servidor da hostinger


# Dependencias:

* pip install schedule
* pip install time
* python -m pip install mysql-connector

# Adicionando script na lista de processos que iniciam com o SO:

* Linux:
```console
WhoAmI@WhoAmI:~$ nohup python2.7 MyScheduledProgram.py &
```


* Windows:
```console
```
