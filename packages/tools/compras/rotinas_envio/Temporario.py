import asyncio
import codecs
import hashlib
import json
from builtins import print

import requests
import pandas as pd
import bth.db_connector as db
import bth.cloud_connector as cloud

params_exec = {
    'clicodigo': '2016',  # ignoreline,,
    'somente_pre_validar': False,  # ignoreline,
    'token': '9d687396-29e1-49b7-a759-66cd549da9ba',  # ignoreline, # Token Service Layer #ignoreline,
    'entidade': '11',  # Nome exato da entidade a qual quer pegar o token do oaut2 #ignoreline,
    'exercicio': 2016,  # ignoreline,
    'db_name': 'Frotas',  # Nome do banco para conexão sybase #ignoreline,
    'db_user': 'desbth',  # Nome do banco para conexão sybase #ignoreline,
    'db_pw': 'HsV0zk3yXYACypmaRsZ1m5POCC4v2y',  # Nome do banco para conexão sybase #ignoreline,
    'db_host': 'localhost',  # Nome do banco para conexão sybase #ignoreline,
    'db_port': '9008',  # ignoreline,

}

# sql = """SELECT  * FROM compras.compras.responsaveis r where cpf_membro3 is null and i_entidades = 11"""
#
# for x in db.consulta_sql(sql, params_exec, index_col='nome_titular'):
#     # print(x)
#     print(f"""update compras.compras.responsaveis set cpf_membro3 = '{cloud.geraCpf()}' where membro_com3 is null i_responsavel in ({x['i_responsavel']}) and i_entidades = 11""")


"""Altera o responsavel da comissão para PRESIDENTE e/ou PREGOEIRO"""
# for x in db.consulta_sql(sql="select * FROM bethadba.controle_migracao_registro cmro where tipo_registro = 'comissoes-licitacao' and i_chave_dsk1 = '11'", params_exec=params_exec,index_col='tipo_registro'):
#     print(x['id_gerado'])
#     url = f"https://compras.betha.cloud/compras-services/api/comissoes-licitacao/{x['id_gerado']}"
#     lista = []
#     lista.append(cloud.buscaServiceLayerSemContent(url=url, token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f'))
#     for z in lista:
#         for indice, y in enumerate(z['membros']):
#             if y['atribuicao']['valor'] != 'PREGOEIRO' and y['atribuicao']['valor'] != 'MEMBRO':
#                     print(f"Comissão: {z['id']} -> Responsavel: {y}")
#                     z['membros'][indice]['atribuicao']['valor'] = 'PREGOEIRO'
#                     z['membros'][indice]['atribuicao']['descricao'] = 'PREGOEIRO'
#                     print(json.dumps(z))
#                     cloud.PUT_service_layer(url=url, token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', data=json.dumps(z))


"""Altera data de exoneração para a mesma data de Expiração"""
# for x in db.consulta_sql(sql="select * FROM bethadba.controle_migracao_registro cmro where tipo_registro = 'comissoes-licitacao' and i_chave_dsk1 = '11'", params_exec=params_exec,index_col='tipo_registro'):
#     print(x['id_gerado'])
#     url = f"https://compras.betha.cloud/compras-services/api/comissoes-licitacao/{x['id_gerado']}"
#     lista = []
#     lista.append(cloud.buscaServiceLayerSemContent(url=url, token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f'))
#     for z in lista:
#         if z['dataExoneracao']:
#             z['dataExoneracao'] = z['dataExpiracao']
#             print(json.dumps(z))
#             cloud.PUT_service_layer(url=url, token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', data=json.dumps(z))


"""Busca os id dos processos administrativos no cloud e verifica se existe no desktop, caso contrario faz update."""
# inicial = int(input("Ano Inicial: "))
# final = int(input("Ano Final: "))
# for percorre in range(inicial,final+1):
#     url = f'https://compras.betha.cloud/compras-services/api/exercicios/{percorre}/processos-administrativo'
#     for x in cloud.buscaServiceLayer(token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', url=url):
#         if len(db.consulta_sql(sql=f"select * from bethadba.controle_migracao_registro where tipo_registro = 'processos-administrativo' and i_chave_dsk1 = 11 and i_chave_dsk2 = {str(x['dataProcesso'])[0:4]} and i_chave_dsk3 ={x['numeroProcesso']} and id_gerado = {x['id']}", index_col='tipo_registro', params_exec=params_exec)) > 0:
#             for dsk in db.consulta_sql(sql=f"select * from bethadba.controle_migracao_registro where tipo_registro = 'processos-administrativo' and i_chave_dsk1 = 11 and i_chave_dsk2 = {str(x['dataProcesso'])[0:4]} and i_chave_dsk3 ={x['numeroProcesso']} and id_gerado = {x['id']}",
#                                        index_col='tipo_registro',
#                                        params_exec=params_exec):
#                 print(dsk)
#         else:
#             print(f"Cloud: {x['id']} Processo: {x['numeroProcesso']} {str(x['dataProcesso'])[0:4]} ")
#             sql = f"update bethadba.controle_migracao_registro set id_gerado = {x['id']} where tipo_registro ='processos-administrativo' and i_chave_dsk1 = 11 and i_chave_dsk2 = {str(x['dataProcesso'])[0:4]} and i_chave_dsk3 = {x['numeroProcesso']}"
#             db.execute_sql(sql=sql, params_exec=params_exec)

# url = 'https://compras.betha.cloud/compras-services/api/materiais'
#
# materiais = db.consulta_sql(sql="select id_gerado, tipo_registro from bethadba.controle_migracao_registro where "
#                                 "tipo_registro = 'materiais' and "
#                                 "i_chave_dsk1 = 11", params_exec=params_exec, index_col='tipo_registro')
#
# header = {'content-type': 'application/json', 'Authorization': f'Bearer 2f862daa-aef8-4fb6-a5c2-57cf0469fb0f'}
#
# for item in materiais:
#     req = requests.get(url=url + '/' + item['id_gerado'], headers=header)
#     if not req.ok:
#         print(item)


"""Gera arquivo para inserir na tabela de materiais do frotas"""
# def gerar_hash_chaves(*args):
# #     chaves = ''
# #     for item in args:
# #         chaves += str(item)
# #     hash_chaves = hashlib.md5(chaves.encode('utf-8')).hexdigest()
# #     return hash_chaves
# # arquivo = codecs.open('temporario.txt', 'w','utf-8')
# # for x in cloud.buscaFonte(token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', url='https://frotas.betha.cloud/frotas/dados/api/materiais',criterio='',fields='descricaoCompleta'):
# #     hash = gerar_hash_chaves('75224482000162', "materiais", x['id'])
# #     sql = f"306|materiais|{hash}|Cadastro de Materiais e Serviços|{x['id']}|{x['descricaoCompleta']}\n"
# #     arquivo.writelines(f'{sql}')


"""Adiciona CPF para funcionariso do frotas"""
# for x in db.consulta_sql(sql='select * from bethadba.funcionarios where cpf is null', params_exec=params_exec, index_col='nome'):
#     conn = db.conectar(params_exec)
#     db.execute_sql(f"call bethadba.pg_setoption('fire_triggers','off');", conn=conn, params_exec=params_exec)
#     db.execute_sql(sql=f"update bethadba.funcionarios set cpf = {cloud.geraCpf()} where i_funcionarios = {x['i_funcionarios']} and cpf is null", conn=conn, params_exec= params_exec)
#
"""Adiciona data de nascimento para funcionario do frotas"""
# for x in db.consulta_sql(sql='select * from bethadba.funcionarios where nascimento is null', params_exec=params_exec, index_col='nome'):
#     conn = db.conectar(params_exec)
#     db.execute_sql(f"call bethadba.pg_setoption('fire_triggers','off');", conn=conn, params_exec=params_exec)
#     db.execute_sql(sql=f"update bethadba.funcionarios set nascimento = '1961-04-25' where i_funcionarios = {x['i_funcionarios']} and nascimento is null", conn=conn, params_exec= params_exec)


"""Preenche o id_gerado dos participantes dos processos (Não Finalizado)"""
# for x in db.consulta_sql(sql="""SELECT 1 as indexreg, cmr.id_gerado, cmr.i_chave_dsk2 as ano, cmr.i_chave_dsk3 as numero, cmr.i_chave_dsk4 as fornecedor, ct.id_gerado
# from bethadba.controle_migracao_registro cmr
# join bethadba.controle_migracao_registro ct on (ct.tipo_registro = 'fornecedores' and ct.i_chave_dsk1 = 11 and ct.i_chave_dsk2 = fornecedor)
# where cmr.tipo_registro = 'participante-licitacao' and cmr.id_gerado is null""",params_exec=params_exec,index_col='indexreg'):
#     print(x)

"""Busca ID Fornecedores Compras"""
# url = 'https://compras.betha.cloud/compras/dados/api/fornecedores'
# for x in db.consulta_sql(sql="""select 1 as inde, i_chave_dsk2,c.cgc from bethadba.controle_migracao_registro cmr
# join compras.compras.credores c on (i_credores = cmr.i_chave_dsk2 and i_entidades = 11)
# where cmr.tipo_registro = 'fornecedores' and cmr.i_chave_dsk1 = 11 and cmr.id_gerado is null and c.cgc is not null""",params_exec=params_exec, index_col='inde'):
#     print(x)
#     req = cloud.buscaFonte(url=url, criterio=f"pessoa.cpfCnpj = '{x['cgc']}'", fields='', token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f')
#     if req:
#         print(req)
#         db.execute_sql(sql=f"update bethadba.controle_migracao_registro set id_gerado = {req[0]['id']} where i_chave_dsk2 = {x['i_chave_dsk2']} and i_chave_dsk1 = 11 and tipo_registro ='fornecedores'",params_exec=params_exec)


"""Busca participante-licitacao"""
# for x in db.consulta_sql(sql="""
# SELECT distinct
# cmro.tipo_registro,
# cmro.id_gerado,
# cmr.i_chave_dsk1 as dsk1,
# cmr.i_chave_dsk2 as dsk2,
# cmr.i_chave_dsk3 as dsk3,
# --cmr.i_chave_dsk4 as dsk4,
# --cc.id_gerado as id_fornecedor,
# dd.id_gerado as id_processo,
# cmro.id_existente,
# cmro.mensagem_erro,
# cmro.mensagem_ajuda
# --cmro.json_enviado,
# --cmro.hash_chave_dsk
# FROM bethadba.controle_migracao_registro_ocor cmro
# INNER JOIN bethadba.controle_migracao_registro cmr on (cmr.hash_chave_dsk = cmro.hash_chave_dsk)
# --FULL JOIN bethadba.controle_migracao_registro cc on (cc.tipo_registro='fornecedores' and cc.i_chave_dsk1=11 and cc.i_chave_dsk2 = dsk4)
# FULL JOIN bethadba.controle_migracao_registro dd on (dd.tipo_registro='processos-administrativo' and dd.i_chave_dsk1=11 and dd.i_chave_dsk2 = dsk2 and dd.i_chave_dsk3 = dsk3)
# where cmro.id_gerado is null and cmro.resolvido = 1 and cmro.tipo_registro = 'participante-licitacao'""", params_exec=params_exec, index_col='tipo_registro'):
#     print(x)
#     url = f"https://compras.betha.cloud/compras-services/api/exercicios/{x['dsk2']}/processos-administrativo/{x['id_processo']}/participante-licitacao"
#     for z in cloud.buscaServiceLayer(token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', url=url):
#         print(f"{z['fornecedor']['id']} -->> {z['id']}")
#         fornecedor = None
#         fornecedor = db.consulta_sql(sql=f"select * from bethadba.controle_migracao_registro where tipo_registro = 'fornecedores' and i_chave_dsk1 = 11 and id_gerado = {z['fornecedor']['id']}",params_exec=params_exec,index_col='sistema')[0]['i_chave_dsk2']
#         if fornecedor is not None:
#             print(f"select * from bethadba.controle_migracao_registro where tipo_registro = 'participante-licitacao' and i_chave_dsk1 = 11 and i_chave_dsk2 = {x['dsk2']} and i_chave_dsk3 = {x['dsk3']} and i_chave_dsk4 = {fornecedor}")
#             db.execute_sql(sql=f"update bethadba.controle_migracao_registro set id_gerado = {z['id']} where tipo_registro = 'participante-licitacao' and i_chave_dsk1 = 11 and i_chave_dsk2 = {x['dsk2']} and i_chave_dsk3 = {x['dsk3']} and i_chave_dsk4 = {fornecedor}", params_exec=params_exec)


"""Busca Despesas para o FROTAS"""
# exercicio_busca = '2015'
# # for x in db.consulta_sql(sql="select * from bethadba.controle_migracao_registro where tipo_registro = 'lancamento-despesa' and id_gerado is null", params_exec=params_exec, index_col='sistema'):
# #     print(x)
# lista = None
#
# async def funcaoDespesa(x):
#     db.execute_sql(sql=f"update bethadba.controle_migracao_registro set id_gerado = {x['id']} where tipo_registro = 'lancamento-despesa' and id_gerado is null and i_chave_dsk1 = {x['numeroLancamentoDespesa']} and i_chave_dsk2 = {exercicio_busca}", params_exec=params_exec)
#
# for z in db.consulta_sql(sql=f"select * from bethadba.controle_migracao_registro where tipo_registro = 'lancamento-despesa' and i_chave_dsk2 = {exercicio_busca} and id_gerado is null", params_exec=params_exec, index_col='sistema' ):
#
#     for x in cloud.buscaFonte(token='2f862daa-aef8-4fb6-a5c2-57cf0469fb0f', url='https://frotas.betha.cloud/frotas/dados/api/lancamentosdespesas',
#                               criterio=f"numeroLancamentoDespesa = {z['i_chave_dsk1']} and dataLancamentoDespesa <= {exercicio_busca}-12-31T00:00:00 and dataLancamentoDespesa >= {exercicio_busca}-01-01T00:00:00",
#                               fields='id, numeroLancamentoDespesa'):
#         print(x['id'])
#         print(x['numeroLancamentoDespesa'])
#         # asyncio.run(funcaoDespesa(x))

