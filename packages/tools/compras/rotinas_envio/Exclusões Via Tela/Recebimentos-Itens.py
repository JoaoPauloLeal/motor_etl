import logging
import time

import bth.cloud_connector as cloud
import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    url = 'https://contratos.betha.cloud/contratos/dados/api/contratacoes'

    ids = ''
    cont = 0
    for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2018,2019,2020,2021)", index_col="tt", params_exec=params_exec):
        print(x)
        filtro = f"entidade.id = 65 and anoProcesso = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
        for y in cloud.buscaFonte(fields='id', criterio=filtro,url=url,token=params_exec['token']):
            fil = f"entidade.id = 65 and solicitacaoFornecimentoRecebimento.solicitacaoFornecimento.contratacao.id = {y['id']}"
            ur = 'https://contratos.betha.cloud/contratos/dados/api/solicitacoesfornecimentosrecebimentositens'
            for z in cloud.buscaFonte(url=ur, fields='id', criterio=fil, token=params_exec['token']):
                print(z)
                cont += 1
                ids = str(ids) + str(z['id']) + ','
    print(cont)
    print(ids)