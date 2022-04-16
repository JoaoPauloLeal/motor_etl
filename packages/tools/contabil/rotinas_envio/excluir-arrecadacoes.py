import bth.cloud_connector as cloud
import bth.db_connector as db
import json
import logging
import asyncio
import requests
from datetime import datetime

tipo_registro = 'arrecadacoesOrcamentarias'
sistema = 1
limite_lote = 1000
# PRODUCAO
url = "https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/arrecadacoes-orcamentarias"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    campos = 'id, exercicio.ano'
    criterio = f"entidade.id = {db.busca_id_entidade_migracao(params_exec)}"
    cont = 0
    for x in cloud.buscaFonte(url=url, fields=campos, criterio=criterio, token=params_exec['token'], params_exec= params_exec):
        cont+= 1
        print(x)
        arrecadacao = {'idIntegracao':'1','idGerado':{'id':x['id']},'content':{'exercicio':x['exercicio'].get('ano')}}
        print(json.dumps(arrecadacao))
        cloud.ExcluirServiceLayerComJson(url='https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/arrecadacoes-orcamentarias', token=params_exec['token'], data=json.dumps(arrecadacao))
    print(cont)