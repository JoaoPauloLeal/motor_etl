import sistema_origem.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import asyncio
import requests
from datetime import datetime

tipo_registro = 'liquidacoes'
sistema = 1
limite_lote = 1000
# PRODUCAO
url = "https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/liquidacoes"
# RELEASE
# url = "https://contabil-sl-release.test.bethacloud.com.br/contabil/service-layer/v2/api/empenhos"
# STABLE
# url = "https://contabil-sl.test.bethacloud.com.br/contabil/service-layer/v2/api/empenhos"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    lista_dados_enviar = []
    hoje = datetime.now()
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)

    print(hoje)

    print("EMPACOU 2")
    token = params_exec.get('token')
    headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}

    for item in req_res:
    # for item in reversed(req_res):
        idGerado = item['idGerado']
        content = item['content']
        data = datetime.strptime(str(content['data']), '%Y-%m-%d')
        lista_conteudo_retorno.append({
            'idGerado': idGerado['id'],
            'exercicio': data.year
        })
        contador += 1

    for item in lista_conteudo_retorno:
        dict_dados = {
            "idIntegracao": str(item['idGerado']),
            "idGerado": {
                "id": item['idGerado']
            },
            "content": {
                "exercicio": item['exercicio']
            }
        }
        contador += 1
        lista_dados_enviar.append(dict_dados)
        urlDelete = str(url)
        print(str(dict_dados).replace("\'", "\""))
        asyncio.run(excluir_assync(urlDelete, headers, dict_dados))

    # print(lista_dados_enviar)
    print('- Busca de dados finalizado.')


async def excluir_assync(urlDelete, headers, dict_dados):
    retorno_req = requests.delete(urlDelete, headers=headers, data=str(dict_dados).replace("\'", "\""))
    print(retorno_req.text)
    print(retorno_req.status_code)
