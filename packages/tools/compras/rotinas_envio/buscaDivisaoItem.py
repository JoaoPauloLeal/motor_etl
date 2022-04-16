import bth.interacao_cloud as interacao_cloud
import json
import asyncio
import logging
import requests
from datetime import datetime

tipo_registro = 'solicitacoes'
sistema = 305
limite_lote = 1000
# url = "https://compras.betha.cloud/compras/api/processosadministrativo"
url = "https://compras.betha.cloud/compras/api/processosadministrativo?filter=&limit=100&offset=0&sort="

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    lista_conteudo_retorno_2 = []
    lista_conteudo_retorno_3 = []
    pula = False
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud_api_tela(params_exec,
                                                          url=url,
                                                          tipo_registro=tipo_registro,
                                                          tamanho_lote=limite_lote)

    # print(req_res)

    print("EMPACOU 2")
    token = params_exec.get('token')
    headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
    ano_proc = '0'
    for item in req_res:
        # print()
        idProcesso = item['id']
        ano_proc = item['dataProcesso'][:4]
        lista_conteudo_retorno.append(idProcesso)
        contador += 1

    for item in lista_conteudo_retorno:
        url_2 = f'https://compras.betha.cloud/compras/api/processosadministrativo/{item}/itens'
        req_res_2 = interacao_cloud.busca_dados_cloud_api_tela(params_exec,
                                                             url=url_2,
                                                             tipo_registro=tipo_registro,
                                                             tamanho_lote=limite_lote)
        if req_res_2 is None:
            continue
        for sub_item in req_res_2:
            idItem = sub_item['id']
            lista_conteudo_retorno_2.append(idItem)

        for sub_sub_item in lista_conteudo_retorno_2:
            url_4 = f"https://compras.betha.cloud/compras/api/processosadministrativo/{item}/itens/configuracoes/{sub_sub_item}/entidadesparticipantes"

            headers = {'authorization': f'bearer {params_exec["token_tela"]}',
                       'app-context': f'{params_exec["app-context"]}',
                       'user-access': f'{params_exec["user-access"]}',
                       }

            req_res_3 = requests.get(url=url_4, params=params_exec, headers=headers)
            if req_res_3 is None:
                continue
            if not req_res_3.ok:
                continue
            if not req_res_3.json():
                continue
            # print(req_res_3.json())
            retorno_json = req_res_3.json()
            if not retorno_json['content']:
                continue
            elif 'content' in retorno_json:
                for i in retorno_json['content']:
                    lista_conteudo_retorno_3.append(i['id'])
            # for sub_sub_sub_item in req_res_3:
            #     idItem = sub_sub_sub_item['id']
            #     lista_conteudo_retorno_3.append(idItem)
        for lista_entidade in lista_conteudo_retorno_3:
            # url_3 = f'https://compras.betha.cloud/compras/api/processosadministrativo/{item}/itens/configuracoes/{sub_sub_item}/entidadesparticipantes/{lista_entidade}'
            url_3 = f'https://compras.betha.cloud/compras-services/api/exercicios/{ano_proc}/processos-administrativo/{item}/itens/{sub_sub_item}/entidades'
            # asyncio.run(deletar_tela(url_3, headers))
            json_enviar = {
                "item": {"id": sub_sub_item},
                "entidadeParticipante": {"id": lista_entidade},
                "quantidadeDistribuida": 0
            }
            print(url_3)
            print(json_enviar)
            # retorno_req = requests.delete(url_3, headers=headers)

            headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
            retorno_req = requests.post(url_3, headers=headers, data=json_enviar)
            print(str(item), retorno_req.status_code)
        lista_conteudo_retorno_3 = []
        lista_conteudo_retorno_2 = []
    # for item in lista_conteudo_retorno:
    #     urlDelete = str(url) + str(item)
    #     print(urlDelete)
    #     retorno_req = requests.delete(urlDelete, headers=headers)
    #     print(str(item), retorno_req.status_code)
    # print(lista_controle_migracao)
    # model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_conteudo_retorno)
    # print(contador)
    print('- Busca de dados finalizado.')

# async def deletar_tela(urlDelete, headers_tela):
#     retorno_req = requests.delete(urlDelete, headers=headers_tela)
#     print(retorno_req.text)
#     print(retorno_req.status_code)

