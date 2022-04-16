import logging
import time

import bth.cloud_connector as cloud
import bth.db_connector as db
import requests


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    # url = "https://compras.betha.cloud/compras/dados/api/processosadministrativosatosfinais"
    # ids = ''
    # cont = 0
    # for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc", index_col="tt", params_exec=params_exec):
    #     print(x)
    #     filtro = f"entidade.id = 65 and processoAdministrativo.parametroExercicio.exercicio = {x['chave_dsk2']} and processoAdministrativo.numeroProcesso = {x['chave_dsk3']}"
    #     for y in cloud.buscaFonte(fields='id, processoAdministrativo.id', criterio=filtro, url=url, token=params_exec['token']):
    #         ids = str(ids) + str(y['id']) + ','
    #         cont += 1
    #         urldelete = f"https://compras.betha.cloud/compras/api/processosadministrativo/{y['processoAdministrativo']['id']}/atosfinais/{y['id']}"
    #         print(urldelete)
    #         req = requests.delete(url=urldelete,
    #                             headers={'authorization': 'Bearer 042f74fe-146d-4b21-b71d-8ddfe24c3279',
    #                                      'user-access': 'XzxZZfPFXVw=',
    #                                      'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTQsImluc3VsYXRpb24iOmZhbHNlfX0='})
    #         print(req.status_code)
    # print(cont)
    # print(ids)

    # contratacoes
    # cont = 0
    # url= 'https://contratos.betha.cloud/contratos/dados/api/contratacoes'
    # ids = ''
    # for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2020,2021)", index_col="tt", params_exec=params_exec):
    #     print(x)
    #     filtro = f"entidade.id = 65 and anoProcesso = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
    #     #cont += 1
    #     for y in cloud.buscaFonte(fields='id', criterio=filtro,url=url,token=params_exec['token']):
    #         ids = str(ids) + str(y['id']) + ','
    #         urldelete = f"https://contratos.betha.cloud/contratos/api/contratacoes/{y['id']}"
    #         print(urldelete)
    #         req = requests.delete(url=urldelete,
    #                               headers={'authorization': 'Bearer d23a4472-b3a0-4bb6-a6a0-97aed4d31d86',
    #                                        'user-access': 'o_V3H4GrVuo=',
    #                                        'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTUsImluc3VsYXRpb24iOmZhbHNlfX0='})
    #         print(req.status_code)
    #         cont += 1
    #         print(cont)
    # print(cont)
    # print(str(ids))


    # CONTRATACOES ITENS

    # cont = 0
    # url= 'https://contratos.betha.cloud/contratos/dados/api/contratacoes'
    # ids = ''
    # for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc", index_col="tt", params_exec=params_exec):
    #     # print(x)
    #     filtro = f"entidade.id = 65 and anoProcesso = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
    #     #cont += 1
    #     for y in cloud.buscaFonte(fields='id', criterio=filtro,url=url,token=params_exec['token']):
    #         fit = f"contratacao.entidade.id = 65 and contratacao.id = {y['id']}"
    #         ur = f"https://contratos.betha.cloud/contratos/dados/api/contratacoesitens"
    #         for z in cloud.buscaFonte(url=ur, criterio=fit, fields='id',token=params_exec['token']):
    #             ids = str(ids) + str(z['id']) + ','
    #             # print(str(z['id']))
    #             cont += 1
    #             print(cont)
    # print(cont)
    # print(str(ids))





# --------------------------------------------#

    # # CONTRATACOES ADITIVOS
    # url = 'https://contratos.betha.cloud/contratos/dados/api/contratacoes'
    #
    # ids = ''
    # cont = 0
    # for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2021)", index_col="tt", params_exec=params_exec):
    #     print(x)
    #     filtro = f"entidade.id = 65 and anoProcesso = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
    #     for y in cloud.buscaFonte(fields='id', criterio=filtro,url=url,token=params_exec['token']):
    #         fil = f"entidade.id = 65 and contratacao.id = {y['id']}"
    #         ur = 'https://contratos.betha.cloud/contratos/dados/api/aditivos'
    #         for z in cloud.buscaFonte(url=ur, fields='id', criterio=fil, token=params_exec['token']):
    #             urldelete = f"https://contratos.betha.cloud/contratos/api/contratacoes/{y['id']}/aditivos/{z['id']}"
    #             print(urldelete)
    #             req = requests.delete(url=urldelete,
    #                                   headers={'authorization': 'Bearer 68529c1c-6d81-4591-be6b-79e6323d7294',
    #                                            'user-access': 'o_V3H4GrVuo=',
    #                                            'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTUsImluc3VsYXRpb24iOmZhbHNlfX0='})
    #             print(req.status_code)
    #             cont += 1
    #             ids = str(ids) + str(z['id']) + ','
    # print(cont)
    # print(ids)

    url = "https://compras.betha.cloud/compras/dados/api/atas-registro-preco-itens"
    ids = ''
    cont = 0
    for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2021) ",
                             index_col="tt", params_exec=params_exec):
        # print(x)
        filtro = f"entidade.id = 65 and processoAdministrativo.parametroExercicio.exercicio = {x['chave_dsk2']} and processoAdministrativo.numeroProcesso = {x['chave_dsk3']}"
        if cloud.buscaFonte(fields='id,ataRegistroPreco.id', criterio=filtro, url=url,
                                  token=params_exec['token']):
            # print('')
            """"""
        else:
            print(x)

    print(cont)
    print(ids)