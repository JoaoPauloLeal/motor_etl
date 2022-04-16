import bth.cloud_connector as cloud
import bth.db_connector as db
import requests


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    url = 'https://contratos.betha.cloud/contratos/dados/api/contratacoes'

    ids = ''
    cont = 0
    for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2014,2015,2016,2017,2018,2019,2020,2021)", index_col="tt", params_exec=params_exec):
        print(x)
        filtro = f"entidade.id = 65 and anoProcesso = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
        for y in cloud.buscaFonte(fields='id', criterio=filtro,url=url,token=params_exec['token']):
            fil = f"entidade.id = 65 and apostilamento.contratacao.id = {y['id']}"
            ur = 'https://contratos.betha.cloud/contratos/dados/api/apostilamentositens'
            for z in cloud.buscaFonte(url=ur, fields='id, apostilamento.id', criterio=fil, token=params_exec['token']):
                urldelete = f"https://contratos.betha.cloud/contratos/api/contratacoes/{y['id']}/apostilamentos/{z['apostilamento']['id']}/itens/{z['id']}"
                print(urldelete)
                req = requests.delete(url=urldelete,
                                      headers={'authorization': 'Bearer 932be2f2-d6b0-40ae-9b18-f4480bef8def',
                                               'user-access': 'o_V3H4GrVuo=',
                                               'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTUsImluc3VsYXRpb24iOmZhbHNlfX0='})
                print(req.status_code)
                cont += 1
                ids = str(ids) + str(z['id']) + ','
                print(cont)
    print(ids)
    print(cont)