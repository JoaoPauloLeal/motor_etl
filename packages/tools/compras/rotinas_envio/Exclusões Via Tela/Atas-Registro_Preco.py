import bth.cloud_connector as cloud
import bth.db_connector as db
import requests


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    url = "https://compras.betha.cloud/compras/dados/api/atasregistropreco"
    ids = ''
    cont = 0
    for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2016,2017,2018,2019,2020,2021) ", index_col="tt", params_exec=params_exec):
        print(x)
        filtro = f"entidade.id = 65 and processoAdministrativo.parametroExercicio.exercicio = {x['chave_dsk2']} and processoAdministrativo.numeroProcesso = {x['chave_dsk3']}"
        for y in cloud.buscaFonte(fields='id', criterio=filtro, url=url, token=params_exec['token']):
            ids = str(ids) + str(y['id']) + ','
            cont += 1
            urldelete = f"https://compras.betha.cloud/compras/api/atasregistropreco/{y['id']}"
            print(urldelete)
            req = requests.delete(url=urldelete,
                                headers={'authorization': 'Bearer 042f74fe-146d-4b21-b71d-8ddfe24c3279',
                                         'user-access': 'XzxZZfPFXVw=',
                                         'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTUsImluc3VsYXRpb24iOmZhbHNlfX0='})
            print(req.status_code)
    print(cont)
    print(ids)