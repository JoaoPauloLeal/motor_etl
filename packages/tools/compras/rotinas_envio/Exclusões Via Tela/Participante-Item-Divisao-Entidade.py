import bth.cloud_connector as cloud
import bth.db_connector as db
import requests


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    url = "https://compras.betha.cloud/compras/dados/api/processosadministrativos"
    ids = ''
    cont = 0
    for x in db.consulta_sql(sql="select 1 as tt, * from bethadba.tempProc where chave_dsk2 in (2017,2018,2019,2020,2021)", index_col="tt", params_exec=params_exec):
        print(x)
        filtro = f"entidade.id = 65 and parametroExercicio.exercicio = {x['chave_dsk2']} and numeroProcesso = {x['chave_dsk3']}"
        for y in cloud.buscaFonte(fields='id', criterio=filtro, url=url, token=params_exec['token']):
            fil = f"entidade.id = 65 and entidadeParticipante.processoAdministrativo.id = {y['id']}"
            # print(fil)
            urldiv = f"https://compras.betha.cloud/compras/dados/api/processoadministrativoentidadeparticipanteitem"
            for z in cloud.buscaFonte(fields='id, item.id', criterio=fil, url=urldiv, token=params_exec['token']):
                ids = str(ids) + str(z['id']) + ','
                cont += 1
                # print(ids)
                urldelete = f"https://compras.betha.cloud/compras/api/processosadministrativo/{y['id']}/itens/configuracoes/{z['item']['id']}/entidadesparticipantes/{z['id']}"
                print(urldelete)
                req = requests.delete(url=urldelete,
                                    headers={'authorization': 'Bearer 06dd1a47-e115-494c-91fc-34f864c490e9',
                                             'user-access': 'XzxZZfPFXVw=',
                                             'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMTQsImluc3VsYXRpb24iOmZhbHNlfX0='})
                print(req.status_code)
    print(cont)
    print(ids)