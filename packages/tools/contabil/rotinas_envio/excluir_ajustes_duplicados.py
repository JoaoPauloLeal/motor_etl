import time

import bth.interacao_cloud as interacao_cloud
import bth.db_connector as db
import requests as req
import json
from datetime import datetime

tipo_registro = 'arrecadacoesOrcamentarias'
sistema = 1
limite_lote = 1000



url_base = 'https://tesouraria-sl.cloud.betha.com.br/tesouraria/service-layer/v2/api'

def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados.')

    sql_busca_ajustes = """
    SELECT id_lote FROM bethadba.controle_migracao_lotes 
    where i_sequencial <= 27751 
    --and tipo_registro = 'ajustes' and id_lote = '618a786cb3d1db006c54b609'
    order by data_hora_env desc 
    """

    for li_cons, liq_db in enumerate(db.consulta_sql(sql_busca_ajustes, params_exec)):
        url_lote = f"{url_base}/lotes/"
        # print(url)

        headers = {'authorization': f"bearer {params_exec['token']}", 'content-type': 'application/json'}
        ret_ajuste = interacao_cloud.busca_dados_cloud_unit(params_exec, url=f"{url_lote}{liq_db['id_lote']}", tipo_registro=tipo_registro,
                                                         tamanho_lote=limite_lote)
        sql_cons = f"""
            select id_gerado from bethadba.controle_migracao_registro
            where id_gerado = {ret_ajuste['retorno'][0].get('idGerado').get('id')} and tipo_registro = 'ajustes'
        """
        if not db.consulta_sql(sql_cons, params_exec):
            print(f"Lote {liq_db['id_lote']} não processado, iniciando processo de exclusão !")
            json_envio = {
                "idIntegracao": f"li{li_cons}id{ret_ajuste['retorno'][0].get('idGerado').get('id')}",
                "idGerado": {
                    "id": ret_ajuste['retorno'][0].get('idGerado').get('id')
                },
                "content": {
                    "exercicio": 2021
                }
            }

            # print(json.dumps(json_envio))
            # print(params_exec['token'])
            # print(f"{url_base}/ajustes")
            ret_delete = req.delete(headers=headers, url=f"{url_base}/ajustes", data=json.dumps(json_envio))
            time.sleep(5)
            ret_lote = interacao_cloud.busca_dados_cloud_unit(params_exec, url=f"{url_lote}{ret_delete.json().get('idLote')}", tipo_registro=tipo_registro,
                                                         tamanho_lote=limite_lote)

            print(ret_lote)

        else:
            print(f"Lote {liq_db['id_lote']} processado com sucesso !")

        # print(ret_ajuste['retorno'][0].get('idGerado').get('id'))






