import time
import bth.interacao_cloud as interacao_cloud
import bth.db_connector as db
import requests as req
import json

tipo_registro = 'empenhos'
sistema = 1
limite_lote = 1000



url_base = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/empenhos'

def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando Correção das TAGs de empenhos.')

    sql_busca_empenhos = f"""
        SELECT tipo_registro , id_gerado , i_chave_dsk3, i_chave_dsk2 FROM bethadba.controle_migracao_registro reg 
        WHERE tipo_registro = 'empenhos' 
	        and i_chave_dsk2 = {params_exec['exercicio']}
	        and i_chave_dsk3  in (
		        select i_empenhos 
		        from sapo.empenhos 
		        where  ano_exerc = {params_exec['exercicio']} 
			        and i_contratos_integ_compras is not null 
			        and licitação is null 
                    and categoria <>'A'
                    order by 1 asc)
                     
    """

    for li_cons, ret_db in enumerate(db.consulta_sql(sql_busca_empenhos, params_exec)):

        headers = {'authorization': f"bearer {params_exec['token']}", 'content-type': 'application/json'}
        ret_cloud = interacao_cloud.busca_dados_cloud_unit(params_exec, url=f"{url_base}/{ret_db['id_gerado']}", tipo_registro=tipo_registro,
                                                         tamanho_lote=limite_lote)


        json_envio = {
            "idIntegracao": f"li{li_cons}id{ret_cloud['idGerado'].get('id')}",
            "idGerado": ret_cloud['idGerado'],
            "content": {
                "validaSaldo": False,
                "recurso": ret_cloud['content'].get('recurso'),
                "contratoRateio": ret_cloud['content'].get('contratoRateio'),
                "tipo": ret_cloud['content'].get('tipo'),
                "data": ret_cloud['content'].get('data'),
                "despesa": ret_cloud['content'].get('despesa'),
                "especificacao": ret_cloud['content'].get('especificacao'),
                "entesConsorciados": ret_cloud['content'].get('entesConsorciados'),
                "despesaLancada": False,
                "exercicio": ret_cloud['content'].get('exercicio'),
                "credor": {
                    "id": ret_cloud['content'].get('credor').get('id')
                },
                "natureza": {
                    "id": ret_cloud['content'].get('natureza').get('id')
                },
                "valor": ret_cloud['content'].get('valor'),
                "numeroCadastro": ret_cloud['content'].get('numeroCadastro')
            }
        }

        if ret_cloud['content'].get('vencimentos') != []:
            vencimentos = {"vencimentos": ret_cloud['content'].get('vencimentos')}
            json_envio['content'].update(vencimentos)

        if ret_cloud['content'].get('subempenhos') != []:
            subempenhos = {"subempenhos": ret_cloud['content'].get('subempenhos')}
            json_envio['content'].update(subempenhos)

        if ret_cloud['content'].get('marcadores') != []:
            marcadores = {"marcadores": ret_cloud['content'].get('marcadores')}
            json_envio['content'].update(marcadores)

        print(json_envio)

        envio = req.put(url=f"{url_base}/credores", headers=headers, data=json.dumps(json_envio))
        print(f"resposta : {envio.status_code} ")



