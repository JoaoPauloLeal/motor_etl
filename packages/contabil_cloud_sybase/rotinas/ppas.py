import simplejson as json
import requests
import bth.db_connector as db
from packages.contabil_cloud_sybase.util import urls

tipo_registro = 'ppas'
descricao_tipo_registro = 'Cadastro de Ppas'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    iniciar_busca(params_exec, dados_assunto)


def coletar_dados(params_exec):
    df = None
    try:
        query = db.get_consulta_conv(params_exec, f'{tipo_registro}.sql')
        df = db.consulta_sql(query, params_exec)

    except Exception as error:
        print(f'Erro ao executar função "coletar_dados". {error}')

    finally:
        return df


def iniciar_busca(params_exec, dados_assunto):
    headers = {'authorization': f'bearer {params_exec["token"]}',
               'content-type': 'application/json'}
    if dados_assunto is not None:
        


def register_controle_migracao(params_exec, registro):
    db.regista_controle_migracao(registro, params_exec)
