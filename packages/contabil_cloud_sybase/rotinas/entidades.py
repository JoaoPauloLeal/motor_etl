import simplejson as json
import requests
import bth.db_connector as db
from packages.contabil_cloud_sybase.util import urls

tipo_registro = 'entidades'
descricao_tipo_registro = 'Cadastro de entidades'


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

    r_token = requests.get(url=urls.urlToken+params_exec["token"], headers=headers)

    if r_token.status_code == 200:
        id_gerado_entidade = r_token.json()['entityId']
        r_entidade = requests.get(url=urls.urlEntidade+id_gerado_entidade, headers=headers)
        if r_entidade.status_code == 200:
            cnpj_entidade = r_entidade.json()['cnpj']
            if dados_assunto[0] == cnpj_entidade:
                print('ERRO AO VALIDAR CNPJ BREAK')
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                descricao_tipo_registro,
                str(id_gerado_entidade),
                str(params_exec['id_entidade'])
            ]
            register_controle_migracao(params_exec, registro)


def register_controle_migracao(params_exec, registro):
    db.regista_controle_migracao(registro, params_exec)
