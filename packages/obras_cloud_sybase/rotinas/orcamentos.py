import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime
import requests

tipo_registro = 'orcamentos'

def procura_contrato(obra, num_contrato, params_exec):
    headers = {
        'authorization': f"Bearer {params_exec['token']}",
        'app-context': params_exec['appcontext'],
        'user-access': params_exec['useraccess'],
    }

    r = requests.get(url=f'https://obras.betha.cloud/obras/api/obras/{obra}/contratosobras', headers=headers, params={
        'filter': f'(nroAnoContrato like "{num_contrato}%")'
    })

    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))
        if len(retorno['content']) > 0:
            return retorno['content'][0]

    return None

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        contrato = procura_contrato(i['id_obra'], i['numero_contrato'], params_exec)

        dict_enviar = {
            "dataInclusao": i['data_inclusao'],
            "responsavelTecnico": {
                "id": i['id_responsavel']
            },
            "tipoPlanilha": i['tipo_planilha'],
            "dataBase": i['data_base'],
            "valorOrcamento": i['valor']
        }

        if i['tipo_planilha'] == 'CONTRATO':
            dict_enviar |= {
                "obraContratacao": {
                    "id": contrato['id'] if contrato else None
                }
            }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(params_exec, tipo_registro, str(i['i_obras']) + '/' + str(i['i_planilhas']), None, None):
            id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/orcamentosobras', json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Orçamentos',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_obras']) + '/' + str(i['i_planilhas'])
                ]
                db.regista_controle_migracao(registro, params_exec)
            else:
                print(f'Erro: {mensagem_erro}')