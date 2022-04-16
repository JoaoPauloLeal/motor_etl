import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime
import requests

tipo_registro = 'medicoes'


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
        if params_exec['ESTADO'] != 'SC':
            query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        else:
            query = db.get_consulta(params_exec, f'{tipo_registro}_sc.sql')
        conn = db.conectar(params_exec)
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada.'
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        if i['numero_contrato'] is not None:
            contrato = procura_contrato(i['id_obra'], i['numero_contrato'], params_exec)
            dict_enviar = {
                "arquivos": [],
                "dataInicial": i['data_medicao'],
                "dataFinal": i['data_medicao'],
                "percentFisico": i['percentual_fisico'],
                "responsavelTecnico": {
                    "id": i['id_responsavel']
                },
                "dataMedicao": i['data_medicao'],
                "tipoMedicao": {
                    "id": i['id_tipo_medicao']
                },
                "obraContratacao": {
                    "id": contrato['id'] if contrato else None
                },
                "observacao": i['observacao']
            }
        else:
            dict_enviar = {
                "arquivos": [],
                "dataInicial": i['data_medicao'],
                "dataFinal": i['data_medicao'],
                "percentFisico": i['percentual_fisico'],
                "responsavelTecnico": {
                    "id": i['id_responsavel']
                },
                "dataMedicao": i['data_medicao'],
                "tipoMedicao": {
                    "id": i['id_tipo_medicao']
                },
                # "obraContratacao": {
                #     "id": contrato['id'] if contrato else None
                # },
                "observacao": i['observacao'],
                "valor": i['valor']
            }
        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(params_exec, tipo_registro,
                                        str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras']), None, None):
            id_registro, mensagem_erro = cloud.envia_registro(
                f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/medir', json_envio, params_exec, True, 'medir')
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Medições',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras'])
                ]
                db.regista_controle_migracao(registro, params_exec)
            else:
                print(f'Problemas ao enviar registros para o cloud! - {mensagem_erro} - {id_registro}')
