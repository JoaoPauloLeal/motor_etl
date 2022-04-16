import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import base64
from pathlib import Path
import logging
from datetime import datetime


tipo_registro = 'parecer-anexos'
sistema = 304
limite_lote = 1
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/parecer-anexos"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec, ano)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec, ano):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql_padrao', ano)
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados

def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    token = params_exec['token']
    contador = 0
    for item in dados:
        arquivo_doc = item["arq_file"]
        # print(arquivo_doc)
        fileObj = Path(arquivo_doc)
        if fileObj.is_file():
            with open(arquivo_doc, "rb") as original_file:
                encoded_string = base64.b64encode(original_file.read())
                encoded_string = encoded_string.decode('ascii')
        else:
            encoded_string = ''
        # print(encoded_string)
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["key1"], item["id_codigo"], item["ano"], item["codigo_parecer"])
        dict_dados = {
            "idIntegracao": hash_chaves,
            "conteudo": {
                "parecer": {
                    "id": item["id_parecer_cloud"],
                    "usuario": item["usuario"]
                    },
                "nomeFile": item["nome_arq"],
                "contentType": item["content_type"],
                "file": encoded_string
            }
        }

        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Anexos do Parecer',
            'id_gerado': None,
            'i_chave_dsk1': item["key1"],
            'i_chave_dsk2': item["id_codigo"],
            'i_chave_dsk3': item["ano"],
            'i_chave_dsk4': item["codigo_parecer"]
        })
    # print(lista_controle_migracao)
    # print(lista_dados_enviar)
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')