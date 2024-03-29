import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'pessoa-juridica'
sistema = 300
limite_lote = 500
url = "https://pessoal.cloud.betha.com.br/service-layer/v1/api/pessoa-juridica"


def iniciar_processo_envio(params_exec, *args, **kwargs):
    if False:
        busca_dados(params_exec)
    if True:
        dados_assunto = coletar_dados(params_exec)
        dados_enviar = pre_validar(params_exec, dados_assunto)
        if not params_exec.get('somente_pre_validar'):
            iniciar_envio(params_exec, dados_enviar, 'POST')
        model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def busca_dados(params_exec):
    print('- Iniciando busca de dados no cloud.')
    registros = interacao_cloud.busca_dados_cloud(params_exec, url=url)
    print(f'- Foram encontrados {len(registros)} registros cadastrados no cloud.')
    registros_formatados = []
    for item in registros:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['cnpj'])
        registros_formatados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pessoa Juridica',
            'id_gerado': item['id'],
            'i_chave_dsk1': item['cnpj']
        })
    model.insere_tabela_controle_migracao_registro(params_exec, lista_req=registros_formatados)
    print('- Busca finalizada. Tabelas de controles atualizas com sucesso.')


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql_padrao')
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
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    contador = 0
    for item in dados:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['cnpj'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'tipo': None if 'tipo' not in item else item['tipo'],
                'cnpj': None if 'cnpj' not in item else item['cnpj'],
                'razaoSocial': None if 'razaosocial' not in item else item['razaosocial'],
                'nomeFantasia': None if 'nomefantasia' not in item else item['nomefantasia'],
                'porte': None if 'porte' not in item else item['porte'],
                'numeroRegistro': None if 'numeroregistro' not in item else item['numeroregistro'],
                'dataRegistro': None if 'dataregistro' not in item else item['dataregistro'],
                'orgaoRegistro': None if 'orgaoregistro' not in item else item['orgaoregistro'],
                'inscricaoMunicipal': None if 'inscricaomunicipal' not in item else item['inscricaomunicipal'],
                'isentoInscricaoEstadual': None if 'isentoinscricaoestadual' not in item else item['isentoinscricaoestadual'],
                'inscricaoEstadual': None if 'inscricaoestadual' not in item else item['inscricaoestadual'],
                'optanteSimples': None if 'optantesimples' not in item else item['optantesimples'],
                'site': None if 'site' not in item else item['site'],
                'sindicato': None if 'sindicato' not in item else item['sindicato'],
                'numeroAns': None if 'numeroans' not in item else item['numeroans'],
                'numeroInep': None if 'numeroinep' not in item else item['numeroinep'],
                'numeroValeTransporte': None if 'numerovaletransporte' not in item else item['numerovaletransporte'],
            }
        }
        if item['emails'] is not None:
            dict_dados['conteudo'].update({
                'emails': []
            })
            lista = item['emails'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['emails'].append({
                    'descricao': campo[0],
                    'endereco': campo[1],
                    'principal': campo[2]
                })
        if item['telefones'] is not None:
            dict_dados['conteudo'].update({
                'telefones': []
            })
            lista = item['telefones'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['telefones'].append({
                    'descricao': campo[0],
                    'tipo': campo[1],
                    'numero': campo[2],
                    'observacao': campo[3],
                    'principal': campo[4]
                })
        if item['enderecos'] is not None:
            dict_dados['conteudo'].update({
                'enderecos': []
            })
            lista = item['enderecos'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['enderecos'].append({
                    'descricao': campo[0],
                    'logradouro': {
                        'id': campo[1]
                    },
                    'bairro': {
                        'id': campo[2]
                    },
                    'cep': campo[3],
                    'numero': campo[4],
                    'complemento': campo[5],
                    'principal': campo[6]
                })
        if item['responsaveis'] is not None:
            dict_dados['conteudo'].update({
                'responsaveis': []
            })
            lista = item['responsaveis'].split('%||%')
            for listacampo in lista:
                campo = listacampo.split('%|%')
                dict_dados['conteudo']['responsaveis'].append({
                    'dataInicio': campo[0],
                    'dataTermino': None if campo[1] == 'null' else campo[1],
                    'qualificacao': campo[2],
                    'responsavel': {
                        'id': campo[3]
                    }
                })
        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Pessoa Juridica',
            'id_gerado': None,
            'i_chave_dsk1': item['cnpj']
        })
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)
        print('- Envio de dados finalizado.')