import settings
from os import path
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from PySimpleGUI import PySimpleGUI as sg
from datetime import datetime

SETTINGS_FILE = path.join(path.dirname(__file__), r'settings_file.cfg')
DEFAULT_SETTINGS = {'db_name': 'test'}

def iniciar():
    print(':: Iniciando migração do sistema Compras')
    global ano_inicial
    # ano_inicial = input("Ano inicial para migração: ")
    ano_inicial = 1
    global ano_final
    # ano_final = input("Ano final para migração: ")
    ano_final = 1
    global ano

    for ano in range(int(ano_inicial), int(ano_final) + 1):
        print("------------- INICIO MIGRAÇÃO DO ANO: " + str(ano) + " --------------")
        params_exec = {
            'clicodigo': '',
            'somente_pre_validar': False,
            'token': '', # Token Service Layer ,
            'entidade': '',  # Nome exato da entidade a qual quer pegar o token do oaut2
            'ano': str(ano),
            'exercicio': 2017,
            'db_name': '',  # Nome do banco para conexão sybase
            'db_user': '',  # Nome do banco para conexão sybase
            'db_pw': '',  # Nome do banco para conexão sybase
            'db_host': 'localhost',  # Nome do banco para conexão sybase
            'db_port': '9099', #ignoreline,

        }
        mensagem_inicio(params_exec)
        interacao_cloud.verifica_token(params_exec['token'])

        # buscar(params_exec, 'alterar_config_exercicios', ano)
        # buscar(params_exec, 'busca_cnpj_entidades', ano)
        # buscar(params_exec, 'analisa_configuracao_exercicios', ano)
        # buscar(params_exec, 'analisa_fornecedor', ano)
        # buscar(params_exec, 'analisa_comissao_licitacao', ano)
        # buscar(params_exnec, 'deleta_divisao_item_entidades', ano)
        # buscar(params_exec, 'busca_id_gerado', ano)
        # buscar(params_exec, 'proposta_participante_situacao', ano)
        # buscar(params_exec, 'proposta_participante_processo', ano)
        # buscar(params_exec, 'busca_aditivo', ano)
        # buscar(params_exec, 'participante_sem_proposta', ano)
        # buscar(params_exec, 'anl_processos', ano)
        # buscar(params_exec, 'busca_solicitacoes_compra', ano)
        # buscar(params_exec, 'busca-administrativos-two', ano)
        # buscar(params_exec, 'busca-administrativos', ano)
        # buscar(params_exec, 'busca_materiais', ano)
        # buscar(params_exec, 'anl_processos', ano)
        # buscar(params_exec, 'excluir-proposta-participante', ano)
        # buscar(params_exec, 'excluir_contratacoes', ano)
        # buscar(params_exec, 'confere_itens_divididos_entidades_processos', ano)
        # buscar(params_exec, 'excluir-processos-adm', ano)
        # buscar(params_exec, 'excluir-participantes-processo-adm', ano)
        buscar(params_exec, 'Lixo', ano)
        # buscar(params_exec, 'Aditivos-Itens', ano)
        # buscar(params_exec, 'SF', ano)
        # buscar(params_exec, 'SF-Itens', ano)
        # buscar(params_exec, 'Recebimentos', ano)
        # buscar(params_exec, 'Recebimentos-Itens', ano)
        # buscar(params_exec, 'Apostilamento-Itens', ano)
        # buscar(params_exec, 'Apostilamento', ano)
        # buscar(params_exec, 'Atas-Registro_Preco', ano)
        # buscar(params_exec, 'Atas-Registro_Preco-Itens', ano)
        # buscar(params_exec, 'Participante-Item-Divisao-Entidade', ano)

        print("------------- TERMINO MIGRAÇÃO DO ANO: " + str(ano) + " -------------")
        ano = ano + 1


def enviar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
        # print(modulo)
        modulo.iniciar_processo_envio(params_exec, ano)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)


def buscar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'

    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_busca'], 0)
        # print(modulo)
        modulo.iniciar_processo_busca(params_exec, ano)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)


def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução ferramenta {settings.BASE_ORIGEM}, utilizando os '
          f'seguintes parâmetros: \n- {params_exec}')


# def verifica_tabelas_controle():
#     pgcnn = model.PostgreSQLConnection()
#     pgcnn.verifica_tabelas_controle()
