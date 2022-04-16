from os import path
import settings
from datetime import datetime
from PySimpleGUI import PySimpleGUI as sg
from json import (load as jsonload, dump as jsondump)

SETTINGS_FILE = path.join(path.dirname(__file__), r'settings_file.cfg')
DEFAULT_SETTINGS = {'db_name': 'oloco'}

def iniciar():
    print(':: Iniciando projeto do sistema Contabil')

    params_exec = {
        'clicodigo': '',
        'somente_pre_validar': False,
        'token': '',  # Token Service Layer
        'entidade':'1', # Numero da entidade
        'exercicio': '2021',
        'db_name': '',  # Nome do banco para conexão sybase
        'db_user': '',  # Nome do banco para conexão sybase
        'db_pw': '',  # Nome do banco para conexão sybase
        'db_host': '',  # Nome do banco para conexão sybase
        'db_port': '9002',  # Nome do banco para conexão sybase
    }
    sg.theme('DarkAmber')
    # sg.theme_previewer()
    retorno = load_settings(SETTINGS_FILE, DEFAULT_SETTINGS)
    if retorno:
        layout = [
            [sg.Text('Nome do banco desktop', size=(40, 1)), sg.Input(retorno['db_name'], key='db_name', size=(40, 1))],
            [sg.Text('ID da entidade desktop', size=(40, 1)), sg.Input(retorno['entidade'], key='id_entidade', size=(40, 1))],
            [sg.Text('Porta configurada do banco desktop', size=(40, 1)), sg.Input(retorno['db_port'], key='db_port', size=(40, 1))],
            [sg.Text('Token', size=(40, 1)), sg.Input(retorno['token'], key='token', size=(40, 1))],
            [sg.Checkbox('Valida Totais Cloud Desk ', size=(20,1), key='valida-totais'),sg.Checkbox('Consulta CNPJ do Token ', size=(20,1), key='consulta-cnpj-entidade')],
            [sg.Checkbox('Excluir Comprovantes',size=(20,1), key='excluir-comprovantes'),sg.Checkbox('Valida Endereços',size=(20,1), key='valida-endereco')],
            [sg.Checkbox('Valida Responsaveis', size=(20,1), key='valida-responsaveis'),sg.Checkbox('Valida Comprovantes S/ Credor', size=(30,1), key='valida-comprovantes-sem-credor')],
            [sg.Checkbox('Busca ID Empenho', size=(20, 1), key='busca-id-empenho'),sg.Checkbox('Excluir Fornecedores', size=(20, 1), key='excluir-fornecedores')],
            [sg.Checkbox('Excluir Empenhos', size=(20, 1), key='excluir-empenhos'),sg.Checkbox('Excluir Anulação de Empenhos', size=(20, 1), key='excluir-anulacao-empenhos')],
            [sg.Checkbox('Excluir Arrecadacoes Orçamentarias', size=(20, 1), key='excluir-arrecadacoes'),sg.Checkbox('Excluir Despesas Extras', size=(20, 1), key='excluir-despesas-extras')],
            [sg.Checkbox('Busca IDS', size=(20, 1), key='busca-todos-ids'),sg.Checkbox('Atualiza Comprovantes das liquidacoes', size=(20, 1), key='atualiza-comprovante-liquidacao')],
            [sg.Checkbox('Ajustes Bancarios', size=(20, 1), key='excluir_ajustes_duplicados'), sg.Checkbox('TAG Empenhos', size=(20, 1), key='alteracao_identificador')],
            [sg.Button('Iniciar')]
        ]

    # Janela
    parametros = {"db_name":"", "db_port":""}
    janela = sg.Window('Contabil', layout, load_settings(SETTINGS_FILE, DEFAULT_SETTINGS))

    execucao = None

    while True:
        eventos, valores = janela.read()
        if eventos == sg.WINDOW_CLOSED:
            break
        if eventos == 'Iniciar':

            params_exec['db_name'] = valores['db_name']
            params_exec['db_port'] = valores['db_port']
            params_exec['entidade'] = valores['id_entidade']
            params_exec['token'] = valores['token']
            save_settings(SETTINGS_FILE, parametros, valores, params_exec)
            if valores['valida-totais']:
                execucao = 'valida-totais'
            if valores['consulta-cnpj-entidade']:
                execucao = 'consulta-cnpj-entidade'
            if valores['excluir-comprovantes']:
                execucao = 'excluir-comprovantes'
            if valores['valida-endereco']:
                execucao = 'valida-endereco'
            if valores['valida-comprovantes-sem-credor']:
                execucao = 'valida-comprovantes-sem-credor'
            if valores['valida-responsaveis']:
                execucao = 'valida-responsaveis'
            if valores['busca-id-empenho']:
                execucao = 'busca-id-empenho'
            if valores['excluir-fornecedores']:
                execucao = 'excluir-fornecedores'
            if valores['excluir-empenhos']:
                execucao = 'excluir-empenhos'
            if valores['excluir-anulacao-empenhos']:
                execucao = 'excluir-anulacao-empenhos'
            if valores['excluir-arrecadacoes']:
                execucao = 'excluir-arrecadacoes'
            if valores['excluir-despesas-extras']:
                execucao = 'excluir-despesas-extras'
            if valores['busca-todos-ids']:
                execucao = 'busca-todos-ids'
            if valores['atualiza-comprovante-liquidacao']:
                execucao = 'atualiza-comprovante-liquidacao'
            if valores['excluir_ajustes_duplicados']:
                execucao = 'excluir_ajustes_duplicados'
            if valores['alteracao_identificador']:
                execucao = 'alteracao_identificador'
            janela.close()

    if execucao:
        buscar(params_exec, execucao)

def save_settings(settings_file, parametros, values, params_exec):
    SETTINGS_KEYS_TO_ELEMENT_KEYS = {'db_name': params_exec['db_name'], 'db_port': params_exec['db_port'],
                                     'entidade':params_exec['entidade'], "token": params_exec['token']}
    if values:      # if there are stuff specified by another window, fill in those values
        for key in SETTINGS_KEYS_TO_ELEMENT_KEYS:  # update window with the values read from settings file
            try:
                parametros[key] = SETTINGS_KEYS_TO_ELEMENT_KEYS[key]
            except Exception as e:
                print(f'Problem updating settings from window values. Key = {key}')

    with open(settings_file, 'w') as f:
        jsondump(parametros, f)

    # sg.popup('Settings saved')

def load_settings(settings_file, default_settings):
    try:
        with open(settings_file, 'r') as f:
            settings = jsonload(f)
    except Exception as e:
        sg.popup_quick_message(f'exception {e}', 'No settings file found... will create one for you', keep_on_top=True, background_color='red', text_color='white')
        settings = default_settings
        save_settings(settings_file, DEFAULT_SETTINGS, None, None)
    return settings

def enviar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    # print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
        # print(modulo)
        modulo.iniciar_processo_envio(params_exec, ano)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)

def buscar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    # print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_busca'], 0)
        # print(modulo)
        modulo.iniciar_processo_busca(params_exec)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except NameError:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro + str(NameError))

def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução ferramenta {settings.BASE_ORIGEM}, utilizando os '
          f'seguintes parâmetros: \n- {params_exec}')
