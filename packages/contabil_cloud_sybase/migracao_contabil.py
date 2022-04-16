import time
import settings
from PySimpleGUI import PySimpleGUI as sg
from datetime import datetime
from os import path
from json import (load as jsonload, dump as jsondump)

SETTINGS_FILE = path.join(path.dirname(__file__), r'settings_file.cfg')
DEFAULT_SETTINGS = {}


def iniciar():
    print('Iniciando migração do sistema contabil.')

    params_exec = {
        'id_entidade': 1,
        'i_plano_contas': 0,
        'sistema': 1,
        'nome_sistema': '',
        'nome_entidade': '',
        'token_tela': '',
        'user-access': '',
        'user_login': '',
        'user_pass': '',
        'db_name': '',
        'db_user': '',
        'db_pw': '',
        'db_host': 'localhost',
        'db_port': '9090',
    }
    """ Carregamento do token de tela """
    # token = buscar(params_exec, 'get_token')
    token = '1'
    if token is not None:
        params_exec['token_tela'] = params_exec['token_tela']
        params_exec['user-access'] = params_exec['user-access']
        """ Inicia envio Contábil """
        """ Dados cadastrais Convenios """
        # enviar(params_exec, 'concedente')
        # enviar(params_exec, 'convenente')
        # enviar(params_exec, 'modalidade')
        # enviar(params_exec, 'tipo_repasse')
        # enviar(params_exec, 'tipo_responsavel')
        # enviar(params_exec, 'responsavel')
        # enviar(params_exec, 'natureza_texto_juridico')
        # enviar(params_exec, 'tipo_ato')
        # enviar(params_exec, 'ato')
        # enviar(params_exec, 'tipo_situacao')
        # enviar(params_exec, 'tipo_aditivo')

        """ Inicia Cadastros principais """
        """ Convenio Recebido """
        # enviar(params_exec, 'convenio_recebido')
        # enviar(params_exec, 'convenio_recebido_aditivo')
        # enviar(params_exec, 'convenio_recebido_situacao')
        # enviar(params_exec, 'convenio_recebido_aditivo_situacao')
        """ Convenio Repassado """
        enviar(params_exec, 'convenio_repassado')
        # enviar(params_exec, 'convenio_repassado_aditivo')
        # enviar(params_exec, 'convenio_repassado_situacao')
        # enviar(params_exec, 'convenio_repassado_aditivo_situacao')


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do cadastro {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.rotinas'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)
    print(f'- Rotina de {tipo_registro} finalizada. '
          f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')


def buscar(params_exec, tipo_registro, *args, **kwargs):
    path_padrao = f'packages.{settings.BASE_ORIGEM}.rotinas'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['get_token_access'], 0)
    return modulo.get_token_access(params_exec)


def valida_campo(dado):
    for item in dado:
        if item is None:
            return False
    return True