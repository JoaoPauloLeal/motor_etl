from datetime import datetime
from bth.cloud_auth import BethaOauth, read_authorization
from os import path
from bth.interacao_cloud import verifica_token_tela


def iniciar():
    print('Iniciando migração do sistema obras.')
    #  db.teste_conexao()

    params_exec = {
        'entidade': 1,
        'data_inicial': '2021-01-01',
        'data_final': '2021-12-31',
        'ESTADO': 'SC',
        'usuario_cloud':'',
        'senha_cloud':'',
        'url_obras':'https://obras.betha.cloud/',
        'entidade_id':'', # Utilizar o Base64 para coletar as informações da base
        'base_id':'', # Utilizar o Base64 para coletar as informações da base
        'token':'',
        'appcontext': '',
        'useraccess': '',
        'db_name': '',  # Nome do banco para conexão sybase
        'db_user': '',  # Nome do banco para conexão sybase
        'db_pw': '',  # Nome do banco para conexão sybase
        'db_host': 'localhost',  # Nome do banco para conexão sybase
        'db_port': '9090'  # Nome do banco para conexão sybase
    }

    if verifica_token_tela(str(read_authorization(path.dirname(__file__))['authorization']).split(' ')[1]):
        params_exec['token'] = read_authorization(path.dirname(__file__))['authorization']
        params_exec['useraccess'] = read_authorization(path.dirname(__file__))['user-access']
    else:
        params_exec = capturaToken(params_exec,
                     params_exec['usuario_cloud'],
                     params_exec['senha_cloud'],
                     params_exec['url_obras'],
                     params_exec['entidade_id'],
                     params_exec['base_id'])


    # """ Inicia envio obras """
    # Dados cadastrais
    # Executar apenas os buscas depois que for migrado ao menos 1 Entidade
    # enviar(params_exec, 'busca-classificacao')
    # enviar(params_exec, 'classificacao')
    # enviar(params_exec, 'busca-categoria')
    # enviar(params_exec, 'categoria')
    # enviar(params_exec, 'busca-tipos-obras')
    # enviar(params_exec, 'tipos-obras')
    # enviar(params_exec, 'busca-tipo-responsavel')
    # enviar(params_exec, 'tipo-responsavel')
    # enviar(params_exec, 'busca-tipo-responsabilidade-tecnica')
    # enviar(params_exec, 'tipo-responsabilidade-tecnica')
    # enviar(params_exec, 'motivo-paralisacao')
    # enviar(params_exec, 'busca-tipo-medicao')
    # enviar(params_exec, 'tipo-medicao')
    # enviar(params_exec, 'responsaveis_sc')
    # enviar(params_exec, 'matriculas-cei')

    # Dados das obras
    # enviar(params_exec, 'obras')
    # enviar(params_exec, 'obra-matriculas-cei') # Não executado para SC
    # enviar(params_exec, 'contratos-obras')
    # enviar(params_exec, 'obra-art')              # Adiciona o 1° responsavel cadastrado no sapo (orçamentario) p/ SC, para PR caso a obra não tenha responsavel ele cadastra o primeiro responsavel migrado para as obras
    # enviar(params_exec, 'iniciar-obras')
    # enviar(params_exec, 'medicoes')
    # enviar(params_exec, 'orcamentos')
    # enviar(params_exec, 'paralisacoes')
    enviar(params_exec, 'conclusoes')


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do cadastro {tipo_registro}')
    tempo_inicio = datetime.now()
    modulo = __import__(f'packages.obras_cloud_sybase.rotinas.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)
    print(f'- Rotina de {tipo_registro} finalizada. '
          f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')

def capturaToken(params_exec, user, password, url, entidade, base):
    client_oaut = BethaOauth(path.dirname(__file__),
                             user, password,
                             url,
                             base, entidade)

    # execute function "two functions, but with treatment"
    aut_user = client_oaut.get_access_token_and_user_access()

    params_exec['token'] = aut_user['authorization']
    params_exec['useraccess'] = aut_user['user-access']
    return params_exec