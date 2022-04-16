import bth.db_connector as db
import bth.cloud_connector as cloud

# Busca os ids das divições já realizadas para o cloud
def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'busca_comissao_licitacao.sql')
    correcao = str(input('Realizar correção ?'))
    grava = str(input('Gravar resultado ?'))
    for x in db.consulta_sql(sql, params_exec, index_col='tipo_registro'):
        resultado = f"Comissão {x['chave_dsk2']} da entidade {x['chave_dsk1']} : "

    # exercicio = ''
    # id_proc_adm = ''
    # id_item = ''
    # id_divisao = ''
    # url = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{id_proc_adm}/itens/{id_item}/entidades/{id_divisao}'
