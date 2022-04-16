import bth.db_connector as db
import bth.cloud_connector as cloud

tipo_registro = 'classificacao-obra'
url = 'https://obras.betha.cloud/obras/api/classificacaoobras'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    try:
        for contador, item in enumerate(
                cloud.buscaApiTela(url=url, token=params_exec['token'], user=params_exec['useraccess'])):
            hash = cloud.gerar_hash_chaves(params_exec['entidade'], tipo_registro, item['id'])
            ssql = f"""insert into bethadba.controle_migracao_registro (sistema,tipo_registro,hash_chave_dsk,
            descricao_tipo_reg,id_gerado,i_chave_dsk1,i_chave_dsk2,i_chave_dsk3) select 1,'classificacao-obra',
            '{hash}','Cadastro de Classificação de Obras','{item['id']}','{params_exec['entidade']}','{contador}','{item['descricao']}' where not 
            EXISTS (SELECT 1 from bethadba.controle_migracao_registro where id_gerado = '{item['id']}' and i_chave_dsk1 = '{params_exec['entidade']}')"""
            # print(ssql)
            db.execute_sql(sql=ssql,
                           params_exec=params_exec)
    except:
        print('Busca não teve retorno!')