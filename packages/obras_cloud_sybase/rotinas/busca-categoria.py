import bth.db_connector as db
import bth.cloud_connector as cloud

tipo_registro = 'categoria-obra'
url = 'https://obras.betha.cloud/obras/api/categoriasobras'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = {
        'Outros':'0',
        'Ampliação':'1',
        'Construção':'2' ,
        'Recuperação ou Manutencao':'3',
        'Reforma':'4',
        'Obra de infra-estrutura':'5',
        'Obra rodoviaria':'6',
        'Obra de arte Rodoviária':'7',
        'Pavimentação':'8',
        'Projeto Completo':'9',
        'Projeto Arquitetônico':'A',
        'Projeto estrutural':'B',
        'Projeto Elétrico Telefonia Lógico':'C',
        'Projeto Hidro-Sanitário':'D',
        'Ampliação e Recuperação':'E',
        'Ampliação e Reforma':'F'}
    try:
        for contador, item in enumerate(
            cloud.buscaApiTela(url=url, token=params_exec['token'], user=params_exec['useraccess'])):
            hash = cloud.gerar_hash_chaves(params_exec['entidade'], tipo_registro, item['id'], contador, item['descricao'])
            db.execute_sql(sql=f"""insert into bethadba.controle_migracao_registro (sistema,tipo_registro,hash_chave_dsk,
            descricao_tipo_reg,id_gerado,i_chave_dsk1,i_chave_dsk2,i_chave_dsk3) select 1,'categoria-obra',
            '{hash}','Cadastro de Categorias de Obras','{item['id']}','{params_exec['entidade']}','{dados_assunto.get(item['descricao'])}','{item['descricao']}' where not 
            EXISTS (SELECT 1 from bethadba.controle_migracao_registro where id_gerado = '{item['id']}' and i_chave_dsk1 = '{params_exec['entidade']}' and tipo_registro = '{tipo_registro}')""",
                           params_exec=params_exec)
    except:
        print('Busca não teve retorno!')
