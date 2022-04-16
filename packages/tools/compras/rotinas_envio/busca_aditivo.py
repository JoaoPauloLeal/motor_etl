import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'busca_aditivo_data_anterior.sql')
    # correcao = str(input('Realizar correção ?'))
    print(':::: Iniciando busca de aditivos com data anterior ao contrato ::::')
    print(db.execute_sql(sql, params_exec))
    # print(type(retorno))
    # if retorno:
    #     print('ddd')
    # else:
    #     print('!!! Sem dados para correção !!!')
        # for x in db.consulta_sql(sql, index_col='col').to_dict('records'):
        #     print(x)

    # sql = db.get_consulta(params_exec, f'busca_aditivo_mesma_data.sql')
    # print(':::: Iniciando busca de aditivos com data igual ::::')
    # for x in db.consulta_sql(sql, index_col='col').to_dict('records'):
    #     print(x)