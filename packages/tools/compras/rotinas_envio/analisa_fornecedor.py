import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'busca_fornecedor.sql')
    correcao = str(input('Realizar correção ?'))
    print(db.consulta_sql(sql, params_exec=params_exec, index_col='tipo_registro'))
    for x in db.consulta_sql(sql, params_exec=params_exec, index_col='tipo_registro'):
        mostra = False
        resultado = f"Fornecedor: {x['chave_dsk2']}"

        """Valida Nome"""
        if x['nome'] == '':
            resultado += ', Nome'
            mostra = True


        """Valida CNPJ/CPF"""
        if x['cpfCnpj'] == '':
            resultado += ', CPF/CNPJ'
            mostra = True


        """Valida Estado"""
        if x['estado'] == '':
            resultado += ', Estado'
            mostra = True


        """Valida Nome"""
        if x['unidadeFederacao'] == '':
            resultado += ', Sigla Estado'
            mostra = True


        """Valida Cidade"""
        if x['cod_cidade'] == '0' or x['cod_cidade'] == '':
            resultado += ', Cidade'
            mostra = True


        """Valida Municipio"""
        if x['municipio'] == '':
            resultado += ', Municipio'
            mostra = True

            """Correção"""
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                                          update compras.credores 
                                          set cidade = 'Migracao'
                                          where i_credores = {x['chave_dsk2']}
                                              and i_entidades = {int(params_exec['entidade'])}""")


        """Valida Bairro"""
        if x['bairro'] == '':
            resultado += ', Bairro'
            mostra = True

            """Correção"""
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                                          update compras.credores 
                                          set bairro = 'centro'
                                          where i_credores = {x['chave_dsk2']}
                                              and i_entidades = {int(params_exec['entidade'])}""")


        """Valida Logradouro"""
        if x['logradouro'] == '' or str(x['logradouro'])[0] == ',':
            resultado += ', Logradouro'
            mostra = True

            """Correção"""
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                               update compras.credores 
                               set endereco = 'Rua Geral, 00'
                               where i_credores = {x['chave_dsk2']}
                                   and i_entidades = {int(params_exec['entidade'])}""")


        if mostra:
            print(resultado)

        # arquivo = open(f"packages/tools/compras/relatorios_incosistencias/entidade{params_exec['entidade']}_fornecedores.txt", 'a')
        # arquivo.writelines(f'{resultado} \n')