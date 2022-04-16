import requests

import bth.db_connector as db
import bth.cloud_connector as cloud

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):

    texto = ''
    for x in db.consulta_sql(sql="""select distinct i_contratos_sup_compras from compras.contratos cc
JOIN bethadba.controle_migracao_registro r on (tipo_registro = 'contratacoes' and cc.i_contratos = i_chave_dsk2)
where natureza <> 1 --and r.id_gerado = '1509252'""", params_exec=params_exec):

        texto = 'Contrato Principal: '+str(x['i_contratos_sup_compras'])
        print(texto)
