import requests

from bth.db_connector import consulta_sql

url = 'https://obras.betha.cloud/obras/api/obras/9022/anotacoesobras/11618'

params_exec = {
    'db_name': 'Teste',  # Nome do banco para conexão sybase
    'db_user': 'desbth',  # Nome do banco para conexão sybase
    'db_pw': 'HsV0zk3yXYACypmaRsZ1m5POCC4v2y',  # Nome do banco para conexão sybase
    'db_host': 'localhost',  # Nome do banco para conexão sybase
    'db_port': '9090'  # Nome do banco para conexão sybase
}

for x in consulta_sql("""
select distinct id = 1,
	i_entidades,
	i_obras,
	nroResponsabilidadeTecnica = '2',
	cast(data_cadastramento as varchar)  as data_inclusao,
	cmr.id_gerado as responsavel,
	cmrr.id_gerado as tipoResponsabilidade,
	cmrrr.id_gerado as id_obra
FROM sapo.obras
INNER JOIN bethadba.controle_migracao_registro cmr ON (cmr.tipo_registro = 'responsaveis' and cmr.i_chave_dsk1 in (1,2,3,4,5,6,7,8,9,10))
INNER JOIN bethadba.controle_migracao_registro cmrr ON (cmrr.i_chave_dsk3 = 'Outra' AND cmrr.tipo_registro = 'tipo-responsabilidade-tecnica' and cmrr.i_chave_dsk1 = 2)
INNER JOIN bethadba.controle_migracao_registro cmrrr ON (cmrrr.tipo_registro = 'obras' and cmrrr.i_chave_dsk1 = 2 and cmrrr.i_chave_dsk2 = i_obras)
WHERE i_entidades = 2 and id_obra = 9022 order by i_obras""", params_exec):
    print(x)
    url = f"https://obras.betha.cloud/obras/api/obras/{x['id_obra']}/anotacoesobras/{x['responsavel']}"
    print(url)
    r = requests.delete(url=url, headers={'authorization':'Bearer 1f056ec1-1d6c-46af-af6e-536df4defc95', 'user-access': '2PWP9rSm9q5tkY5M2Yqqrg==', 'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMjIsImluc3VsYXRpb24iOmZhbHNlfX0='})
    print(str(r.status_code)+ str(r.json()))