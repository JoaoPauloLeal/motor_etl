import requests
import asyncio
import bth.db_connector as db
import bth.cloud_connector as cloud

# Script pega todas as baixas e realiza a exclusão.


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    # entidadeCloud = kwargs.get('entidadeCloud')
    url = 'https://patrimonio.betha.cloud/patrimonio-services/api/baixas'
    for x in cloud.buscaServiceLayer(url=url, token=params_exec['token']):
        url = 'https://patrimonio.betha.cloud/patrimonio-services/api/baixas'
        url += f"/{x['id']}"
        if str(cloud.ExcluirServiceLayerSemJson(url=url, token=params_exec['token'])) in ('200', '204'):
            print('Excluído com Sucesso !')