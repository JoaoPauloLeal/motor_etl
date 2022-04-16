import bth.cloud_connector as cloud


# Exclui do ano X ao ano Y todas as solicitações de compras do token inserido no enviar.py

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    exercicioInicial = int(input('Ano inicial para excluir? '))
    exercicioFinal = int(input('Ano Final para excluir? '))
    exercicioT = exercicioFinal - exercicioInicial
    for ano in range(exercicioInicial, exercicioFinal + 1):
        urlGet = f'https://compras.betha.cloud/compras-services/api/exercicios/{ano}/solicitacoes'
        for item in (cloud.buscaServiceLayer(url=urlGet, token=params_exec['token'])):
            url = f"https://compras.betha.cloud/compras-services/api/exercicios/{ano}/solicitacoes/{item['id']}"
            cloud.ExcluirServiceLayerSemJson(token=params_exec['token'], url=url)
            print('Excluindo..')
    print('Todos Enviados para Exclusão!')
