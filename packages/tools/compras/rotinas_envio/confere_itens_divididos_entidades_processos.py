import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    url = 'https://compras.betha.cloud/compras/dados/api/processoadministrativoentidadeparticipanteitem'
    campos = 'entidadeParticipante.processoAdministrativo.numeroProcesso, entidadeParticipante.processoAdministrativo.parametroExercicio.exercicio, item.numero, item.quantidade,quantidadeDistribuida'
    criterio = 'entidade.id = 2924'

    for x in cloud.buscaFonte(url=url, criterio=criterio, fields=campos, token='1f5d6d28-6087-4a97-9142-756bb7618e0d')['content']:
        print(x['item']['numero'])