import bth.cloud_connector as cloud

# Exclui a divisão dos itens por entidade de acordo com o id do processo e o exercicio colocados na hora da execução.

processoid = str(input('Id do processo : '))
exercicio = str(input('Exercicio do processo: '))

url = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoid}/itens'
token = 'a1c9349a-c29c-4626-9e21-1f0a8922c180'
for x in cloud.buscaServiceLayer(token=token, url=url):
    print(x['id'])

    url = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoid}/itens/{x["id"]}/entidades'

    for z in cloud.buscaServiceLayer(token=token, url=url):
        print(z)
        url = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoid}/itens/{x["id"]}/entidades/{z["id"]}'
        cloud.ExcluirServiceLayerSemJson(token=token, url=url)