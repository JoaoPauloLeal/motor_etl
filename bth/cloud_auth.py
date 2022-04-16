# MIT License
#
# Copyright (c) 2021 JoaoPauloLeal
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import subprocess
import time
from os import path

'''
Biblioteca para consumo de phantom com back end em java + selenium
Padrão dos campos:
"path_default" é o caminho absoluto base do main que esta chamando a biblioteca
"aldo.garcia" -> usuário
"123456" -> senha
"http://contabil.test.bethacloud.com.br" -> url de login qualquer url, desde que seja o sistema que necessita oaut2
"189988" -> código do database (obrigatório apenas quando deseja obter o user-access)
"7550" -> código da entidade (obrigatório apenas quando deseja obter o user-access)
'''

class BethaOauth:
    def __init__(self, path_default, consumer_login, consumer_pass, consumer_url, consumer_data_base, consumer_entity):
        self.consumer_login = consumer_login
        self.consumer_pass = consumer_pass
        self.consumer_url = consumer_url
        self.consumer_data_base = consumer_data_base
        self.consumer_entity = consumer_entity
        self.path_default = path_default

    def get_access_token_and_user_access(self):
        if self.consumer_data_base and self.consumer_entity is not None:
            subprocess.call(
                ['java', '-jar', get_driver_phantom(), str(self.consumer_login),
                 str(self.consumer_pass),
                 str(self.consumer_url),
                 str(self.consumer_data_base), str(self.consumer_entity)])
            return read_authorization(self.path_default)
        else:
            return self.get_access_token()

    def get_access_token(self):
        subprocess.call(['java', '-jar', get_driver_phantom(), str(self.consumer_login), str(self.consumer_pass),
                         str(self.consumer_url)])
        return open_authorization(self.path_default).replace('Authorization: ', '')


def get_driver_phantom():
    return path.join(path.dirname(__file__), 'cloud_auth/connector/CloudAuth.jar')


def read_authorization(path_default):
    f = open_authorization(path_default)
    aut_parc = str(f).split("User-Access: ")
    authentication = {
        'authorization': aut_parc[0].strip().replace('Authorization: ', ''),
        'user-access': aut_parc[1].strip()
    }
    return authentication


def open_authorization(path_default):
    time.sleep(1.5)
    caminho = str(path_default).split('\\packages')[0]
    return open(f"{caminho}/Authorization.txt", "r").read()