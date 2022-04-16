from bth.cloud_auth import BethaOauth
from os import path

# create object
client_oaut = BethaOauth(path.dirname(__file__), 'michel.graciano', '1245', 'https://contabil.cloud.betha.com.br/', 'data_base', 'entity')
# execute function "two functions, but with treatment"
aut_user = client_oaut.get_access_token_and_user_access()

print(aut_user['authorization'])
print(aut_user['user-access'])
