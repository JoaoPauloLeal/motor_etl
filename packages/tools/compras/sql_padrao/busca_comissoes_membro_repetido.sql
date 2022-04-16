-- BUSCA TODAS AS COMISSOES ONDE UM MEMBRO POSSUA MAIS DE UM CARGO
SELECT * FROM compras.responsaveis r
WHERE nome_resp_setor in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_diretor, nome_secret)
	or nome_diretor in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_resp_setor, nome_secret)
	or nome_secret in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_resp_setor, nome_diretor);